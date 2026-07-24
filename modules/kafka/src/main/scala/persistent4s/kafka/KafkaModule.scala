/*
 * Copyright 2026 Antonio Jimenez and Bastien Jolidon
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package persistent4s.kafka

import java.time.Instant
import java.util.UUID

import scala.util.Try

import cats.Parallel
import cats.effect.{Async, Resource}
import cats.syntax.all.*
import fs2.{Chunk, Stream}
import fs2.kafka.*
import io.circe.{Json, parser}

import persistent4s.{Event, EventCodec, EventEnvelope, EventMetadata, EventTypeName, Outbox, Tag}
import persistent4s.EventSubscriber
import persistent4s.EventPublisher

/** Entry point for the Kafka components: [[publisher]], [[subscriber]], and [[relay]]. Each is independent — build only
  * what you need. The relay drains any [[Outbox]] implementation, so this module never depends on a specific event
  * store.
  */
object KafkaModule:

  val EventIdHeaderName = "persistent4s.eventId"

  val GlobalPositionHeaderName = "persistent4s.globalPosition"

  val EventTypeHeaderName = "persistent4s.eventType"

  val TagsHeaderName = "persistent4s.tags"

  val TimestampHeaderName = "persistent4s.timestamp"

  val MetaVersionHeaderName = "persistent4s.metaVersion"

  /** Build a Kafka [[EventPublisher]] backed by an fs2-kafka producer (resource-managed).
    *
    * Event metadata is written as record headers and the payload is encoded with `codec`; the record key comes from
    * `config.recordKey`. The producer runs with `enable.idempotence=true`, so retries never duplicate or reorder
    * records. Batch publishes are pipelined but keep input order.
    */
  def publisher[F[_]: Async: Parallel, A <: Event](
    config: KafkaProducerConfig[A],
    codec: EventCodec[A],
  ): Resource[F, EventPublisher[F, A]] =
    val settings: ProducerSettings[F, String, String] =
      ProducerSettings[F, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withProperties(config.producerProperties)
        .withEnableIdempotence(true)

    def buildHeaders(envelope: EventEnvelope[A]): Headers =
      val tagsJson = Json
        .arr(envelope.metadata.tags.toSeq.sortBy(_.value).map(t => Json.fromString(t.value))*)
        .noSpaces
      Headers.empty
        .append(Header(EventIdHeaderName, envelope.metadata.id.toString))
        .append(Header(GlobalPositionHeaderName, envelope.metadata.globalPosition.toString))
        .append(Header(EventTypeHeaderName, envelope.metadata.eventType.value))
        .append(Header(TagsHeaderName, tagsJson))
        .append(Header(TimestampHeaderName, envelope.metadata.timestamp.toString))
        .append(Header(MetaVersionHeaderName, "1"))

    def toRecord(topic: String, envelope: EventEnvelope[A]): F[ProducerRecord[String, String]] =
      codec.encode(envelope.payload) match
        case Right(value) =>
          val key = config.recordKey(envelope)
          val headers = buildHeaders(envelope)
          Async[F].pure(ProducerRecord(topic, key, value).withHeaders(headers))
        case Left(error) =>
          Async[F].raiseError(new RuntimeException(s"EventCodec failed to encode event: ${error.getMessage}", error))

    KafkaProducer.resource(settings).map { producer =>
      new EventPublisher[F, A] {

        override def publish(topic: String, envelope: EventEnvelope[A]): F[Unit] =
          toRecord(topic, envelope).flatMap(record => producer.produceOne_(record).flatten.void)

        override def publish(topic: String, envelopes: List[EventEnvelope[A]]): F[Unit] =
          if envelopes.isEmpty then Async[F].unit
          else
            envelopes
              .traverse(toRecord(topic, _))
              .flatMap(records => producer.produce(Chunk.from(records)).flatten.void)

      }
    }

  /** Build a Kafka [[EventSubscriber]] backed by an fs2-kafka consumer (auto-commit disabled).
    *
    * Each envelope is paired with its offset's `commit` action; commit only after processing for at-least-once
    * delivery, which becomes effectively-once when paired with an idempotent downstream write.
    *
    * '''Ordering:''' Kafka orders records only within a partition, and the partition is chosen by the record key
    * ([[KafkaProducerConfig.recordKey]]). Events sharing a key arrive in ascending `globalPosition`; events on
    * different partitions have no relative order. To order a whole tag scope, key it onto one partition (or use a
    * constant key for total ordering). Under DCB this suffices: causally independent events need no order.
    *
    * '''Throughput:''' commits happen per record and all partitions are consumed on one stream, so a single subscriber
    * does not parallelize. To scale, add partitions and run more consumer instances in the same `groupId` (Kafka
    * rebalances partitions across them) rather than expecting concurrency from one subscriber.
    *
    * @param fromBeginning
    *   if true and no offset is committed for the group, start at the earliest record; otherwise follow the broker's
    *   `auto.offset.reset`.
    */
  def subscriber[F[_]: Async, A <: Event](
    config: KafkaConsumerConfig,
    codec: EventCodec[A],
  ): Resource[F, EventSubscriber[F, A]] =
    val baseSettings: ConsumerSettings[F, String, String] =
      ConsumerSettings[F, String, String]
        .withBootstrapServers(config.bootstrapServers)
        .withGroupId(config.groupId)
        .withProperties(config.consumerProperties)
        .withEnableAutoCommit(false)

    def header(name: String, headers: Headers): Either[Throwable, String] = headers(name).map(_.as[String]) match
      case Some(s) => Right(s)
      case None    => Left(new RuntimeException(s"missing required header: $name"))

    def envelopeFromRecord(record: ConsumerRecord[String, String]): F[EventEnvelope[A]] =
      val headers = record.headers
      val decodedMessage = for
        metaVersion <- header(MetaVersionHeaderName, headers)
        _           <-
          if metaVersion == "1" then Right(()) else Left(new RuntimeException(s"unsupported metaVersion: $metaVersion"))
        eventIdStr       <- header(EventIdHeaderName, headers)
        eventId          <- Try(UUID.fromString(eventIdStr)).toEither
        positionStr      <- header(GlobalPositionHeaderName, headers)
        position         <- Try(positionStr.toLong).toEither
        eventTypeNameStr <- header(EventTypeHeaderName, headers)
        eventTypeName     = EventTypeName.fromString(eventTypeNameStr)
        tagsStr          <- header(TagsHeaderName, headers)
        tags             <- parser.parse(tagsStr).left.map(e => e: Throwable).flatMap { json =>
                  json.asArray.toRight(new RuntimeException(s"tags header is not a JSON array: $tagsStr")).flatMap {
                    arr =>
                      arr.toList.traverse { v =>
                        v.asString
                          .toRight(new RuntimeException(s"tag is not a string: $v"))
                          .flatMap(s => Tag.fromString(s).toRight(new RuntimeException(s"invalid tag: $s")))
                      }.map(_.toSet)
                  }
                }
        timestampStr <- header(TimestampHeaderName, headers)
        timestamp    <- Try(Instant.parse(timestampStr)).toEither
        payload      <- codec.decode(eventTypeName, record.value)
      yield EventEnvelope(EventMetadata(position, eventId, tags, eventTypeName, true, timestamp), payload)
      decodedMessage match
        case Right(envelope) => Async[F].pure(envelope)
        case Left(error)     =>
          Async[F].raiseError(
            new RuntimeException(
              s"Failed to decode record with key ${Option(record.key).getOrElse("<none>")}: ${error.getMessage}",
              error,
            ),
          )

    Resource.pure(new EventSubscriber[F, A]:

      override def subscribe(
        topic: String,
        fromBeginning: Boolean,
      ): Stream[F, (EventEnvelope[A], F[Unit])] =
        val settings =
          if fromBeginning then baseSettings.withAutoOffsetReset(AutoOffsetReset.Earliest)
          else baseSettings.withAutoOffsetReset(AutoOffsetReset.Latest)

        KafkaConsumer
          .stream(settings)
          .subscribeTo(topic)
          .records
          .evalMap { committable =>
            envelopeFromRecord(committable.record).map(envelope => (envelope, committable.offset.commit))
          })

  /** Build a [[KafkaRelay]] draining `outbox` into `topic`. Does not start it — call `.run` (typically in a background
    * fiber). Run at most one relay per outbox/topic; see [[KafkaRelay]] for the rationale.
    */
  def relay[F[_]: Async: Parallel, A <: Event](
    outbox: Outbox[F, A],
    config: KafkaProducerConfig[A],
    codec: EventCodec[A],
    topic: String,
    batchSize: Int = 128,
  ): Resource[F, KafkaRelay[F, A]] =
    KafkaModule.publisher[F, A](config, codec).map { publisher =>
      KafkaRelay(outbox, publisher, topic, batchSize)
    }
