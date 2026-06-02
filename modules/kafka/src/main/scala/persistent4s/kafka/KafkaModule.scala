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

/** Entry point for building the Kafka-side components of the library.
  *
  * Each helper is independent: an application can build only a publisher, only a subscriber, or wire up a full relay.
  * The module never imports the postgres module — the relay consumes any [[Outbox]] implementation.
  */
object KafkaModule:

  val EventIdHeaderName = "persistent4s.eventId"

  val GlobalPositionHeaderName = "persistent4s.globalPosition"

  val EventTypeHeaderName = "persistent4s.eventType"

  val TagsHeaderName = "persistent4s.tags"

  val TimestampHeaderName = "persistent4s.timestamp"

  val MetaVersionHeaderName = "persistent4s.metaVersion"

  /** Build a Kafka [[EventPublisher]] backed by an fs2-kafka producer.
    *
    * Event metadata (id, globalPosition, eventType, tags, timestamp) is serialised as record headers; the payload is
    * encoded via the supplied [[EventCodec]]. The record key is determined by `config.recordKey`, which controls
    * partition assignment and therefore per-key ordering.
    *
    * The underlying producer is configured with `enable.idempotence=true`, so retries on transient broker errors do
    * not produce duplicate or reordered records. The producer is released when the resource is finalized.
    *
    * Batch publishes pipeline all records in a single request but preserve the order they appear in the input list.
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

  /** Build a Kafka [[EventSubscriber]] backed by an fs2-kafka consumer.
    *
    * Auto-commit is disabled; each emitted envelope is paired with its offset's `commit` action so the caller decides
    * when to acknowledge. Invoking the action only after successful processing gives at-least-once delivery; combining
    * it with an idempotent downstream write yields effectively-once semantics.
    *
    * ==Ordering guarantee==
    *
    * Within any single tag scope, events are delivered in the order they were appended (ascending `globalPosition`).
    * This holds because the event store serializes appends that share at least one tag.
    *
    * Across disjoint tag scopes, ordering matches the relay's publication order (commit order of the appending
    * transactions), which is **not** necessarily ascending `globalPosition`. Two independently-tagged events with
    * positions 5 and 6 may arrive as `(6, 5)` if the writer of 6 committed first. Under DCB, causally independent
    * events have no defined relative order, so this is acceptable.
    *
    * Each call to `subscribe` creates an independent consumer using the `groupId` from [[KafkaConsumerConfig]],
    * tracking its own offsets per topic.
    *
    * @param fromBeginning
    *   if true and no committed offset exists for the consumer group, seek to the earliest offset; otherwise rely on
    *   the broker's `auto.offset.reset` policy.
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
            new RuntimeException(s"Failed to decode record with key ${record.key}: ${error.getMessage}", error),
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

  /** Build a [[KafkaRelay]] that drains the given outbox into the supplied Kafka `topic`.
    *
    * Note: this does NOT start the relay; call `.run` on the returned value (typically forked into a background fiber).
    *
    * '''Deployment constraint:''' run at most one relay per service against the same outbox/topic. See [[KafkaRelay]]
    * for the rationale and the producer-idempotence requirement that backs the ordering guarantee.
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
