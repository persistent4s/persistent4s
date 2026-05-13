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

import cats.effect.{Async, Resource}
import persistent4s.{Event, EventCodec, Outbox}
import fs2.kafka.*
import persistent4s.EventEnvelope
import cats.Parallel
import cats.syntax.all.*
import fs2.Chunk
import io.circe.Json

/** Entry point for building the Kafka-side components of the library.
  *
  * Each helper is independent: an application can build only a publisher, only a subscriber, or wire up a full relay.
  * The module never imports the postgres module — the relay consumes any [[Outbox]] implementation.
  */
object KafkaModule:

  /** Build a Kafka [[EventPublisher]] backed by an fs2-kafka producer. The underlying producer is released when the
    * resource is finalized.
    */
  def publisher[F[_]: Async: Parallel, A <: Event](
    config: KafkaProducerConfig[A],
    codec: EventCodec[A],
  ): Resource[F, EventPublisher[F, A]] = {
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
        .append(Header("persistent4s.eventId", envelope.metadata.id.toString))
        .append(Header("persistent4s.globalPosition", envelope.metadata.globalPosition.toString))
        .append(Header("persistent4s.eventType", envelope.metadata.eventType.value))
        .append(Header("persistent4s.tags", tagsJson))
        .append(Header("persistent4s.timestamp", envelope.metadata.timestamp.toString))
        .append(Header("persistent4s.metaVersion", "1"))

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

  }

  /** Build a Kafka [[EventSubscriber]] backed by an fs2-kafka consumer. */
  def subscriber[F[_]: Async: Parallel, A <: Event](
    config: KafkaConsumerConfig,
    codec: EventCodec[A],
  ): Resource[F, EventSubscriber[F, A]] = ???

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
  ): Resource[F, KafkaRelay[F, A]] = ???
