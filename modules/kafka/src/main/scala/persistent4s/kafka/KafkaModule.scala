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

/** Entry point for building the Kafka-side components of the library.
  *
  * Each helper is independent: an application can build only a publisher, only a subscriber, or wire up a full relay.
  * The module never imports the postgres module — the relay consumes any [[Outbox]] implementation.
  */
object KafkaModule:

  /** Build a Kafka [[EventPublisher]] backed by an fs2-kafka producer. The underlying producer is released when the
    * resource is finalized.
    */
  def publisher[F[_]: Async, A <: Event](
    config: KafkaProducerConfig[A],
    codec: EventCodec[A],
  ): Resource[F, EventPublisher[F, A]] = ???

  /** Build a Kafka [[EventSubscriber]] backed by an fs2-kafka consumer. */
  def subscriber[F[_]: Async, A <: Event](
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
  def relay[F[_]: Async, A <: Event](
    outbox: Outbox[F, A],
    config: KafkaProducerConfig[A],
    codec: EventCodec[A],
    topic: String,
    batchSize: Int = 128,
  ): Resource[F, KafkaRelay[F, A]] = ???
