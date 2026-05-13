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

import persistent4s.{Event, EventEnvelope}

/** Publishes [[EventEnvelope]]s to Kafka. The destination topic is supplied per call so a single publisher can fan out
  * to multiple topics; the producer configuration (bootstrap servers, acks, partitioner, etc.) is supplied by the
  * caller via [[KafkaModule]].
  *
  * Event metadata is carried as Kafka record headers; the payload is serialized through the supplied `EventCodec[A]`.
  */
trait EventPublisher[F[_], A <: Event]:

  /** Publish a single envelope to `topic`. Semantically completes only once the broker has acknowledged the record
    * according to the configured `acks` policy.
    */
  def publish(topic: String, envelope: EventEnvelope[A]): F[Unit]

  /** Publish a batch of envelopes to `topic` in order. Implementations may pipeline records but must preserve per-key
    * ordering.
    */
  def publish(topic: String, envelopes: List[EventEnvelope[A]]): F[Unit]
