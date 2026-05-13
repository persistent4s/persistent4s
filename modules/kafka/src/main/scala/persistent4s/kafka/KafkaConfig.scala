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

/** Caller-supplied configuration for the Kafka publisher.
  *
  * The destination topic is not part of this config — it is supplied per call via [[EventPublisher.publish]] so a
  * single publisher can fan out to multiple topics. Partitioning is deliberately left to the caller via [[recordKey]].
  * For the DCB global-ordering / one-partition case, return a constant key (e.g. the microservice id) so every record
  * from this service hashes to the same partition.
  *
  * @param bootstrapServers
  *   comma-separated `host:port` list
  * @param recordKey
  *   function deriving the Kafka record key from an envelope, applied uniformly across all topics this publisher
  *   writes to
  * @param producerProperties
  *   raw Kafka producer properties merged on top of fs2-kafka defaults (e.g. `acks`, `linger.ms`).
  *
  * '''Required setting:''' `enable.idempotence=true` must be set (it is the Kafka client default since 3.0). The
  * [[KafkaRelay]] ordering guarantee assumes the producer will not reorder records within a partition on retries,
  * which is only true with idempotence enabled.
  */
final case class KafkaProducerConfig[A <: Event](
  bootstrapServers: String,
  recordKey: EventEnvelope[A] => String,
  producerProperties: Map[String, String] = Map.empty,
)

/** Caller-supplied configuration for the Kafka subscriber.
  *
  * The source topic is not part of this config — it is supplied per call via [[EventSubscriber.subscribe]]. The same
  * `groupId` is used for every subscription created by a single [[EventSubscriber]]; if you need independent
  * consumer-group identities for different topics, build separate subscribers.
  *
  * @param bootstrapServers
  *   comma-separated `host:port` list
  * @param groupId
  *   consumer group id; durable offset commits are keyed on this (per topic+partition within the group)
  * @param consumerProperties
  *   raw Kafka consumer properties merged on top of fs2-kafka defaults
  */
final case class KafkaConsumerConfig(
  bootstrapServers: String,
  groupId: String,
  consumerProperties: Map[String, String] = Map.empty,
)
