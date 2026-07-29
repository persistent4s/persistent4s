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

/** Configuration for the Kafka publisher.
  *
  * The topic is passed per call to [[EventPublisher.publish]], so one publisher can fan out to several topics.
  * [[recordKey]] chooses the partition and hence the ordering scope: events sharing a key are ordered; return a
  * constant key for total per-service ordering. The publisher forces `enable.idempotence=true` (required for the
  * ordering guarantee), overriding [[producerProperties]].
  *
  * @param bootstrapServers
  *   comma-separated `host:port` list
  * @param recordKey
  *   derives the Kafka record key from an envelope
  * @param producerProperties
  *   extra producer properties merged on top of fs2-kafka defaults (e.g. `acks`, `linger.ms`)
  */
final case class KafkaProducerConfig[A <: Event](
  bootstrapServers: String,
  recordKey: EventEnvelope[A] => String,
  producerProperties: Map[String, String] = Map.empty,
)

/** Configuration for the Kafka subscriber.
  *
  * The topic is passed per call to [[EventSubscriber.subscribe]]. All subscriptions from one subscriber share
  * [[groupId]] (offsets are committed per group and topic-partition); build separate subscribers for independent
  * consumer-group identities.
  *
  * @param bootstrapServers
  *   comma-separated `host:port` list
  * @param groupId
  *   consumer group id
  * @param consumerProperties
  *   extra consumer properties merged on top of fs2-kafka defaults. Two are not yours to set: `enable.auto.commit` is
  *   forced to `false` (the subscriber hands you an explicit acknowledge action instead) and `auto.offset.reset` is
  *   determined by the `fromBeginning` argument of each `subscribe` call. Both are applied after this map, so values
  *   given here for them are silently ignored.
  */
final case class KafkaConsumerConfig(
  bootstrapServers: String,
  groupId: String,
  consumerProperties: Map[String, String] = Map.empty,
)

/** Configuration for the Kafka message publisher. Topic and key come from each [[OutgoingMessage]]; the publisher
  * forces `enable.idempotence=true`, overriding [[producerProperties]].
  */
final case class KafkaMessageProducerConfig(
  bootstrapServers: String,
  producerProperties: Map[String, String] = Map.empty,
)
