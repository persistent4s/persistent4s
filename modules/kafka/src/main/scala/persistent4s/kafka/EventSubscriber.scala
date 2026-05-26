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

import fs2.Stream
import fs2.kafka.CommittableOffset
import persistent4s.{Event, EventEnvelope}

/** Subscribes to a Kafka topic and decodes records back into [[EventEnvelope]]s.
  *
  * Each emitted element is paired with its [[fs2.kafka.CommittableOffset]] so the consumer decides when offsets are
  * committed. This keeps end-to-end semantics (at-least-once vs. effectively-once via idempotent downstream writes) in
  * the caller's hands.
  *
  * ==Ordering guarantee==
  *
  * Within any single tag scope, events are delivered in the order they were appended (i.e. in ascending
  * `globalPosition`). This holds because the event store serializes appends that share at least one tag.
  *
  * Across disjoint tag scopes, ordering matches the relay's publication order, which is the commit order of the
  * appending transactions — **not** necessarily ascending `globalPosition`. Two independently-tagged events with
  * `globalPosition` 5 and 6 may be delivered as `(6, 5)` if the writer of 6 committed first. This is acceptable under
  * DCB: causally independent events have no defined relative order.
  */
trait EventSubscriber[F[_], A <: Event]:

  /** Subscribe to `topic` and stream decoded envelopes paired with their committable offset.
    *
    * Each invocation creates an independent consumer using the [[KafkaConsumerConfig]] supplied at construction time
    * (notably the consumer `groupId`). Calling `subscribe` with different topics from the same `EventSubscriber`
    * results in independent consumers, each tracking its own offsets per topic.
    *
    * @param topic
    *   the Kafka topic to subscribe to
    * @param fromBeginning
    *   if true and no committed offset exists for the consumer group, seek to the earliest offset; otherwise rely on
    *   the broker's `auto.offset.reset` policy.
    */
  def subscribe(topic: String, fromBeginning: Boolean): Stream[F, (EventEnvelope[A], CommittableOffset[F])]
