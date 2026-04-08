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

package persistent4s

import fs2.Stream

/** An EventStore is a component that allows you to append and read events in an event-sourced system. Appending events
  * to the store is done with optimistic concurrency control.
  */
trait EventStore[F[_], A]:

  /** Append events to the event store. The expected index is used for optimistic concurrency control. If the actual
    * index in the event store does not match the expected index, an IndexConflictException is thrown and none of the
    * events are appended.
    *
    * @param expectedIndex
    *   the expected index of the event store before appending the events
    * @param events
    *   the events to append, each with a set of tags and an event type
    * @return
    *   a F[Unit] that completes when the events have been appended, or fails with an IndexConflictException if the
    *   expected index does not match the actual index
    */
  def append(expectedIndex: Long, events: List[(Set[Tag], String, A)]*): F[Unit]

  /** Read events from the event store, filtering by event types and tags. The returned Stream will emit
    * EventEnvelope[A] instances that match the specified event types and tags. The Stream will complete when there are
    * no more matching events to read.
    *
    * @param eventTypes
    *   the types of events to read
    * @param tags
    *   the tags to filter events by
    * @return
    *   a Stream of EventEnvelope[A] containing the matching events
    */
  def read(eventTypes: List[String], tags: Set[Tag]*): Stream[F, EventEnvelope[A]]
