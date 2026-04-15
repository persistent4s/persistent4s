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
  *
  * @tparam F
  *   the effect type, such as IO
  * @tparam A
  *   the event type, which must extend the Event trait
  */
trait EventStore[F[_], A <: Event]:

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
  def append(eventFilter: EventFilter, expectedIndex: Long, events: List[(Set[Tag], EventTypeName, A)]*): F[Unit]

  /** Read events from the event store starting from a specific position, filtering by event types and tags. The
    * returned Stream will emit EventEnvelope[A] instances that match the specified event types and tags. The Stream
    * will complete when there are no more matching events to read.
    *
    * @param fromPosition
    *   the position in the event store to start reading from (exclusive)
    * @param eventFilter
    *   the filter to apply to the events
    * @return
    *   a Stream of EventEnvelope[A] containing the matching events
    */
  def readFrom(fromPosition: Long, eventFilter: EventFilter): Stream[F, EventEnvelope[A]]
