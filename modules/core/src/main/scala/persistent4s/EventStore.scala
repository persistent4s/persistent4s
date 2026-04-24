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
  *   the effect type, such as `cats.effect.IO`
  * @tparam A
  *   the event type, which must extend the Event trait
  */
trait EventStore[F[_], A <: Event]:

  /** Append events to the event store using optimistic concurrency control.
    *
    * The `expectedIndex` must equal the global position of the most recent event that matches `eventFilter`. If another
    * event matching the filter has been appended concurrently, the actual index will be higher and an
    * [[IndexConflictException]] is raised with no events written. Pass `expectedIndex = 0` when no prior matching
    * events are expected (i.e. this is the first append for that filter scope).
    *
    * The `events` parameter is variadic so that callers who build events in separate groups can pass multiple lists
    * without flattening them first. All lists are treated as a single ordered sequence — there is no semantic
    * difference between one list of N events and N lists of one event each.
    *
    * If all lists are empty the call is a no-op: no events are written and no notification is emitted.
    *
    * @param eventFilter
    *   the filter used to determine the concurrency scope (which prior events are checked for conflicts)
    * @param expectedIndex
    *   the global position of the last known matching event, or 0 if none are expected
    * @param events
    *   one or more lists of events to append, each event paired with its tags and type name
    * @return
    *   a F[Unit] that completes when the events have been written, or fails with [[IndexConflictException]] on conflict
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
    * @param maxEvents
    *   the maximum number of events to read, or None to read all available events
    * @return
    *   a Stream of EventEnvelope[A] containing the matching events
    */
  def readFrom(fromPosition: Long, eventFilter: EventFilter, maxEvents: Option[Int] = None): Stream[F, EventEnvelope[A]]
