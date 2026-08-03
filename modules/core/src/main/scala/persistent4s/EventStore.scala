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

  /** Describe the schema this store will persist for `event`, when the backend owns a schema-aware codec. High-level
    * handlers use this to fail fast if a shadowed/local EventSchema disagrees with the storage registry.
    */
  def storageSchema(event: A): Option[EventStorageSchema] = None

  /** Return the global position of the latest event matching `eventFilter`, or `0` when none exists. This is the
    * authoritative revision used by optimistic concurrency and to validate disposable command snapshots.
    */
  def currentRevision(eventFilter: EventFilter): F[Long]

  /** Append events to the event store using optimistic concurrency control.
    *
    * The `expectedIndex` must equal the global position of the most recent event that matches `eventFilter`. Any
    * mismatch (behind or ahead of the authoritative revision) raises [[IndexConflictException]] with no events written.
    * Pass `expectedIndex = 0` when no prior matching events are expected (i.e. this is the first append for that filter
    * scope).
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
    *   each events represented as a [[PendingEvent]] carrying its payload, tags, type name, external flag, optional id,
    *   and headers
    * @return
    *   a F[List[EventEnvelope[A]]] that completes when the events have been written, or fails with
    *   [[IndexConflictException]] on conflict
    */
  def append(
    eventFilter: EventFilter,
    expectedIndex: Long,
    events: List[PendingEvent[A]]*,
  ): F[List[EventEnvelope[A]]]

  /** Append events to the event store WITHOUT optimistic concurrency control.
    *
    * Unlike [[append]], this method performs no filter-scoped conflict check and acquires no advisory locks. Use it
    * when the caller does not need to protect a local invariant — typically when re-ingesting events that were already
    * ordered by some external authority. The common case is a subscriber importing events from another service's Kafka
    * topic into the local store: the source service has already serialized those events under its own concurrency
    * model, so re-checking on the receiving side is meaningless.
    *
    * The events are still assigned fresh `globalPosition` values in commit order and emit the same notification on
    * commit, so projections wake up normally.
    *
    * The `events` parameter is variadic with the same semantics as [[append]]: multiple lists are flattened into a
    * single ordered sequence, and an empty call is a no-op (no writes, no notification).
    *
    * ==Idempotency==
    *
    * At-least-once delivery from a broker means the same event may arrive twice. To make this method idempotent in the
    * face of redelivery, pass the source event's UUID via [[PendingEvent.id]]. The unique constraint on `event_id` then
    * makes a duplicate append a silent no-op: the original row is left untouched and its existing metadata (position,
    * timestamp) is returned rather than a new one being written. Passing `None` skips this safeguard and is appropriate
    * only if duplicates are impossible by construction.
    *
    * @param events
    *   each events represented as a [[PendingEvent]] carrying its payload, tags, type name, external flag, optional id,
    *   and headers
    * @return
    *   a F[List[EventEnvelope[A]]] that completes when the events have been written.
    */
  def appendUnchecked(events: List[PendingEvent[A]]*): F[List[EventEnvelope[A]]]

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
