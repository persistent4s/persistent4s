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

/** Appends and reads events in an event-sourced system. Appending is done with optimistic concurrency control. */
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
    * Unlike [[append]], this method performs no filter-scoped conflict check. Use it when the caller does not need to
    * protect a local invariant — typically when re-ingesting events that were already ordered by an external authority.
    * The source has already serialized those events under its own concurrency model, so re-checking on the receiving
    * side is meaningless.
    *
    * The events are still assigned fresh `globalPosition` values and emit the same notification on commit, so
    * projections wake up normally. Positions are allocated when written, not when committed, so concurrent writes may
    * become visible out of position order; projections are protected from this by [[readForProjection]].
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
    *   a F[List[EventEnvelope[A]]] that completes when the events have been written. On a duplicate `event_id` the
    *   existing event's envelope is returned rather than a new row being written — see the idempotency note above.
    */
  def appendUnchecked(events: List[PendingEvent[A]]*): F[List[EventEnvelope[A]]]

  /** Read a snapshot of events from the store starting at `fromPosition` (exclusive). The stream completes once all
    * currently matching events have been emitted — it is a one-shot read, not a live subscription.
    *
    * @param fromPosition
    *   position to start from (exclusive); pass 0 to read from the beginning
    * @param eventFilter
    *   filter to apply; see [[EventFilter]] for matching semantics
    * @param maxEvents
    *   cap on the number of events returned; `None` means no cap
    */
  def readFrom(fromPosition: Long, eventFilter: EventFilter, maxEvents: Option[Int] = None): Stream[F, EventEnvelope[A]]

  /** Read the next events a projection should handle: those strictly after `after` in delivery order (see
    * [[ReadCursor]]), at most `maxEvents` of them.
    *
    * Unlike [[readFrom]], every event returned is final in delivery order: no event that is not visible yet can later
    * turn up before the last one returned. A projection can therefore checkpoint any returned event's
    * [[EventMetadata.cursor]] without risking to skip an event that commits late. Events the store has to withhold for
    * that reason are reported through [[ProjectionRead.heldBack]].
    *
    * There is deliberately no default implementation: a decorator that forgot to forward this method would silently
    * lose the guarantee. A store whose positions only become visible in order can use [[ProjectionRead.byPosition]].
    *
    * @param after
    *   the cursor of the last event already handled; [[ReadCursor.Start]] to read from the beginning
    * @param eventFilter
    *   filter to apply; see [[EventFilter]] for matching semantics
    * @param maxEvents
    *   cap on the number of events returned
    */
  def readForProjection(after: ReadCursor, eventFilter: EventFilter, maxEvents: Int): F[ProjectionRead[A]]

  /** The cursor to give a projection so that it resumes right after the event at `globalPosition` in delivery order.
    * This is how an operator moves a checkpoint by position (see [[EventStoreNotification.UpdateCheckpointIndex]]).
    *
    * When no event exists at `globalPosition` (a gap, or past the last event), the nearest event before it is used. A
    * position of 0 or less, or one before every event, gives [[ReadCursor.Start]]. Because delivery order is not
    * exactly position order, events whose writes overlapped the one at `globalPosition` may fall on either side of the
    * returned cursor regardless of their position.
    */
  def cursorAt(globalPosition: Long): F[ReadCursor]
