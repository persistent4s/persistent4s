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

import cats.effect.Concurrent
import cats.syntax.all.*

/** A CommandHandler defines how a command is processed in an event-sourced system. It reads events from the store to
  * build the current state, validates the command against that state, and decides which new events to produce.
  *
  * @tparam C
  *   the command type
  * @tparam S
  *   the state type, derived by folding events
  * @tparam E
  *   the event type
  */
trait CommandHandler[C, S, E <: Event]:

  /** Which tags to read from the event store for this command. */
  def tags(command: C): Set[Tag]

  /** The event types that this handler is interested in for building the state. If not specified, all events with the
    * relevant tags will be included.
    */
  def eventTypes: Option[Set[EventTypeName]] = None

  /** The initial state before any events have been applied. */
  def initial: S

  /** Fold a single event into the current state. */
  def evolve(command: C, state: S, event: E): S

  /** Validate the command against the current state. Should raise an error if the command is invalid. */
  def validate(state: S, command: C): Either[Throwable, Unit]

  /** Produce the events that result from applying the command, each with its own set of tags. Only called after
    * validation passes.
    */
  def decide(state: S, command: C): List[(Set[Tag], E)]

  /** A unique identifier for this handler, mainly used for snapshotting. Snapshot keys are derived from this ID and the
    * command's tags.
    */
  def handlerId: String = getClass.getName()

  /** After how many events should a snapshot be taken for this handler? Override to customize. Default is 100. */
  def snapshotThreshold: Int = 100

  /** Maximum number of retry attempts on optimistic concurrency conflicts. Override to customize. Default is 3. */
  def maxRetries: Int = 3

  /** Execute a command using this handler and the given event store. On optimistic concurrency conflict
    * (IndexConflictException), the command is automatically retried with fresh state up to maxRetries times. The
    * command is re-validated against the new state and may still succeed.
    */
  def run[F[_]: Concurrent](command: C)(using
    eventStore: EventStore[F, E],
    snapshotStore: SnapshotStore[F],
    codec: SnapshotCodec[S],
  ): F[Unit] =
    runWithRetry(command, maxRetries)

  private def runWithRetry[F[_]: Concurrent](command: C, retriesLeft: Int)(using
    eventStore: EventStore[F, E],
    snapshotStore: SnapshotStore[F],
    codec: SnapshotCodec[S],
  ): F[Unit] =
    attempt(command).handleErrorWith {
      case _: IndexConflictException if retriesLeft > 0 =>
        runWithRetry(command, retriesLeft - 1)
      case e =>
        Concurrent[F].raiseError(e)
    }

  private def attempt[F[_]: Concurrent](
    command: C,
  )(using
    eventStore: EventStore[F, E],
    snapshotStore: SnapshotStore[F],
    codec: SnapshotCodec[S],
  ): F[Unit] =
    for
      tags          <- Concurrent[F].pure(tags(command))
      filter         = EventFilter(eventTypes.getOrElse(Set.empty), tags)
      maybeSnapshot <- snapshotStore.load[S](handlerId, tags).recover { case _: SnapshotDecodeException => None }
      fromPosition   = maybeSnapshot.map(_.globalPosition).getOrElse(0L)
      envelopes     <- eventStore.readFrom(fromPosition, filter).compile.toList
      baseState      = maybeSnapshot.map(_.state).getOrElse(initial)
      state          = envelopes.foldLeft(baseState)((s, env) => evolve(command, s, env.payload))
      index          = envelopes.lastOption
                .map(_.metadata.globalPosition)
                .orElse(maybeSnapshot.map(_.globalPosition))
                .getOrElse(0L)
      _ <- validate(state, command) match
             case Left(e)  => Concurrent[F].raiseError(e)
             case Right(_) => Concurrent[F].unit
      decided = decide(state, command)
      events  = decided.map((tags, event) => (tags, EventTypeName.fromInstance(event), event))
      _      <- eventStore.append(filter, index, events)
      _      <-
        if envelopes.size >= snapshotThreshold then snapshotStore.save[S](handlerId, tags, Snapshot(state, index))
        else Concurrent[F].unit
    yield ()
