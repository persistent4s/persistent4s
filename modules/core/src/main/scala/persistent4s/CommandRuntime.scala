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

/** A stable identifier for a command snapshot. Keep it unchanged once snapshots have been written in production. */
opaque type SnapshotId = String

object SnapshotId:

  def apply(value: String): SnapshotId =
    require(value.nonEmpty, "Snapshot id must not be empty")
    value

  extension (id: SnapshotId) def value: String = id

/** Serialization for command state snapshots. Snapshot formats are deliberately independent from event formats. */
trait SnapshotCodec[S]:

  def encode(state: S): String

  def decode(payload: String): Either[Throwable, S]

/** Controls whether snapshot-cache outages affect command availability. */
enum SnapshotFailureMode:

  /** Fall back to event replay when snapshot load/save/delete fails. */
  case BestEffort

  /** Treat snapshot failures as command infrastructure failures. */
  case Strict

/** The raw snapshot representation stored by a [[CommandSnapshotStore]]. Snapshots are caches: event history remains
  * the source of truth and a missing snapshot always falls back to replay.
  */
final case class StoredCommandSnapshot(
  globalPosition: Long,
  eventCount: Long,
  filterFingerprint: String,
  payload: String,
)

/** Storage for serialized command snapshots.
  *
  * Implementations must make saves monotonic by `globalPosition`: a concurrent save at an older position must not
  * replace a newer snapshot.
  */
trait CommandSnapshotStore[F[_]]:

  def load(snapshotId: SnapshotId, key: String, version: Int): F[Option[StoredCommandSnapshot]]

  def save(snapshotId: SnapshotId, key: String, version: Int, snapshot: StoredCommandSnapshot): F[Unit]

  def delete(snapshotId: SnapshotId, key: String, version: Int): F[Unit]

/** Runtime dependencies required to execute an event-sourced command. Keeping snapshots next to the event store makes
  * them available without coupling [[EventStore]] implementations to arbitrary command-state serialization.
  */
final case class CommandRuntime[F[_], A <: Event](
  eventStore: EventStore[F, A],
  snapshots: Option[CommandSnapshotStore[F]] = None,
):

  /** Execute `command` with `handler`. The runtime fixes both `F` and the event root, so neither needs to be supplied
    * at the call site.
    */
  def execute[C, S, R](
    handler: EventSourcedCommandHandler[C, S, A, R],
    command: C,
  )(using Concurrent[F]): F[Either[R, List[A]]] =
    handler.run(command)(using summon[Concurrent[F]], this)

  /** Execute a command while discarding accepted events but preserving a typed rejection. */
  def executeUnit[C, S, R](
    handler: EventSourcedCommandHandler[C, S, A, R],
    command: C,
  )(using Concurrent[F]): F[Either[R, Unit]] =
    execute(handler, command).map(_.void)

  /** Execute a command and translate only its typed domain rejection into an application error. */
  def runOrRaise[C, S, R](
    handler: EventSourcedCommandHandler[C, S, A, R],
    command: C,
  )(
    mapRejection: R => Throwable,
  )(using Concurrent[F]): F[List[A]] =
    handler.runOrRaise(command)(mapRejection)(using summon[Concurrent[F]], this)

  /** Execute a command for its success/failure outcome, translate its typed rejection, and discard accepted events. */
  def executeOrRaise[C, S, R](
    handler: EventSourcedCommandHandler[C, S, A, R],
    command: C,
  )(
    mapRejection: R => Throwable,
  )(using Concurrent[F]): F[Unit] =
    runOrRaise(handler, command)(mapRejection).void

object CommandRuntime:

  /** Build a runtime without snapshots. This is useful in tests and preserves the lightweight EventStore-only setup. */
  def eventStoreOnly[F[_], A <: Event](eventStore: EventStore[F, A]): CommandRuntime[F, A] =
    CommandRuntime(eventStore, None)

  /** Allow existing applications that only provide an EventStore to use typed command handlers without extra wiring. */
  given fromEventStore[F[_], A <: Event](using eventStore: EventStore[F, A]): CommandRuntime[F, A] =
    eventStoreOnly(eventStore)
