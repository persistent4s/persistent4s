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

import cats.Applicative

/** A SnapshotStore manages snapshots of the current state for command handlers. This allows the system to avoid
  * replaying the entire event history on every command execution, improving performance. Snapshots are typically taken
  * after a certain number of events have been processed, as defined by the CommandHandler's snapshotThreshold. The
  * SnapshotStore interface abstracts over the storage mechanism, which could be in-memory, on disk, or in a database.
  *
  * @tparam F
  *   the effect type, such as `cats.effect.IO`
  */
trait SnapshotStore[F[_]]:

  /** Load a snapshot for the given handler and tags, if it exists. */
  def load[S: SnapshotCodec](handlerName: String, tags: Set[Tag]): F[Option[Snapshot[S]]]

  /** Save a snapshot for the given handler and tags. */
  def save[S: SnapshotCodec](handlerName: String, tags: Set[Tag], snapshot: Snapshot[S]): F[Unit]

object SnapshotStore:

  /** A no-op SnapshotStore that never stores or returns snapshots. Resolved automatically when no real implementation
    * is provided, allowing CommandHandler.run to compile without a snapshot backend.
    */
  given noop[F[_]: Applicative]: SnapshotStore[F] with
    def load[S: SnapshotCodec](handlerName: String, tags: Set[Tag]): F[Option[Snapshot[S]]] =
      Applicative[F].pure(None)
    def save[S: SnapshotCodec](handlerName: String, tags: Set[Tag], snapshot: Snapshot[S]): F[Unit] =
      Applicative[F].unit

case class Snapshot[S](state: S, globalPosition: Long)

/** Raised when a snapshot payload cannot be decoded into the expected state type. Caught by CommandHandler to trigger a
  * full event replay rather than failing the command.
  */
final case class SnapshotDecodeException(cause: Throwable)
    extends RuntimeException("Failed to decode snapshot state", cause)
