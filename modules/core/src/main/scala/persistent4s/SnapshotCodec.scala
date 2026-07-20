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

/** Serialization interface for snapshot state. Encodes state to a String for storage and decodes it back. Concrete
  * implementations are provided by backend modules (e.g. the postgres module provides a circe-based given).
  *
  * @tparam S
  *   the state type to serialize
  */
trait SnapshotCodec[S]:

  /** Encode a state value to a String for storage. */
  def encode(state: S): String

  /** Decode a state value from a stored String. */
  def decode(payload: String): Either[Throwable, S]

private trait LowPrioritySnapshotCodec:

  /** A no-op SnapshotCodec that throws if actually called. Resolved automatically at low priority when no real
    * implementation is in scope, allowing CommandHandler.run to compile without snapshot serialization. Only safe when
    * paired with the noop SnapshotStore — a real store with this codec will fail at runtime.
    */
  given noop[S]: SnapshotCodec[S] with

    def encode(state: S): String =
      throw new UnsupportedOperationException(
        "noop SnapshotCodec — provide a real SnapshotCodec[S] to enable snapshots",
      )

    def decode(payload: String): Either[Throwable, S] =
      Left(
        new UnsupportedOperationException(
          "noop SnapshotCodec — provide a real SnapshotCodec[S] to enable snapshots",
        ),
      )

object SnapshotCodec extends LowPrioritySnapshotCodec
