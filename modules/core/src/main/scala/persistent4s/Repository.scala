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

/** Durable storage for a [[Projection]]'s state. Before processing a batch of events the projector calls [[findMany]]
  * for the affected keys, and afterwards calls [[persist]] with the resulting changes. The backing store is up to the
  * implementation (SQL database, key-value store, etc.).
  *
  * @tparam F
  *   the effect type, such as IO
  * @tparam K
  *   the state key type
  * @tparam S
  *   the state value type
  */
trait Repository[F[_], K, S]:

  /** Fetch the current state for the given keys. Keys with no stored state map to `None`.
    *
    * @param keys
    *   the keys to look up
    */
  def findMany(keys: List[K]): F[Map[K, Option[S]]]

  /** Apply the state changes from a single batch: upsert each entry in `upserts`, remove each key in `deletes`. The two
    * should be applied atomically (ideally in one transaction) so a failure never leaves state partially applied.
    *
    * @param upserts
    *   the new state for each key
    * @param deletes
    *   the keys whose state should be removed
    */
  def persist(upserts: Map[K, S], deletes: List[K]): F[Unit]

object Repository:

  /** A no-op repository for projections that keep no state. */
  def empty[F[_]: Applicative]: Repository[F, Unit, Unit] =
    new Repository[F, Unit, Unit]:
      def findMany(keys: List[Unit]): F[Map[Unit, Option[Unit]]] =
        Applicative[F].pure(keys.map(_ -> None).toMap)
      def persist(upserts: Map[Unit, Unit], deletes: List[Unit]): F[Unit] =
        Applicative[F].unit
