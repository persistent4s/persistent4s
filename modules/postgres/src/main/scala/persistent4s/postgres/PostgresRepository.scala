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

package persistent4s.postgres

import cats.syntax.all.*
import cats.effect.Async
import persistent4s.Repository
import cats.effect.Resource
import skunk.Session

/** A PostgresRepository is a specialized Repository that provides separate methods for upserting and deleting state,
  * which can be more efficient when using a relational database like PostgreSQL.
  *
  * @tparam F
  *   the effect type, such as IO
  * @tparam K
  *   the key type for fetching and persisting state
  * @tparam S
  *   the state type for the projection
  */
trait PostgresRepository[F[_]: Async, K, S] extends Repository[F, K, S] {

  val pool: Resource[F, Session[F]]

  override def persistStates(states: Map[K, Option[S]]): F[Unit] =
    val toDelete = states.collect { case (key, None) => key }.toList
    val toUpsert = states.collect { case (key, Some(state)) => key -> state }.toMap
    pool.use { session =>
      session.transaction.use { _ =>
        for {
          _ <- if (toDelete.nonEmpty) deleteMany(session, toDelete) else ().pure[F]
          _ <- if (toUpsert.nonEmpty) upsertMany(session, toUpsert) else ().pure[F]
        } yield ()
      }
    }

  /** Persist a batch of updated states. The input is a map from key to value, where each entry represents the new state
    * for that key. The implementation should upsert these values in durable storage so that they can be retrieved later
    * by `fetchStates`.
    *
    * @param states
    *   a map from key to value, where each entry represents the new state for that key
    * @return
    *   an effect representing the completion of the operation
    */
  def upsertMany(session: Session[F], states: Map[K, S]): F[Unit]

  /** Delete a batch of keys and their associated state. The implementation should remove these keys from durable
    * storage so that they are no longer returned by `fetchStates`.
    *
    * @param keys
    *   the list of keys to delete
    * @return
    *   an effect representing the completion of the operation
    */
  def deleteMany(session: Session[F], keys: List[K]): F[Unit]

}
