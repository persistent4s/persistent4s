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
import cats.{Monad, MonadThrow}

import skunk.Session

/** Session-bound SQL operations for one PostgreSQL projection table.
  *
  * The callbacks contain the repository-specific SQL while [[PostgresAtomicRepository]] owns empty-input handling, key
  * completion, chunking, operation ordering and transaction boundaries. Delete callbacks run before upsert callbacks,
  * so an entry present in both change sets is ultimately upserted.
  *
  * Every callback receives the active repository session. In an atomic projection commit this is the same session and
  * transaction used for the checkpoint compare-and-set; callbacks must not acquire a nested session from a pool.
  */
final class PostgresRepositoryTable[F[_], K, S] private (
  private[postgres] val fetchRows: (Session[F], List[K]) => F[List[(K, S)]],
  private val deleteRows: (Session[F], List[K]) => F[Unit],
  private val upsertRows: (Session[F], List[(K, S)]) => F[Unit],
  val batchSize: Int,
)(using F: Monad[F]):

  require(batchSize > 0, "PostgreSQL repository batch size must be greater than zero")

  /** Replace the generated or declared fetch callback while preserving the other operations. */
  def withFetch(value: (Session[F], List[K]) => F[List[(K, S)]]): PostgresRepositoryTable[F, K, S] =
    new PostgresRepositoryTable(value, deleteRows, upsertRows, batchSize)

  /** Replace the generated or declared delete callback while preserving the other operations. */
  def withDelete(value: (Session[F], List[K]) => F[Unit]): PostgresRepositoryTable[F, K, S] =
    new PostgresRepositoryTable(fetchRows, value, upsertRows, batchSize)

  /** Replace the generated or declared upsert callback while preserving the other operations. */
  def withUpsert(value: (Session[F], List[(K, S)]) => F[Unit]): PostgresRepositoryTable[F, K, S] =
    new PostgresRepositoryTable(fetchRows, deleteRows, value, batchSize)

  /** Change the chunk size while preserving all callbacks. */
  def withBatchSize(value: Int): PostgresRepositoryTable[F, K, S] =
    new PostgresRepositoryTable(fetchRows, deleteRows, upsertRows, value)

  private[postgres] def write(
    session: Session[F],
    upserts: Map[K, S],
    deletes: List[K],
  ): F[Unit] =
    val deleteChunks = deletes.distinct.grouped(batchSize).toList
    val upsertChunks = upserts.toList.grouped(batchSize).toList

    deleteChunks.traverse_(deleteRows(session, _)) *>
      upsertChunks.traverse_(upsertRows(session, _))

object PostgresRepositoryTable:

  /** Describe the common case where each persisted state row contains its own repository key.
    *
    * `keyOf` is applied to fetched rows to reconstruct keyed results. Before an upsert callback runs, the same function
    * verifies that every state's key matches its repository map key. The upsert callback receives state rows directly;
    * use [[apply]] when keys are stored separately from state or must also be supplied to the upsert SQL.
    */
  def rows[F[_]: MonadThrow, K, S](
    keyOf: S => K,
    fetch: (Session[F], List[K]) => F[List[S]],
    delete: (Session[F], List[K]) => F[Unit],
    upsert: (Session[F], List[S]) => F[Unit],
    batchSize: Int = 500,
  ): PostgresRepositoryTable[F, K, S] =
    val F = MonadThrow[F]

    apply(
      fetch = (session, keys) => fetch(session, keys).map(_.map(state => keyOf(state) -> state)),
      delete = delete,
      upsert = (session, entries) =>
        entries.traverse { case (key, state) =>
          val stateKey = keyOf(state)
          if key == stateKey then F.pure(state)
          else
            F.raiseError(
              new IllegalArgumentException(
                s"PostgresRepositoryTable.rows key mismatch: upsert key [$key] does not match state key [$stateKey]",
              ),
            )
        }
          .flatMap(upsert(session, _)),
      batchSize = batchSize,
    )

  /** Describe the explicit SQL operations for one projection table.
    *
    * `fetch` returns only rows that exist; the repository reconstructs `None` for every requested missing key. `delete`
    * and `upsert` are invoked with non-empty chunks no larger than `batchSize`.
    */
  def apply[F[_]: Monad, K, S](
    fetch: (Session[F], List[K]) => F[List[(K, S)]],
    delete: (Session[F], List[K]) => F[Unit],
    upsert: (Session[F], List[(K, S)]) => F[Unit],
    batchSize: Int = 500,
  ): PostgresRepositoryTable[F, K, S] =
    new PostgresRepositoryTable(fetch, delete, upsert, batchSize)
