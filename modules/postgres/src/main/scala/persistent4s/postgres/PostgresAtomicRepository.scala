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

import cats.effect.{Async, Resource}
import cats.syntax.all.*

import persistent4s.{AtomicRepository, ProjectionCommit, ReadCursor}

import skunk.Session
import skunk.data.{TransactionAccessMode, TransactionIsolationLevel}

/** PostgreSQL base class for projection repositories that commit state and checkpoint atomically.
  *
  * Implementors describe their explicit SQL callbacks with [[PostgresRepositoryTable]]. This class owns empty-input
  * handling, key deduplication, missing-key reconstruction, chunked reads and writes, session acquisition, transactions
  * and the optimistic checkpoint update. The pool must point to the same database as the
  * [[PostgresProjectionCheckpoint]] supplied to the projector.
  */
abstract class PostgresAtomicRepository[F[_]: Async, K, S](
  pool: Resource[F, Session[F]],
) extends AtomicRepository[F, K, S]:

  /** The session-bound SQL operations for this projection table. */
  protected val table: PostgresRepositoryTable[F, K, S]

  /** Run an additional repository query with a pooled session. */
  final protected def useSession[A](f: Session[F] => F[A]): F[A] = pool.use(f)

  final override def findMany(keys: List[K]): F[Map[K, Option[S]]] =
    if keys.isEmpty then Async[F].pure(Map.empty)
    else pool.use(findMany(_, keys))

  /** Like [[findMany]], using the caller's connection and transaction, e.g. inside [[readWithCursor]]. */
  final def findMany(session: Session[F], keys: List[K]): F[Map[K, Option[S]]] =
    val distinctKeys = keys.distinct
    distinctKeys
      .grouped(table.batchSize)
      .toList
      .traverse(table.fetchRows(session, _))
      .map { chunks =>
        val found = chunks.flatten.toMap
        distinctKeys.iterator.map(key => key -> found.get(key)).toMap
      }

  /** Find one projection state using the table's batch query. */
  final def find(key: K): F[Option[S]] =
    findMany(key :: Nil).map(_.getOrElse(key, None))

  /** Like [[find]], using the caller's connection and transaction, e.g. inside [[readWithCursor]]. */
  final def find(session: Session[F], key: K): F[Option[S]] =
    findMany(session, key :: Nil).map(_.getOrElse(key, None))

  /** Run `read` and load the cursor of `projectionName` in one read-only snapshot.
    *
    * The projection commits its state changes and its checkpoint in one transaction, so the result of `read` reflects
    * exactly the events up to the returned cursor: no later event, and none missing. Use it to tell a caller how fresh
    * a view is, or to compare it with the position of an event it is waiting for. The cursor is
    * [[persistent4s.ReadCursor.Start]] before the projection's first checkpoint.
    *
    * `read` must use the supplied session, for instance through [[find]], [[findMany]] or a filtered query's
    * `run(session)`; a read on another pooled connection would not share the snapshot.
    *
    * @param projectionName
    *   the projection that writes the rows `read` loads
    * @param read
    *   the view query, run inside the snapshot
    * @return
    *   the query result and the cursor of the projection it reflects
    */
  final def readWithCursor[A](projectionName: String)(read: Session[F] => F[A]): F[(A, ReadCursor)] =
    pool.use { session =>
      session.transaction(TransactionIsolationLevel.RepeatableRead, TransactionAccessMode.ReadOnly).use { _ =>
        (read(session), PostgresProjectionCheckpoint.cursor(session, projectionName)).tupled
      }
    }

  final override def persist(upserts: Map[K, S], deletes: List[K]): F[Unit] =
    if upserts.isEmpty && deletes.isEmpty then Async[F].unit
    else pool.use(session => session.transaction.use(_ => table.write(session, upserts, deletes)))

  final override def persistAtomically(commit: ProjectionCommit[K, S]): F[Unit] =
    pool.use { session =>
      session.transaction.use { _ =>
        table.write(session, commit.upserts, commit.deletes) *>
          PostgresProjectionCheckpoint.compareAndSet(session, commit.expected, commit.checkpoint)
      }
    }
