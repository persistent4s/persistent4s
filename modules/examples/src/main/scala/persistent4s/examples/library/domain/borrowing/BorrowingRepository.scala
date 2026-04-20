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

package persistent4s.examples.library.domain.borrowing

import cats.effect.*
import cats.syntax.all.*
import skunk.*
import skunk.implicits.*
import skunk.codec.all.*

import persistent4s.examples.library.application.Repository
import persistent4s.examples.library.domain.borrowing.BorrowingState
import java.util.UUID

final class BorrowingRepository[F[_]: Async] private (
  pool: Resource[F, Session[F]],
) extends Repository[F, (UUID, UUID), BorrowingState]:

  import BorrowingRepository.*

  def find(key: (UUID, UUID)): F[Option[BorrowingState]] =
    pool.use(_.option(findQuery)(key))

  def findMany(keys: List[(UUID, UUID)]): F[Map[(UUID, UUID), Option[BorrowingState]]] =
    if keys.isEmpty then Map.empty.pure[F]
    else
      val (bookIds, memberIds) = keys.unzip
      pool.use(_.execute(findManyQuery(keys.size))((bookIds, memberIds))).map { states =>
        val found = states.map(s => (s.bookId, s.memberId) -> s).toMap
        keys.map(k => k -> found.get(k)).toMap
      }

  def save(key: (UUID, UUID), value: BorrowingState): F[Unit] =
    pool.use(_.execute(upsertCommand)(value)).void

  def delete(key: (UUID, UUID)): F[Unit] =
    pool.use(_.execute(deleteCommand)(key)).void

  def persistMany(states: Map[(UUID, UUID), Option[BorrowingState]]): F[Unit] =
    val toUpsert = states.collect { case (_, Some(v)) => v }.toList
    val toDelete = states.collect { case (k, None) => k }.toList
    val upsertEffect =
      if toUpsert.isEmpty then Async[F].unit
      else pool.use(_.execute(upsertManyCommand(toUpsert.size))(toUpsert)).void
    val deleteEffect =
      if toDelete.isEmpty then Async[F].unit
      else
        val (bookIds, memberIds) = toDelete.unzip
        pool.use(_.execute(deleteManyCommand(toDelete.size))((bookIds, memberIds))).void
    upsertEffect >> deleteEffect

  def getBorrowings: F[List[BorrowingState]] =
    pool.use(_.execute(getBorrowingsQuery))

  def getActiveBorrowings: F[List[BorrowingState]] =
    pool.use(_.execute(getActiveBorrowingsQuery))

  def getMemberBorrowings(memberId: UUID): F[List[BorrowingState]] =
    pool.use(_.execute(getMemberBorrowingsQuery)(memberId))

object BorrowingRepository:

  private val borrowingStateCodec: Codec[BorrowingState] =
    (uuid *: uuid *: timestamptz *: timestamptz *: timestamptz.opt).to[BorrowingState]

  private def findManyQuery(n: Int): Query[(List[UUID], List[UUID]), BorrowingState] =
    sql"""
      SELECT b.book_id, b.member_id, b.borrowed_at, b.due_date, b.returned_at
      FROM borrowings b
      JOIN (
        SELECT unnest(ARRAY[${uuid.list(n)}]) AS book_id,
               unnest(ARRAY[${uuid.list(n)}]) AS member_id
      ) k ON b.book_id = k.book_id AND b.member_id = k.member_id
    """.query(borrowingStateCodec)

  private val findQuery: Query[(UUID, UUID), BorrowingState] =
    sql"""
      SELECT book_id, member_id, borrowed_at, due_date, returned_at
      FROM borrowings
      WHERE book_id = $uuid AND member_id = $uuid
    """.query(borrowingStateCodec)

  private val upsertCommand: Command[BorrowingState] =
    sql"""
      INSERT INTO borrowings (book_id, member_id, borrowed_at, due_date, returned_at)
      VALUES ($borrowingStateCodec)
      ON CONFLICT (book_id, member_id) DO UPDATE SET
        borrowed_at = EXCLUDED.borrowed_at,
        due_date    = EXCLUDED.due_date,
        returned_at = EXCLUDED.returned_at
    """.command

  private val deleteCommand: Command[(UUID, UUID)] =
    sql"DELETE FROM borrowings WHERE book_id = $uuid AND member_id = $uuid".command

  private def upsertManyCommand(n: Int): Command[List[BorrowingState]] =
    sql"""
      INSERT INTO borrowings (book_id, member_id, borrowed_at, due_date, returned_at)
      VALUES ${borrowingStateCodec.list(n)}
      ON CONFLICT (book_id, member_id) DO UPDATE SET
        borrowed_at = EXCLUDED.borrowed_at,
        due_date    = EXCLUDED.due_date,
        returned_at = EXCLUDED.returned_at
    """.command

  private def deleteManyCommand(n: Int): Command[(List[UUID], List[UUID])] =
    sql"""
      DELETE FROM borrowings
      WHERE (book_id, member_id) IN (
        SELECT unnest(ARRAY[${uuid.list(n)}]), unnest(ARRAY[${uuid.list(n)}])
      )
    """.command

  private val getBorrowingsQuery: Query[Void, BorrowingState] =
    sql"SELECT book_id, member_id, borrowed_at, due_date, returned_at FROM borrowings".query(borrowingStateCodec)

  private val getActiveBorrowingsQuery: Query[Void, BorrowingState] =
    sql"""
      SELECT book_id, member_id, borrowed_at, due_date, returned_at
      FROM borrowings
      WHERE returned_at IS NULL
    """.query(borrowingStateCodec)

  private val getMemberBorrowingsQuery: Query[UUID, BorrowingState] =
    sql"""
      SELECT book_id, member_id, borrowed_at, due_date, returned_at
      FROM borrowings
      WHERE member_id = $uuid
    """.query(borrowingStateCodec)

  def make[F[_]: Async](pool: Resource[F, Session[F]]): BorrowingRepository[F] =
    new BorrowingRepository(pool)
