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

package persistent4s.examples.library.domain.book

import cats.effect.*
import cats.syntax.all.*
import skunk.*
import skunk.implicits.*
import skunk.codec.all.*

import persistent4s.examples.library.domain.book.BookState
import java.util.UUID
import persistent4s.postgres.PostgresRepository

final class BookRepository[F[_]: Async] private (
  val pool: Resource[F, Session[F]],
) extends PostgresRepository[F, UUID, BookState]:

  import BookRepository.*

  override def findMany(keys: List[UUID]): F[Map[UUID, Option[BookState]]] =
    if keys.isEmpty then Map.empty.pure[F]
    else
      pool.use(_.execute(findManyQuery(keys.size))(keys)).map { states =>
        val found = states.map(s => s.bookId -> s).toMap
        keys.map(k => k -> found.get(k)).toMap
      }

  override def upsertMany(session: Session[F], states: Map[UUID, BookState]): F[Unit] =
    if states.isEmpty then Async[F].unit
    else
      states.toList
        .grouped(MaxUpsertChunkSize)
        .toList
        .traverse_(chunk => session.execute(upsertManyCommand(chunk.size))(chunk.map(_._2)))
        .void

  override def deleteMany(session: Session[F], keys: List[UUID]): F[Unit] =
    if keys.isEmpty then Async[F].unit
    else session.execute(deleteManyCommand(keys.size))(keys).void

  def find(key: UUID): F[Option[BookState]] =
    pool.use(_.option(findQuery)(key))

  def getBooks: F[List[BookState]] =
    pool.use(_.execute(getBooksQuery))

object BookRepository:

  private val MaxUpsertChunkSize = 500

  private val bookStateCodec: Codec[BookState] =
    (uuid *: text *: text *: int4 *: int4).to[BookState]

  private def findManyQuery(n: Int): Query[List[UUID], BookState] =
    sql"""
      SELECT book_id, title, author, total_copies, available_copies
      FROM books
      WHERE book_id = ANY(ARRAY[${uuid.list(n)}])
    """.query(bookStateCodec)

  private val findQuery: Query[UUID, BookState] =
    sql"""
      SELECT book_id, title, author, total_copies, available_copies
      FROM books
      WHERE book_id = $uuid
    """.query(bookStateCodec)

  private def upsertManyCommand(n: Int): Command[List[BookState]] =
    sql"""
      INSERT INTO books (book_id, title, author, total_copies, available_copies)
      VALUES ${bookStateCodec.values.list(n)}
      ON CONFLICT (book_id) DO UPDATE SET
        title            = EXCLUDED.title,
        author           = EXCLUDED.author,
        total_copies     = EXCLUDED.total_copies,
        available_copies = EXCLUDED.available_copies
    """.command

  private def deleteManyCommand(n: Int): Command[List[UUID]] =
    sql"DELETE FROM books WHERE book_id = ANY(ARRAY[${uuid.list(n)}])".command

  private val getBooksQuery: Query[Void, BookState] =
    sql"SELECT book_id, title, author, total_copies, available_copies FROM books".query(bookStateCodec)

  def make[F[_]: Async](pool: Resource[F, Session[F]]): BookRepository[F] =
    new BookRepository(pool)
