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

import java.util.UUID

import cats.effect.{IO, Resource}

import persistent4s.postgres.{DerivedPostgresRepository, PostgresTable}

import skunk.Session

final class BookRepository private (
  pool: Resource[IO, Session[IO]],
) extends DerivedPostgresRepository[IO, UUID, BookState](pool, BookRepository.table)

object BookRepository:

  private val table: PostgresTable[UUID, BookState] =
    PostgresTable.derived[BookState]("books").key(_.bookId)

  def make(pool: Resource[IO, Session[IO]]): BookRepository =
    new BookRepository(pool)
