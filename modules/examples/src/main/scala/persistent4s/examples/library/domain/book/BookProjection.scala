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

import persistent4s.*
import persistent4s.examples.library.domain.{BookAdded, BookBorrowed, BookReturned, LibraryEvent}
import java.util.UUID

final case class BookState(
  bookId: UUID,
  title: String,
  author: String,
  totalCopies: Int,
  availableCopies: Int,
)

final class BookProjection[F[_]: Async] private (
  protected val repository: Repository[F, UUID, BookState],
) extends EventSourcedProjection[F, LibraryEvent, UUID, BookState]:

  override val name: String = "book-projection"

  override val handlers = List(
    on[BookAdded](_.bookId) { (_, e) =>
      BookState(e.bookId, e.title, e.author, e.totalCopies, e.totalCopies).some
    },
    on[BookBorrowed](_.bookId) { (state, _) =>
      state.map(s => s.copy(availableCopies = s.availableCopies - 1))
    },
    on[BookReturned](_.bookId) { (state, _) =>
      state.map(s => s.copy(availableCopies = s.availableCopies + 1))
    },
  )

object BookProjection:

  def make[F[_]: Async](repository: Repository[F, UUID, BookState]): F[BookProjection[F]] =
    Async[F].pure(new BookProjection(repository))
