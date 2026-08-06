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

import cats.effect.IO

import persistent4s.*
import persistent4s.examples.library.domain.borrowing.{BookBorrowed, BookReturned}
import persistent4s.examples.library.domain.{LibraryEvent, LibraryScopes}

final case class BookState(
  bookId: UUID,
  title: String,
  author: String,
  totalCopies: Int,
  availableCopies: Int,
)

final class BookProjection(
  protected val repository: BookRepository,
) extends ExactlyOnceEventSourcedProjection[IO, LibraryEvent, UUID, BookState]:

  override val name: String = "book-projection"

  override protected val eventHandlers = handlersBy(LibraryScopes.Book):
    on[BookAdded].create: event =>
      BookState(event.bookId, event.title, event.author, event.totalCopies, event.totalCopies)

    on[BookBorrowed].update: state =>
      state.copy(availableCopies = state.availableCopies - 1)

    on[BookReturned].update: state =>
      state.copy(availableCopies = state.availableCopies + 1)
