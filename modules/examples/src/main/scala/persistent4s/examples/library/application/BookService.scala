/*
 * Copyright 2026 persistent4s
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

package persistent4s.examples.library.application

import java.util.UUID

import cats.effect.IO

import persistent4s.EventStore
import persistent4s.examples.library.api.*
import persistent4s.examples.library.domain.LibraryEvent
import persistent4s.examples.library.domain.book.*
import persistent4s.examples.library.infrastructure.LibraryModule

class BookServiceImpl(module: LibraryModule) extends BookService[IO]:

  private given EventStore[IO, LibraryEvent] = module.store

  def addBook(title: String, author: String, totalCopies: Int): IO[AddBookOutput] =
    for
      bookId <- IO(UUID.randomUUID().toString)
      _      <- AddBookHandler.run[IO](AddBook(bookId, title, author, totalCopies))
    yield AddBookOutput(bookId)

  def getBooks(): IO[GetBooksOutput] =
    module.bookProjection.getBooks.map(books =>
      GetBooksOutput(books.map(b => BookItem(b.bookId, b.title, b.author, b.totalCopies, b.availableCopies))),
    )

  def getBook(bookId: String): IO[GetBookOutput] =
    module.bookProjection.getBook(bookId).flatMap {
      case Some(b) =>
        IO.pure(GetBookOutput(BookItem(b.bookId, b.title, b.author, b.totalCopies, b.availableCopies)))
      case None => IO.raiseError(new Exception(s"Book not found: $bookId"))
    }
