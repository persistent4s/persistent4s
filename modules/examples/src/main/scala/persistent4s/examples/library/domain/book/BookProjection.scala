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

final case class BookView(
  bookId: String,
  title: String,
  author: String,
  totalCopies: Int,
  availableCopies: Int,
)

final class BookProjection[F[_]: Async] private (
  state: Ref[F, Map[String, BookView]],
) extends Projection[F, LibraryEvent]:

  val name: String = "book-projection"

  val filter: EventFilter = EventFilter(
    eventTypes = Set("BookAdded", "BookBorrowed", "BookReturned"),
  )

  def handle(event: EventEnvelope[LibraryEvent]): F[Unit] =
    event.payload match
      case BookAdded(bookId, title, author, totalCopies) =>
        state.update(_.updated(bookId, BookView(bookId, title, author, totalCopies, totalCopies)))
      case BookBorrowed(bookId, _, _, _) =>
        state.update(_.updatedWith(bookId) {
          case Some(view) => Some(view.copy(availableCopies = view.availableCopies - 1))
          case None       => throw new Exception(s"Book with ID $bookId not found in projection")
        })
      case BookReturned(bookId, _, _) =>
        state.update(_.updatedWith(bookId) {
          case Some(view) => Some(view.copy(availableCopies = view.availableCopies + 1))
          case None       => throw new Exception(s"Book with ID $bookId not found in projection")
        })
      case _ => Async[F].unit

  def getBooks: F[List[BookView]] = state.get.map(_.values.toList)

  def getBook(bookId: String): F[Option[BookView]] = state.get.map(_.get(bookId))

object BookProjection:

  def make[F[_]: Async]: F[BookProjection[F]] =
    for state <- Ref.of[F, Map[String, BookView]](Map.empty)
    yield new BookProjection(state)
