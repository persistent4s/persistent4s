/*
 * Copyright 2026 Bastien Jolidon
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
) extends Projection[F, LibraryEvent, UUID, BookState]:

  override val name: String = "book-projection"

  override val filter: Set[EventTypeName] = Set(
    EventTypeName.of[BookAdded],
    EventTypeName.of[BookBorrowed],
    EventTypeName.of[BookReturned],
  )

  override def resolveKeys(event: EventEnvelope[LibraryEvent]): List[UUID] = event.payload match
    case BookAdded(bookId, _, _, _)    => List(bookId)
    case BookBorrowed(bookId, _, _, _) => List(bookId)
    case BookReturned(bookId, _, _)    => List(bookId)
    case _                             => Nil

  override def handle(state: Option[BookState], event: EventEnvelope[LibraryEvent]): F[Option[BookState]] =
    (state, event.payload) match
      case (None, BookAdded(bookId, title, author, totalCopies)) =>
        BookState(bookId, title, author, totalCopies, totalCopies).some.pure[F]
      case (Some(s), BookBorrowed(bookId, _, _, _)) =>
        Some(s.copy(availableCopies = s.availableCopies - 1)).pure[F]
      case (Some(s), BookReturned(bookId, _, _)) =>
        Some(s.copy(availableCopies = s.availableCopies + 1)).pure[F]
      case _ => Async[F].raiseError(new RuntimeException(s"Unexpected event: ${event.payload} for state: $state"))

object BookProjection:

  def make[F[_]: Async](repository: Repository[F, UUID, BookState]): F[BookProjection[F]] =
    Async[F].pure(new BookProjection(repository))
