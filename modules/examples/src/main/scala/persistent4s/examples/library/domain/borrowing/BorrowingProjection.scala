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

import persistent4s.*
import persistent4s.examples.library.domain.{BookBorrowed, BookReturned, LibraryEvent}
import persistent4s.examples.library.application.Repository
import java.time.OffsetDateTime
import java.util.UUID

final case class BorrowingState(
  bookId: UUID,
  memberId: UUID,
  borrowedAt: OffsetDateTime,
  dueDate: OffsetDateTime,
  returnedAt: Option[OffsetDateTime],
)

final class BorrowingProjection[F[_]: Async] private (
  repository: Repository[F, (UUID, UUID), BorrowingState],
) extends Projection[F, LibraryEvent, (UUID, UUID), BorrowingState]:

  override val name: String = "borrowing-projection"

  override val filter: EventFilter = EventFilter(
    eventTypes = Set(EventTypeName.of[BookBorrowed], EventTypeName.of[BookReturned]),
  )

  override def resolveKeys(event: EventEnvelope[LibraryEvent]): List[(UUID, UUID)] = event.payload match
    case BookBorrowed(bookId, memberId, _, _) => List((bookId, memberId))
    case BookReturned(bookId, memberId, _)    => List((bookId, memberId))
    case _                                    => Nil

  override def fetchStates(keys: List[(UUID, UUID)]): F[Map[(UUID, UUID), Option[BorrowingState]]] =
    repository.findMany(keys)

  override def handle(state: Option[BorrowingState], event: EventEnvelope[LibraryEvent]): F[Option[BorrowingState]] =
    (state, event.payload) match
      case (None, BookBorrowed(bookId, memberId, borrowedAt, dueDate)) =>
        BorrowingState(bookId, memberId, borrowedAt, dueDate, None).some.pure[F]
      case (Some(borrowingState), BookBorrowed(bookId, memberId, borrowedAt, dueDate))
          if borrowingState.returnedAt.nonEmpty =>
        BorrowingState(bookId, memberId, borrowedAt, dueDate, None).some.pure[F]
      case (Some(borrowingState), BookReturned(bookId, memberId, returnedAt)) =>
        BorrowingState(bookId, memberId, borrowingState.borrowedAt, borrowingState.dueDate, Some(returnedAt)).some
          .pure[F]
      case _ => Async[F].raiseError(new RuntimeException(s"Unexpected event: ${event.payload} for state: $state"))

  override def persistStates(states: Map[(UUID, UUID), Option[BorrowingState]]): F[Unit] =
    repository.persistMany(states)

object BorrowingProjection:

  def make[F[_]: Async](repository: Repository[F, (UUID, UUID), BorrowingState]): F[BorrowingProjection[F]] =
    Async[F].pure(new BorrowingProjection(repository))
