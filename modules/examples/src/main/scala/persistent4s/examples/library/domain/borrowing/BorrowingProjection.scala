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
import java.time.OffsetDateTime

final case class BorrowingView(
  bookId: String,
  memberId: String,
  borrowedAt: OffsetDateTime,
  dueDate: OffsetDateTime,
  returnedAt: Option[OffsetDateTime],
)

final class BorrowingProjection[F[_]: Async] private (
  state: Ref[F, Map[(String, String), BorrowingView]],
) extends Projection[F, LibraryEvent]:

  val name: String = "borrowing-projection"

  val filter: EventFilter = EventFilter(
    eventTypes = Set("BookBorrowed", "BookReturned"),
  )

  def handle(event: EventEnvelope[LibraryEvent]): F[Unit] =
    event.payload match
      case BookBorrowed(bookId, memberId, borrowedAt, dueDate) =>
        val borrowingKey = (bookId, memberId)
        state.update(
          _.updated(borrowingKey, BorrowingView(bookId, memberId, borrowedAt, dueDate, None)),
        )
      case BookReturned(bookId, memberId, returnedAt) =>
        val borrowingKey = (bookId, memberId)
        state.update(_.updatedWith(borrowingKey) {
          case Some(view) => Some(view.copy(returnedAt = Some(returnedAt)))
          case None       => None
        })
      case _ => Async[F].unit

  def getBorrowings: F[List[BorrowingView]] = state.get.map(_.values.toList)

  def getActiveBorrowings: F[List[BorrowingView]] =
    state.get.map(_.values.filter(_.returnedAt.isEmpty).toList)

  def getBorrowingsByMember(memberId: String): F[List[BorrowingView]] =
    state.get.map(_.values.filter(_.memberId == memberId).toList)

  def getBorrowingsByBook(bookId: String): F[List[BorrowingView]] =
    state.get.map(_.values.filter(_.bookId == bookId).toList)

object BorrowingProjection:

  def make[F[_]: Async]: F[BorrowingProjection[F]] =
    Ref.of[F, Map[(String, String), BorrowingView]](Map.empty).map(new BorrowingProjection(_))
