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

package persistent4s.examples.library.application

import cats.effect.IO
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.otel4s.metrics.Meter

import persistent4s.EventStore
import persistent4s.examples.library.api.*
import persistent4s.examples.library.domain.LibraryEvent
import persistent4s.examples.library.domain.borrowing.*

import java.util.UUID
import smithy4s.time.Timestamp

class BorrowingServiceImpl(repository: BorrowingRepository[IO])(using
  EventStore[IO, LibraryEvent],
  Tracer[IO],
  Meter[IO],
) extends BorrowingService[IO]:

  def borrowBook(bookId: String, memberId: String): IO[Unit] =
    BorrowBookHandler
      .run[IO](BorrowBook(UUID.fromString(bookId), UUID.fromString(memberId)))
      .adaptError {
        case e if e.getMessage.contains("not found") => NotFoundError(e.getMessage)
        case e                                       => ValidationError(e.getMessage)
      }

  def returnBook(bookId: String, memberId: String): IO[Unit] =
    ReturnBookHandler
      .run[IO](ReturnBook(UUID.fromString(bookId), UUID.fromString(memberId)))
      .adaptError {
        case e if e.getMessage.contains("not found") => NotFoundError(e.getMessage)
        case e                                       => ValidationError(e.getMessage)
      }

  def getBorrowings(): IO[GetBorrowingsOutput] =
    repository.getBorrowings.map(borrowings => GetBorrowingsOutput(borrowings.map(toBorrowingItem)))

  def getActiveBorrowings(): IO[GetActiveBorrowingsOutput] =
    repository.getActiveBorrowings.map(borrowings => GetActiveBorrowingsOutput(borrowings.map(toBorrowingItem)))

  def getMemberBorrowings(memberId: String): IO[GetMemberBorrowingsOutput] =
    repository
      .getMemberBorrowings(UUID.fromString(memberId))
      .map(borrowings => GetMemberBorrowingsOutput(borrowings.map(toBorrowingItem)))

  private def toBorrowingItem(b: BorrowingState): BorrowingItem =
    BorrowingItem(
      bookId = b.bookId.toString(),
      memberId = b.memberId.toString(),
      borrowedAt = Timestamp.fromInstant(b.borrowedAt.toInstant),
      dueDate = Timestamp.fromInstant(b.dueDate.toInstant),
      returnedAt = b.returnedAt.map(returnedAt => Timestamp.fromInstant(returnedAt.toInstant)),
    )
