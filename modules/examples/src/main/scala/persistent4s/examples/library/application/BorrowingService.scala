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

import java.util.UUID

import cats.effect.IO

import persistent4s.CommandRuntime
import persistent4s.examples.library.api.*
import persistent4s.examples.library.domain.LibraryEvent
import persistent4s.examples.library.domain.borrowing.*

import smithy4s.time.Timestamp

class BorrowingServiceImpl(repository: BorrowingRepository)(using commands: CommandRuntime[IO, LibraryEvent])
    extends BorrowingService[IO]:

  def borrowBook(bookId: String, memberId: String): IO[Unit] =
    commands.executeOrRaise(
      BorrowBook.Handler,
      BorrowBook(UUID.fromString(bookId), UUID.fromString(memberId)),
    ):
      case BorrowBook.Error.BookNotFound(id)                   => NotFoundError(s"Book not found: $id")
      case BorrowBook.Error.MemberNotFound(id)                 => NotFoundError(s"Member not found: $id")
      case BorrowBook.Error.NoCopiesAvailable(id)              => ValidationError(s"No copies available for book: $id")
      case BorrowBook.Error.MemberAlreadyHasBook(book, member) =>
        ValidationError(s"Member $member already has book $book")

  def returnBook(bookId: String, memberId: String): IO[Unit] =
    commands.executeOrRaise(
      ReturnBook.Handler,
      ReturnBook(UUID.fromString(bookId), UUID.fromString(memberId)),
    ):
      case ReturnBook.Error.BorrowingNotFound(book, member) =>
        NotFoundError(s"Borrowing not found for book $book and member $member")
      case ReturnBook.Error.AlreadyReturned(book, member) =>
        ValidationError(s"Book $book was already returned by member $member")

  def getBorrowings(): IO[GetBorrowingsOutput] =
    repository.all.map(borrowings => GetBorrowingsOutput(borrowings.map(toBorrowingItem)))

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
