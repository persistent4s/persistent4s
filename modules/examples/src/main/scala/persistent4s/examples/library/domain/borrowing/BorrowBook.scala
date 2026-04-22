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

import java.time.temporal.ChronoUnit

import persistent4s.{CommandHandler, EventTypeName, Tag}
import persistent4s.examples.library.domain.*
import java.time.OffsetDateTime
import java.util.UUID

final case class BorrowBook(
  bookId: UUID,
  memberId: UUID,
)

final case class BorrowBookState(
  bookExists: Boolean,
  memberExists: Boolean,
  totalCopies: Int,
  borrowedCopies: Int,
  memberHasBook: Boolean,
)

object BorrowBookHandler extends CommandHandler[BorrowBook, BorrowBookState, LibraryEvent]:

  override def eventTypes: Option[Set[EventTypeName]] =
    Some(
      Set(
        EventTypeName.of[BookAdded],
        EventTypeName.of[MemberRegistered],
        EventTypeName.of[BookBorrowed],
        EventTypeName.of[BookReturned],
      ),
    )

  def tags(command: BorrowBook): Set[Tag] =
    Set(Tag("book", command.bookId), Tag("member", command.memberId))

  def initial: BorrowBookState =
    BorrowBookState(
      bookExists = false, memberExists = false, totalCopies = 0, borrowedCopies = 0, memberHasBook = false,
    )

  def evolve(command: BorrowBook, state: BorrowBookState, event: LibraryEvent): BorrowBookState =
    event match
      case BookAdded(command.bookId, _, _, totalCopies) =>
        state.copy(bookExists = true, totalCopies = totalCopies)

      case MemberRegistered(command.memberId, _, _) =>
        state.copy(memberExists = true)

      case BookBorrowed(command.bookId, command.memberId, _, _) =>
        state.copy(
          borrowedCopies = state.borrowedCopies + 1,
          memberHasBook = true,
        )
      case BookBorrowed(command.bookId, otherMemberId, _, _) if otherMemberId != command.memberId =>
        state.copy(borrowedCopies = state.borrowedCopies + 1)

      case BookReturned(command.bookId, command.memberId, _) =>
        state.copy(
          borrowedCopies = state.borrowedCopies - 1,
          memberHasBook = false,
        )
      case BookReturned(command.bookId, otherMemberId, _) if otherMemberId != command.memberId =>
        state.copy(borrowedCopies = state.borrowedCopies - 1)

      case _ => state

  def validate(state: BorrowBookState, command: BorrowBook): Either[Throwable, Unit] =
    if (!state.bookExists) Left(new Exception("Book not found"))
    else if (!state.memberExists) Left(new Exception("Member not found"))
    else if (state.borrowedCopies >= state.totalCopies) Left(new Exception("No copies available"))
    else if (state.memberHasBook) Left(new Exception("Member already has this book"))
    else Right(())

  def decide(state: BorrowBookState, command: BorrowBook): List[(Set[Tag], LibraryEvent)] =
    val now = OffsetDateTime.now()
    val dueDate = now.plus(14, ChronoUnit.DAYS)
    List(
      (
        Set(Tag("book", command.bookId), Tag("member", command.memberId)),
        BookBorrowed(command.bookId, command.memberId, now, dueDate),
      ),
    )
