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

import java.time.OffsetDateTime
import java.util.UUID

import io.circe.{Decoder, Encoder}

import persistent4s.EventSourcedCommandHandler
import persistent4s.circe.given
import persistent4s.examples.library.domain.book.BookAdded
import persistent4s.examples.library.domain.member.MemberRegistered
import persistent4s.examples.library.domain.{LibraryEvent, LibraryScopes}

final case class BorrowBook(
  bookId: UUID,
  memberId: UUID,
  borrowedAt: OffsetDateTime = OffsetDateTime.now(),
)

object BorrowBook:

  final case class State(
    bookExists: Boolean = false,
    memberExists: Boolean = false,
    totalCopies: Int = 0,
    borrowedCopies: Int = 0,
    memberHasBook: Boolean = false,
  ) derives Encoder,
        Decoder

  enum Error:

    case BookNotFound(bookId: UUID)

    case MemberNotFound(memberId: UUID)

    case NoCopiesAvailable(bookId: UUID)

    case MemberAlreadyHasBook(bookId: UUID, memberId: UUID)

  object Handler extends EventSourcedCommandHandler[BorrowBook, State, LibraryEvent, Error]:

    override protected val behavior = handler(State()):
      scope(LibraryScopes.Book)(_.bookId)
      scope(LibraryScopes.Member)(_.memberId)

      snapshot("library.borrow-book")

      on[BookAdded].evolve((state, event) => state.copy(bookExists = true, totalCopies = event.totalCopies))

      on[MemberRegistered].evolve(state => state.copy(memberExists = true))

      on[BookBorrowed]
        .within(LibraryScopes.Book)
        .evolve: (state, command, event) =>
          state.copy(
            borrowedCopies = state.borrowedCopies + 1,
            memberHasBook = state.memberHasBook || event.memberId == command.memberId,
          )

      on[BookReturned]
        .within(LibraryScopes.Book)
        .evolve: (state, command, event) =>
          state.copy(
            borrowedCopies = state.borrowedCopies - 1,
            memberHasBook = state.memberHasBook && event.memberId != command.memberId,
          )

      reject:
        case (state, command) if !state.bookExists                         => Error.BookNotFound(command.bookId)
        case (state, command) if !state.memberExists                       => Error.MemberNotFound(command.memberId)
        case (state, command) if state.borrowedCopies >= state.totalCopies =>
          Error.NoCopiesAvailable(command.bookId)
        case (state, command) if state.memberHasBook =>
          Error.MemberAlreadyHasBook(command.bookId, command.memberId)

      emit: command =>
        BookBorrowed(command.bookId, command.memberId, command.borrowedAt, command.borrowedAt.plusDays(14))
