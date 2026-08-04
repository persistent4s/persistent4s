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
import persistent4s.examples.library.domain.{LibraryEvent, LibraryScopes}

final case class ReturnBook(
  bookId: UUID,
  memberId: UUID,
  returnedAt: OffsetDateTime = OffsetDateTime.now(),
)

object ReturnBook:

  final case class State(
    hasBorrowing: Boolean = false,
    alreadyReturned: Boolean = false,
  ) derives Encoder,
        Decoder

  enum Error:

    case BorrowingNotFound(bookId: UUID, memberId: UUID)

    case AlreadyReturned(bookId: UUID, memberId: UUID)

  object Handler extends EventSourcedCommandHandler[ReturnBook, State, LibraryEvent, Error]:

    override protected val behavior = handler(State()):
      scope(LibraryScopes.Book)(_.bookId)
      scope(LibraryScopes.Member)(_.memberId)
      snapshot("library.return-book")

      on[BookBorrowed].withinAll.evolve(state => state.copy(hasBorrowing = true, alreadyReturned = false))

      on[BookReturned].withinAll.evolve(state => state.copy(alreadyReturned = true))

      reject:
        case (state, command) if !state.hasBorrowing =>
          Error.BorrowingNotFound(command.bookId, command.memberId)
        case (state, command) if state.alreadyReturned =>
          Error.AlreadyReturned(command.bookId, command.memberId)

      emit: command =>
        BookReturned(command.bookId, command.memberId, command.returnedAt)
