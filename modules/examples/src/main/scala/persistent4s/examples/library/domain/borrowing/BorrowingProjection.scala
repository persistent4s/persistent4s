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

import cats.effect.IO

import persistent4s.*
import persistent4s.examples.library.domain.{LibraryEvent, LibraryScopes}

final case class BorrowingState(
  bookId: UUID,
  memberId: UUID,
  borrowedAt: OffsetDateTime,
  dueDate: OffsetDateTime,
  returnedAt: Option[OffsetDateTime],
)

final class BorrowingProjection(
  protected val repository: BorrowingRepository,
) extends ExactlyOnceEventSourcedProjection[IO, LibraryEvent, (UUID, UUID), BorrowingState]:

  override val name: String = "borrowing-projection"

  override protected val eventHandlers = handlersBy(LibraryScopes.Book, LibraryScopes.Member):
    on[BookBorrowed]
      .reject:
        case (Some(state), event) if state.returnedAt.isEmpty =>
          new RuntimeException(s"Book ${event.bookId} is already borrowed by member ${event.memberId}")
      .set: event =>
        BorrowingState(event.bookId, event.memberId, event.borrowedAt, event.dueDate, None)

    on[BookReturned]
      .reject:
        case (None, event) => new RuntimeException(s"Cannot return non-borrowed book ${event.bookId}")
      .update: (state, event) =>
        state.copy(returnedAt = Some(event.returnedAt))
