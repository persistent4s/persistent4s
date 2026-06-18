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
import java.util.UUID

final case class BorrowingState(
  bookId: UUID,
  memberId: UUID,
  borrowedAt: OffsetDateTime,
  dueDate: OffsetDateTime,
  returnedAt: Option[OffsetDateTime],
)

final class BorrowingProjection[F[_]: Async] private (
  protected val repository: Repository[F, (UUID, UUID), BorrowingState],
) extends EventSourcedProjection[F, LibraryEvent, (UUID, UUID), BorrowingState]:

  override val name: String = "borrowing-projection"

  override val handlers = List(
    onF[BookBorrowed](e => (e.bookId, e.memberId)) { (state, e) =>
      state match
        case Some(s) if s.returnedAt.isEmpty =>
          Async[F].raiseError(new RuntimeException(s"Book ${e.bookId} is already borrowed by member ${e.memberId}"))
        case _ =>
          BorrowingState(e.bookId, e.memberId, e.borrowedAt, e.dueDate, None).some.pure[F]
    },
    onF[BookReturned](e => (e.bookId, e.memberId)) { (state, e) =>
      state match
        case Some(s) => s.copy(returnedAt = Some(e.returnedAt)).some.pure[F]
        case None    => Async[F].raiseError(new RuntimeException(s"Cannot return non-borrowed book ${e.bookId}"))
    },
  )

object BorrowingProjection:

  def make[F[_]: Async](repository: Repository[F, (UUID, UUID), BorrowingState]): F[BorrowingProjection[F]] =
    Async[F].pure(new BorrowingProjection(repository))
