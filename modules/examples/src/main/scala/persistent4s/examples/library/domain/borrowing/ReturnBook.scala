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

import cats.MonadThrow
import cats.syntax.all.*

import persistent4s.{CommandHandler, EventTypeName, Tag}
import persistent4s.examples.library.domain.*
import java.time.OffsetDateTime
import java.util.UUID

final case class ReturnBook(
  bookId: UUID,
  memberId: UUID,
)

final case class ReturnBookState(
  hasBorrowing: Boolean,
  alreadyReturned: Boolean,
)

object ReturnBookHandler extends CommandHandler[ReturnBook, ReturnBookState, LibraryEvent]:

  override def eventTypes: Option[Set[EventTypeName]] =
    Some(Set(EventTypeName.of[BookBorrowed], EventTypeName.of[BookReturned]))

  def tags(command: ReturnBook): Set[Tag] =
    Set(Tag("book", command.bookId), Tag("member", command.memberId))

  def initial: ReturnBookState =
    ReturnBookState(hasBorrowing = false, alreadyReturned = false)

  def evolve(command: ReturnBook, state: ReturnBookState, event: LibraryEvent): ReturnBookState =
    event match
      case BookBorrowed(command.bookId, command.memberId, _, _) =>
        state.copy(hasBorrowing = true, alreadyReturned = false)
      case BookReturned(command.bookId, command.memberId, _) =>
        state.copy(alreadyReturned = true)
      case _ => state

  def validate[F[_]: MonadThrow](state: ReturnBookState, command: ReturnBook): F[Unit] =
    MonadThrow[F].raiseError(new Exception("Borrowing not found")).whenA(!state.hasBorrowing) *>
      MonadThrow[F].raiseError(new Exception("Book already returned")).whenA(state.alreadyReturned)

  def decide(state: ReturnBookState, command: ReturnBook): List[(Set[Tag], LibraryEvent)] =
    List(
      (
        Set(Tag("book", command.bookId), Tag("member", command.memberId)),
        BookReturned(command.bookId, command.memberId, OffsetDateTime.now()),
      ),
    )
