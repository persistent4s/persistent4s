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

package persistent4s.examples.library.domain.book

import cats.MonadThrow
import cats.syntax.all.*

import persistent4s.{CommandHandler, Tag}
import persistent4s.examples.library.domain.{BookAdded, LibraryEvent}
import java.util.UUID

final case class AddBook(
  bookId: UUID,
  title: String,
  author: String,
  totalCopies: Int,
)

final case class AddBookState(exists: Boolean)

object AddBookHandler extends CommandHandler[AddBook, AddBookState, LibraryEvent]:

  def tags(command: AddBook): Set[Tag] =
    Set(Tag("book", command.bookId))

  def initial: AddBookState =
    AddBookState(exists = false)

  override def evolve(command: AddBook, state: AddBookState, event: LibraryEvent): AddBookState =
    event match
      case _: BookAdded => state.copy(exists = true)
      case _            => state

  def validate[F[_]: MonadThrow](state: AddBookState, command: AddBook): F[Unit] =
    MonadThrow[F].raiseError(new Exception("Book already exists")).whenA(state.exists)

  def decide(state: AddBookState, command: AddBook): List[(Set[Tag], LibraryEvent)] =
    List(
      (
        Set(Tag("book", command.bookId)),
        BookAdded(command.bookId, command.title, command.author, command.totalCopies),
      ),
    )
