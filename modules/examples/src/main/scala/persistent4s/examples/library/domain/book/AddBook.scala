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

import java.util.UUID

import persistent4s.EventSourcedCommandHandler
import persistent4s.examples.library.domain.{LibraryEvent, LibraryScopes}

final case class AddBook(
  bookId: UUID,
  title: String,
  author: String,
  totalCopies: Int,
)

object AddBook:

  enum Error:

    case AlreadyExists(bookId: UUID)

  object Handler extends EventSourcedCommandHandler[AddBook, Boolean, LibraryEvent, Error]:

    override protected val behavior = handler(initial = false):
      scope(LibraryScopes.Book)(_.bookId)

      on[BookAdded].evolve(_ => true)

      reject:
        case (true, command) => Error.AlreadyExists(command.bookId)

      emit: command =>
        BookAdded(command.bookId, command.title, command.author, command.totalCopies)
