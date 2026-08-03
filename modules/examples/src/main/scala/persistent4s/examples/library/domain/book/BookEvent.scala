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

import io.circe.{Decoder, Encoder}

import persistent4s.EventSchema
import persistent4s.examples.library.domain.{LibraryEvent, LibraryScopes}

sealed trait BookEvent extends LibraryEvent

final case class BookAdded(
  bookId: UUID,
  title: String,
  author: String,
  totalCopies: Int,
) extends BookEvent derives Encoder, Decoder

object BookAdded:

  given EventSchema[BookAdded] =
    EventSchema[BookAdded]("library.book-added")
      .withAlias("BookAdded")
      .scopedBy(LibraryScopes.Book)(_.bookId)
