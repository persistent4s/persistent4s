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

import persistent4s.EventSchema
import persistent4s.examples.library.domain.{LibraryEvent, LibraryScopes}

sealed trait BorrowingEvent extends LibraryEvent

final case class BookBorrowed(
  bookId: UUID,
  memberId: UUID,
  borrowedAt: OffsetDateTime,
  dueDate: OffsetDateTime,
) extends BorrowingEvent derives Encoder, Decoder

object BookBorrowed:

  given EventSchema[BookBorrowed] =
    EventSchema[BookBorrowed]("library.book-borrowed")
      .withAlias("BookBorrowed")
      .scopedBy(LibraryScopes.Book)(_.bookId)
      .scopedBy(LibraryScopes.Member)(_.memberId)

final case class BookReturned(
  bookId: UUID,
  memberId: UUID,
  returnedAt: OffsetDateTime,
) extends BorrowingEvent derives Encoder, Decoder

object BookReturned:

  given EventSchema[BookReturned] =
    EventSchema[BookReturned]("library.book-returned")
      .withAlias("BookReturned")
      .scopedBy(LibraryScopes.Book)(_.bookId)
      .scopedBy(LibraryScopes.Member)(_.memberId)
