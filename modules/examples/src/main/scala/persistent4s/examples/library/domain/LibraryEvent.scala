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

package persistent4s.examples.library.domain

import io.circe.{Decoder, Encoder}

import java.time.OffsetDateTime
import java.util.UUID

import persistent4s.Event

sealed trait LibraryEvent extends Event

sealed trait BookEvent extends LibraryEvent

sealed trait MemberEvent extends LibraryEvent

sealed trait BorrowingEvent extends LibraryEvent

// Book Events
final case class BookAdded(
  bookId: UUID,
  title: String,
  author: String,
  totalCopies: Int,
) extends BookEvent derives Encoder, Decoder

// Member Events
final case class MemberRegistered(
  memberId: UUID,
  name: String,
  email: String,
) extends MemberEvent derives Encoder, Decoder

// Borrowing Events
final case class BookBorrowed(
  bookId: UUID,
  memberId: UUID,
  borrowedAt: OffsetDateTime,
  dueDate: OffsetDateTime,
) extends BorrowingEvent derives Encoder, Decoder

final case class BookReturned(
  bookId: UUID,
  memberId: UUID,
  returnedAt: OffsetDateTime,
) extends BorrowingEvent derives Encoder, Decoder
