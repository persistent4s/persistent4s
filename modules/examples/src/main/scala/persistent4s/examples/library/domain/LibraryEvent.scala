/*
 * Copyright 2026 persistent4s
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

import io.circe.*
import io.circe.syntax.*

import java.time.OffsetDateTime

sealed trait LibraryEvent

sealed trait BookEvent extends LibraryEvent

sealed trait MemberEvent extends LibraryEvent

sealed trait BorrowingEvent extends LibraryEvent

// Book Events
final case class BookAdded(
  bookId: String,
  title: String,
  author: String,
  totalCopies: Int,
) extends BookEvent derives Encoder, Decoder

// Member Events
final case class MemberRegistered(
  memberId: String,
  name: String,
  email: String,
) extends MemberEvent derives Encoder, Decoder

// Borrowing Events
final case class BookBorrowed(
  bookId: String,
  memberId: String,
  borrowedAt: OffsetDateTime,
  dueDate: OffsetDateTime,
) extends BorrowingEvent derives Encoder, Decoder

final case class BookReturned(
  bookId: String,
  memberId: String,
  returnedAt: OffsetDateTime,
) extends BorrowingEvent derives Encoder, Decoder

object LibraryEvent:

  def encoder(event: LibraryEvent): Json =
    event match
      case e: BookAdded        => e.asJson
      case e: MemberRegistered => e.asJson
      case e: BookBorrowed     => e.asJson
      case e: BookReturned     => e.asJson

  def decoder(eventType: String, json: Json): Either[DecodingFailure, LibraryEvent] =
    eventType match
      case "BookAdded"        => json.as[BookAdded]
      case "MemberRegistered" => json.as[MemberRegistered]
      case "BookBorrowed"     => json.as[BookBorrowed]
      case "BookReturned"     => json.as[BookReturned]
      case other              => Left(DecodingFailure(s"Unknown event type: $other", Nil))
