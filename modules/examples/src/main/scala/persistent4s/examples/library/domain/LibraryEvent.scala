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

import persistent4s.circe.CirceEventCodec
import persistent4s.examples.library.domain.book.BookEvent
import persistent4s.examples.library.domain.borrowing.BorrowingEvent
import persistent4s.examples.library.domain.member.MemberEvent
import persistent4s.{Event, EventCodec}

trait LibraryEvent extends Event

object LibraryEvent:

  val eventCodec: EventCodec[LibraryEvent] =
    CirceEventCodec
      .builder[LibraryEvent]
      .add[BookEvent]
      .add[MemberEvent]
      .add[BorrowingEvent]
      .build
