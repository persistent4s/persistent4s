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

package persistent4s.examples.library.domain.member

import java.util.UUID

import cats.effect.IO

import persistent4s.*
import persistent4s.examples.library.domain.borrowing.{BookBorrowed, BookReturned}
import persistent4s.examples.library.domain.{LibraryEvent, LibraryScopes}

final case class MemberState(
  memberId: UUID,
  name: String,
  email: String,
  borrowedBooks: Int,
)

final class MemberProjection(
  protected val repository: MemberRepository,
) extends ExactlyOnceEventSourcedProjection[IO, LibraryEvent, UUID, MemberState]:

  override val name: String = "member-projection"

  override protected val eventHandlers = handlersBy(LibraryScopes.Member):
    on[MemberRegistered].create: event =>
      MemberState(event.memberId, event.name, event.email, borrowedBooks = 0)

    on[BookBorrowed].update: state =>
      state.copy(borrowedBooks = state.borrowedBooks + 1)

    on[BookReturned].update: state =>
      state.copy(borrowedBooks = state.borrowedBooks - 1)

object MemberProjection:

  def apply(repository: MemberRepository): MemberProjection =
    new MemberProjection(repository)
