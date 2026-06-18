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

import cats.effect.*
import cats.syntax.all.*

import persistent4s.*
import persistent4s.examples.library.domain.{LibraryEvent, MemberRegistered, BookBorrowed, BookReturned}
import java.util.UUID

final case class MemberState(
  memberId: UUID,
  name: String,
  email: String,
  borrowedBooks: Int,
)

final class MemberProjection[F[_]: Async] private (
  protected val repository: Repository[F, UUID, MemberState],
) extends EventSourcedProjection[F, LibraryEvent, UUID, MemberState]:

  override val name: String = "member-projection"

  override val handlers = List(
    on[MemberRegistered](_.memberId) { (_, e) =>
      MemberState(e.memberId, e.name, e.email, borrowedBooks = 0).some
    },
    on[BookBorrowed](_.memberId) { (state, _) =>
      state.map(s => s.copy(borrowedBooks = s.borrowedBooks + 1))
    },
    on[BookReturned](_.memberId) { (state, _) =>
      state.map(s => s.copy(borrowedBooks = s.borrowedBooks - 1))
    },
  )

object MemberProjection:

  def make[F[_]: Async](repository: Repository[F, UUID, MemberState]): F[MemberProjection[F]] =
    Async[F].pure(new MemberProjection(repository))
