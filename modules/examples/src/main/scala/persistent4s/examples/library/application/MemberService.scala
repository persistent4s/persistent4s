/*
 * Copyright 2026 Bastien Jolidon
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

package persistent4s.examples.library.application

import java.util.UUID

import cats.effect.IO

import persistent4s.EventStore
import persistent4s.examples.library.api.*
import persistent4s.examples.library.domain.LibraryEvent
import persistent4s.examples.library.domain.member.*

class MemberServiceImpl(repository: MemberRepository[IO])(using EventStore[IO, LibraryEvent]) extends MemberService[IO]:

  def registerMember(name: String, email: String): IO[RegisterMemberOutput] =
    (for
      memberId <- IO(UUID.randomUUID())
      _        <- RegisterMemberHandler.run[IO](RegisterMember(memberId, name, email))
    yield RegisterMemberOutput(memberId.toString())).adaptError { case e => ValidationError(e.getMessage) }

  def getMembers(): IO[GetMembersOutput] =
    repository.getMembers
      .map(members =>
        GetMembersOutput(members.map(m => MemberItem(m.memberId.toString(), m.name, m.email, m.borrowedBooks))),
      )

  def getMember(memberId: String): IO[GetMemberOutput] =
    repository.find(UUID.fromString(memberId)).flatMap {
      case Some(m) => IO.pure(GetMemberOutput(MemberItem(m.memberId.toString(), m.name, m.email, m.borrowedBooks)))
      case None    => IO.raiseError(NotFoundError(s"Member not found: $memberId"))
    }
