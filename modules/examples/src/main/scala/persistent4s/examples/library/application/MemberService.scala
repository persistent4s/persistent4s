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

package persistent4s.examples.library.application

import java.util.UUID

import cats.effect.IO

import persistent4s.EventStore
import persistent4s.examples.library.api.*
import persistent4s.examples.library.domain.LibraryEvent
import persistent4s.examples.library.domain.member.*
import persistent4s.examples.library.infrastructure.LibraryModule

class MemberServiceImpl(module: LibraryModule) extends MemberService[IO]:

  private given EventStore[IO, LibraryEvent] = module.store

  def registerMember(name: String, email: String): IO[RegisterMemberOutput] =
    for
      memberId <- IO(UUID.randomUUID().toString)
      _        <- RegisterMemberHandler.run[IO](RegisterMember(memberId, name, email))
    yield RegisterMemberOutput(memberId)

  def getMembers(): IO[GetMembersOutput] =
    module.memberProjection.getMembers.map(members =>
      GetMembersOutput(members.map(m => MemberItem(m.memberId, m.name, m.email, m.borrowedBooks))),
    )

  def getMember(memberId: String): IO[GetMemberOutput] =
    module.memberProjection.getMember(memberId).flatMap {
      case Some(m) => IO.pure(GetMemberOutput(MemberItem(m.memberId, m.name, m.email, m.borrowedBooks)))
      case None    => IO.raiseError(new Exception(s"Member not found: $memberId"))
    }
