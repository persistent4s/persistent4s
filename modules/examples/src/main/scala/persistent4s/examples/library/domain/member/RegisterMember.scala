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

import persistent4s.EventSourcedCommandHandler
import persistent4s.examples.library.domain.{LibraryEvent, LibraryScopes}

final case class RegisterMember(
  memberId: UUID,
  name: String,
  email: String,
)

object RegisterMember:

  enum Error:

    case AlreadyRegistered(memberId: UUID)

  object Handler extends EventSourcedCommandHandler[RegisterMember, Boolean, LibraryEvent, Error]:

    override protected val behavior = handler(initial = false):
      scope(LibraryScopes.Member)(_.memberId)

      on[MemberRegistered].evolve(_ => true)

      reject:
        case (true, command) => Error.AlreadyRegistered(command.memberId)

      emit: command =>
        MemberRegistered(command.memberId, command.name, command.email)
