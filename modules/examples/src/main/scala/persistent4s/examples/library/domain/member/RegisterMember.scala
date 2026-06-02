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

import io.circe.{Decoder, Encoder}
import persistent4s.{CommandHandler, Tag}
import persistent4s.examples.library.domain.{LibraryEvent, MemberRegistered}
import java.util.UUID

final case class RegisterMember(
  memberId: UUID,
  name: String,
  email: String,
)

final case class RegisterMemberState(exists: Boolean) derives Encoder, Decoder

object RegisterMemberHandler extends CommandHandler[RegisterMember, RegisterMemberState, LibraryEvent]:

  def tags(command: RegisterMember): Set[Tag] =
    Set(Tag("member", command.memberId))

  def initial: RegisterMemberState =
    RegisterMemberState(exists = false)

  def evolve(command: RegisterMember, state: RegisterMemberState, event: LibraryEvent): RegisterMemberState =
    event match
      case _: MemberRegistered => state.copy(exists = true)
      case _                   => state

  def validate(state: RegisterMemberState, command: RegisterMember): Either[Throwable, Unit] =
    if (state.exists) Left(new Exception("Member already registered")) else Right(())

  def decide(state: RegisterMemberState, command: RegisterMember): List[(Set[Tag], LibraryEvent)] =
    List(
      (
        Set(Tag("member", command.memberId)),
        MemberRegistered(command.memberId, command.name, command.email),
      ),
    )
