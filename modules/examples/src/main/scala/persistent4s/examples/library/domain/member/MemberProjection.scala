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
import persistent4s.examples.library.application.Repository
import persistent4s.examples.library.domain.{LibraryEvent, MemberRegistered, BookBorrowed, BookReturned}
import java.util.UUID

final case class MemberState(
  memberId: UUID,
  name: String,
  email: String,
  borrowedBooks: Int,
)

final class MemberProjection[F[_]: Async] private (
  repository: Repository[F, UUID, MemberState],
) extends Projection[F, LibraryEvent, UUID]:

  type State = MemberState

  override val name: String = "member-projection"

  override val filter: EventFilter = EventFilter(
    eventTypes = Set(EventTypeName.of[MemberRegistered], EventTypeName.of[BookBorrowed], EventTypeName.of[BookReturned]),
  )

  override def resolveKeys(event: EventEnvelope[LibraryEvent]): List[UUID] = event.payload match
    case MemberRegistered(memberId, _, _) => List(memberId)
    case BookBorrowed(_, memberId, _, _)  => List(memberId)
    case BookReturned(_, memberId, _)     => List(memberId)
    case _                                => Nil

  override def fetchStates(keys: List[UUID]): F[Map[UUID, Option[MemberState]]] = repository.findMany(keys)

  override def handle(state: Option[MemberState], event: EventEnvelope[LibraryEvent]): F[Option[MemberState]] =
    (state, event.payload) match
      case (None, MemberRegistered(memberId, name, email)) =>
        MemberState(memberId, name, email, borrowedBooks = 0).some.pure[F]
      case (Some(s), BookBorrowed(_, memberId, _, _)) =>
        Some(s.copy(borrowedBooks = s.borrowedBooks + 1)).pure[F]
      case (Some(s), BookReturned(_, memberId, _)) =>
        Some(s.copy(borrowedBooks = s.borrowedBooks - 1)).pure[F]
      case _ => Async[F].raiseError(new RuntimeException(s"Unexpected event: ${event.payload} for state: $state"))

  override def persist(key: UUID, state: Option[MemberState]): F[Unit] = state match
    case Some(memberState) => repository.save(key, memberState)
    case None              => repository.delete(key)

object MemberProjection:

  def make[F[_]: Async](repository: Repository[F, UUID, MemberState]): F[MemberProjection[F]] =
    Async[F].pure(new MemberProjection(repository))
