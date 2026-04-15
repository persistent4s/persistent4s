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

final case class MemberView(
  memberId: String,
  name: String,
  email: String,
  borrowedBooks: Int,
)

final class MemberProjection[F[_]: Async] private (
  state: Ref[F, Map[String, MemberView]],
) extends StatelessProjection[F, LibraryEvent]:

  val name: String = "member-projection"

  val filter: EventFilter = EventFilter(
    eventTypes = Set(EventTypeName.of[MemberRegistered], EventTypeName.of[BookBorrowed], EventTypeName.of[BookReturned]),
  )

  def handle(event: EventEnvelope[LibraryEvent]): F[Unit] =
    event.payload match
      case MemberRegistered(memberId, name, email) =>
        state.update(_.updated(memberId, MemberView(memberId, name, email, 0)))
      case BookBorrowed(_, memberId, _, _) =>
        state.update(_.updatedWith(memberId) {
          case Some(view) => Some(view.copy(borrowedBooks = view.borrowedBooks + 1))
          case None       => None
        })
      case BookReturned(_, memberId, _) =>
        state.update(_.updatedWith(memberId) {
          case Some(view) => Some(view.copy(borrowedBooks = view.borrowedBooks - 1))
          case None       => None
        })
      case _ => Async[F].unit

  def getMembers: F[List[MemberView]] = state.get.map(_.values.toList)

  def getMember(memberId: String): F[Option[MemberView]] = state.get.map(_.get(memberId))

object MemberProjection:

  def make[F[_]: Async]: F[MemberProjection[F]] =
    Ref.of[F, Map[String, MemberView]](Map.empty).map(new MemberProjection(_))
