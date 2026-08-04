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

package persistent4s.examples.courses.enrollment.domain.student

import java.util.UUID

import cats.effect.*
import cats.syntax.all.*

import persistent4s.*
import persistent4s.examples.courses.enrollment.domain.{SchoolEvent, StudentRegistered}

final case class StudentState(
  studentId: UUID,
  name: String,
  email: String,
)

final class StudentProjection[F[_]: Async] private (
  protected val repository: Repository[F, UUID, StudentState],
) extends Projection[F, SchoolEvent, UUID, StudentState]:

  override val name: String = "student-projection"

  override val filter: Set[EventTypeName] = Set(EventTypeName.of[StudentRegistered])

  override def resolveKeys(event: EventEnvelope[SchoolEvent]): List[UUID] = event.payload match
    case StudentRegistered(id, _, _) => List(id)
    case _                           => Nil

  override def handle(state: Option[StudentState], event: EventEnvelope[SchoolEvent]): F[Option[StudentState]] =
    (state, event.payload) match
      case (None, StudentRegistered(id, name, email)) =>
        StudentState(id, name, email).some.pure[F]
      case _ =>
        Async[F].raiseError(new RuntimeException(s"Unexpected event: ${event.payload} for state: $state"))

object StudentProjection:

  def make[F[_]: Async](repository: Repository[F, UUID, StudentState]): F[StudentProjection[F]] =
    Async[F].pure(new StudentProjection(repository))
