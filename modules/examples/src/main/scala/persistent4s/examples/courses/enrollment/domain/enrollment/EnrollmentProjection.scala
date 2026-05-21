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

package persistent4s.examples.courses.enrollment.domain.enrollment

import java.time.OffsetDateTime
import java.util.UUID

import cats.effect.*
import cats.syntax.all.*

import persistent4s.*
import persistent4s.examples.courses.enrollment.domain.{SchoolEvent, StudentDropped, StudentEnrolled}

final case class EnrollmentRecord(
  studentId: UUID,
  courseId: UUID,
  enrolledAt: OffsetDateTime,
  droppedAt: Option[OffsetDateTime],
)

final class EnrollmentProjection[F[_]: Async] private (
  protected val repository: Repository[F, (UUID, UUID), EnrollmentRecord],
) extends Projection[F, SchoolEvent, (UUID, UUID), EnrollmentRecord]:

  override val name: String = "enrollment-projection"

  override val filter: Set[EventTypeName] = Set(
    EventTypeName.of[StudentEnrolled],
    EventTypeName.of[StudentDropped],
  )

  override def resolveKeys(event: EventEnvelope[SchoolEvent]): List[(UUID, UUID)] = event.payload match
    case StudentEnrolled(s, c, _) => List((s, c))
    case StudentDropped(s, c, _)  => List((s, c))
    case _                        => Nil

  override def handle(
    state: Option[EnrollmentRecord],
    event: EventEnvelope[SchoolEvent],
  ): F[Option[EnrollmentRecord]] =
    (state, event.payload) match
      case (None, StudentEnrolled(s, c, t)) =>
        EnrollmentRecord(s, c, t, None).some.pure[F]
      case (Some(existing), StudentEnrolled(s, c, t)) if existing.droppedAt.nonEmpty =>
        // Re-enrollment after a drop: start a fresh active window
        EnrollmentRecord(s, c, t, None).some.pure[F]
      case (Some(existing), StudentDropped(_, _, t)) =>
        Some(existing.copy(droppedAt = Some(t))).pure[F]
      case _ =>
        Async[F].raiseError(new RuntimeException(s"Unexpected event: ${event.payload} for state: $state"))

object EnrollmentProjection:

  def make[F[_]: Async](
    repository: Repository[F, (UUID, UUID), EnrollmentRecord],
  ): F[EnrollmentProjection[F]] =
    Async[F].pure(new EnrollmentProjection(repository))
