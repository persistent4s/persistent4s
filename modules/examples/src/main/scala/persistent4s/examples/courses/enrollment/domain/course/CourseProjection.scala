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

package persistent4s.examples.courses.enrollment.domain.course

import cats.effect.*
import cats.syntax.all.*

import java.util.UUID
import persistent4s.Repository
import persistent4s.Projection
import persistent4s.examples.courses.enrollment.domain.*
import persistent4s.EventTypeName
import persistent4s.EventEnvelope

final case class CourseState(
  courseId: UUID,
  code: String,
  title: String,
  capacity: Int,
  instructor: String,
  isOpen: Boolean,
)

final class CourseProjection[F[_]: Async] private (
  protected val repository: Repository[F, UUID, CourseState],
) extends Projection[F, SchoolEvent, UUID, CourseState]:

  override val name: String = "course-projection"

  override val filter: Set[EventTypeName] =
    Set(EventTypeName.of[CourseOpened], EventTypeName.of[CapacityChanged], EventTypeName.of[CourseClosed])

  override def resolveKeys(event: EventEnvelope[SchoolEvent]): List[UUID] = event.payload match
    case CourseOpened(courseId, code, title, capacity, instructor) => List(courseId)
    case CourseClosed(courseId)                                    => List(courseId)
    case CapacityChanged(courseId, newCapacity)                    => List(courseId)
    case _                                                         => Nil

  override def handle(state: Option[CourseState], event: EventEnvelope[SchoolEvent]): F[Option[CourseState]] =
    (state, event.payload) match
      case (None, CourseOpened(courseId, code, title, capacity, instructor)) =>
        CourseState(courseId, code, title, capacity, instructor, true).some.pure[F]
      case (Some(s), CourseClosed(courseId)) =>
        s.copy(isOpen = false).some.pure[F]
      case (Some(s), CapacityChanged(courseId, newCapacity)) =>
        s.copy(capacity = newCapacity).some.pure[F]
      case _ =>
        Async[F].raiseError(new RuntimeException(s"Unexpected event: ${event.payload} for state: $state"))

object CourseProjection:

  def make[F[_]: Async](repository: Repository[F, UUID, CourseState]): F[CourseProjection[F]] =
    Async[F].pure(new CourseProjection(repository))
