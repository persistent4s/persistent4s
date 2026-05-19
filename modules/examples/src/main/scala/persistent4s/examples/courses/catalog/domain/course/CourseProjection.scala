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

package persistent4s.examples.courses.catalog.domain.course

import java.util.UUID

import cats.effect.*
import cats.syntax.all.*

import persistent4s.*
import persistent4s.examples.courses.catalog.domain.{CapacityChanged, CatalogEvent, CourseClosed, CourseOpened}

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
) extends Projection[F, CatalogEvent, UUID, CourseState]:

  override val name: String = "course-projection"

  override val filter: Set[EventTypeName] = Set(
    EventTypeName.of[CourseOpened],
    EventTypeName.of[CapacityChanged],
    EventTypeName.of[CourseClosed],
  )

  override def resolveKeys(event: EventEnvelope[CatalogEvent]): List[UUID] = event.payload match
    case CourseOpened(id, _, _, _, _) => List(id)
    case CapacityChanged(id, _)       => List(id)
    case CourseClosed(id)             => List(id)

  override def handle(state: Option[CourseState], event: EventEnvelope[CatalogEvent]): F[Option[CourseState]] =
    (state, event.payload) match
      case (None, CourseOpened(id, code, title, capacity, instructor)) =>
        CourseState(id, code, title, capacity, instructor, isOpen = true).some.pure[F]
      case (Some(s), CapacityChanged(_, newCapacity)) =>
        Some(s.copy(capacity = newCapacity)).pure[F]
      case (Some(s), CourseClosed(_)) =>
        Some(s.copy(isOpen = false)).pure[F]
      case _ =>
        Async[F].raiseError(new RuntimeException(s"Unexpected event: ${event.payload} for state: $state"))

object CourseProjection:

  def make[F[_]: Async](repository: Repository[F, UUID, CourseState]): F[CourseProjection[F]] =
    Async[F].pure(new CourseProjection(repository))
