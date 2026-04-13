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

package persistent4s.examples.school.domain.course

import persistent4s.*
import persistent4s.examples.school.domain.SchoolEvent
import persistent4s.examples.school.domain.enrollment.StudentEnrolled
import cats.effect.*
import cats.syntax.all.*

final case class CourseView(courseId: String, title: String, capacity: Int, enrolledCount: Int)

final class CourseProjection[F[_]: Async] private (
  state: Ref[F, Map[String, CourseView]],
) extends Projection[F, SchoolEvent]:

  val name: String = "course-projection"

  val filter: EventFilter = EventFilter(
    eventTypes = Set(EventTypeName.of[CourseCreated], EventTypeName.of[StudentEnrolled]),
  )

  def handle(event: EventEnvelope[SchoolEvent]): F[Unit] =
    event.payload match
      case CourseCreated(courseId, title, capacity) =>
        state.update(_.updated(courseId, CourseView(courseId, title, capacity, enrolledCount = 0)))
      case StudentEnrolled(_, courseId) =>
        state.update(_.updatedWith(courseId)(_.map(c => c.copy(enrolledCount = c.enrolledCount + 1))))
      case _ => Async[F].unit

  def getCourses: F[List[CourseView]] = state.get.map(_.values.toList)

object CourseProjection:

  def make[F[_]: Async]: F[CourseProjection[F]] =
    Ref.of[F, Map[String, CourseView]](Map.empty).map(new CourseProjection(_))
