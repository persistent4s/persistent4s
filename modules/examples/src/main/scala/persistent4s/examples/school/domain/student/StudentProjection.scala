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

package persistent4s.examples.school.domain.student

import persistent4s.*
import persistent4s.examples.school.domain.SchoolEvent
import persistent4s.examples.school.domain.student.{StudentCreated, StudentDeleted}
import persistent4s.examples.school.domain.enrollment.StudentEnrolled
import cats.effect.*
import cats.syntax.all.*

final case class StudentView(studentId: String, name: String, email: String, nbCourses: Int = 0)

final class StudentProjection[F[_]: Async] private (
  state: Ref[F, Map[String, StudentView]],
) extends StatelessProjection[F, SchoolEvent]:

  val name: String = "student-projection"

  val filter: EventFilter = EventFilter(
    eventTypes =
      Set(EventTypeName.of[StudentCreated], EventTypeName.of[StudentDeleted], EventTypeName.of[StudentEnrolled]),
  )

  def handle(event: EventEnvelope[SchoolEvent]): F[Unit] =
    event.payload match
      case StudentCreated(studentId, name, email) =>
        state.update(_.updated(studentId, StudentView(studentId, name, email)))
      case StudentDeleted(studentId) =>
        state.update(_.removed(studentId))
      case StudentEnrolled(studentId, courseId) =>
        state.update(_.updatedWith(studentId) {
          case Some(view) => Some(view.copy(nbCourses = view.nbCourses + 1))
          case None       => None
        })
      case _ => Async[F].unit

  def getStudents: F[List[StudentView]] = state.get.map(_.values.toList)

object StudentProjection:

  def make[F[_]: Async]: F[StudentProjection[F]] =
    Ref.of[F, Map[String, StudentView]](Map.empty).map(new StudentProjection(_))
