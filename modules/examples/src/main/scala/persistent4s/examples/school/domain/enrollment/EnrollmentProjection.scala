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

package persistent4s.examples.school.domain.enrollment

import persistent4s.*
import cats.effect.*
import cats.syntax.all.*
import persistent4s.examples.school.domain.SchoolEvent

final class EnrollmentProjection[F[_]: Async] private (
  state: Ref[F, Set[(String, String)]],
) extends Projection[F, SchoolEvent]:

  val name: String = "enrollment-projection"

  val filter: EventFilter = EventFilter(
    eventTypes = Set("StudentEnrolled"),
  )

  def handle(event: EventEnvelope[SchoolEvent]): F[Unit] =
    event.payload match
      case StudentEnrolled(studentId, courseId) =>
        state.update(_ + ((courseId, studentId)))
      case _ => Async[F].unit

  def getEnrollments(courseId: String): F[Set[String]] =
    state.get.map(_.collect { case (`courseId`, studentId) => studentId })

object EnrollmentProjection:

  def make[F[_]: Async]: F[EnrollmentProjection[F]] =
    Ref.of[F, Set[(String, String)]](Set.empty).map(new EnrollmentProjection(_))
