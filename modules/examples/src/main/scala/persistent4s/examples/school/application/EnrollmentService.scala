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

package persistent4s.examples.school.application

import cats.effect.IO

import persistent4s.EventStore
import persistent4s.examples.school.api.{EnrollmentService, GetCourseEnrollmentsOutput}
import persistent4s.examples.school.domain.SchoolEvent
import persistent4s.examples.school.domain.enrollment.*
import persistent4s.examples.school.infrastructure.SchoolModule

class EnrollmentServiceImpl(module: SchoolModule) extends EnrollmentService[IO]:

  private given EventStore[IO, SchoolEvent] = module.store

  def enrollStudent(studentId: String, courseId: String): IO[Unit] =
    EnrollStudentHandler.run[IO](EnrollStudent(studentId, courseId))

  def getCourseEnrollments(courseId: String): IO[GetCourseEnrollmentsOutput] =
    module.enrollmentProjection.getEnrollments(courseId).map(ids => GetCourseEnrollmentsOutput(ids.toList))
