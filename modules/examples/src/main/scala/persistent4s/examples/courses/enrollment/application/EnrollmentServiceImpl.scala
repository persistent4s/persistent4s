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

package persistent4s.examples.courses.enrollment.application

import java.util.UUID

import cats.effect.IO

import org.typelevel.otel4s.trace.Tracer

import persistent4s.{CommandHandlerMetrics, EventStore}
import persistent4s.examples.courses.enrollment.api.*
import persistent4s.examples.courses.enrollment.domain.SchoolEvent
import persistent4s.examples.courses.enrollment.domain.enrollment.*
import smithy4s.time.Timestamp

class EnrollmentServiceImpl(
  repository: EnrollmentRepository[IO],
)(using EventStore[IO, SchoolEvent], CommandHandlerMetrics[IO], Tracer[IO])
    extends EnrollmentService[IO]:

  def enrollStudent(studentId: String, courseId: String): IO[Unit] =
    EnrollStudentHandler
      .run[IO](EnrollStudent(UUID.fromString(studentId), UUID.fromString(courseId)))
      .adaptError {
        case e if e.getMessage.contains("not found") || e.getMessage.contains("not registered") =>
          NotFoundError(e.getMessage)
        case e => ValidationError(e.getMessage)
      }
      .void

  def dropStudent(studentId: String, courseId: String): IO[Unit] =
    DropStudentHandler
      .run[IO](DropStudent(UUID.fromString(studentId), UUID.fromString(courseId)))
      .adaptError {
        case e if e.getMessage.contains("No active enrollment") => NotFoundError(e.getMessage)
        case e                                                  => ValidationError(e.getMessage)
      }
      .void

  def getEnrollments(): IO[GetEnrollmentsOutput] =
    repository.getEnrollments.map(rs => GetEnrollmentsOutput(rs.map(toItem)))

  def getStudentEnrollments(studentId: String): IO[GetStudentEnrollmentsOutput] =
    repository.getByStudent(UUID.fromString(studentId)).map(rs => GetStudentEnrollmentsOutput(rs.map(toItem)))

  def getCourseEnrollments(courseId: String): IO[GetCourseEnrollmentsOutput] =
    repository.getByCourse(UUID.fromString(courseId)).map(rs => GetCourseEnrollmentsOutput(rs.map(toItem)))

  private def toItem(r: EnrollmentRecord): EnrollmentItem =
    EnrollmentItem(
      studentId = r.studentId.toString,
      courseId = r.courseId.toString,
      enrolledAt = Timestamp.fromInstant(r.enrolledAt.toInstant),
      droppedAt = r.droppedAt.map(d => Timestamp.fromInstant(d.toInstant)),
    )
