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

import persistent4s.EventStore
import persistent4s.examples.courses.enrollment.api.*
import persistent4s.examples.courses.enrollment.domain.EnrollmentEvent
import persistent4s.examples.courses.enrollment.domain.student.*

class StudentServiceImpl(repository: StudentRepository[IO])(using EventStore[IO, EnrollmentEvent])
    extends StudentService[IO]:

  def registerStudent(name: String, email: String): IO[RegisterStudentOutput] =
    (for
      studentId <- IO(UUID.randomUUID())
      _         <- RegisterStudentHandler.run[IO](RegisterStudent(studentId, name, email))
    yield RegisterStudentOutput(studentId.toString)).adaptError { case e => ValidationError(e.getMessage) }

  def getStudents(): IO[GetStudentsOutput] =
    repository.getStudents.map(ss => GetStudentsOutput(ss.map(toItem)))

  def getStudent(studentId: String): IO[GetStudentOutput] =
    repository.find(UUID.fromString(studentId)).flatMap {
      case Some(s) => IO.pure(GetStudentOutput(toItem(s)))
      case None    => IO.raiseError(NotFoundError(s"Student not found: $studentId"))
    }

  private def toItem(s: StudentState): StudentItem =
    StudentItem(s.studentId.toString, s.name, s.email)
