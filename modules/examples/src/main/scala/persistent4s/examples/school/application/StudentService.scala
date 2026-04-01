/*
 * Copyright 2026 persistent4s
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

import java.util.UUID

import cats.effect.IO

import persistent4s.EventStore
import persistent4s.examples.school.api.{CreateStudentOutput, GetStudentsOutput, StudentItem, StudentService}
import persistent4s.examples.school.domain.SchoolEvent
import persistent4s.examples.school.domain.student.*
import persistent4s.examples.school.infrastructure.SchoolModule

class StudentServiceImpl(module: SchoolModule) extends StudentService[IO]:

  private given EventStore[IO, SchoolEvent] = module.store

  def createStudent(name: String, email: String): IO[CreateStudentOutput] =
    for
      studentId <- IO(UUID.randomUUID().toString)
      _         <- CreateStudentHandler.run[IO](CreateStudent(studentId, name, email))
    yield CreateStudentOutput(studentId)

  def deleteStudent(studentId: String): IO[Unit] =
    DeleteStudentHandler.run[IO](DeleteStudent(studentId))

  def getStudents(): IO[GetStudentsOutput] =
    module.studentProjection.getStudents.map(students =>
      GetStudentsOutput(students.map(s => StudentItem(s.studentId, s.name, s.email, s.nbCourses))),
    )
