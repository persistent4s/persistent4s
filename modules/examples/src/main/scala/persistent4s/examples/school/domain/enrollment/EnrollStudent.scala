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

package persistent4s.examples.school.domain.enrollment

import cats.MonadThrow
import cats.syntax.all.*

import persistent4s.{CommandHandler, Tag}
import persistent4s.examples.school.domain.SchoolEvent
import persistent4s.examples.school.domain.course.CourseCreated
import persistent4s.examples.school.domain.student.StudentCreated

final case class EnrollStudent(studentId: String, courseId: String)

final case class EnrollStudentState(
  studentExists: Boolean,
  courseExists: Boolean,
  courseCapacity: Int,
  enrolledStudents: Set[String],
)

object EnrollStudentHandler extends CommandHandler[EnrollStudent, EnrollStudentState, SchoolEvent]:

  override def eventTypes: Option[Set[String]] =
    Some(Set("StudentCreated", "CourseCreated", "StudentEnrolled"))

  def tags(command: EnrollStudent): Set[Tag] =
    Set(Tag("student", command.studentId), Tag("course", command.courseId))

  def initial: EnrollStudentState =
    EnrollStudentState(studentExists = false, courseExists = false, courseCapacity = 0, enrolledStudents = Set.empty)

  def evolve(state: EnrollStudentState, event: SchoolEvent): EnrollStudentState =
    event match
      case _: StudentCreated             => state.copy(studentExists = true)
      case CourseCreated(_, _, capacity) => state.copy(courseExists = true, courseCapacity = capacity)
      case StudentEnrolled(studentId, _) => state.copy(enrolledStudents = state.enrolledStudents + studentId)
      case _                             => state

  def validate[F[_]: MonadThrow](state: EnrollStudentState, command: EnrollStudent): F[Unit] =
    MonadThrow[F].raiseError(new Exception("Student not found")).whenA(!state.studentExists) *>
      MonadThrow[F].raiseError(new Exception("Course not found")).whenA(!state.courseExists) *>
      MonadThrow[F]
        .raiseError(new Exception("Course is full"))
        .whenA(state.enrolledStudents.size >= state.courseCapacity) *>
      MonadThrow[F]
        .raiseError(new Exception("Already enrolled"))
        .whenA(state.enrolledStudents.contains(command.studentId))

  def decide(state: EnrollStudentState, command: EnrollStudent): List[(Set[Tag], SchoolEvent)] =
    List(
      (
        Set(Tag("student", command.studentId), Tag("course", command.courseId)),
        StudentEnrolled(command.studentId, command.courseId),
      ),
    )
