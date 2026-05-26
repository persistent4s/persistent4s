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

package persistent4s.examples.courses.enrollment.domain.enrollment

import java.time.OffsetDateTime
import java.util.UUID

import persistent4s.*
import persistent4s.examples.courses.enrollment.domain.*

final case class EnrollStudent(
  studentId: UUID,
  courseId: UUID,
)

final case class EnrollStudentState(
  studentRegistered: Boolean,
  activeForThisStudent: Boolean,
  courseExists: Boolean,
  courseCapacity: Int,
  nbEnrollments: Int,
)

object EnrollStudentHandler extends CommandHandler[EnrollStudent, EnrollStudentState, SchoolEvent]:

  override def tags(command: EnrollStudent): Set[Tag] =
    Set(Tag("student", command.studentId), Tag("course", command.courseId))

  override def initial: EnrollStudentState = EnrollStudentState(
    studentRegistered = false, activeForThisStudent = false, courseExists = false, courseCapacity = 0, nbEnrollments = 0,
  )

  override def evolve(command: EnrollStudent, state: EnrollStudentState, event: SchoolEvent): EnrollStudentState =
    event match
      case StudentRegistered(studentId, _, _)                                                                        => state.copy(studentRegistered = true)
      case StudentEnrolled(studentId, courseId, _) if command.studentId == studentId && command.courseId == courseId =>
        state.copy(activeForThisStudent = true, nbEnrollments = state.nbEnrollments + 1)
      case StudentEnrolled(studentId, courseId, _) if command.courseId == courseId && command.studentId != studentId =>
        state.copy(nbEnrollments = state.nbEnrollments + 1)
      case StudentDropped(studentId, courseId, _) if command.studentId == studentId && command.courseId == courseId =>
        state.copy(activeForThisStudent = false, nbEnrollments = state.nbEnrollments - 1)
      case StudentDropped(studentId, courseId, _) if command.courseId == courseId && command.studentId != studentId =>
        state.copy(nbEnrollments = state.nbEnrollments - 1)
      case CourseOpened(courseId, _, _, capacity, _) =>
        state.copy(courseExists = true, courseCapacity = capacity)
      case CapacityChanged(courseId, newCapacity) =>
        state.copy(courseCapacity = newCapacity)
      case CourseClosed(courseId) =>
        state.copy(courseExists = false, courseCapacity = 0)
      case _ => state

  override def validate(state: EnrollStudentState, command: EnrollStudent): Either[Throwable, Unit] =
    if !state.studentRegistered then Left(new Exception(s"Student not registered: ${command.studentId}"))
    else if !state.courseExists then Left(new Exception(s"Course not found: ${command.courseId}"))
    else if state.activeForThisStudent then
      Left(new Exception(s"Student ${command.studentId} already enrolled in course ${command.courseId}"))
    else if state.nbEnrollments >= state.courseCapacity then Left(new Exception(s"Course ${command.courseId} is full"))
    else Right(())

  override def decide(state: EnrollStudentState, command: EnrollStudent): List[(Set[Tag], SchoolEvent)] =
    List(
      (
        Set(Tag("student", command.studentId), Tag("course", command.courseId)),
        StudentEnrolled(command.studentId, command.courseId, OffsetDateTime.now()),
      ),
    )
