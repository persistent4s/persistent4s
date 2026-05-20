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

import persistent4s.{CommandHandler, EventTypeName, Tag}
import persistent4s.examples.courses.enrollment.domain.{EnrollmentEvent, StudentDropped, StudentEnrolled}

final case class DropStudent(
  studentId: UUID,
  courseId: UUID,
)

final case class DropStudentState(activeEnrollment: Boolean)

object DropStudentHandler extends CommandHandler[DropStudent, DropStudentState, EnrollmentEvent]:

  override def eventTypes: Option[Set[EventTypeName]] =
    Some(
      Set(
        EventTypeName.of[StudentEnrolled],
        EventTypeName.of[StudentDropped],
      ),
    )

  def tags(command: DropStudent): Set[Tag] =
    Set(Tag("student", command.studentId), Tag("course", command.courseId))

  def initial: DropStudentState = DropStudentState(activeEnrollment = false)

  def evolve(command: DropStudent, state: DropStudentState, event: EnrollmentEvent): DropStudentState =
    event match
      case StudentEnrolled(command.studentId, command.courseId, _) =>
        state.copy(activeEnrollment = true)
      case StudentDropped(command.studentId, command.courseId, _) =>
        state.copy(activeEnrollment = false)
      case _ => state

  def validate(state: DropStudentState, command: DropStudent): Either[Throwable, Unit] =
    if !state.activeEnrollment then
      Left(new Exception(s"No active enrollment to drop for student=${command.studentId} course=${command.courseId}"))
    else Right(())

  def decide(state: DropStudentState, command: DropStudent): List[(Set[Tag], EnrollmentEvent)] =
    List(
      (
        Set(Tag("student", command.studentId), Tag("course", command.courseId)),
        StudentDropped(command.studentId, command.courseId, OffsetDateTime.now()),
      ),
    )
