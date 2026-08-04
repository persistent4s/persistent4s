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

package persistent4s.examples.courses.enrollment.domain.student

import java.util.UUID

import persistent4s.{CommandHandler, EventTypeName, Tag}
import persistent4s.examples.courses.enrollment.domain.{SchoolEvent, StudentRegistered}

final case class RegisterStudent(
  studentId: UUID,
  name: String,
  email: String,
)

final case class RegisterStudentState(exists: Boolean)

object RegisterStudentHandler extends CommandHandler[RegisterStudent, RegisterStudentState, SchoolEvent]:

  override def eventTypes: Option[Set[EventTypeName]] =
    Some(Set(EventTypeName.of[StudentRegistered]))

  def tags(command: RegisterStudent): Set[Tag] =
    Set(Tag("student", command.studentId))

  def initial: RegisterStudentState = RegisterStudentState(exists = false)

  def evolve(command: RegisterStudent, state: RegisterStudentState, event: SchoolEvent): RegisterStudentState =
    event match
      case _: StudentRegistered => state.copy(exists = true)
      case _                    => state

  def validate(state: RegisterStudentState, command: RegisterStudent): Either[Throwable, Unit] =
    if state.exists then Left(new Exception(s"Student already registered: ${command.studentId}"))
    else Right(())

  def decide(state: RegisterStudentState, command: RegisterStudent): List[(Set[Tag], SchoolEvent)] =
    List(
      (
        Set(Tag("student", command.studentId)),
        StudentRegistered(command.studentId, command.name, command.email),
      ),
    )
