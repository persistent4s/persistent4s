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

package persistent4s.examples.school.domain.student

import cats.MonadThrow
import cats.syntax.all.*

import persistent4s.{CommandHandler, Tag}
import persistent4s.examples.school.domain.SchoolEvent

final case class CreateStudent(studentId: String, name: String, email: String)

final case class CreateStudentState(exists: Boolean)

object CreateStudentHandler extends CommandHandler[CreateStudent, CreateStudentState, SchoolEvent]:

  def tags(command: CreateStudent): Set[Tag] =
    Set(Tag("student", command.studentId))

  def initial: CreateStudentState =
    CreateStudentState(exists = false)

  def evolve(state: CreateStudentState, event: SchoolEvent): CreateStudentState =
    event match
      case _: StudentCreated => state.copy(exists = true)
      case _                 => state

  def validate[F[_]: MonadThrow](state: CreateStudentState, command: CreateStudent): F[Unit] =
    MonadThrow[F].raiseError(new Exception("Student already exists")).whenA(state.exists)

  def decide(state: CreateStudentState, command: CreateStudent): List[(Set[Tag], SchoolEvent)] =
    List(
      (Set(Tag("student", command.studentId)), StudentCreated(command.studentId, command.name, command.email)),
    )
