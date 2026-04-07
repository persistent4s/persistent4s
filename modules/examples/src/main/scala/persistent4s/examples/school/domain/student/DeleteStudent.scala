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

final case class DeleteStudent(studentId: String)

final case class DeleteStudentState(exists: Boolean)

object DeleteStudentHandler extends CommandHandler[DeleteStudent, DeleteStudentState, SchoolEvent]:

  def tags(command: DeleteStudent): Set[Tag] =
    Set(Tag("student", command.studentId))

  def initial: DeleteStudentState =
    DeleteStudentState(exists = false)

  def evolve(state: DeleteStudentState, event: SchoolEvent): DeleteStudentState =
    event match
      case _: StudentCreated => state.copy(exists = true)
      case _                 => state

  def validate[F[_]: MonadThrow](state: DeleteStudentState, command: DeleteStudent): F[Unit] =
    MonadThrow[F].raiseError(new Exception("Student does not exist")).whenA(!state.exists)

  def decide(state: DeleteStudentState, command: DeleteStudent): List[(Set[Tag], SchoolEvent)] =
    List(
      (Set(Tag("student", command.studentId)), StudentDeleted(command.studentId)),
    )
