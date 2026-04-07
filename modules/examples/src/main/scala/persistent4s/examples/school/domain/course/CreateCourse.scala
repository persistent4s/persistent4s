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

package persistent4s.examples.school.domain.course

import cats.MonadThrow
import cats.syntax.all.*

import persistent4s.{CommandHandler, Tag}
import persistent4s.examples.school.domain.SchoolEvent

final case class CreateCourse(courseId: String, title: String, capacity: Int)

final case class CreateCourseState(exists: Boolean)

object CreateCourseHandler extends CommandHandler[CreateCourse, CreateCourseState, SchoolEvent]:

  def tags(command: CreateCourse): Set[Tag] =
    Set(Tag("course", command.courseId))

  def initial: CreateCourseState =
    CreateCourseState(exists = false)

  def evolve(state: CreateCourseState, event: SchoolEvent): CreateCourseState =
    event match
      case _: CourseCreated => state.copy(exists = true)
      case _                => state

  def validate[F[_]: MonadThrow](state: CreateCourseState, command: CreateCourse): F[Unit] =
    MonadThrow[F].raiseError(new Exception("Course already exists")).whenA(state.exists)

  def decide(state: CreateCourseState, command: CreateCourse): List[(Set[Tag], SchoolEvent)] =
    List(
      (Set(Tag("course", command.courseId)), CourseCreated(command.courseId, command.title, command.capacity)),
    )
