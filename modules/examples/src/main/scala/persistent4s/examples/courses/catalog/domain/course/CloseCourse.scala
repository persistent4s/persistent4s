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

package persistent4s.examples.courses.catalog.domain.course

import java.util.UUID

import persistent4s.{CommandHandler, EventTypeName, Tag}
import persistent4s.examples.courses.catalog.domain.{CatalogEvent, CourseClosed, CourseOpened}

final case class CloseCourse(courseId: UUID)

final case class CloseCourseState(exists: Boolean, isOpen: Boolean)

object CloseCourseHandler extends CommandHandler[CloseCourse, CloseCourseState, CatalogEvent]:

  override def eventTypes: Option[Set[EventTypeName]] =
    Some(
      Set(
        EventTypeName.of[CourseOpened],
        EventTypeName.of[CourseClosed],
      ),
    )

  def tags(command: CloseCourse): Set[Tag] =
    Set(Tag("course", command.courseId))

  def initial: CloseCourseState = CloseCourseState(exists = false, isOpen = false)

  def evolve(command: CloseCourse, state: CloseCourseState, event: CatalogEvent): CloseCourseState =
    event match
      case _: CourseOpened => state.copy(exists = true, isOpen = true)
      case _: CourseClosed => state.copy(isOpen = false)
      case _               => state

  def validate(state: CloseCourseState, command: CloseCourse): Either[Throwable, Unit] =
    if !state.exists then Left(new Exception(s"Course not found: ${command.courseId}"))
    else if !state.isOpen then Left(new Exception(s"Course already closed: ${command.courseId}"))
    else Right(())

  def decide(state: CloseCourseState, command: CloseCourse): List[(Set[Tag], CatalogEvent)] =
    List(
      (
        Set(Tag("course", command.courseId)),
        CourseClosed(command.courseId),
      ),
    )
