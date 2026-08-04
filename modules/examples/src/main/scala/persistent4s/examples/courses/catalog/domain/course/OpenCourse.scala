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
import persistent4s.examples.courses.catalog.domain.{CatalogEvent, CourseOpened}

final case class OpenCourse(
  courseId: UUID,
  code: String,
  title: String,
  capacity: Int,
  instructor: String,
)

final case class OpenCourseState(exists: Boolean)

object OpenCourseHandler extends CommandHandler[OpenCourse, OpenCourseState, CatalogEvent]:

  override def eventTypes: Option[Set[EventTypeName]] =
    Some(Set(EventTypeName.of[CourseOpened]))

  def tags(command: OpenCourse): Set[Tag] =
    Set(Tag("course", command.courseId))

  def initial: OpenCourseState = OpenCourseState(exists = false)

  def evolve(command: OpenCourse, state: OpenCourseState, event: CatalogEvent): OpenCourseState =
    event match
      case _: CourseOpened => state.copy(exists = true)
      case _               => state

  def validate(state: OpenCourseState, command: OpenCourse): Either[Throwable, Unit] =
    if state.exists then Left(new Exception(s"Course already opened: ${command.courseId}"))
    else if command.capacity <= 0 then Left(new Exception("Capacity must be positive"))
    else Right(())

  def decide(state: OpenCourseState, command: OpenCourse): List[(Set[Tag], CatalogEvent)] =
    List(
      (
        Set(Tag("course", command.courseId)),
        CourseOpened(command.courseId, command.code, command.title, command.capacity, command.instructor),
      ),
    )
