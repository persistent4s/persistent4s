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
import persistent4s.examples.courses.catalog.domain.{CapacityChanged, CatalogEvent, CourseClosed, CourseOpened}

final case class ChangeCapacity(
  courseId: UUID,
  newCapacity: Int,
)

final case class ChangeCapacityState(exists: Boolean, isOpen: Boolean)

object ChangeCapacityHandler extends CommandHandler[ChangeCapacity, ChangeCapacityState, CatalogEvent]:

  override def eventTypes: Option[Set[EventTypeName]] =
    Some(
      Set(
        EventTypeName.of[CourseOpened],
        EventTypeName.of[CourseClosed],
      ),
    )

  def tags(command: ChangeCapacity): Set[Tag] =
    Set(Tag("course", command.courseId))

  def initial: ChangeCapacityState = ChangeCapacityState(exists = false, isOpen = false)

  def evolve(command: ChangeCapacity, state: ChangeCapacityState, event: CatalogEvent): ChangeCapacityState =
    event match
      case _: CourseOpened => state.copy(exists = true, isOpen = true)
      case _: CourseClosed => state.copy(isOpen = false)
      case _               => state

  def validate(state: ChangeCapacityState, command: ChangeCapacity): Either[Throwable, Unit] =
    if !state.exists then Left(new Exception(s"Course not found: ${command.courseId}"))
    else if !state.isOpen then Left(new Exception(s"Course is closed: ${command.courseId}"))
    else if command.newCapacity <= 0 then Left(new Exception("Capacity must be positive"))
    else Right(())

  def decide(state: ChangeCapacityState, command: ChangeCapacity): List[(Set[Tag], CatalogEvent)] =
    List(
      (
        Set(Tag("course", command.courseId)),
        CapacityChanged(command.courseId, command.newCapacity),
      ),
    )
