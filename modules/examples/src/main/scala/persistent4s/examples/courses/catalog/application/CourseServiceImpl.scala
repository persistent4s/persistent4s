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

package persistent4s.examples.courses.catalog.application

import java.util.UUID

import cats.effect.IO

import persistent4s.EventStore
import persistent4s.examples.courses.catalog.api.*
import persistent4s.examples.courses.catalog.domain.CatalogEvent
import persistent4s.examples.courses.catalog.domain.course.*

class CourseServiceImpl(repository: CourseRepository[IO])(using EventStore[IO, CatalogEvent])
    extends CourseService[IO]:

  def openCourse(code: String, title: String, capacity: Int, instructor: String): IO[OpenCourseOutput] =
    (for
      courseId <- IO(UUID.randomUUID())
      _        <- OpenCourseHandler.run[IO](OpenCourse(courseId, code, title, capacity, instructor))
    yield OpenCourseOutput(courseId.toString)).adaptError { case e => ValidationError(e.getMessage) }

  def changeCapacity(courseId: String, newCapacity: Int): IO[Unit] =
    ChangeCapacityHandler
      .run[IO](ChangeCapacity(UUID.fromString(courseId), newCapacity))
      .adaptError {
        case e if e.getMessage.contains("not found") => NotFoundError(e.getMessage)
        case e                                       => ValidationError(e.getMessage)
      }

  def closeCourse(courseId: String): IO[Unit] =
    CloseCourseHandler
      .run[IO](CloseCourse(UUID.fromString(courseId)))
      .adaptError {
        case e if e.getMessage.contains("not found") => NotFoundError(e.getMessage)
        case e                                       => ValidationError(e.getMessage)
      }

  def getCourses(): IO[GetCoursesOutput] =
    repository.getCourses.map(courses =>
      GetCoursesOutput(courses.map(toCourseItem)),
    )

  def getCourse(courseId: String): IO[GetCourseOutput] =
    repository.find(UUID.fromString(courseId)).flatMap {
      case Some(c) => IO.pure(GetCourseOutput(toCourseItem(c)))
      case None    => IO.raiseError(NotFoundError(s"Course not found: $courseId"))
    }

  private def toCourseItem(c: CourseState): CourseItem =
    CourseItem(
      courseId = c.courseId.toString,
      code = c.code,
      title = c.title,
      capacity = c.capacity,
      instructor = c.instructor,
      isOpen = c.isOpen,
    )
