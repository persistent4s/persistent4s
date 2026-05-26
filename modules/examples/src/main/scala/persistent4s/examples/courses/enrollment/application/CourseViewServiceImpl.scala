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

package persistent4s.examples.courses.enrollment.application

import java.util.UUID

import cats.effect.IO

import persistent4s.examples.courses.enrollment.api.*
import persistent4s.examples.courses.enrollment.domain.course.{CourseState, CourseRepository}

class CourseViewServiceImpl(repository: CourseRepository[IO]) extends CourseViewService[IO]:

  def getCourseView(): IO[GetCourseViewOutput] =
    repository.findAll.map(cs => GetCourseViewOutput(cs.map(toItem)))

  def getCourseViewItem(courseId: String): IO[GetCourseViewItemOutput] =
    repository.find(UUID.fromString(courseId)).flatMap {
      case Some(c) => IO.pure(GetCourseViewItemOutput(toItem(c)))
      case None    => IO.raiseError(NotFoundError(s"Course not in local view: $courseId"))
    }

  private def toItem(c: CourseState): CourseViewItem =
    CourseViewItem(
      courseId = c.courseId.toString, code = c.code, title = c.title, capacity = c.capacity, instructor = c.instructor,
      isOpen = c.isOpen,
    )
