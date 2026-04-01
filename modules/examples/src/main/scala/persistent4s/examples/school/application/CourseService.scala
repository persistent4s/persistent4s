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

package persistent4s.examples.school.application

import java.util.UUID

import cats.effect.IO

import persistent4s.EventStore
import persistent4s.examples.school.api.{CourseItem, CourseService, CreateCourseOutput, GetCoursesOutput}
import persistent4s.examples.school.domain.SchoolEvent
import persistent4s.examples.school.domain.course.*
import persistent4s.examples.school.infrastructure.SchoolModule

class CourseServiceImpl(module: SchoolModule) extends CourseService[IO]:

  private given EventStore[IO, SchoolEvent] = module.store

  def createCourse(title: String, capacity: Int): IO[CreateCourseOutput] =
    for
      courseId <- IO(UUID.randomUUID().toString)
      _        <- CreateCourseHandler.run[IO](CreateCourse(courseId, title, capacity))
    yield CreateCourseOutput(courseId)

  def getCourses(): IO[GetCoursesOutput] =
    module.courseProjection.getCourses.map(courses =>
      GetCoursesOutput(courses.map(c => CourseItem(c.courseId, c.title, c.capacity, c.enrolledCount))),
    )
