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

package persistent4s.examples.school.infrastructure

import cats.effect.*

import persistent4s.examples.school.domain.SchoolEvent
import persistent4s.examples.school.domain.course.CourseProjection
import persistent4s.examples.school.domain.enrollment.EnrollmentProjection
import persistent4s.examples.school.domain.student.StudentProjection
import persistent4s.testkit.{InMemoryEventStore, InMemoryProjectionCheckpoint, InMemoryProjector}

final class SchoolModule private (
  val store: InMemoryEventStore[IO, SchoolEvent],
  val studentProjection: StudentProjection[IO],
  val courseProjection: CourseProjection[IO],
  val enrollmentProjection: EnrollmentProjection[IO],
)

object SchoolModule:

  def make: Resource[IO, SchoolModule] =
    for
      store          <- Resource.eval(InMemoryEventStore.make[IO, SchoolEvent])
      checkpoint     <- Resource.eval(InMemoryProjectionCheckpoint.make[IO])
      studentProj    <- Resource.eval(StudentProjection.make[IO])
      courseProj     <- Resource.eval(CourseProjection.make[IO])
      enrollmentProj <- Resource.eval(EnrollmentProjection.make[IO])
      projector       = InMemoryProjector[IO, SchoolEvent](store, checkpoint)
      _              <- projector.run(studentProj).compile.drain.background
      _              <- projector.run(courseProj).compile.drain.background
      _              <- projector.run(enrollmentProj).compile.drain.background
    yield new SchoolModule(store, studentProj, courseProj, enrollmentProj)
