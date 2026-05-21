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

package persistent4s.examples.courses.enrollment.domain.enrollment

import java.time.OffsetDateTime
import java.util.UUID

import cats.effect.Async
import cats.syntax.all.*

import persistent4s.*
import persistent4s.examples.courses.enrollment.domain.{
  EnrollmentEvent,
  StudentDropped,
  StudentEnrolled,
  StudentRegistered,
}
import persistent4s.examples.courses.enrollment.domain.courseview.{CourseView, CourseViewRepository}

/** DCB-style enrollment: validates a student-into-course command against a multi-tag scope.
  *
  * The handler reads events filtered by `{student:S, course:C}` and folds them into a single state covering:
  *   - whether the student is registered;
  *   - whether this (student, course) pair is currently actively enrolled;
  *   - how many other students are currently actively enrolled in the course.
  *
  * Course capacity and openness come from `course_view`, populated by [[CatalogEventConsumer]] from the Kafka topic
  * `catalog.events`. The optimistic-concurrency `append` covers the full multi-tag set, so two concurrent enrollments
  * to a near-full course cannot both succeed.
  */
final case class EnrollStudent(
  studentId: UUID,
  courseId: UUID,
)

final case class EnrollStudentState(
  studentRegistered: Boolean,
  activeForThisStudent: Boolean,
  activeOtherStudents: Int,
)

final class EnrollStudentHandler[F[_]: Async](
  eventStore: EventStore[F, EnrollmentEvent],
  courseView: CourseViewRepository[F],
  maxRetries: Int = 3,
):

  def run(command: EnrollStudent): F[Unit] = attempt(command, maxRetries)

  private def attempt(command: EnrollStudent, retriesLeft: Int): F[Unit] =
    runOnce(command).handleErrorWith {
      case _: IndexConflictException if retriesLeft > 0 => attempt(command, retriesLeft - 1)
      case other                                        => Async[F].raiseError(other)
    }

  private def runOnce(command: EnrollStudent): F[Unit] =
    val tags = Set(Tag("student", command.studentId), Tag("course", command.courseId))
    val filter = EventFilter(
      Set(
        EventTypeName.of[StudentRegistered],
        EventTypeName.of[StudentEnrolled],
        EventTypeName.of[StudentDropped],
      ),
      tags,
    )

    for
      envelopes <- eventStore.readFrom(0L, filter).compile.toList
      state      = envelopes.foldLeft(
                EnrollStudentState(studentRegistered = false, activeForThisStudent = false, activeOtherStudents = 0),
              )((s, env) => fold(command, s, env.payload))
      expectedIndex = envelopes.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
      view         <- courseView.find(command.courseId)
      _            <- validate(state, command, view) match
             case Left(e)  => Async[F].raiseError(e)
             case Right(_) => Async[F].unit
      event = StudentEnrolled(command.studentId, command.courseId, OffsetDateTime.now())
      _    <- eventStore.append(filter, expectedIndex, List((None, tags, EventTypeName.fromInstance(event), event)))
    yield ()

  private def fold(command: EnrollStudent, s: EnrollStudentState, e: EnrollmentEvent): EnrollStudentState = e match
    case StudentRegistered(command.studentId, _, _) =>
      s.copy(studentRegistered = true)

    case StudentEnrolled(command.studentId, command.courseId, _) =>
      s.copy(activeForThisStudent = true)
    case StudentDropped(command.studentId, command.courseId, _) =>
      s.copy(activeForThisStudent = false)

    case StudentEnrolled(otherStudent, command.courseId, _) if otherStudent != command.studentId =>
      s.copy(activeOtherStudents = s.activeOtherStudents + 1)
    case StudentDropped(otherStudent, command.courseId, _) if otherStudent != command.studentId =>
      s.copy(activeOtherStudents = s.activeOtherStudents - 1)

    case _ => s

  private def validate(
    state: EnrollStudentState,
    command: EnrollStudent,
    view: Option[CourseView],
  ): Either[Throwable, Unit] =
    view match
      case None =>
        Left(new Exception(s"Course not found in local view: ${command.courseId}"))
      case Some(c) if !c.isOpen =>
        Left(new Exception(s"Course is closed: ${command.courseId}"))
      case Some(_) if !state.studentRegistered =>
        Left(new Exception(s"Student not registered: ${command.studentId}"))
      case Some(_) if state.activeForThisStudent =>
        Left(new Exception(s"Student already enrolled in this course"))
      case Some(c) if state.activeOtherStudents >= c.capacity =>
        Left(new Exception(s"Course is full (capacity ${c.capacity})"))
      case Some(_) =>
        Right(())
