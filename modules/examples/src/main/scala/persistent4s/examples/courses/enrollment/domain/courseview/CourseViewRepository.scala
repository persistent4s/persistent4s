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

package persistent4s.examples.courses.enrollment.domain.courseview

import java.util.UUID

import cats.effect.*
import cats.syntax.all.*
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

final class CourseViewRepository[F[_]: Async] private (
  pool: Resource[F, Session[F]],
):

  import CourseViewRepository.*

  def find(courseId: UUID): F[Option[CourseView]] =
    pool.use(_.option(findQuery)(courseId))

  def findAll: F[List[CourseView]] =
    pool.use(_.execute(findAllQuery))

  def upsert(view: CourseView): F[Unit] =
    pool.use(_.execute(upsertCommand)(view)).void

  def delete(courseId: UUID): F[Unit] =
    pool.use(_.execute(deleteCommand)(courseId)).void

object CourseViewRepository:

  private val courseViewCodec: Codec[CourseView] =
    (uuid *: text *: text *: int4 *: text *: bool).to[CourseView]

  private val findQuery: Query[UUID, CourseView] =
    sql"""
      SELECT course_id, code, title, capacity, instructor, is_open
      FROM course_view
      WHERE course_id = $uuid
    """.query(courseViewCodec)

  private val findAllQuery: Query[Void, CourseView] =
    sql"""
      SELECT course_id, code, title, capacity, instructor, is_open
      FROM course_view
    """.query(courseViewCodec)

  private val upsertCommand: Command[CourseView] =
    sql"""
      INSERT INTO course_view (course_id, code, title, capacity, instructor, is_open)
      VALUES ${courseViewCodec.values}
      ON CONFLICT (course_id) DO UPDATE SET
        code       = EXCLUDED.code,
        title      = EXCLUDED.title,
        capacity   = EXCLUDED.capacity,
        instructor = EXCLUDED.instructor,
        is_open    = EXCLUDED.is_open
    """.command

  private val deleteCommand: Command[UUID] =
    sql"DELETE FROM course_view WHERE course_id = $uuid".command

  def make[F[_]: Async](pool: Resource[F, Session[F]]): CourseViewRepository[F] =
    new CourseViewRepository(pool)
