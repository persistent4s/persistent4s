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

import cats.effect.*
import cats.syntax.all.*
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

import persistent4s.Repository

final class CourseRepository[F[_]: Async] private (
  pool: Resource[F, Session[F]],
) extends Repository[F, UUID, CourseState]:

  import CourseRepository.*

  override def findMany(keys: List[UUID]): F[Map[UUID, Option[CourseState]]] =
    if keys.isEmpty then Map.empty.pure[F]
    else
      pool.use(_.execute(findManyQuery(keys.size))(keys)).map { states =>
        val found = states.map(s => s.courseId -> s).toMap
        keys.map(k => k -> found.get(k)).toMap
      }

  override def upsertMany(states: Map[UUID, CourseState]): F[Unit] =
    if states.isEmpty then Async[F].unit
    else
      states.toList
        .grouped(MaxUpsertChunkSize)
        .toList
        .traverse_(chunk => pool.use(_.execute(upsertManyCommand(chunk.size))(chunk.map(_._2))).void)

  override def deleteMany(keys: List[UUID]): F[Unit] =
    if keys.isEmpty then Async[F].unit
    else pool.use(_.execute(deleteManyCommand(keys.size))(keys)).void

  def find(key: UUID): F[Option[CourseState]] =
    pool.use(_.option(findQuery)(key))

  def getCourses: F[List[CourseState]] =
    pool.use(_.execute(getCoursesQuery))

object CourseRepository:

  private val MaxUpsertChunkSize = 500

  private val courseStateCodec: Codec[CourseState] =
    (uuid *: text *: text *: int4 *: text *: bool).to[CourseState]

  private def findManyQuery(n: Int): Query[List[UUID], CourseState] =
    sql"""
      SELECT course_id, code, title, capacity, instructor, is_open
      FROM courses
      WHERE course_id = ANY(ARRAY[${uuid.list(n)}])
    """.query(courseStateCodec)

  private val findQuery: Query[UUID, CourseState] =
    sql"""
      SELECT course_id, code, title, capacity, instructor, is_open
      FROM courses
      WHERE course_id = $uuid
    """.query(courseStateCodec)

  private def upsertManyCommand(n: Int): Command[List[CourseState]] =
    sql"""
      INSERT INTO courses (course_id, code, title, capacity, instructor, is_open)
      VALUES ${courseStateCodec.values.list(n)}
      ON CONFLICT (course_id) DO UPDATE SET
        code       = EXCLUDED.code,
        title      = EXCLUDED.title,
        capacity   = EXCLUDED.capacity,
        instructor = EXCLUDED.instructor,
        is_open    = EXCLUDED.is_open
    """.command

  private def deleteManyCommand(n: Int): Command[List[UUID]] =
    sql"DELETE FROM courses WHERE course_id = ANY(ARRAY[${uuid.list(n)}])".command

  private val getCoursesQuery: Query[Void, CourseState] =
    sql"SELECT course_id, code, title, capacity, instructor, is_open FROM courses".query(courseStateCodec)

  def make[F[_]: Async](pool: Resource[F, Session[F]]): CourseRepository[F] =
    new CourseRepository(pool)
