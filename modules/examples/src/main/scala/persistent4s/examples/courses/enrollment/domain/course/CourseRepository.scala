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

package persistent4s.examples.courses.enrollment.domain.course

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

  override def persist(upserts: Map[UUID, CourseState], deletes: List[UUID]): F[Unit] =
    if upserts.isEmpty && deletes.isEmpty then Async[F].unit
    else
      pool.use { session =>
        val upsertAll =
          upserts.toList
            .grouped(MaxUpsertChunkSize)
            .toList
            .traverse_(chunk => session.execute(upsertManyCommand(chunk.size))(chunk.map(_._2)).void)

        val deleteAll =
          if deletes.isEmpty then Async[F].unit
          else session.execute(deleteManyCommand(deletes.size))(deletes).void

        session.transaction.use(_ => upsertAll *> deleteAll)
      }

  def find(courseId: UUID): F[Option[CourseState]] =
    pool.use(_.option(findQuery)(courseId))

  def findAll: F[List[CourseState]] =
    pool.use(_.execute(findAllQuery))

object CourseRepository:

  private val MaxUpsertChunkSize = 300

  private val CourseStateCodec: Codec[CourseState] =
    (uuid *: text *: text *: int4 *: text *: bool).to[CourseState]

  private val findQuery: Query[UUID, CourseState] =
    sql"""
      SELECT course_id, code, title, capacity, instructor, is_open
      FROM course_view
      WHERE course_id = $uuid
    """.query(CourseStateCodec)

  private val findAllQuery: Query[Void, CourseState] =
    sql"""
      SELECT course_id, code, title, capacity, instructor, is_open
      FROM course_view
    """.query(CourseStateCodec)

  private def findManyQuery(n: Int): Query[List[UUID], CourseState] =
    sql"""
      SELECT course_id, code, title, capacity, instructor, is_open
      FROM course_view
      WHERE course_id = ANY(ARRAY[${uuid.list(n)}])
    """.query(CourseStateCodec)

  private def upsertManyCommand(n: Int): Command[List[CourseState]] =
    sql"""
      INSERT INTO course_view (course_id, code, title, capacity, instructor, is_open)
      VALUES ${CourseStateCodec.values.list(n)}
      ON CONFLICT (course_id) DO UPDATE SET
        code       = EXCLUDED.code,
        title      = EXCLUDED.title,
        capacity   = EXCLUDED.capacity,
        instructor = EXCLUDED.instructor,
        is_open    = EXCLUDED.is_open
    """.command

  private def deleteManyCommand(n: Int): Command[List[UUID]] =
    sql"DELETE FROM course_view WHERE course_id = ANY(ARRAY[${uuid.list(n)}])".command

  def make[F[_]: Async](pool: Resource[F, Session[F]]): CourseRepository[F] =
    new CourseRepository(pool)
