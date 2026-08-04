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

package persistent4s.examples.courses.enrollment.domain.student

import java.util.UUID

import cats.effect.*
import cats.syntax.all.*
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

import persistent4s.Repository

final class StudentRepository[F[_]: Async] private (
  pool: Resource[F, Session[F]],
) extends Repository[F, UUID, StudentState]:

  import StudentRepository.*

  override def findMany(keys: List[UUID]): F[Map[UUID, Option[StudentState]]] =
    if keys.isEmpty then Map.empty.pure[F]
    else
      pool.use(_.execute(findManyQuery(keys.size))(keys)).map { states =>
        val found = states.map(s => s.studentId -> s).toMap
        keys.map(k => k -> found.get(k)).toMap
      }

  override def persist(upserts: Map[UUID, StudentState], deletes: List[UUID]): F[Unit] =
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

  def find(key: UUID): F[Option[StudentState]] =
    pool.use(_.option(findQuery)(key))

  def getStudents: F[List[StudentState]] =
    pool.use(_.execute(getStudentsQuery))

object StudentRepository:

  private val MaxUpsertChunkSize = 500

  private val studentStateCodec: Codec[StudentState] =
    (uuid *: text *: text).to[StudentState]

  private def findManyQuery(n: Int): Query[List[UUID], StudentState] =
    sql"""
      SELECT student_id, name, email
      FROM students
      WHERE student_id = ANY(ARRAY[${uuid.list(n)}])
    """.query(studentStateCodec)

  private val findQuery: Query[UUID, StudentState] =
    sql"""
      SELECT student_id, name, email
      FROM students
      WHERE student_id = $uuid
    """.query(studentStateCodec)

  private def upsertManyCommand(n: Int): Command[List[StudentState]] =
    sql"""
      INSERT INTO students (student_id, name, email)
      VALUES ${studentStateCodec.values.list(n)}
      ON CONFLICT (student_id) DO UPDATE SET
        name  = EXCLUDED.name,
        email = EXCLUDED.email
    """.command

  private def deleteManyCommand(n: Int): Command[List[UUID]] =
    sql"DELETE FROM students WHERE student_id = ANY(ARRAY[${uuid.list(n)}])".command

  private val getStudentsQuery: Query[Void, StudentState] =
    sql"SELECT student_id, name, email FROM students".query(studentStateCodec)

  def make[F[_]: Async](pool: Resource[F, Session[F]]): StudentRepository[F] =
    new StudentRepository(pool)
