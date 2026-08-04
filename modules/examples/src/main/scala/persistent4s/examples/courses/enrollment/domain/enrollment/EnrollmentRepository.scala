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

import java.util.UUID

import cats.effect.*
import cats.syntax.all.*
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

import persistent4s.Repository

final class EnrollmentRepository[F[_]: Async] private (
  pool: Resource[F, Session[F]],
) extends Repository[F, (UUID, UUID), EnrollmentRecord]:

  import EnrollmentRepository.*

  override def findMany(keys: List[(UUID, UUID)]): F[Map[(UUID, UUID), Option[EnrollmentRecord]]] =
    if keys.isEmpty then Map.empty.pure[F]
    else
      val (studentIds, courseIds) = keys.unzip
      pool.use(_.execute(findManyQuery(keys.size))((studentIds, courseIds))).map { records =>
        val found = records.map(r => (r.studentId, r.courseId) -> r).toMap
        keys.map(k => k -> found.get(k)).toMap
      }

  override def persist(upserts: Map[(UUID, UUID), EnrollmentRecord], deletes: List[(UUID, UUID)]): F[Unit] =
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
          else
            val (studentIds, courseIds) = deletes.unzip
            session.execute(deleteManyCommand(deletes.size))((studentIds, courseIds)).void

        session.transaction.use(_ => upsertAll *> deleteAll)
      }

  def getEnrollments: F[List[EnrollmentRecord]] =
    pool.use(_.execute(getAllQuery))

  def getByStudent(studentId: UUID): F[List[EnrollmentRecord]] =
    pool.use(_.execute(getByStudentQuery)(studentId))

  def getByCourse(courseId: UUID): F[List[EnrollmentRecord]] =
    pool.use(_.execute(getByCourseQuery)(courseId))

object EnrollmentRepository:

  private val MaxUpsertChunkSize = 500

  private val enrollmentCodec: Codec[EnrollmentRecord] =
    (uuid *: uuid *: timestamptz *: timestamptz.opt).to[EnrollmentRecord]

  private def findManyQuery(n: Int): Query[(List[UUID], List[UUID]), EnrollmentRecord] =
    sql"""
      SELECT e.student_id, e.course_id, e.enrolled_at, e.dropped_at
      FROM enrollments e
      JOIN (
        SELECT unnest(ARRAY[${uuid.list(n)}]) AS student_id,
               unnest(ARRAY[${uuid.list(n)}]) AS course_id
      ) k ON e.student_id = k.student_id AND e.course_id = k.course_id
    """.query(enrollmentCodec)

  private def upsertManyCommand(n: Int): Command[List[EnrollmentRecord]] =
    sql"""
      INSERT INTO enrollments (student_id, course_id, enrolled_at, dropped_at)
      VALUES ${enrollmentCodec.values.list(n)}
      ON CONFLICT (student_id, course_id) DO UPDATE SET
        enrolled_at = EXCLUDED.enrolled_at,
        dropped_at  = EXCLUDED.dropped_at
    """.command

  private def deleteManyCommand(n: Int): Command[(List[UUID], List[UUID])] =
    sql"""
      DELETE FROM enrollments
      WHERE (student_id, course_id) IN (
        SELECT unnest(ARRAY[${uuid.list(n)}]), unnest(ARRAY[${uuid.list(n)}])
      )
    """.command

  private val getAllQuery: Query[Void, EnrollmentRecord] =
    sql"SELECT student_id, course_id, enrolled_at, dropped_at FROM enrollments".query(enrollmentCodec)

  private val getByStudentQuery: Query[UUID, EnrollmentRecord] =
    sql"""
      SELECT student_id, course_id, enrolled_at, dropped_at
      FROM enrollments WHERE student_id = $uuid
    """.query(enrollmentCodec)

  private val getByCourseQuery: Query[UUID, EnrollmentRecord] =
    sql"""
      SELECT student_id, course_id, enrolled_at, dropped_at
      FROM enrollments WHERE course_id = $uuid
    """.query(enrollmentCodec)

  def make[F[_]: Async](pool: Resource[F, Session[F]]): EnrollmentRepository[F] =
    new EnrollmentRepository(pool)
