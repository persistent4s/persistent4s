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

package persistent4s.examples.library.domain.member

import cats.effect.*
import cats.syntax.all.*
import skunk.*
import skunk.implicits.*
import skunk.codec.all.*

import persistent4s.Repository
import persistent4s.examples.library.domain.member.MemberState
import java.util.UUID

final class MemberRepository[F[_]: Async] private (
  pool: Resource[F, Session[F]],
) extends Repository[F, UUID, MemberState]:

  import MemberRepository.*

  override def findMany(keys: List[UUID]): F[Map[UUID, Option[MemberState]]] =
    if keys.isEmpty then Map.empty.pure[F]
    else
      pool.use(_.execute(findManyQuery(keys.size))(keys)).map { states =>
        val found = states.map(s => s.memberId -> s).toMap
        keys.map(k => k -> found.get(k)).toMap
      }

  override def persist(upserts: Map[UUID, MemberState], deletes: List[UUID]): F[Unit] =
    if upserts.isEmpty && deletes.isEmpty then Async[F].unit
    else
      pool.use { session =>
        session.transaction.use { _ =>
          val deleteF =
            if deletes.isEmpty then Async[F].unit
            else session.execute(deleteManyCommand(deletes.size))(deletes).void
          val upsertF =
            upserts.toList
              .grouped(MaxUpsertChunkSize)
              .toList
              .traverse_(chunk => session.execute(upsertManyCommand(chunk.size))(chunk.map(_._2)).void)
          deleteF *> upsertF
        }
      }

  def find(key: UUID): F[Option[MemberState]] =
    pool.use(_.option(findQuery)(key))

  def getMembers: F[List[MemberState]] =
    pool.use(_.execute(getMembersQuery))

object MemberRepository:

  private val MaxUpsertChunkSize = 500

  private val memberStateCodec: Codec[MemberState] =
    (uuid *: text *: text *: int4).to[MemberState]

  private def findManyQuery(n: Int): Query[List[UUID], MemberState] =
    sql"""
      SELECT member_id, name, email, borrowed_books
      FROM members
      WHERE member_id = ANY(ARRAY[${uuid.list(n)}])
    """.query(memberStateCodec)

  private val findQuery: Query[UUID, MemberState] =
    sql"""
      SELECT member_id, name, email, borrowed_books
      FROM members
      WHERE member_id = $uuid
    """.query(memberStateCodec)

  private def upsertManyCommand(n: Int): Command[List[MemberState]] =
    sql"""
      INSERT INTO members (member_id, name, email, borrowed_books)
      VALUES ${memberStateCodec.values.list(n)}
      ON CONFLICT (member_id) DO UPDATE SET
        name           = EXCLUDED.name,
        email          = EXCLUDED.email,
        borrowed_books = EXCLUDED.borrowed_books
    """.command

  private def deleteManyCommand(n: Int): Command[List[UUID]] =
    sql"DELETE FROM members WHERE member_id = ANY(ARRAY[${uuid.list(n)}])".command

  private val getMembersQuery: Query[Void, MemberState] =
    sql"""
      SELECT member_id, name, email, borrowed_books
      FROM members
    """.query(memberStateCodec)

  def make[F[_]: Async](pool: Resource[F, Session[F]]): MemberRepository[F] =
    new MemberRepository(pool)
