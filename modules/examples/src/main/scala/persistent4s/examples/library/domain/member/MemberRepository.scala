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

import persistent4s.examples.library.application.Repository
import persistent4s.examples.library.domain.member.MemberState
import java.util.UUID

final class MemberRepository[F[_]: Async] private (
  pool: Resource[F, Session[F]],
) extends Repository[F, UUID, MemberState]:

  import MemberRepository.*

  def find(key: UUID): F[Option[MemberState]] =
    pool.use(_.option(findQuery)(key))

  def save(key: UUID, value: MemberState): F[Unit] =
    pool.use(_.execute(upsertCommand)(value)).void

  def delete(key: UUID): F[Unit] =
    pool.use(_.execute(deleteCommand)(key)).void

  def getMembers: F[List[MemberState]] =
    pool.use(_.execute(getMembersQuery))

object MemberRepository:

  private val memberStateCodec: Codec[MemberState] =
    (uuid *: text *: text *: int4).to[MemberState]

  private val findQuery: Query[UUID, MemberState] =
    sql"""
      SELECT member_id, name, email, borrowed_books
      FROM members
      WHERE member_id = $uuid
    """.query(memberStateCodec)

  private val upsertCommand: Command[MemberState] =
    sql"""
      INSERT INTO members (member_id, name, email, borrowed_books)
      VALUES ($memberStateCodec)
      ON CONFLICT (member_id) DO UPDATE SET
        name          = EXCLUDED.name,
        email         = EXCLUDED.email,
        borrowed_books = EXCLUDED.borrowed_books
    """.command

  private val deleteCommand: Command[UUID] =
    sql"DELETE FROM members WHERE member_id = $uuid".command

  private val getMembersQuery: Query[Void, MemberState] =
    sql"""
      SELECT member_id, name, email, borrowed_books
      FROM members
    """.query(memberStateCodec)

  def make[F[_]: Async](pool: Resource[F, Session[F]]): MemberRepository[F] =
    new MemberRepository(pool)
