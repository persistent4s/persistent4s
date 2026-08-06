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

package persistent4s.postgres

import java.time.OffsetDateTime
import java.util.UUID

import scala.concurrent.duration.*

import cats.effect.*
import cats.syntax.all.*
import skunk.*
import skunk.codec.all.*
import skunk.data.Identifier
import skunk.implicits.*

import persistent4s.*
import java.time.ZoneOffset
import java.time.Instant

/** PostgreSQL-backed [[SagaRepository]]. A single `saga_instances` table holds the instances of every saga, keyed by
  * the deterministic id derived from `(sagaName, key)` — so that primary key alone makes a replayed trigger a no-op and
  * keeps one instance per correlation key without a separate unique constraint.
  *
  * @param claimTtl
  *   how far [[claimExpired]] pushes a claimed instance's deadline out, i.e. how long a handler has to finish before
  *   the instance can be claimed again
  * @param channelId
  *   channel notified when [[start]] enqueues messages, so a message relay wakes up without waiting for its poll
  */
final case class PostgresSagaRepository[F[_]: Async] private (
  pool: Resource[F, Session[F]],
  claimTtl: FiniteDuration,
  channelId: Identifier,
) extends SagaRepository[F]:

  import PostgresSagaRepository.*

  override def start(
    id: UUID,
    sagaName: String,
    key: String,
    data: String,
    deadline: Option[Instant],
    messages: List[OutgoingMessage],
  ): F[Boolean] =
    pool.use { session =>
      session.transaction.use { _ =>
        session.option(insertInstanceQuery)(id, sagaName, key, data, deadline.map(_.atOffset(ZoneOffset.UTC))).flatMap {
          case None    => Async[F].pure(false)
          case Some(_) => enqueueMessages(session, messages).as(true)
        }
      }
    }

  override def find(id: UUID): F[Option[SagaRecord]] =
    pool.use(_.option(selectByIdQuery)(id)).map(_.map(fromRow))

  override def advance(
    id: UUID,
    expectedStep: Int,
    status: SagaStatus,
    step: Int,
    data: String,
    deadline: Option[Instant],
  ): F[Boolean] =
    pool
      .use(_.option(advanceQuery)(status, step, data, deadline.map(_.atOffset(ZoneOffset.UTC)), id, expectedStep))
      .map(_.isDefined)

  override def claimExpired(sagaName: String, limit: Int)(handle: List[SagaRecord] => F[Unit]): F[Int] =
    pool.use { session =>
      session.transaction.use { _ =>
        session
          .prepare(claimExpiredQuery)
          .flatMap(_.stream((claimTtl.toMillis, sagaName, limit), limit).compile.toList)
      }
    }.flatMap { rows =>
      val records = rows.map(fromRow)
      if records.isEmpty then Async[F].pure(0) else handle(records).as(records.size)
    }

  private def enqueueMessages(session: Session[F], messages: List[OutgoingMessage]): F[Unit] =
    if messages.isEmpty then Async[F].unit
    else
      session.prepare(PostgresMessageOutbox.insertCommand).flatMap(cmd => messages.traverse_(cmd.execute)) *>
        session.channel(channelId).notify("")

object PostgresSagaRepository:

  def apply[F[_]: Async](
    pool: Resource[F, Session[F]],
    claimTtl: FiniteDuration = 30.seconds,
    channelId: Identifier = PostgresMessageOutbox.NotificationChannel,
  ): PostgresSagaRepository[F] = new PostgresSagaRepository[F](pool, claimTtl, channelId)

  private val statusCodec: Codec[SagaStatus] =
    text.eimap(s => SagaStatus.fromString(s).toRight(s"unknown saga status: '$s'"))(_.toString)

  private type SagaRow = UUID *: String *: String *: SagaStatus *: Int *: String *: Option[OffsetDateTime] *:
    OffsetDateTime *: OffsetDateTime *: EmptyTuple

  private val sagaRowCodec: Codec[SagaRow] =
    uuid *: text *: text *: statusCodec *: int4 *: text *: timestamptz.opt *: timestamptz *: timestamptz

  private def fromRow(row: SagaRow): SagaRecord = row match
    case id *: sagaName *: key *: status *: step *: data *: deadline *: createdAt *: updatedAt *: EmptyTuple =>
      SagaRecord(
        id = id, sagaName = sagaName, key = key, status = status, step = step, data = data,
        deadline = deadline.map(_.toInstant), createdAt = createdAt.toInstant, updatedAt = updatedAt.toInstant,
      )

  private[postgres] val createTableCommand: Command[Void] =
    sql"""
      CREATE TABLE IF NOT EXISTS saga_instances (
        id          UUID        PRIMARY KEY,
        saga_name   TEXT        NOT NULL,
        saga_key    TEXT        NOT NULL,
        status      TEXT        NOT NULL,
        step        INT         NOT NULL,
        data        TEXT        NOT NULL,
        deadline    TIMESTAMPTZ,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
        updated_at  TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
      )
    """.command

  private[postgres] val createDueIndexCommand: Command[Void] =
    sql"""
      CREATE INDEX IF NOT EXISTS idx_saga_instances_due
        ON saga_instances (saga_name, deadline)
        WHERE status = 'Pending' AND deadline IS NOT NULL
    """.command

  private val insertInstanceQuery: Query[(UUID, String, String, String, Option[OffsetDateTime]), UUID] =
    sql"""
      INSERT INTO saga_instances (id, saga_name, saga_key, status, step, data, deadline)
      VALUES (
        $uuid, $text, $text, 'Pending', 0, $text,
        ${timestamptz.opt}
      )
      ON CONFLICT (id) DO NOTHING
      RETURNING id
    """.query(uuid)

  private val selectByIdQuery: Query[UUID, SagaRow] =
    sql"""
      SELECT id, saga_name, saga_key, status, step, data, deadline, created_at, updated_at
      FROM saga_instances
      WHERE id = $uuid
    """.query(sagaRowCodec)

  private val advanceQuery: Query[(SagaStatus, Int, String, Option[OffsetDateTime], UUID, Int), UUID] =
    sql"""
      UPDATE saga_instances
      SET status = $statusCodec,
          step = $int4,
          data = $text,
          deadline = ${timestamptz.opt},
          updated_at = clock_timestamp()
      WHERE id = $uuid
        AND step = $int4
        AND status = 'Pending'
      RETURNING id
    """.query(uuid)

  private val claimExpiredQuery: Query[(Long, String, Int), SagaRow] =
    sql"""
      UPDATE saga_instances
      SET deadline = clock_timestamp() + ($int8 * interval '1 millisecond'),
          updated_at = clock_timestamp()
      WHERE id IN (
        SELECT id
        FROM saga_instances
        WHERE saga_name = $text
          AND status = 'Pending'
          AND deadline IS NOT NULL
          AND deadline < clock_timestamp()
        ORDER BY deadline ASC
        LIMIT $int4
        FOR UPDATE SKIP LOCKED
      )
      RETURNING id, saga_name, saga_key, status, step, data, deadline, created_at, updated_at
    """.query(sagaRowCodec)
