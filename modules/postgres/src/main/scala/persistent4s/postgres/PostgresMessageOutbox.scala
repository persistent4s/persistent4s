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

import cats.effect.*
import cats.syntax.all.*
import fs2.Stream
import io.circe.Json
import skunk.*
import skunk.circe.codec.all.jsonb
import skunk.codec.all.*
import skunk.data.{Arr, Identifier}
import skunk.implicits.*

import persistent4s.*

/** PostgreSQL-backed [[MessageOutbox]]. A single `message_outbox` table; each row is a self-routing message. Draining
  * claims a bounded batch, publishes it, and deletes it — all in one short transaction.
  */
final case class PostgresMessageOutbox[F[_]: Async] private (
  pool: Resource[F, Session[F]],
  channelId: Identifier,
) extends MessageOutbox[F]:

  import PostgresMessageOutbox.*

  override def enqueue(messages: List[OutgoingMessage]): F[Unit] =
    if messages.isEmpty then Async[F].unit
    else
      pool.use { session =>
        session.transaction.use { _ =>
          session.prepare(insertCommand).flatMap(cmd => messages.traverse_(cmd.execute)) *>
            session.channel(channelId).notify("")
        }
      }

  override def drainBatch(batchSize: Int)(publish: List[(Long, OutgoingMessage)] => F[Unit]): F[Int] =
    pool.use { session =>
      session.transaction.use { _ =>
        session.prepare(selectBatchQuery).flatMap(_.stream(batchSize, batchSize).compile.toList).flatMap { rows =>
          val entries = rows.map(fromRow)
          if entries.isEmpty then Async[F].pure(0)
          else
            publish(entries) *>
              session.prepare(deleteCommand).flatMap(_.execute(Arr.fromFoldable(entries.map(_._1)))).as(entries.size)
        }
      }
    }

  override def notifications: Stream[F, Unit] =
    Stream.resource(pool).flatMap(session => session.channel(channelId).listen(1024).void)

object PostgresMessageOutbox:

  val NotificationChannel: Identifier =
    Identifier.fromValue("persistent4s_messages").getOrElse(sys.error("Invalid channel identifier"))

  def apply[F[_]: Async](
    pool: Resource[F, Session[F]],
    channelId: Identifier = NotificationChannel,
  ): PostgresMessageOutbox[F] = new PostgresMessageOutbox[F](pool, channelId)

  private type MessageRow = Long *: String *: Option[String] *: String *: Map[String, String] *: EmptyTuple

  private def fromRow(row: MessageRow): (Long, OutgoingMessage) = row match
    case id *: topic *: key *: payload *: headers *: EmptyTuple => (id, OutgoingMessage(topic, key, payload, headers))

  private val headersCodec: Codec[Map[String, String]] =
    jsonb.eimap[Map[String, String]] { json =>
      json.asObject.toRight(s"headers is not a Json object: ${json.noSpaces}").flatMap { obj =>
        obj.toList.traverse { case (k, v) => v.asString.toRight(s"header '$k' is not a string").map(k -> _) }
          .map(_.toMap)
      }
    }(m => Json.obj(m.toSeq.map((k, v) => k -> Json.fromString(v))*))

  private val messageRowCodec: Codec[MessageRow] = int8 *: text *: text.opt *: text *: headersCodec

  private[postgres] val createTableCommand: Command[Void] =
    sql"""
      CREATE TABLE IF NOT EXISTS message_outbox (
        id BIGSERIAL PRIMARY KEY,
        topic TEXT NOT NULL,
        message_key TEXT,
        payload TEXT NOT NULL,
        headers JSONB NOT NULL DEFAULT '{}',
        enqueud_at TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    """.command

  private[postgres] val insertCommand: Command[OutgoingMessage] =
    sql"""
      INSERT INTO message_outbox (topic, message_key, payload, headers)
      VALUES ($text, ${text.opt}, $text, $headersCodec)
    """.command.contramap(m => m.topic *: m.key *: m.payload *: m.headers *: EmptyTuple)

  private val selectBatchQuery: Query[Int, MessageRow] =
    sql"""
      SELECT id, topic, message_key, payload, headers
      FROM message_outbox
      ORDER BY id ASC
      LIMIT $int4
      FOR UPDATE SKIP LOCKED
    """.query(messageRowCodec)

  private val deleteCommand: Command[Arr[Long]] =
    sql"DELETE FROM message_outbox WHERE id = ANY($_int8)".command
