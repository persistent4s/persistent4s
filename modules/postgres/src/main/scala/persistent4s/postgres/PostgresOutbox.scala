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

import scala.concurrent.duration.*

import cats.effect.*
import cats.syntax.all.*
import fs2.Stream
import skunk.*
import skunk.codec.all.*
import skunk.data.Identifier
import skunk.implicits.*

import persistent4s.*
import io.circe.Json
import skunk.data.Arr

/** PostgreSQL-backed [[Outbox]] implementation.
  *
  * Schema: a single `event_outbox` table holding one row per unpublished event, written transactionally by
  * [[PostgresEventStore.append]]. Rows are deleted by [[markPublished]] once the relay has handed them off; the events
  * themselves remain in the `events` table, which is the source of truth.
  *
  * The wake-up [[notifications]] stream reuses the same NOTIFY channel as the event store, received through the shared
  * [[PostgresNotificationHub]], so a relay observes new outbox entries within one round-trip of the appending
  * transaction committing without holding a session of its own.
  *
  * @param pool
  *   shared session pool with the event store (same database, same transaction scope on append)
  * @param codec
  *   event codec used to decode payloads when streaming entries
  * @param notificationHub
  *   shared listener the wake-up signals come from; it must listen on `channelId`
  * @param channelId
  *   NOTIFY channel used for wake-up signals
  * @param pollInterval
  *   fallback polling cadence when no notification is received (also catches missed signals)
  */
final class PostgresOutbox[F[_]: Async, A <: Event] private (
  pool: Resource[F, Session[F]],
  codec: EventCodec[A],
  notificationHub: PostgresNotificationHub[F],
  channelId: Identifier,
  pollInterval: FiniteDuration,
) extends Outbox[F, A]:

  import PostgresOutbox.*

  override def stream(batchSize: Int): Stream[F, EventEnvelope[A]] =
    val nextPass = Stream
      .resource(pool)
      .flatMap { session =>
        Stream.resource(session.transaction).flatMap { _ =>
          Stream
            .eval(session.prepare(selectUnpublishedQuery))
            .flatMap(_.stream(Void, batchSize))
            .evalMapChunk {
              case globalPosition *: eventId *: eventType *: eventVersion *: tags *: payload *: isExternal *: headers *:
                  recordedAt *: commitGroup *: EmptyTuple =>
                val eventTypeName = EventTypeName.fromString(eventType)
                parsePayload(eventTypeName, eventVersion, payload).map { event =>
                  EventEnvelope(
                    EventMetadata(
                      globalPosition, eventId, tags, eventTypeName, isExternal, recordedAt.toInstant, headers,
                      eventVersion, commitGroup,
                    ),
                    event,
                  )
                }
            }
        }
      }
    val wait = notifications.merge(Stream.awakeEvery[F](pollInterval).void).head
    (nextPass ++ Stream.exec(wait.compile.drain)).repeat

  override def markPublished(globalPosition: Long): F[Unit] = markPublished(List(globalPosition))

  override def markPublished(globalPositions: List[Long]): F[Unit] =
    if globalPositions.isEmpty then Async[F].unit
    else
      pool.use { session =>
        session.prepare(markPublishedCommand).flatMap { cmd =>
          cmd.execute(Arr.fromFoldable(globalPositions))
        }
      }.void

  override def notifications: Stream[F, Unit] =
    Stream.resource(notificationHub.listenR(channelId)).flatten.collect {
      case PostgresNotificationHub.Message.CatchUp => ()
      case PostgresNotificationHub.Message.Payload(value)
          if PostgresNotification.decode(value) == EventStoreNotification.EventsAppended =>
        ()
    }

  private def parsePayload(eventType: EventTypeName, eventVersion: Int, payload: Json): F[A] =
    codec.decode(eventType, eventVersion, payload.noSpaces) match
      case Right(event) => Async[F].pure(event)
      case Left(error)  => Async[F].raiseError(error)

object PostgresOutbox:

  val DefaultPollInterval: FiniteDuration = 1.second

  def apply[F[_]: Async, A <: Event](
    pool: Resource[F, Session[F]],
    codec: EventCodec[A],
    notificationHub: PostgresNotificationHub[F],
    channelId: Identifier = PostgresEventStore.NotificationChannel,
    pollInterval: FiniteDuration = DefaultPollInterval,
  ): PostgresOutbox[F, A] =
    new PostgresOutbox[F, A](pool, codec, notificationHub, channelId, pollInterval)

  private[postgres] val createTableCommand: Command[Void] =
    sql"""
      CREATE TABLE IF NOT EXISTS event_outbox (
        global_position BIGINT      PRIMARY KEY REFERENCES events(sequence_number) ON DELETE CASCADE,
        enqueued_at     TIMESTAMPTZ NOT NULL DEFAULT now()
      )
    """.command

  private[postgres] val insertCommand: Command[Long] =
    sql"""
      INSERT INTO event_outbox (global_position) VALUES ($int8)
    """.command

  private val markPublishedCommand: Command[Arr[Long]] =
    sql"""
      DELETE FROM event_outbox
      WHERE global_position = ANY($_int8)
    """.command

  private val selectUnpublishedQuery: Query[Void, PostgresEventStore.EventRow] =
    sql"""
      SELECT e.sequence_number, e.event_id, e.event_type, e.event_version, e.tags, e.payload, e.is_external,
             e.headers, e.recorded_at, e.commit_group
      FROM event_outbox o
      JOIN events e ON e.sequence_number = o.global_position
      ORDER BY o.global_position ASC
    """.query(PostgresEventStore.eventDecoder)
