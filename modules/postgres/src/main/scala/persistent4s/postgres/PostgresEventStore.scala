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
import io.circe.parser.parse as parseJson
import skunk.*
import skunk.circe.codec.all.jsonb
import skunk.codec.all.*
import skunk.data.Identifier
import skunk.implicits.*

import persistent4s.*

/** A PostgreSQL-backed implementation of the EventStore trait. This implementation uses Skunk for database access and
  * implements optimistic concurrency control for event appending.
  *
  * Events are stored in a table with the following schema:
  *   - sequence_number: BIGSERIAL PRIMARY KEY (global position)
  *   - event_type: TEXT NOT NULL
  *   - tags: JSONB NOT NULL (array of tag strings)
  *   - payload: JSONB NOT NULL
  *   - recorded_at: TIMESTAMPTZ NOT NULL
  *
  * Notifications are sent via PostgreSQL NOTIFY/LISTEN mechanism, enabling cross-process and cross-machine event
  * notifications for horizontal scaling.
  *
  * @param pool
  *   a resource for obtaining database sessions
  * @param codec
  *   the event codec for serializing/deserializing events
  */
final class PostgresEventStore[F[_]: Async, A] private (
  pool: Resource[F, Session[F]],
  codec: EventCodec[A],
  channelId: Identifier,
) extends EventStore[F, A]
    with EventNotification[F]:

  import PostgresEventStore.*

  override def append(
    expectedIndex: Long,
    events: List[(Set[Tag], String, A)]*,
  ): F[Unit] =
    val flatEvents = events.flatten.toList
    if flatEvents.isEmpty then Async[F].unit
    else
      val allTags = flatEvents.flatMap(_._1).toSet
      pool.use { session =>
        session.transaction.use { _ =>
          for
            _ <- acquireAppendLocks(session, allTags)
            _ <- checkForConflicts(session, allTags, expectedIndex)
            _ <- flatEvents.traverse_ { case (tags, eventType, event) =>
                   insertEvent(session, tags, eventType, event).void
                 }
            _ <- session.channel(channelId).notify("")
          yield ()
        }
      }

  override def readFrom(
    fromPosition: Long,
    eventFilter: EventFilter,
  ): Stream[F, EventEnvelope[A]] =
    Stream.resource(pool).flatMap { session =>
      val eventTypesList = eventFilter.eventTypes.toList
      val tagsList = eventFilter.tags.map(_.value).toList

      val eventsF: F[List[EventRow]] =
        (eventTypesList.isEmpty, tagsList.isEmpty) match
          case (true, true) =>
            session.execute(readAllQuery)(fromPosition)
          case (false, true) =>
            session.execute(readByEventTypesQuery(eventTypesList.size))(
              fromPosition *: eventTypesList *: EmptyTuple,
            )
          case (true, false) =>
            session.execute(readByTagsQuery(tagsList.size))(
              fromPosition *: tagsList *: EmptyTuple,
            )
          case (false, false) =>
            session.execute(
              readByBothQuery(eventTypesList.size, tagsList.size),
            )(fromPosition *: eventTypesList *: tagsList *: EmptyTuple)

      Stream.eval(eventsF.map(_.toList)).flatMap { events =>
        Stream
          .emits(events)
          .evalMap { case (seqNum, eventType, tags, payload, recordedAt) =>
            parsePayload(eventType, payload).map { event =>
              EventEnvelope(
                EventMetadata(
                  globalPosition = seqNum,
                  tags = tags,
                  eventType = eventType,
                  timestamp = recordedAt.toInstant,
                ),
                event,
              )
            }
          }
      }
    }

  /** Returns a stream that emits Unit whenever new events are appended to the store. Uses PostgreSQL NOTIFY/LISTEN for
    * cross-process notifications, enabling horizontal scaling of projectors across multiple application instances.
    */
  override def notification: Stream[F, Unit] =
    Stream.resource(pool).flatMap { session =>
      session.channel(channelId).listen(1024).void
    }

  private def checkForConflicts(
    session: Session[F],
    tags: Set[Tag],
    expectedIndex: Long,
  ): F[Unit] =
    if tags.isEmpty then Async[F].unit
    else
      val tagList = tags.toList.map(_.value)
      for
        actualIndex <- session.unique(lastConflictingSequenceQuery(tagList.size))(
                         expectedIndex *: tagList *: EmptyTuple,
                       )
        _ <-
          if actualIndex > 0 then Async[F].raiseError(IndexConflictException(expectedIndex, actualIndex))
          else Async[F].unit
      yield ()

  private def acquireAppendLocks(
    session: Session[F],
    tags: Set[Tag],
  ): F[Unit] =
    tags.toList
      .sortBy(_.value)
      .traverse_(tag => session.unique(acquireTagLockQuery)(tag.value).void)

  private def insertEvent(
    session: Session[F],
    tags: Set[Tag],
    eventType: String,
    event: A,
  ): F[Long] =
    val tagsJson = tagsToJson(tags)
    val payloadJson = parseJson(codec.encode(event)).getOrElse(Json.obj())
    for
      sequenceNumber <- session.unique(insertEventQuery)(
                          eventType *: tagsJson *: payloadJson *: EmptyTuple,
                        )
      _ <- insertEventTags(session, sequenceNumber, tags)
    yield sequenceNumber

  private def insertEventTags(
    session: Session[F],
    sequenceNumber: Long,
    tags: Set[Tag],
  ): F[Unit] =
    tags.toList.traverse_ { tag =>
      session
        .execute(insertEventTagCommand)(
          tag.value *: sequenceNumber *: EmptyTuple,
        )
        .void
    }

  private def parsePayload(eventType: String, payload: Json): F[A] =
    codec.decode(eventType, payload.noSpaces) match
      case Right(event) => Async[F].pure(event)
      case Left(error)  => Async[F].raiseError(error)

  private def tagsToJson(tags: Set[Tag]): Json =
    Json.arr(tags.map(t => Json.fromString(t.value)).toSeq*)

object PostgresEventStore:

  /** The PostgreSQL channel name used for NOTIFY/LISTEN event notifications. */
  val NotificationChannel: Identifier =
    Identifier
      .fromString("persistent4s_events")
      .getOrElse(
        sys.error("Invalid channel identifier"),
      )

  /** Create a new PostgresEventStore.
    *
    * @param pool
    *   a resource for obtaining database sessions
    * @param codec
    *   the event codec for serializing/deserializing events
    * @param channelId
    *   the PostgreSQL channel identifier for NOTIFY/LISTEN (default: "persistent4s_events")
    * @return
    *   a new PostgresEventStore instance
    */
  def apply[F[_]: Async, A](
    pool: Resource[F, Session[F]],
    codec: EventCodec[A],
    channelId: Identifier = NotificationChannel,
  ): PostgresEventStore[F, A] =
    new PostgresEventStore[F, A](pool, codec, channelId)

  private val tagsCodec: Codec[Set[Tag]] = jsonb.imap { json =>
    json.asArray
      .map(_.flatMap(_.asString).flatMap(Tag.fromString).toSet)
      .getOrElse(Set.empty)
  }(tags => Json.arr(tags.map(t => Json.fromString(t.value)).toSeq*))

  private val eventDecoder: Decoder[
    Long *: String *: Set[Tag] *: Json *: java.time.OffsetDateTime *: EmptyTuple,
  ] =
    int8 *: text *: tagsCodec *: jsonb *: timestamptz

  private val acquireTagLockQuery: Query[String, String] =
    sql"""SELECT pg_advisory_xact_lock(hashtextextended($text, 0))::text""".query(text)

  private val insertEventQuery: Query[String *: Json *: Json *: EmptyTuple, Long] =
    sql"""
      INSERT INTO events (event_type, tags, payload)
      VALUES ($text, $jsonb, $jsonb)
      RETURNING sequence_number
    """.query(int8)

  private val insertEventTagCommand: Command[String *: Long *: EmptyTuple] =
    sql"""
      INSERT INTO event_tags (tag, sequence_number)
      VALUES ($text, $int8)
      ON CONFLICT DO NOTHING
    """.command

  private def lastConflictingSequenceQuery(
    n: Int,
  ): Query[Long *: List[String] *: EmptyTuple, Long] =
    sql"""
      SELECT COALESCE(MAX(sequence_number), 0)
      FROM event_tags
      WHERE sequence_number > $int8
        AND tag = ANY(ARRAY[${text.list(n)}])
    """.query(int8)

  private type EventRow =
    Long *: String *: Set[Tag] *: Json *: java.time.OffsetDateTime *: EmptyTuple

  private val readAllQuery: Query[Long, EventRow] =
    sql"""
      SELECT sequence_number, event_type, tags, payload, recorded_at
      FROM events
      WHERE sequence_number > $int8
      ORDER BY sequence_number ASC
    """.query(eventDecoder)

  private def readByEventTypesQuery(
    numEventTypes: Int,
  ): Query[Long *: List[String] *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_type, tags, payload, recorded_at
      FROM events
      WHERE sequence_number > $int8
        AND event_type = ANY(ARRAY[${text.list(numEventTypes)}])
      ORDER BY sequence_number ASC
    """.query(eventDecoder)

  private def readByTagsQuery(
    numTags: Int,
  ): Query[Long *: List[String] *: EmptyTuple, EventRow] =
    sql"""
      SELECT DISTINCT ON (e.sequence_number)
        e.sequence_number, e.event_type, e.tags, e.payload, e.recorded_at
      FROM event_tags et
      JOIN events e ON e.sequence_number = et.sequence_number
      WHERE et.sequence_number > $int8
        AND et.tag = ANY(ARRAY[${text.list(numTags)}])
      ORDER BY e.sequence_number ASC
    """.query(eventDecoder)

  private def readByBothQuery(
    numEventTypes: Int,
    numTags: Int,
  ): Query[Long *: List[String] *: List[String] *: EmptyTuple, EventRow] =
    sql"""
      SELECT DISTINCT ON (e.sequence_number)
        e.sequence_number, e.event_type, e.tags, e.payload, e.recorded_at
      FROM event_tags et
      JOIN events e ON e.sequence_number = et.sequence_number
      WHERE et.sequence_number > $int8
        AND e.event_type = ANY(ARRAY[${text.list(numEventTypes)}])
        AND et.tag = ANY(ARRAY[${text.list(numTags)}])
      ORDER BY e.sequence_number ASC
    """.query(eventDecoder)
