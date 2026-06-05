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
  * [[readFrom]] streams events lazily using a PostgreSQL server-side cursor (fetching 256 rows per round-trip), so it
  * is safe to use on stores with millions of events without loading them all into memory. A read-only transaction is
  * held open for the lifetime of the stream; callers must ensure the stream is fully consumed or cancelled to release
  * the connection back to the pool.
  *
  * Notifications are sent via PostgreSQL NOTIFY/LISTEN mechanism, enabling cross-process and cross-machine event
  * notifications for horizontal scaling.
  *
  * @param pool
  *   a resource for obtaining database sessions
  * @param codec
  *   the event codec for serializing/deserializing events
  */
final class PostgresEventStore[F[_]: Async, A <: Event] private (
  pool: Resource[F, Session[F]],
  codec: EventCodec[A],
  channelId: Identifier,
) extends EventStore[F, A]
    with EventNotification[F]:

  import PostgresEventStore.*

  override def append(
    eventFilter: EventFilter,
    expectedIndex: Long,
    events: List[(Set[Tag], EventTypeName, A)]*,
  ): F[List[A]] =
    val flatEvents = events.flatten.toList
    if flatEvents.isEmpty then Async[F].pure(List.empty)
    else
      val allTags = eventFilter.tags
      val eventTypes = eventFilter.eventTypes
      pool.use { session =>
        session.transaction.use { _ =>
          for
            _ <- acquireAppendLocks(session, allTags)
            _ <- checkForConflicts(session, allTags, eventTypes, expectedIndex)
            _ <- flatEvents.traverse_ { case (tags, eventType, event) =>
                   insertEvent(session, tags, eventType, event).void
                 }
            _ <- session.channel(channelId).notify(PostgresNotification.encode(EventStoreNotification.EventsAppended))
          yield flatEvents.map(_._3)
        }
      }

  override def readFrom(
    fromPosition: Long,
    eventFilter: EventFilter,
    maxEvents: Option[Int] = None,
  ): Stream[F, EventEnvelope[A]] =
    val eventTypesList = eventFilter.eventTypes.toList.map(_.value)
    val tagsList = eventFilter.tags.map(_.value).toList

    Stream.resource(pool).flatMap { session =>
      Stream.resource(session.transaction).flatMap { _ =>
        val rowStream: Stream[F, EventRow] =
          (eventTypesList.isEmpty, tagsList.isEmpty, maxEvents) match
            case (true, true, None) =>
              Stream
                .eval(session.prepare(readAllQuery))
                .flatMap(
                  _.stream(fromPosition, FetchSize),
                )
            case (true, true, Some(max)) =>
              Stream
                .eval(session.prepare(readAllLimitedQuery))
                .flatMap(
                  _.stream(fromPosition *: max *: EmptyTuple, FetchSize),
                )
            case (false, true, None) =>
              Stream
                .eval(session.prepare(readByEventTypesQuery(eventTypesList.size)))
                .flatMap(
                  _.stream(fromPosition *: eventTypesList *: EmptyTuple, FetchSize),
                )
            case (false, true, Some(max)) =>
              Stream
                .eval(session.prepare(readByEventTypesLimitedQuery(eventTypesList.size)))
                .flatMap(
                  _.stream(fromPosition *: eventTypesList *: max *: EmptyTuple, FetchSize),
                )
            case (true, false, None) =>
              Stream
                .eval(session.prepare(readByTagsQuery(tagsList.size)))
                .flatMap(
                  _.stream(fromPosition *: tagsList *: EmptyTuple, FetchSize),
                )
            case (true, false, Some(max)) =>
              Stream
                .eval(session.prepare(readByTagsLimitedQuery(tagsList.size)))
                .flatMap(
                  _.stream(fromPosition *: tagsList *: max *: EmptyTuple, FetchSize),
                )
            case (false, false, None) =>
              Stream
                .eval(session.prepare(readByBothQuery(eventTypesList.size, tagsList.size)))
                .flatMap(
                  _.stream(fromPosition *: eventTypesList *: tagsList *: EmptyTuple, FetchSize),
                )
            case (false, false, Some(max)) =>
              Stream
                .eval(session.prepare(readByBothLimitedQuery(eventTypesList.size, tagsList.size)))
                .flatMap(
                  _.stream(fromPosition *: eventTypesList *: tagsList *: max *: EmptyTuple, FetchSize),
                )

        rowStream.evalMap { case (seqNum, eventType, tags, payload, recordedAt) =>
          val eventTypeName = EventTypeName.fromString(eventType)
          parsePayload(eventTypeName, payload).map { event =>
            EventEnvelope(
              EventMetadata(
                globalPosition = seqNum,
                tags = tags,
                eventType = eventTypeName,
                timestamp = recordedAt.toInstant,
              ),
              event,
            )
          }
        }
      }
    }

  /** Returns a stream of notifications for new events appended to the store. Notifications are sent via PostgreSQL's
    * NOTIFY/LISTEN mechanism. Each notification payload is decoded into a EventStoreNotification.
    */
  override def notification(projectionName: String): Stream[F, EventStoreNotification] =
    Stream.resource(pool).flatMap { session =>
      session
        .channel(channelId)
        .listen(1024)
        .map { notif =>
          PostgresNotification.decode(notif.value)
        }
        .collect {
          case p @ EventStoreNotification.EventsAppended                                               => p
          case p @ EventStoreNotification.PauseProjection(proj) if proj == projectionName              => p
          case p @ EventStoreNotification.ResumeProjection(proj) if proj == projectionName             => p
          case p @ EventStoreNotification.UpdateCheckpointIndex(proj, index) if proj == projectionName => p
        }
    }

  /** Sends a notification to all listeners on the PostgreSQL channel.
    *
    * @param n
    *   the notification to send
    */
  def notify(n: EventStoreNotification): F[Unit] =
    pool.use(_.channel(channelId).notify(PostgresNotification.encode(n)))

  private def checkForConflicts(
    session: Session[F],
    tags: Set[Tag],
    eventTypes: Set[EventTypeName],
    expectedIndex: Long,
  ): F[Unit] =
    val tagList = tags.toList.map(_.value)
    val eventTypeList = eventTypes.toList.map(_.value)

    for
      actualIndex <- (tagList.isEmpty, eventTypeList.isEmpty) match
                       case (true, true) =>
                         session.unique(lastConflictingSequenceQuery)(expectedIndex)
                       case (true, false) =>
                         session.unique(lastConflictingSequenceByEventTypesQuery(eventTypeList.size))(
                           expectedIndex *: eventTypeList *: EmptyTuple,
                         )
                       case (false, true) =>
                         session.unique(lastConflictingSequenceByTagsQuery(tagList.size))(
                           expectedIndex *: tagList *: EmptyTuple,
                         )
                       case (false, false) =>
                         session.unique(lastConflictingSequenceByBothQuery(tagList.size, eventTypeList.size))(
                           expectedIndex *: tagList *: eventTypeList *: EmptyTuple,
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
    eventType: EventTypeName,
    event: A,
  ): F[Long] =
    val tagsJson = tagsToJson(tags)
    val payloadJson = parseJson(codec.encode(event)).getOrElse(Json.obj())
    for
      sequenceNumber <- session.unique(insertEventQuery)(
                          eventType.value *: tagsJson *: payloadJson *: EmptyTuple,
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

  private def parsePayload(eventType: EventTypeName, payload: Json): F[A] =
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

  /** Number of rows fetched per cursor round-trip when streaming events from PostgreSQL. */
  private val FetchSize: Int = 256

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
  def apply[F[_]: Async, A <: Event](
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

  private val lastConflictingSequenceQuery: Query[Long, Long] =
    sql"""
      SELECT COALESCE(MAX(sequence_number), 0)
      FROM events
      WHERE sequence_number > $int8
    """.query(int8)

  private def lastConflictingSequenceByEventTypesQuery(
    numEventTypes: Int,
  ): Query[Long *: List[String] *: EmptyTuple, Long] =
    sql"""
      SELECT COALESCE(MAX(sequence_number), 0)
      FROM events
      WHERE sequence_number > $int8
        AND event_type = ANY(ARRAY[${text.list(numEventTypes)}])
    """.query(int8)

  private def lastConflictingSequenceByTagsQuery(
    numTags: Int,
  ): Query[Long *: List[String] *: EmptyTuple, Long] =
    sql"""
      SELECT COALESCE(MAX(et.sequence_number), 0)
      FROM event_tags et
      WHERE et.sequence_number > $int8
        AND et.tag = ANY(ARRAY[${text.list(numTags)}])
    """.query(int8)

  private def lastConflictingSequenceByBothQuery(
    numTags: Int,
    numEventTypes: Int,
  ): Query[Long *: List[String] *: List[String] *: EmptyTuple, Long] =
    sql"""
      SELECT COALESCE(MAX(et.sequence_number), 0)
      FROM event_tags et
      JOIN events e ON e.sequence_number = et.sequence_number
      WHERE et.sequence_number > $int8
        AND et.tag = ANY(ARRAY[${text.list(numTags)}])
        AND e.event_type = ANY(ARRAY[${text.list(numEventTypes)}])
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

  private val readAllLimitedQuery: Query[Long *: Int *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_type, tags, payload, recorded_at
      FROM events
      WHERE sequence_number > $int8
      ORDER BY sequence_number ASC
      LIMIT $int4
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

  private def readByEventTypesLimitedQuery(
    numEventTypes: Int,
  ): Query[Long *: List[String] *: Int *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_type, tags, payload, recorded_at
      FROM events
      WHERE sequence_number > $int8
        AND event_type = ANY(ARRAY[${text.list(numEventTypes)}])
      ORDER BY sequence_number ASC
      LIMIT $int4
    """.query(eventDecoder)

  private def readByTagsQuery(
    numTags: Int,
  ): Query[Long *: List[String] *: EmptyTuple, EventRow] =
    sql"""
      SELECT e.sequence_number, e.event_type, e.tags, e.payload, e.recorded_at
      FROM events e
      WHERE e.sequence_number > $int8
        AND EXISTS (
          SELECT 1
          FROM event_tags et
          WHERE et.sequence_number = e.sequence_number
            AND et.tag = ANY(ARRAY[${text.list(numTags)}])
        )
      ORDER BY e.sequence_number ASC
    """.query(eventDecoder)

  private def readByTagsLimitedQuery(
    numTags: Int,
  ): Query[Long *: List[String] *: Int *: EmptyTuple, EventRow] =
    sql"""
      SELECT e.sequence_number, e.event_type, e.tags, e.payload, e.recorded_at
      FROM events e
      WHERE e.sequence_number > $int8
        AND EXISTS (
          SELECT 1
          FROM event_tags et
          WHERE et.sequence_number = e.sequence_number
            AND et.tag = ANY(ARRAY[${text.list(numTags)}])
        )
      ORDER BY e.sequence_number ASC
      LIMIT $int4
    """.query(eventDecoder)

  private def readByBothQuery(
    numEventTypes: Int,
    numTags: Int,
  ): Query[Long *: List[String] *: List[String] *: EmptyTuple, EventRow] =
    sql"""
      SELECT e.sequence_number, e.event_type, e.tags, e.payload, e.recorded_at
      FROM events e
      WHERE e.sequence_number > $int8
        AND e.event_type = ANY(ARRAY[${text.list(numEventTypes)}])
        AND EXISTS (
          SELECT 1
          FROM event_tags et
          WHERE et.sequence_number = e.sequence_number
            AND et.tag = ANY(ARRAY[${text.list(numTags)}])
        )
      ORDER BY e.sequence_number ASC
    """.query(eventDecoder)

  private def readByBothLimitedQuery(
    numEventTypes: Int,
    numTags: Int,
  ): Query[Long *: List[String] *: List[String] *: Int *: EmptyTuple, EventRow] =
    sql"""
      SELECT e.sequence_number, e.event_type, e.tags, e.payload, e.recorded_at
      FROM events e
      WHERE e.sequence_number > $int8
        AND e.event_type = ANY(ARRAY[${text.list(numEventTypes)}])
        AND EXISTS (
          SELECT 1
          FROM event_tags et
          WHERE et.sequence_number = e.sequence_number
            AND et.tag = ANY(ARRAY[${text.list(numTags)}])
        )
      ORDER BY e.sequence_number ASC
      LIMIT $int4
    """.query(eventDecoder)
