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
import java.util.UUID
import java.time.OffsetDateTime

/** PostgreSQL-backed [[EventStore]] using Skunk for database access.
  *
  * Events are stored in an `events` table with columns: `sequence_number` (BIGSERIAL PK), `event_id` (UUID, unique),
  * `event_type` (TEXT), `tags` (JSONB array), `payload` (JSONB), `is_external` (BOOL), `recorded_at` (TIMESTAMPTZ).
  *
  * [[readFrom]] streams events lazily via a server-side cursor (256 rows per round-trip), safe on stores with millions
  * of events. A transaction is held open for the stream's lifetime; callers must consume or cancel the stream to
  * release the connection.
  *
  * Notifications are delivered via PostgreSQL NOTIFY/LISTEN, enabling cross-process wake-ups without polling.
  *
  * @param pool
  *   session pool shared with other store operations
  * @param codec
  *   codec for serializing/deserializing event payloads
  */
final class PostgresEventStore[F[_]: Async, A <: Event] private (
  pool: Resource[F, Session[F]],
  codec: EventCodec[A],
  channelId: Identifier,
  outboxEnabled: Boolean,
) extends EventStore[F, A]
    with EventNotification[F]:

  import PostgresEventStore.*

  override def append(
    eventFilter: EventFilter,
    expectedIndex: Long,
    events: List[PendingEvent[A]]*,
  ): F[List[A]] =
    runAppend(Some((eventFilter, expectedIndex)), events.flatten.toList, Nil)

  override def appendUnchecked(
    events: List[PendingEvent[A]]*,
  ): F[List[A]] =
    runAppend(None, events.flatten.toList, Nil)

  /** Atomic append with optimistic-concurrency check, plus message enqueue in the same transaction. */
  def appendWithMessages(
    eventFilter: EventFilter,
    expectedIndex: Long,
    messages: List[OutgoingMessage],
    events: List[PendingEvent[A]]*,
  ): F[List[A]] =
    runAppend(Some((eventFilter, expectedIndex)), events.flatten.toList, messages)

  /** Atomic append without OCC, plus message enqueue in the same transaction. */
  def appendUncheckedWithMessages(
    messages: List[OutgoingMessage],
    events: List[PendingEvent[A]]*,
  ): F[List[A]] =
    runAppend(None, events.flatten.toList, messages)

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

        rowStream.evalMap { case (seqNum, eventId, eventType, tags, payload, isExternal, headers, recordedAt) =>
          val eventTypeName = EventTypeName.fromString(eventType)
          parsePayload(eventTypeName, payload).map { event =>
            EventEnvelope(
              EventMetadata(
                globalPosition = seqNum, id = eventId, tags = tags, eventType = eventTypeName, isExternal = isExternal,
                timestamp = recordedAt.toInstant, headers = headers,
              ),
              event,
            )
          }
        }
      }
    }

  /** Returns a stream of notifications for new events appended to the store. Notifications are sent via PostgreSQL's
    * NOTIFY/LISTEN mechanism. Each notification payload is decoded into an [[EventStoreNotification]].
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

  private def runAppend(
    occ: Option[(EventFilter, Long)],
    flatEvents: List[PendingEvent[A]],
    messages: List[OutgoingMessage],
  ): F[List[A]] =
    if flatEvents.isEmpty && messages.isEmpty then Async[F].pure(List.empty)
    else
      pool.use { session =>
        session.transaction.use { _ =>
          for
            _ <- occ match
                   case Some((filter, expectedIndex)) if flatEvents.nonEmpty =>
                     acquireAppendLocks(session, filter.tags) *>
                       checkForConflicts(session, filter.tags, filter.eventTypes, expectedIndex)
                   case _ => Async[F].unit
            _ <- insertAll(session, flatEvents)
            _ <- enqueueMessages(session, messages)
            _ <-
              if flatEvents.nonEmpty then
                session.channel(channelId).notify(PostgresNotification.encode(EventStoreNotification.EventsAppended))
              else Async[F].unit
            // Assumes the message relay listens on PostgresMessageOutbox's default channel, as PostgresModule wires
            // both sides. If the outbox is built with a custom channel, this notify would need to be told about it.
            _ <-
              if messages.nonEmpty then session.channel(PostgresMessageOutbox.NotificationChannel).notify("")
              else Async[F].unit
          yield flatEvents.map(_.payload)
        }
      }

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
    pending: PendingEvent[A],
  ): F[(Long, Boolean)] =
    val tags = pending.tags
    val tagsJson = tagsToJson(tags)
    val headersJson = headersToJson(pending.headers)
    val eventType = pending.eventType.value
    val isExternal = pending.isExternal
    for
      payloadJson <- encodePayload(pending.payload)
      result      <- pending.id match
                  case Some(eventId) =>
                    session
                      .option(insertEventWithIdQuery)(
                        eventId *: eventType *: tagsJson *: payloadJson *: isExternal *: headersJson *: EmptyTuple,
                      )
                      .flatMap {
                        case Some(pos) => Async[F].pure((pos, true))
                        case None      =>
                          session.unique(selectSequenceNumberByEventIdQuery)(eventId).map((_, false))
                      }
                  case None =>
                    session
                      .unique(insertEventQuery)(
                        eventType *: tagsJson *: payloadJson *: isExternal *: headersJson *: EmptyTuple,
                      )
                      .map((_, true))
      (sequenceNumber, insertedNow) = result
      _                            <- if insertedNow then insertEventTags(session, sequenceNumber, tags) else Async[F].unit
    yield (sequenceNumber, insertedNow)

  private def insertAll(session: Session[F], flatEvents: List[PendingEvent[A]]): F[Unit] =
    flatEvents.traverse_ { pending =>
      insertEvent(session, pending).flatMap { case (pos, insertedNow) =>
        if insertedNow && !pending.isExternal then enqueueOutbox(session, pos) else Async[F].unit
      }
    }

  /** Run the codec and parse its String output into circe `Json`. Both stages have explicit failure channels; either
    * failure is raised into `F` so the surrounding transaction rolls back.
    */
  private def encodePayload(event: A): F[Json] =
    codec.encode(event) match
      case Left(error) =>
        Async[F].raiseError(new RuntimeException(s"EventCodec failed to encode event: ${error.getMessage}", error))
      case Right(encoded) =>
        parseJson(encoded) match
          case Right(json) => Async[F].pure(json)
          case Left(error) =>
            Async[F].raiseError(
              new RuntimeException(s"EventCodec produced invalid JSON: ${error.getMessage}", error),
            )

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

  /** Insert an outbox row for the given event within the appending transaction, when outbox publishing is enabled.
    * Running on the same session keeps the outbox row and the event row in the same transaction.
    */
  private def enqueueOutbox(session: Session[F], globalPosition: Long): F[Unit] =
    if outboxEnabled then session.execute(PostgresOutbox.insertCommand)(globalPosition).void
    else Async[F].unit

  private def enqueueMessages(session: Session[F], messages: List[OutgoingMessage]): F[Unit] =
    if messages.isEmpty then Async[F].unit
    else session.prepare(PostgresMessageOutbox.insertCommand).flatMap(cmd => messages.traverse_(cmd.execute))

  private def parsePayload(eventType: EventTypeName, payload: Json): F[A] =
    codec.decode(eventType, payload.noSpaces) match
      case Right(event) => Async[F].pure(event)
      case Left(error)  => Async[F].raiseError(error)

  private def tagsToJson(tags: Set[Tag]): Json =
    Json.arr(tags.map(t => Json.fromString(t.value)).toSeq*)

  private def headersToJson(headers: Map[String, String]): Json =
    Json.obj(headers.view.mapValues(Json.fromString).toSeq*)

object PostgresEventStore:

  /** The PostgreSQL channel name used for NOTIFY/LISTEN event notifications. */
  val NotificationChannel: Identifier =
    Identifier
      .fromValue("persistent4s_events")
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
    * @param outboxEnabled
    *   when true, every appended event also enqueues a row in `event_outbox` for later publication; defaults to false
    *   so callers who don't use the outbox pay no overhead and don't need the table
    * @return
    *   a new PostgresEventStore instance
    */
  def apply[F[_]: Async, A <: Event](
    pool: Resource[F, Session[F]],
    codec: EventCodec[A],
    channelId: Identifier = NotificationChannel,
    outboxEnabled: Boolean = false,
  ): PostgresEventStore[F, A] =
    new PostgresEventStore[F, A](pool, codec, channelId, outboxEnabled)

  private val tagsCodec: Codec[Set[Tag]] =
    jsonb.eimap[Set[Tag]] { json =>
      json.asArray.toRight(s"tags column is not a JSON array: ${json.noSpaces}").flatMap { values =>
        values.toList.traverse { v =>
          v.asString
            .toRight(s"tag element is not a string: ${v.noSpaces}")
            .flatMap(s => Tag.fromString(s).toRight(s"invalid tag: '$s'"))
        }.map(_.toSet)
      }
    }(tags => Json.arr(tags.map(t => Json.fromString(t.value)).toSeq*))

  private val headersCodec: Codec[Map[String, String]] = jsonb.imap { json =>
    json.asObject
      .map(_.toMap.flatMap { case (k, v) => v.asString.map(k -> _) })
      .getOrElse(Map.empty)
  }(headers => Json.obj(headers.view.mapValues(Json.fromString).toSeq*))

  private[postgres] val eventDecoder: Decoder[
    Long *: UUID *: String *: Set[Tag] *: Json *: Boolean *: Map[String, String] *: OffsetDateTime *: EmptyTuple,
  ] =
    int8 *: uuid *: text *: tagsCodec *: jsonb *: bool *: headersCodec *: timestamptz

  private val acquireTagLockQuery: Query[String, String] =
    sql"""SELECT pg_advisory_xact_lock(hashtextextended($text, 0))::text""".query(text)

  private val insertEventQuery: Query[String *: Json *: Json *: Boolean *: Json *: EmptyTuple, Long] =
    sql"""
      INSERT INTO events (event_type, tags, payload, is_external, headers)
      VALUES ($text, $jsonb, $jsonb, $bool, $jsonb)
      RETURNING sequence_number
    """.query(int8)

  private val insertEventWithIdQuery: Query[UUID *: String *: Json *: Json *: Boolean *: Json *: EmptyTuple, Long] =
    sql"""
      INSERT INTO events (event_id, event_type, tags, payload, is_external, headers)
      VALUES ($uuid, $text, $jsonb, $jsonb, $bool, $jsonb)
      ON CONFLICT (event_id) DO NOTHING
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

  private[postgres] type EventRow =
    Long *: UUID *: String *: Set[Tag] *: Json *: Boolean *: Map[String, String] *: OffsetDateTime *: EmptyTuple

  private val readAllQuery: Query[Long, EventRow] =
    sql"""
      SELECT sequence_number, event_id, event_type, tags, payload, is_external, headers, recorded_at
      FROM events
      WHERE sequence_number > $int8
      ORDER BY sequence_number ASC
    """.query(eventDecoder)

  private val readAllLimitedQuery: Query[Long *: Int *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_id, event_type, tags, payload, is_external, headers, recorded_at
      FROM events
      WHERE sequence_number > $int8
      ORDER BY sequence_number ASC
      LIMIT $int4
    """.query(eventDecoder)

  private def readByEventTypesQuery(
    numEventTypes: Int,
  ): Query[Long *: List[String] *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_id, event_type, tags, payload, is_external, headers, recorded_at
      FROM events
      WHERE sequence_number > $int8
        AND event_type = ANY(ARRAY[${text.list(numEventTypes)}])
      ORDER BY sequence_number ASC
    """.query(eventDecoder)

  private def readByEventTypesLimitedQuery(
    numEventTypes: Int,
  ): Query[Long *: List[String] *: Int *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_id, event_type, tags, payload, is_external, headers, recorded_at
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
      SELECT e.sequence_number, e.event_id, e.event_type, e.tags, e.payload, e.is_external, e.headers, e.recorded_at
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
      SELECT e.sequence_number, e.event_id, e.event_type, e.tags, e.payload, e.is_external, e.headers, e.recorded_at
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
      SELECT e.sequence_number, e.event_id, e.event_type, e.tags, e.payload, e.is_external, e.headers, e.recorded_at
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
      SELECT e.sequence_number, e.event_id, e.event_type, e.tags, e.payload, e.is_external, e.headers, e.recorded_at
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

  private val selectSequenceNumberByEventIdQuery: Query[UUID, Long] =
    sql"""
      SELECT sequence_number
      FROM events
      WHERE event_id = $uuid
    """.query(int8)
