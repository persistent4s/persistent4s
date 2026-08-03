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

import cats.effect.*
import cats.effect.std.{SecureRandom, UUIDGen}
import cats.syntax.all.*

import fs2.Stream

import io.circe.Json
import io.circe.parser.parse as parseJson

import persistent4s.*

import skunk.*
import skunk.circe.codec.all.jsonb
import skunk.codec.all.*
import skunk.data.Identifier
import skunk.implicits.*

/** A PostgreSQL-backed implementation of the EventStore trait. This implementation uses Skunk for database access and
  * implements optimistic concurrency control for event appending.
  *
  * Events are stored in a table with the following schema:
  *   - sequence_number: BIGSERIAL PRIMARY KEY (global position)
  *   - event_type: TEXT NOT NULL
  *   - event_version: INTEGER NOT NULL
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
  *   the event codec for serializing/deserializing events; encoded payload strings must be valid JSON
  */
/** A pending event's storage form: the schema the codec owns plus its payload parsed as JSON. */
final private case class EncodedStorageRow(
  eventType: EventTypeName,
  version: Int,
  payload: Json,
)

final class PostgresEventStore[F[_]: Async: SecureRandom, A <: Event] private (
  pool: Resource[F, Session[F]],
  codec: EventCodec[A],
  channelId: Identifier,
) extends EventStore[F, A]
    with EventNotification[F]:

  import PostgresEventStore.*

  override def storageSchema(event: A): Option[EventStorageSchema] =
    Some(EventStorageSchema(codec.eventType(event), codec.eventVersion(event)))

  override def currentRevision(eventFilter: EventFilter): F[Long] =
    pool.use(session => currentRevision(session, eventFilter.tags, eventFilter.eventTypes))

  override def append(
    eventFilter: EventFilter,
    expectedIndex: Long,
    events: List[PendingEvent[A]]*,
  ): F[List[EventEnvelope[A]]] =
    val flatEvents = events.flatten.toList
    if flatEvents.isEmpty then Async[F].pure(List.empty)
    else
      val allTags = eventFilter.tags
      val eventTypes = eventFilter.eventTypes
      pool.use { session =>
        session.transaction.use { _ =>
          for
            _         <- acquireAppendLocks(session, allTags)
            _         <- checkForConflicts(session, allTags, eventTypes, expectedIndex)
            envelopes <- insertEvents(session, flatEvents)
            _         <- session.channel(channelId).notify(PostgresNotification.encode(EventStoreNotification.EventsAppended))
          yield envelopes
        }
      }

  override def appendUnchecked(
    events: List[PendingEvent[A]]*,
  ): F[List[EventEnvelope[A]]] =
    val flatEvents = events.flatten.toList
    if flatEvents.isEmpty then Async[F].pure(List.empty)
    else
      pool.use { session =>
        session.transaction.use { _ =>
          for
            envelopes <- insertEvents(session, flatEvents)
            _         <- session.channel(channelId).notify(PostgresNotification.encode(EventStoreNotification.EventsAppended))
          yield envelopes
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

        rowStream.evalMap {
          case (seqNum, eventId, eventType, eventVersion, tags, payload, isExternal, headers, recordedAt) =>
            val eventTypeName = EventTypeName.fromString(eventType)
            parsePayload(eventTypeName, eventVersion, payload).map { event =>
              EventEnvelope(
                EventMetadata(
                  globalPosition = seqNum, id = eventId, tags = tags, eventType = eventTypeName,
                  isExternal = isExternal, timestamp = recordedAt.toInstant, headers = headers,
                  eventVersion = eventVersion,
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
    for
      actualIndex <- currentRevision(session, tags, eventTypes)
      _           <-
        if actualIndex != expectedIndex then Async[F].raiseError(IndexConflictException(expectedIndex, actualIndex))
        else Async[F].unit
    yield ()

  private def currentRevision(
    session: Session[F],
    tags: Set[Tag],
    eventTypes: Set[EventTypeName],
  ): F[Long] =
    val tagList = tags.toList.map(_.value)
    val eventTypeList = eventTypes.toList.map(_.value)

    (tagList.isEmpty, eventTypeList.isEmpty) match
      case (true, true) =>
        session.unique(lastSequenceQuery)
      case (true, false) =>
        session.unique(lastSequenceByEventTypesQuery(eventTypeList.size))(eventTypeList)
      case (false, true) =>
        session.unique(lastSequenceByTagsQuery(tagList.size))(tagList)
      case (false, false) =>
        session.unique(lastSequenceByBothQuery(tagList.size, eventTypeList.size))(
          tagList *: eventTypeList *: EmptyTuple,
        )

  private def acquireAppendLocks(
    session: Session[F],
    tags: Set[Tag],
  ): F[Unit] =
    tags.toList
      .sortBy(_.value)
      .traverse_(tag => session.unique(acquireTagLockQuery)(tag.value).void)

  private def insertEvents(
    session: Session[F],
    events: List[PendingEvent[A]],
  ): F[List[EventEnvelope[A]]] =
    for
      encoded  <- events.traverse(pending => encodeForStorage(pending).map(pending -> _))
      resolved <- encoded.traverse { case (pending, enc) =>
                    resolveId(pending.id).map(id => (id, pending, enc))
                  }
      uniqueByEventId = resolved.distinctBy(_._1)
      rows            = uniqueByEventId.map { case (id, pending, enc) =>
               id *: enc.eventType.value *: enc.version *: tagsToJson(pending.tags) *: enc.payload *:
                 pending.isExternal *: headersToJson(pending.headers) *: EmptyTuple
             }
      idToRow <- chunked(rows, paramsPerRow = 7)
                   .flatTraverse(chunk => session.execute(insertEventsQuery(chunk.size))(chunk))
                   .map(_.map { case id *: seq *: recordedAt *: EmptyTuple => id -> (seq, recordedAt) }.toMap)
      tagPairs = uniqueByEventId.flatMap { case (id, pending, _) =>
                   pending.tags.toList.map(tag => tag.value *: idToRow(id)._1 *: EmptyTuple)
                 }
      _ <- chunked(tagPairs, paramsPerRow = 2)
             .traverse_(chunk => session.execute(insertEventTagsCommand(chunk.size))(chunk).void)
    yield resolved.map { case (id, pending, enc) =>
      val (sequenceNumber, recordedAt) = idToRow(id)
      EventEnvelope(
        EventMetadata(
          globalPosition = sequenceNumber, id = id, tags = pending.tags, eventType = enc.eventType,
          isExternal = pending.isExternal, timestamp = recordedAt.toInstant, headers = pending.headers,
          eventVersion = enc.version,
        ),
        pending.payload,
      )
    }

  /** The storage form of a pending event: the schema the codec owns plus its payload as JSON.
    *
    * Fails fast when the caller's declared `eventType` disagrees with the codec's registered schema (the legacy
    * class-name identity is still accepted), and surfaces payload-encoding errors instead of silently writing `{}`.
    */
  private def encodeForStorage(pending: PendingEvent[A]): F[EncodedStorageRow] =
    val encoded = codec.encodeWithSchema(pending.payload)
    val storage = EventStorageSchema(encoded.eventType, encoded.version)
    val declared = EventStorageSchema(pending.eventType, encoded.version)
    val legacyEventType = EventTypeName.fromInstance(pending.payload)

    if pending.eventType != encoded.eventType && pending.eventType != legacyEventType then
      Async[F].raiseError(EventSchemaMismatch(declared, storage, pending.payload.getClass.getName))
    else
      parseJson(encoded.payload) match
        case Left(error)        => Async[F].raiseError(error)
        case Right(payloadJson) => Async[F].pure(EncodedStorageRow(encoded.eventType, encoded.version, payloadJson))

  private def resolveId(idOpt: Option[UUID]): F[UUID] = idOpt.fold(UUIDGen.randomUUID[F])(_.pure[F])

  private def chunked[T](rows: List[T], paramsPerRow: Int): List[List[T]] =
    rows.grouped(MaxUsableBindParams / paramsPerRow).toList

  private def parsePayload(eventType: EventTypeName, eventVersion: Int, payload: Json): F[A] =
    codec.decode(eventType, eventVersion, payload.noSpaces) match
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

  /** Max number of parameters supported by PostgreSQL within a query */
  private val MaxBindParams: Int = 32767

  private val BindParamSafetyMargin: Int = 16

  private val MaxUsableBindParams: Int = MaxBindParams - BindParamSafetyMargin

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
  def apply[F[_]: Async: SecureRandom, A <: Event](
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

  private val headersCodec: Codec[Map[String, String]] = jsonb.imap { json =>
    json.asObject
      .map(_.toMap.flatMap { case (k, v) => v.asString.map(k -> _) })
      .getOrElse(Map.empty)
  }(headers => Json.obj(headers.view.mapValues(Json.fromString).toSeq*))

  private val eventDecoder: Decoder[
    Long *: UUID *: String *: Int *: Set[Tag] *: Json *: Boolean *: Map[String, String] *: OffsetDateTime *: EmptyTuple,
  ] =
    int8 *: uuid *: text *: int4 *: tagsCodec *: jsonb *: bool *: headersCodec *: timestamptz

  private val acquireTagLockQuery: Query[String, String] =
    sql"""SELECT pg_advisory_xact_lock(hashtextextended($text, 0))::text""".query(text)

  private def insertEventsQuery(
    n: Int,
  ): Query[
    List[UUID *: String *: Int *: Json *: Json *: Boolean *: Json *: EmptyTuple],
    UUID *: Long *: OffsetDateTime *: EmptyTuple,
  ] =
    val rows = (uuid *: text *: int4 *: jsonb *: jsonb *: bool *: jsonb).values.list(n)
    sql"""
      INSERT INTO events (event_id, event_type, event_version, tags, payload, is_external, headers)
      VALUES $rows
      ON CONFLICT (event_id) DO UPDATE SET event_id = EXCLUDED.event_id
      RETURNING event_id, sequence_number, recorded_at
    """.query(uuid *: int8 *: timestamptz)

  private def insertEventTagsCommand(
    n: Int,
  ): Command[List[String *: Long *: EmptyTuple]] =
    val pairs = (text *: int8).values.list(n)
    sql"""
      INSERT INTO event_tags (tag, sequence_number)
      VALUES $pairs
      ON CONFLICT DO NOTHING
    """.command

  private val lastSequenceQuery: Query[Void, Long] =
    sql"""
      SELECT COALESCE(MAX(sequence_number), 0)
      FROM events
    """.query(int8)

  private def lastSequenceByEventTypesQuery(
    numEventTypes: Int,
  ): Query[List[String], Long] =
    sql"""
      SELECT COALESCE(MAX(sequence_number), 0)
      FROM events
      WHERE event_type = ANY(ARRAY[${text.list(numEventTypes)}])
    """.query(int8)

  private def lastSequenceByTagsQuery(
    numTags: Int,
  ): Query[List[String], Long] =
    sql"""
      SELECT COALESCE(MAX(et.sequence_number), 0)
      FROM event_tags et
      WHERE et.tag = ANY(ARRAY[${text.list(numTags)}])
    """.query(int8)

  private def lastSequenceByBothQuery(
    numTags: Int,
    numEventTypes: Int,
  ): Query[List[String] *: List[String] *: EmptyTuple, Long] =
    sql"""
      SELECT COALESCE(MAX(et.sequence_number), 0)
      FROM event_tags et
      JOIN events e ON e.sequence_number = et.sequence_number
      WHERE et.tag = ANY(ARRAY[${text.list(numTags)}])
        AND e.event_type = ANY(ARRAY[${text.list(numEventTypes)}])
    """.query(int8)

  private type EventRow =
    Long *: UUID *: String *: Int *: Set[Tag] *: Json *: Boolean *: Map[String, String] *: OffsetDateTime *: EmptyTuple

  private val readAllQuery: Query[Long, EventRow] =
    sql"""
      SELECT sequence_number, event_id, event_type, event_version, tags, payload, is_external, headers, recorded_at
      FROM events
      WHERE sequence_number > $int8
      ORDER BY sequence_number ASC
    """.query(eventDecoder)

  private val readAllLimitedQuery: Query[Long *: Int *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_id, event_type, event_version, tags, payload, is_external, headers, recorded_at
      FROM events
      WHERE sequence_number > $int8
      ORDER BY sequence_number ASC
      LIMIT $int4
    """.query(eventDecoder)

  private def readByEventTypesQuery(
    numEventTypes: Int,
  ): Query[Long *: List[String] *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_id, event_type, event_version, tags, payload, is_external, headers, recorded_at
      FROM events
      WHERE sequence_number > $int8
        AND event_type = ANY(ARRAY[${text.list(numEventTypes)}])
      ORDER BY sequence_number ASC
    """.query(eventDecoder)

  private def readByEventTypesLimitedQuery(
    numEventTypes: Int,
  ): Query[Long *: List[String] *: Int *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_id, event_type, event_version, tags, payload, is_external, headers, recorded_at
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
      SELECT e.sequence_number, e.event_id, e.event_type, e.event_version, e.tags, e.payload, e.is_external,
             e.headers, e.recorded_at
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
      SELECT e.sequence_number, e.event_id, e.event_type, e.event_version, e.tags, e.payload, e.is_external,
             e.headers, e.recorded_at
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
      SELECT e.sequence_number, e.event_id, e.event_type, e.event_version, e.tags, e.payload, e.is_external,
             e.headers, e.recorded_at
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
      SELECT e.sequence_number, e.event_id, e.event_type, e.event_version, e.tags, e.payload, e.is_external,
             e.headers, e.recorded_at
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
