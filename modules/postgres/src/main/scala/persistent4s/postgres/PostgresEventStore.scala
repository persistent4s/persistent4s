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
import java.util.concurrent.TimeoutException
import scala.concurrent.duration.*

import cats.effect.*
import cats.effect.std.{SecureRandom, UUIDGen}
import cats.syntax.all.*

import fs2.Stream

import io.circe.Json
import io.circe.parser.parse as parseJson
import org.typelevel.log4cats.Logger

import persistent4s.*

import skunk.*
import skunk.circe.codec.all.jsonb
import skunk.codec.all.*
import skunk.data.{Arr, Identifier}
import skunk.implicits.*
import java.time.ZoneOffset

/** A pending event's storage form: the schema the codec owns plus its payload parsed as JSON. */
final private case class EncodedStorageRow(
  eventType: EventTypeName,
  version: Int,
  payload: Json,
)

final private case class SafeReadBoundary(cap: Option[Long], bridgedGapEnd: Option[Long])

final case class AppendTimeoutException(timeout: FiniteDuration, cause: Throwable)
    extends RuntimeException(
      s"Append did not complete within $timeout; the transaction was cancelled and rolled back. Any " +
        "sequence_number it reserved is now a permanent gap that readers bridge once gapTimeout elapses.",
      cause,
    )

/** A PostgreSQL-backed implementation of the EventStore trait. This implementation uses Skunk for database access and
  * implements optimistic concurrency control for event appending.
  *
  * Events are stored in a table with the following schema:
  *   - sequence_number: BIGSERIAL PRIMARY KEY (global position)
  *   - event_id: UUID NOT NULL UNIQUE (idempotency key)
  *   - event_type: TEXT NOT NULL
  *   - event_version: INTEGER NOT NULL
  *   - tags: JSONB NOT NULL (array of tag strings)
  *   - payload: JSONB NOT NULL
  *   - is_external: BOOLEAN NOT NULL
  *   - headers: JSONB NOT NULL
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
  * @param channelId
  *   the PostgreSQL channel identifier for NOTIFY/LISTEN (default: "persistent4s_events")
  * @param outboxEnabled
  *   when true, every newly-inserted non-external event also enqueues an outbox row in the same transaction
  * @param gapTimeout
  *   the duration after which a gap in sequence numbers is considered permanent and bridged by readers
  * @param appendTimeout
  *   the duration after which an append operation is considered to have failed and is rolled back
  */
final class PostgresEventStore[F[_]: Async: Logger: SecureRandom, A <: Event] private (
  pool: Resource[F, Session[F]],
  codec: EventCodec[A],
  channelId: Identifier,
  outboxEnabled: Boolean,
  gapTimeout: FiniteDuration,
  appendTimeout: FiniteDuration,
) extends EventStore[F, A]
    with EventNotification[F]
    with TransactionalMessages[F, A]:

  require(
    gapTimeout >= appendTimeout * 2,
    s"gapTimeout ($gapTimeout) must be at least 2x appendTimeout ($appendTimeout) — recorded_at reflects " +
      "transaction start, not commit, so a smaller ratio can let a reader bridge a gap before the transaction " +
      "that could still fill it is guaranteed to have resolved. A real margin beyond 2x (3-4x) is recommended to " +
      "absorb clock drift and scheduling jitter.",
  )

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
    runAppend(Some((eventFilter, expectedIndex)), events.flatten.toList, Nil)

  override def appendUnchecked(
    events: List[PendingEvent[A]]*,
  ): F[List[EventEnvelope[A]]] =
    runAppend(None, events.flatten.toList, Nil)

  /** Atomic append with optimistic-concurrency check, plus message enqueue in the same transaction. */
  override def appendWithMessages(
    eventFilter: EventFilter,
    expectedIndex: Long,
    messages: List[OutgoingMessage],
    events: List[PendingEvent[A]]*,
  ): F[List[EventEnvelope[A]]] =
    runAppend(Some((eventFilter, expectedIndex)), events.flatten.toList, messages)

  /** Atomic append without OCC, plus message enqueue in the same transaction. */
  override def appendUncheckedWithMessages(
    messages: List[OutgoingMessage],
    events: List[PendingEvent[A]]*,
  ): F[List[EventEnvelope[A]]] =
    runAppend(None, events.flatten.toList, messages)

  /** Shared transactional body for all four append entry points: optional OCC check, batched event insert, outbox and
    * message enqueues, then the notifications — all committed together or not at all.
    */
  private def runAppend(
    occ: Option[(EventFilter, Long)],
    flatEvents: List[PendingEvent[A]],
    messages: List[OutgoingMessage],
  ): F[List[EventEnvelope[A]]] =
    if flatEvents.isEmpty && messages.isEmpty then Async[F].pure(List.empty)
    else
      pool.use { session =>
        Async[F]
          .timeout(
            session.transaction.use { _ =>
              for
                _ <- occ match
                       case Some((filter, expectedIndex)) if flatEvents.nonEmpty =>
                         acquireAppendLocks(session, filter.tags) *>
                           checkForConflicts(session, filter.tags, filter.eventTypes, expectedIndex)
                       case _ => Async[F].unit
                envelopes <-
                  if flatEvents.nonEmpty then insertEvents(session, flatEvents) else Async[F].pure(List.empty)
                _ <- enqueueMessages(session, messages)
                _ <-
                  if flatEvents.nonEmpty then
                    session
                      .channel(channelId)
                      .notify(PostgresNotification.encode(EventStoreNotification.EventsAppended))
                  else Async[F].unit
                // Assumes the message relay listens on PostgresMessageOutbox's default channel, as PostgresModule wires
                // both sides. If the outbox is built with a custom channel, this notify would need to be told about it.
                _ <-
                  if messages.nonEmpty then session.channel(PostgresMessageOutbox.NotificationChannel).notify("")
                  else Async[F].unit
              yield envelopes
            },
            appendTimeout,
          )
          .adaptError { case e: TimeoutException => AppendTimeoutException(appendTimeout, e) }
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
        Stream.eval(computeSafeBoundary(session, fromPosition, maxEvents)).flatMap {
          case SafeReadBoundary(None, _)                           => Stream.empty
          case SafeReadBoundary(Some(safeBoundary), bridgedGapEnd) =>
            val logBridgedGap = bridgedGapEnd match
              case Some(endPos) =>
                Logger[F].warn(
                  s"readFrom($fromPosition, $eventFilter) bridged a permanently-skipped sequence_number gap ending " +
                    s"just before $endPos",
                )
              case None => Async[F].unit

            Stream.eval(logBridgedGap) >> {

              val rowStream: Stream[F, EventRow] =
                (eventTypesList.isEmpty, tagsList.isEmpty, maxEvents) match
                  case (true, true, None) =>
                    Stream
                      .eval(session.prepare(readAllQuery))
                      .flatMap(
                        _.stream(fromPosition *: safeBoundary *: EmptyTuple, FetchSize),
                      )
                  case (true, true, Some(max)) =>
                    Stream
                      .eval(session.prepare(readAllLimitedQuery))
                      .flatMap(
                        _.stream(fromPosition *: safeBoundary *: max *: EmptyTuple, FetchSize),
                      )
                  case (false, true, None) =>
                    Stream
                      .eval(session.prepare(readByEventTypesQuery(eventTypesList.size)))
                      .flatMap(
                        _.stream(fromPosition *: safeBoundary *: eventTypesList *: EmptyTuple, FetchSize),
                      )
                  case (false, true, Some(max)) =>
                    Stream
                      .eval(session.prepare(readByEventTypesLimitedQuery(eventTypesList.size)))
                      .flatMap(
                        _.stream(fromPosition *: safeBoundary *: eventTypesList *: max *: EmptyTuple, FetchSize),
                      )
                  case (true, false, None) =>
                    Stream
                      .eval(session.prepare(readByTagsQuery(tagsList.size)))
                      .flatMap(
                        _.stream(fromPosition *: safeBoundary *: tagsList *: EmptyTuple, FetchSize),
                      )
                  case (true, false, Some(max)) =>
                    Stream
                      .eval(session.prepare(readByTagsLimitedQuery(tagsList.size)))
                      .flatMap(
                        _.stream(fromPosition *: safeBoundary *: tagsList *: max *: EmptyTuple, FetchSize),
                      )
                  case (false, false, None) =>
                    Stream
                      .eval(session.prepare(readByBothQuery(eventTypesList.size, tagsList.size)))
                      .flatMap(
                        _.stream(fromPosition *: safeBoundary *: eventTypesList *: tagsList *: EmptyTuple, FetchSize),
                      )
                  case (false, false, Some(max)) =>
                    Stream
                      .eval(session.prepare(readByBothLimitedQuery(eventTypesList.size, tagsList.size)))
                      .flatMap(
                        _.stream(
                          fromPosition *: safeBoundary *: eventTypesList *: tagsList *: max *: EmptyTuple,
                          FetchSize,
                        ),
                      )
              rowStream.evalMap {
                case (seqNum, eventId, eventType, eventVersion, tags, payload, isExternal, headers, recordedAt) =>
                  val eventTypeName = EventTypeName.fromString(eventType)
                  parsePayload(seqNum, eventTypeName, eventVersion, payload).map { event =>
                    EventEnvelope(
                      EventMetadata(globalPosition = seqNum, id = eventId, tags = tags, eventType = eventTypeName,
                        isExternal = isExternal, timestamp = recordedAt.toInstant, headers = headers,
                        eventVersion = eventVersion),
                      event,
                    )
                  }
              }
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
        session.unique(lastSequenceByTagsQuery)(Arr(tagList*))
      case (false, false) =>
        session.unique(lastSequenceByBothQuery(tagList.size, eventTypeList.size))(
          tagList *: eventTypeList *: EmptyTuple,
        )

  private def computeSafeBoundary(
    session: Session[F],
    fromPosition: Long,
    maxEvents: Option[Int],
  ): F[SafeReadBoundary] =
    Clock[F].realTimeInstant.flatMap { now =>
      val cutoff = OffsetDateTime.ofInstant(now.minusMillis(gapTimeout.toMillis), ZoneOffset.UTC)
      val lookahead = maxEvents.getOrElse(UnboundedGapLookahead)
      session
        .prepare(safeBoundaryQuery)
        .flatMap(_.unique(fromPosition *: fromPosition *: lookahead *: cutoff *: cutoff *: EmptyTuple))
        .map { case cap *: bridgedGapEnd *: EmptyTuple => SafeReadBoundary(cap, bridgedGapEnd) }
    }

  private def acquireAppendLocks(
    session: Session[F],
    tags: Set[Tag],
  ): F[Unit] =
    val sortedTags = tags.toList.map(_.value).sorted
    if sortedTags.isEmpty then Async[F].unit
    else session.execute(acquireTagLocksQuery)(Arr(sortedTags*)).void

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
                   .map(_.map { case id *: seq *: recordedAt *: inserted *: EmptyTuple =>
                     id -> (seq, recordedAt, inserted)
                   }.toMap)
      tagPairs = uniqueByEventId.flatMap { case (id, pending, _) =>
                   pending.tags.toList.map(tag => tag.value *: idToRow(id)._1 *: EmptyTuple)
                 }
      _ <- chunked(tagPairs, paramsPerRow = 2)
             .traverse_(chunk => session.execute(insertEventTagsCommand(chunk.size))(chunk).void)
      // Only genuinely-new, non-external events get relayed: a redelivered event_id resolves to the existing row
      // (inserted = false) and must not be published a second time.
      _ <- enqueueOutbox(
             session,
             uniqueByEventId.collect {
               case (id, pending, _) if idToRow(id)._3 && !pending.isExternal => idToRow(id)._1
             },
           )
    yield resolved.map { case (id, pending, enc) =>
      val (sequenceNumber, recordedAt, _) = idToRow(id)
      EventEnvelope(
        EventMetadata(
          globalPosition = sequenceNumber, id = id, tags = pending.tags, eventType = enc.eventType,
          isExternal = pending.isExternal, timestamp = recordedAt.toInstant, headers = pending.headers,
          eventVersion = enc.version,
        ),
        pending.payload,
      )
    }

  private def enqueueOutbox(session: Session[F], globalPositions: List[Long]): F[Unit] =
    if !outboxEnabled || globalPositions.isEmpty then Async[F].unit
    else session.prepare(PostgresOutbox.insertCommand).flatMap(cmd => globalPositions.traverse_(cmd.execute))

  private def enqueueMessages(session: Session[F], messages: List[OutgoingMessage]): F[Unit] =
    if messages.isEmpty then Async[F].unit
    else session.prepare(PostgresMessageOutbox.insertCommand).flatMap(cmd => messages.traverse_(cmd.execute))

  /** The storage form of a pending event: the schema the codec owns plus its payload as JSON.
    *
    * Fails fast when the caller's declared `eventType` disagrees with the codec's registered schema (the legacy
    * class-name identity is still accepted), and surfaces payload-encoding errors instead of silently writing `{}`.
    */
  private def encodeForStorage(pending: PendingEvent[A]): F[EncodedStorageRow] =
    val schemaEventType = codec.eventType(pending.payload)
    val version = codec.eventVersion(pending.payload)
    val storage = EventStorageSchema(schemaEventType, version)
    val declared = EventStorageSchema(pending.eventType, version)
    val legacyEventType = EventTypeName.fromInstance(pending.payload)

    if pending.eventType != schemaEventType && pending.eventType != legacyEventType then
      Async[F].raiseError(EventSchemaMismatch(declared, storage, pending.payload.getClass.getName))
    else
      codec.encodeWithSchema(pending.payload) match
        case Left(error) =>
          Async[F].raiseError(new RuntimeException(s"EventCodec failed to encode event: ${error.getMessage}", error))
        case Right(encoded) =>
          parseJson(encoded.payload) match
            case Left(error)        => Async[F].raiseError(error)
            case Right(payloadJson) => Async[F].pure(EncodedStorageRow(encoded.eventType, encoded.version, payloadJson))

  private def resolveId(idOpt: Option[UUID]): F[UUID] = idOpt.fold(UUIDGen.randomUUID[F])(_.pure[F])

  private def chunked[T](rows: List[T], paramsPerRow: Int): List[List[T]] =
    rows.grouped(MaxUsableBindParams / paramsPerRow).toList

  private def parsePayload(
    globalPosition: Long,
    eventType: EventTypeName,
    eventVersion: Int,
    payload: Json,
  ): F[A] =
    codec.decode(eventType, eventVersion, payload.noSpaces) match
      case Right(event) => Async[F].pure(event)
      case Left(error)  =>
        Logger[F].error(error)(
          s"Failed to decode event of type ${eventType.value} at position $globalPosition",
        ) *> Async[F].raiseError(error)

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

  /** Lookahead cap for the gap-safety scan on a fully unbounded readfrom call, bounding its scan cost regardless of
    * table size.
    */
  private val UnboundedGapLookahead: Int = 8192

  /** Number of rows fetched per cursor round-trip when streaming events from PostgreSQL. */
  private val FetchSize: Int = 256

  private val DefaultGapTimeout = 6.seconds

  /** Create a new PostgresEventStore.
    *
    * @param pool
    *   a resource for obtaining database sessions
    * @param codec
    *   the event codec for serializing/deserializing events
    * @param channelId
    *   the PostgreSQL channel identifier for NOTIFY/LISTEN (default: "persistent4s_events")
    * @param outboxEnabled
    *   when true, every newly-inserted non-external event also enqueues an outbox row in the same transaction
    * @param gapTimeout
    *   the duration after which a gap in sequence numbers is considered permanent and bridged by readers
    * @param appendTimeout
    *   the duration after which an append operation is considered to have failed and is rolled back
    * @return
    *   a new PostgresEventStore instance
    */
  def apply[F[_]: Async: Logger: SecureRandom, A <: Event](
    pool: Resource[F, Session[F]],
    codec: EventCodec[A],
    channelId: Identifier = NotificationChannel,
    outboxEnabled: Boolean = false,
    gapTimeout: FiniteDuration = DefaultGapTimeout,
    appendTimeout: FiniteDuration = DefaultGapTimeout / 3,
  ): PostgresEventStore[F, A] =
    new PostgresEventStore[F, A](pool, codec, channelId, outboxEnabled, gapTimeout, appendTimeout)

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

  private[postgres] val eventDecoder: Decoder[EventRow] =
    int8 *: uuid *: text *: int4 *: tagsCodec *: jsonb *: bool *: headersCodec *: timestamptz

  private val acquireTagLocksQuery: Query[Arr[String], String] =
    sql"""
      SELECT pg_advisory_xact_lock(hashtextextended(t.tag, 0))::text
      FROM (SELECT unnest($_text) AS tag ORDER BY 1) AS t
    """.query(text)

  /** The no-op `DO UPDATE` makes a duplicate `event_id` still produce a `RETURNING` row, so the caller always gets the
    * committed envelope. `xmax = 0` then distinguishes a freshly inserted row from one that already existed: on a real
    * insert the row carries no updating transaction id, whereas the upsert path leaves `xmax` set. That flag is what
    * keeps the outbox from re-publishing a redelivered event.
    */
  private def insertEventsQuery(
    n: Int,
  ): Query[
    List[UUID *: String *: Int *: Json *: Json *: Boolean *: Json *: EmptyTuple],
    UUID *: Long *: OffsetDateTime *: Boolean *: EmptyTuple,
  ] =
    val rows = (uuid *: text *: int4 *: jsonb *: jsonb *: bool *: jsonb).values.list(n)
    sql"""
      INSERT INTO events (event_id, event_type, event_version, tags, payload, is_external, headers)
      VALUES $rows
      ON CONFLICT (event_id) DO UPDATE SET event_id = EXCLUDED.event_id
      RETURNING event_id, sequence_number, recorded_at, (xmax = 0) AS inserted
    """.query(uuid *: int8 *: timestamptz *: bool)

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

  private val lastSequenceByTagsQuery: Query[Arr[String], Long] =
    sql"""
      SELECT COALESCE(MAX(perTag.latest), 0)
      FROM unnest($_text) AS t(tag)
      CROSS JOIN LATERAL (
        SELECT MAX(et.sequence_number) AS latest
        FROM event_tags et
        WHERE et.tag = t.tag
      ) AS perTag
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

  // Widened so PostgresOutbox can reuse the same row shape and decoder for its join against `events`; keeping one
  // definition is what stops the two SELECT column lists from drifting apart.
  private[postgres] type EventRow =
    Long *: UUID *: String *: Int *: Set[Tag] *: Json *: Boolean *: Map[String, String] *: OffsetDateTime *: EmptyTuple

  private val safeBoundaryQuery: Query[Long *: Long *: Int *: OffsetDateTime *: OffsetDateTime *: EmptyTuple, Option[
    Long,
  ] *: Option[Long] *: EmptyTuple] =
    sql"""
      WITH candidates AS (
        SELECT sequence_number, recorded_at,
               sequence_number - LAG(sequence_number, 1, GREATEST($int8, 0)) OVER (ORDER BY sequence_number) AS gap_size
        FROM events
        WHERE sequence_number > $int8
        ORDER BY sequence_number
        LIMIT $int4
      )
      SELECT 
        COALESCE(MIN(sequence_number) FILTER (WHERE gap_size > 1 AND recorded_at > $timestamptz) - 1, MAX(sequence_number)),
        MIN(sequence_number) FILTER (WHERE gap_size > 1 AND recorded_at <= $timestamptz)
      FROM candidates
    """.query(int8.opt *: int8.opt)

  private val readAllQuery: Query[Long *: Long *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_id, event_type, event_version, tags, payload, is_external, headers, recorded_at
      FROM events
      WHERE sequence_number > $int8
        AND sequence_number <= $int8
      ORDER BY sequence_number ASC
    """.query(eventDecoder)

  private val readAllLimitedQuery: Query[Long *: Long *: Int *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_id, event_type, event_version, tags, payload, is_external, headers, recorded_at
      FROM events
      WHERE sequence_number > $int8
        AND sequence_number <= $int8
      ORDER BY sequence_number ASC
      LIMIT $int4
    """.query(eventDecoder)

  private def readByEventTypesQuery(
    numEventTypes: Int,
  ): Query[Long *: Long *: List[String] *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_id, event_type, event_version, tags, payload, is_external, headers, recorded_at
      FROM events
      WHERE sequence_number > $int8
        AND sequence_number <= $int8
        AND event_type = ANY(ARRAY[${text.list(numEventTypes)}])
      ORDER BY sequence_number ASC
    """.query(eventDecoder)

  private def readByEventTypesLimitedQuery(
    numEventTypes: Int,
  ): Query[Long *: Long *: List[String] *: Int *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_id, event_type, event_version, tags, payload, is_external, headers, recorded_at
      FROM events
      WHERE sequence_number > $int8 
        AND sequence_number <= $int8
        AND event_type = ANY(ARRAY[${text.list(numEventTypes)}])
      ORDER BY sequence_number ASC
      LIMIT $int4
    """.query(eventDecoder)

  private def readByTagsQuery(
    numTags: Int,
  ): Query[Long *: Long *: List[String] *: EmptyTuple, EventRow] =
    sql"""
      SELECT e.sequence_number, e.event_id, e.event_type, e.event_version, e.tags, e.payload, e.is_external,
             e.headers, e.recorded_at
      FROM events e
      WHERE e.sequence_number > $int8
        AND e.sequence_number <= $int8
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
  ): Query[Long *: Long *: List[String] *: Int *: EmptyTuple, EventRow] =
    sql"""
      SELECT e.sequence_number, e.event_id, e.event_type, e.event_version, e.tags, e.payload, e.is_external,
             e.headers, e.recorded_at
      FROM events e
      WHERE e.sequence_number > $int8
        AND e.sequence_number <= $int8
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
  ): Query[Long *: Long *: List[String] *: List[String] *: EmptyTuple, EventRow] =
    sql"""
      SELECT e.sequence_number, e.event_id, e.event_type, e.event_version, e.tags, e.payload, e.is_external,
             e.headers, e.recorded_at
      FROM events e
      WHERE e.sequence_number > $int8
        AND e.sequence_number <= $int8
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
  ): Query[Long *: Long *: List[String] *: List[String] *: Int *: EmptyTuple, EventRow] =
    sql"""
      SELECT e.sequence_number, e.event_id, e.event_type, e.event_version, e.tags, e.payload, e.is_external,
             e.headers, e.recorded_at
      FROM events e
      WHERE e.sequence_number > $int8
        AND e.sequence_number <= $int8
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
