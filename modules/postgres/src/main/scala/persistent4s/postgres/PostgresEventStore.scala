/*
 * Copyright 2026 persistent4s
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
      pool.use { session =>
        session.transaction.use { _ =>
          for
            _ <- checkForConflicts(session, flatEvents, expectedIndex)
            _ <- flatEvents.traverse_ { case (tags, eventType, event) =>
                   insertEvent(session, tags, eventType, event)
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

      // Execute the appropriate query based on filter combination
      val eventsF: F[List[EventRow]] =
        (eventTypesList.isEmpty, tagsList.isEmpty) match
          case (true, true) =>
            // No filters - fetch all events
            session.execute(readAllQuery)(fromPosition)
          case (false, true) =>
            // Filter by event types only
            session.execute(readByEventTypesQuery(eventTypesList.size))(
              fromPosition *: eventTypesList *: EmptyTuple,
            )
          case (true, false) =>
            // Filter by tags only
            session.execute(readByTagsQuery(tagsList.size))(
              fromPosition *: tagsList *: EmptyTuple,
            )
          case (false, false) =>
            // Filter by both event types and tags
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
    events: List[(Set[Tag], String, A)],
    expectedIndex: Long,
  ): F[Unit] =
    val allTags = events.flatMap(_._1).toSet
    if allTags.isEmpty then Async[F].unit
    else
      val tagList = allTags.map(_.value).toList
      for
        conflictCount <- session.unique(conflictCountQuery(tagList.size))(
                           expectedIndex *: tagList *: EmptyTuple,
                         )
        _ <-
          if conflictCount > 0 then
            session
              .unique(lastSequenceByTagsQuery(tagList.size))(tagList)
              .flatMap { actualIndex =>
                Async[F].raiseError(
                  IndexConflictException(expectedIndex, actualIndex),
                )
              }
          else Async[F].unit
      yield ()

  private def insertEvent(
    session: Session[F],
    tags: Set[Tag],
    eventType: String,
    event: A,
  ): F[Unit] =
    val tagsJson = tagsToJson(tags)
    val payloadJson = parseJson(codec.encode(event)).getOrElse(Json.obj())
    session
      .execute(insertEventCommand)(
        eventType *: tagsJson *: payloadJson *: EmptyTuple,
      )
      .void

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

  // Codec for parsing tags from JSONB
  private val tagsCodec: Codec[Set[Tag]] = jsonb.imap { json =>
    json.asArray
      .map(_.flatMap(_.asString).flatMap(Tag.fromString).toSet)
      .getOrElse(Set.empty)
  }(tags => Json.arr(tags.map(t => Json.fromString(t.value)).toSeq*))

  // Decoder for reading events
  private val eventDecoder: Decoder[
    Long *: String *: Set[Tag] *: Json *: java.time.OffsetDateTime *: EmptyTuple,
  ] =
    int8 *: text *: tagsCodec *: jsonb *: timestamptz

  // Command to insert a new event
  private val insertEventCommand: Command[String *: Json *: Json *: EmptyTuple] =
    sql"""
      INSERT INTO events (event_type, tags, payload)
      VALUES ($text, $jsonb, $jsonb)
    """.command

  // Query to count conflicting events (events with matching tags after expected index)
  private def conflictCountQuery(
    n: Int,
  ): Query[Long *: List[String] *: EmptyTuple, Long] =
    sql"""
      SELECT COUNT(*)
      FROM events
      WHERE sequence_number > $int8
        AND jsonb_exists_any(tags, ${text.list(n)})
    """.query(int8)

  // Query to get the last sequence number for events with matching tags
  private def lastSequenceByTagsQuery(n: Int): Query[List[String], Long] =
    sql"""
      SELECT COALESCE(MAX(sequence_number), 0)
      FROM events
      WHERE jsonb_exists_any(tags, ${text.list(n)})
    """.query(int8)

  // Type alias for event row tuple
  private type EventRow =
    Long *: String *: Set[Tag] *: Json *: java.time.OffsetDateTime *: EmptyTuple

  // Query to read all events (no filters)
  private val readAllQuery: Query[Long, EventRow] =
    sql"""
      SELECT sequence_number, event_type, tags, payload, recorded_at
      FROM events
      WHERE sequence_number > $int8
      ORDER BY sequence_number ASC
    """.query(eventDecoder)

  // Query to read events filtered by event types only
  private def readByEventTypesQuery(
    numEventTypes: Int,
  ): Query[Long *: List[String] *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_type, tags, payload, recorded_at
      FROM events
      WHERE sequence_number > $int8
        AND event_type = ANY(${text.list(numEventTypes)})
      ORDER BY sequence_number ASC
    """.query(eventDecoder)

  // Query to read events filtered by tags only
  private def readByTagsQuery(
    numTags: Int,
  ): Query[Long *: List[String] *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_type, tags, payload, recorded_at
      FROM events
      WHERE sequence_number > $int8
        AND jsonb_exists_any(tags, ${text.list(numTags)})
      ORDER BY sequence_number ASC
    """.query(eventDecoder)

  // Query to read events filtered by both event types and tags
  private def readByBothQuery(
    numEventTypes: Int,
    numTags: Int,
  ): Query[Long *: List[String] *: List[String] *: EmptyTuple, EventRow] =
    sql"""
      SELECT sequence_number, event_type, tags, payload, recorded_at
      FROM events
      WHERE sequence_number > $int8
        AND event_type = ANY(${text.list(numEventTypes)})
        AND jsonb_exists_any(tags, ${text.list(numTags)})
      ORDER BY sequence_number ASC
    """.query(eventDecoder)
