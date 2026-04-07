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

import java.time.Instant

import cats.effect.*
import cats.effect.std.Console
import cats.syntax.all.*
import fs2.Stream
import fs2.io.net.Network
import natchez.Trace
import skunk.*
import skunk.codec.all.*
import skunk.data.Arr
import skunk.implicits.*

import persistent4s.*

final class PostgresEventStore[F[_]: Temporal: Trace, A] private (
  pool: Resource[F, Session[F]],
  codec: EventCodec[A],
) extends EventStore[F, A]:

  override def append(expectedIndex: Long, events: List[(Set[Tag], String, A)]*): F[Unit] =
    pool.use { session =>
      session.transaction.use { _ =>
        val allEvents = events.flatten
        val incomingTags = Arr.fromFoldable(allEvents.flatMap(_._1).map(_.value))

        for
          actualIndex <- session
                           .prepare(Queries.maxPositionForTags)
                           .flatMap(_.unique(incomingTags))

          _ <-
            if actualIndex != expectedIndex then
              Temporal[F].raiseError(IndexConflictException(expectedIndex, actualIndex))
            else Temporal[F].unit

          _ <- allEvents.traverse_ { case (tags, eventType, event) =>
                 val tagArr = Arr.fromFoldable(tags.map(_.value).toList)
                 val payload = codec.encode(event)
                 session
                   .prepare(Queries.insertEvent)
                   .flatMap(_.execute(tagArr, eventType, payload))
                   .void
               }
        yield ()
      }
    }

  override def read(eventTypes: List[String], tags: Set[Tag]*): Stream[F, EventEnvelope[A]] =
    val tagArr = Arr.fromFoldable(tags.flatten.map(_.value))

    val rows: Stream[F, (Long, Arr[String], String, String, Instant)] =
      if eventTypes.isEmpty then
        Stream.resource(pool).flatMap { session =>
          Stream.eval(session.prepare(Queries.readByTags)).flatMap(_.stream(tagArr, 64))
        }
      else
        val typeArr = Arr.fromFoldable(eventTypes)
        Stream.resource(pool).flatMap { session =>
          Stream.eval(session.prepare(Queries.readByTagsAndTypes)).flatMap(_.stream((tagArr, typeArr), 64))
        }

    rows.evalMap { case (globalPosition, dbTags, eventType, payload, timestamp) =>
      val parsedTags = dbTags.flattenTo(List).flatMap(Tag.fromString).toSet
      codec.decode(payload) match
        case Right(event) =>
          EventEnvelope(
            EventMetadata(globalPosition, parsedTags, eventType, timestamp),
            event,
          ).pure[F]
        case Left(error) =>
          Temporal[F].raiseError(error)
    }

object PostgresEventStore:

  final case class Config(
    host: String,
    port: Int = 5450,
    database: String,
    user: String,
    password: String,
    maxConnections: Int = 10,
  )

  def make[F[_]: Temporal: Console: Network: Trace, A](
    config: Config,
    codec: EventCodec[A],
  ): Resource[F, PostgresEventStore[F, A]] =
    Session
      .pooled[F](
        host = config.host, port = config.port, database = config.database, user = config.user,
        password = Some(config.password), max = config.maxConnections,
      )
      .map(pool => new PostgresEventStore[F, A](pool, codec))

private object Queries:

  private val instantCodec: skunk.Codec[Instant] =
    timestamptz.imap(_.toInstant)(java.time.OffsetDateTime.ofInstant(_, java.time.ZoneOffset.UTC))

  val maxPositionForTags: Query[Arr[String], Long] =
    sql"""
      SELECT COALESCE(MAX(global_position), 0)
      FROM events
      WHERE tags && ${_text}
    """.query(int8)

  val insertEvent: Command[Arr[String] *: String *: String *: EmptyTuple] =
    sql"""
      INSERT INTO events (tags, event_type, payload)
      VALUES (${_text *: text *: text})
    """.command

  val readByTags: Query[Arr[String], (Long, Arr[String], String, String, Instant)] =
    sql"""
      SELECT global_position, tags, event_type, payload, timestamp
      FROM events
      WHERE tags && ${_text}
      ORDER BY global_position ASC
    """.query(int8 *: _text *: text *: text *: instantCodec)

  val readByTagsAndTypes: Query[(Arr[String], Arr[String]), (Long, Arr[String], String, String, Instant)] =
    sql"""
      SELECT global_position, tags, event_type, payload, timestamp
      FROM events
      WHERE tags && ${_text} AND event_type = ANY(${_text})
      ORDER BY global_position ASC
    """.query(int8 *: _text *: text *: text *: instantCodec)
