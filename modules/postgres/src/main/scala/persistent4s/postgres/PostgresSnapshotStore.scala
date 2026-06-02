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

import persistent4s.SnapshotStore
import persistent4s.Tag
import persistent4s.Snapshot
import persistent4s.SnapshotCodec
import persistent4s.SnapshotDecodeException
import skunk.{Session, Command, Query, Void}
import skunk.codec.all.*
import skunk.implicits.*
import cats.effect.Resource
import cats.effect.Async
import cats.syntax.all.*

/** A PostgreSQL-backed implementation of SnapshotStore. Snapshots are stored in a `snapshots` table, keyed by handler
  * name and tags. This allows command handlers to load and save snapshots of their state, improving performance by
  * avoiding the need to replay the entire event history on every command execution.
  *
  * @param pool
  *   a resource for obtaining database sessions
  * @tparam F
  *   the effect type, such as `cats.effect.IO`
  */
final class PostgresSnapshotStore[F[_]: Async](
  pool: Resource[F, Session[F]],
) extends SnapshotStore[F]:

  import PostgresSnapshotStore.*

  override def load[S: SnapshotCodec](handlerName: String, tags: Set[Tag]): F[Option[Snapshot[S]]] =
    pool
      .use(session =>
        val serializedTags = tags.toList.map(_.value).sorted.mkString("|")
        for
          row    <- session.option(loadSnapshotQuery)(handlerName *: serializedTags *: EmptyTuple)
          result <- row.traverse { case stateStr *: globalPos *: EmptyTuple =>
                      Async[F].fromEither(
                        summon[SnapshotCodec[S]]
                          .decode(stateStr)
                          .map(Snapshot(_, globalPos))
                          .left
                          .map(SnapshotDecodeException(_)),
                      )
                    }
        yield result,
      )

  override def save[S: SnapshotCodec](handlerName: String, tags: Set[Tag], snapshot: Snapshot[S]): F[Unit] =
    pool.use(session =>
      val tagStr = tags.toList.map(_.value).sorted.mkString("|")
      val snapshotStr = summon[SnapshotCodec[S]].encode(snapshot.state)
      session
        .execute(upsertSnapshotCommand)(
          handlerName *: tagStr *: snapshotStr *: snapshot.globalPosition *: EmptyTuple,
        )
        .void,
    )

object PostgresSnapshotStore:

  private type SnapshotRow =
    String *: Long *: EmptyTuple

  private val loadSnapshotQuery: Query[String *: String *: EmptyTuple, SnapshotRow] =
    sql"""
      SELECT state, global_position
      FROM snapshots
      WHERE handler_name = $text AND tags = $text
    """.query(text *: int8)

  private val upsertSnapshotCommand: Command[String *: String *: String *: Long *: EmptyTuple] =
    sql"""
      INSERT INTO snapshots (handler_name, tags, state, global_position)
      VALUES ($text, $text, $text, $int8)
      ON CONFLICT (handler_name, tags) DO UPDATE SET
        state = EXCLUDED.state,
        global_position = EXCLUDED.global_position
    """.command

  val createTableCommand: Command[Void] =
    sql"""
      CREATE TABLE IF NOT EXISTS snapshots (
        handler_name TEXT        NOT NULL,
        tags         TEXT       NOT NULL,
        state        TEXT       NOT NULL,
        global_position BIGINT   NOT NULL,
        recorded_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (handler_name, tags)
      )
    """.command
