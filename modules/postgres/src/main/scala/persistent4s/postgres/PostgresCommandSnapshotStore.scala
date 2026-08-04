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

import cats.effect.{Async, Resource}
import cats.syntax.all.*

import persistent4s.{CommandSnapshotStore, SnapshotId, StoredCommandSnapshot}

import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

/** PostgreSQL-backed command snapshot cache. Older concurrent saves cannot overwrite a newer snapshot position. */
final class PostgresCommandSnapshotStore[F[_]: Async] private[postgres] (
  pool: Resource[F, Session[F]],
) extends CommandSnapshotStore[F]:

  import PostgresCommandSnapshotStore.*

  override def load(snapshotId: SnapshotId, key: String, version: Int): F[Option[StoredCommandSnapshot]] =
    pool.use(_.option(loadQuery)(snapshotId.value *: key *: version *: EmptyTuple))

  override def save(
    snapshotId: SnapshotId,
    key: String,
    version: Int,
    snapshot: StoredCommandSnapshot,
  ): F[Unit] =
    pool
      .use(
        _.execute(saveCommand)(
          snapshotId.value *:
            key *:
            version *:
            snapshot.globalPosition *:
            snapshot.eventCount *:
            snapshot.filterFingerprint *:
            snapshot.payload *:
            EmptyTuple,
        ),
      )
      .void

  override def delete(snapshotId: SnapshotId, key: String, version: Int): F[Unit] =
    pool.use(_.execute(deleteCommand)(snapshotId.value *: key *: version *: EmptyTuple)).void

object PostgresCommandSnapshotStore:

  private val loadQuery: Query[String *: String *: Int *: EmptyTuple, StoredCommandSnapshot] =
    sql"""
      SELECT global_position, event_count, filter_fingerprint, payload
      FROM command_snapshots
      WHERE snapshot_id = $text
        AND snapshot_key = $text
        AND snapshot_version = $int4
    """.query(int8 *: int8 *: text *: text).to[StoredCommandSnapshot]

  private val saveCommand: Command[String *: String *: Int *: Long *: Long *: String *: String *: EmptyTuple] =
    sql"""
      INSERT INTO command_snapshots (
        snapshot_id,
        snapshot_key,
        snapshot_version,
        global_position,
        event_count,
        filter_fingerprint,
        payload
      )
      VALUES ($text, $text, $int4, $int8, $int8, $text, $text)
      ON CONFLICT (snapshot_id, snapshot_key, snapshot_version) DO UPDATE SET
        global_position = EXCLUDED.global_position,
        event_count = EXCLUDED.event_count,
        filter_fingerprint = EXCLUDED.filter_fingerprint,
        payload = EXCLUDED.payload,
        updated_at = now()
      WHERE command_snapshots.global_position <= EXCLUDED.global_position
    """.command

  private val deleteCommand: Command[String *: String *: Int *: EmptyTuple] =
    sql"""
      DELETE FROM command_snapshots
      WHERE snapshot_id = $text
        AND snapshot_key = $text
        AND snapshot_version = $int4
    """.command

  private[postgres] val createTableCommand: Command[Void] =
    sql"""
      CREATE TABLE IF NOT EXISTS command_snapshots (
        snapshot_id        TEXT        NOT NULL,
        snapshot_key       TEXT        NOT NULL,
        snapshot_version   INTEGER     NOT NULL,
        global_position    BIGINT      NOT NULL CHECK (global_position >= 0),
        event_count        BIGINT      NOT NULL CHECK (event_count >= 0),
        filter_fingerprint TEXT        NOT NULL,
        payload            TEXT        NOT NULL,
        updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
        PRIMARY KEY (snapshot_id, snapshot_key, snapshot_version)
      )
    """.command

  def make[F[_]: Async](pool: Resource[F, Session[F]]): PostgresCommandSnapshotStore[F] =
    new PostgresCommandSnapshotStore(pool)
