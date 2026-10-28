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

import cats.{Functor, MonadThrow}
import cats.effect.*
import cats.syntax.all.*
import org.typelevel.log4cats.Logger

import persistent4s.{ProjectionCheckpoint, ProjectionCheckpointConflict, ProjectionCheckpointState, ReadCursor}

import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

/** A PostgreSQL-backed implementation of ProjectionCheckpoint. Checkpoints are stored in a `projection_checkpoints`
  * table, keyed by projection name. This survives application restarts, allowing projectors to resume from where they
  * left off.
  *
  * @param pool
  *   a resource for obtaining database sessions
  */
final class PostgresProjectionCheckpoint[F[_]: Async: Logger] private (
  pool: Resource[F, Session[F]],
) extends ProjectionCheckpoint[F]:

  import PostgresProjectionCheckpoint.*

  override def load(projectionName: String): F[Option[ProjectionCheckpointState]] =
    pool.use(_.option(loadQuery)(projectionName))

  override def save(state: ProjectionCheckpointState): F[Unit] =
    pool
      .use(
        _.execute(upsertCommand)(
          state.projectionName *: state.cursor.commitGroup *: state.cursor.globalPosition *: state.running *:
            state.error *: EmptyTuple,
        ),
      )
      .handleErrorWith { error =>
        Logger[F].error(error)(
          s"Failed to save checkpoint for projection ${state.projectionName} at ${state.cursor}",
        ) *> Async[F].raiseError(error)
      }
      .void

  /** Load all checkpoints stored in the database.
    *
    * Returns the current state of every projection that has ever been saved. Useful for monitoring and admin tooling
    * where a full view across all projections is needed.
    *
    * @return
    *   a list of all stored [[persistent4s.ProjectionCheckpointState]] values
    */
  override def loadAll(): F[List[ProjectionCheckpointState]] =
    pool.use(_.execute(loadAllQuery))

object PostgresProjectionCheckpoint:

  /** The cursor `projectionName` has reached, read with the caller's session and transaction, or
    * [[persistent4s.ReadCursor.Start]] before its first checkpoint. Read in the same snapshot as the projection's view
    * to learn exactly which events that view reflects, as [[PostgresAtomicRepository.readWithCursor]] does.
    */
  def cursor[F[_]: Functor](session: Session[F], projectionName: String): F[ReadCursor] =
    session.option(loadQuery)(projectionName).map(_.fold(ReadCursor.Start)(_.cursor))

  private[postgres] def compareAndSet[F[_]: MonadThrow](
    session: Session[F],
    expected: ReadCursor,
    state: ProjectionCheckpointState,
  ): F[Unit] =
    val update =
      if expected == ReadCursor.Start then
        session.option(insertInitialQuery)(
          state.projectionName *:
            state.cursor.commitGroup *:
            state.cursor.globalPosition *:
            state.running *:
            state.error *:
            EmptyTuple,
        )
      else
        session.option(updateIfCurrentQuery)(
          state.cursor.commitGroup *:
            state.cursor.globalPosition *:
            state.running *:
            state.error *:
            state.projectionName *:
            expected.commitGroup *:
            expected.globalPosition *:
            EmptyTuple,
        )

    update.flatMap {
      case Some(_) => ().pure[F]
      case None    => ProjectionCheckpointConflict(state.projectionName, expected).raiseError[F, Unit]
    }

  private val checkpointDecoder: Decoder[ProjectionCheckpointState] =
    (text *: int8 *: int8 *: bool *: text.opt).map {
      case name *: commitGroup *: globalPosition *: running *: error *: EmptyTuple =>
        ProjectionCheckpointState(name, ReadCursor(commitGroup, globalPosition), running, error)
    }

  private val loadQuery: Query[String, ProjectionCheckpointState] =
    sql"""
      SELECT projection_name, commit_group, global_position, running, error
      FROM projection_checkpoints
      WHERE projection_name = $text
    """.query(checkpointDecoder)

  private val loadAllQuery: Query[Void, ProjectionCheckpointState] =
    sql"""
      SELECT projection_name, commit_group, global_position, running, error
      FROM projection_checkpoints
      ORDER BY projection_name ASC
    """.query(checkpointDecoder)

  private val upsertCommand: Command[String *: Long *: Long *: Boolean *: Option[String] *: EmptyTuple] =
    sql"""
      INSERT INTO projection_checkpoints (projection_name, commit_group, global_position, running, error)
      VALUES ($text, $int8, $int8, $bool, ${text.opt})
      ON CONFLICT (projection_name) DO UPDATE SET
        commit_group = EXCLUDED.commit_group,
        global_position = EXCLUDED.global_position,
        running = EXCLUDED.running,
        error = EXCLUDED.error
    """.command

  private val insertInitialQuery: Query[String *: Long *: Long *: Boolean *: Option[String] *: EmptyTuple, Long] =
    sql"""
      INSERT INTO projection_checkpoints (projection_name, commit_group, global_position, running, error)
      VALUES ($text, $int8, $int8, $bool, ${text.opt})
      ON CONFLICT (projection_name) DO UPDATE SET
        commit_group = EXCLUDED.commit_group,
        global_position = EXCLUDED.global_position,
        running = EXCLUDED.running,
        error = EXCLUDED.error
      WHERE projection_checkpoints.commit_group = 0
        AND projection_checkpoints.global_position = -1
      RETURNING global_position
    """.query(int8)

  private val updateIfCurrentQuery
    : Query[Long *: Long *: Boolean *: Option[String] *: String *: Long *: Long *: EmptyTuple, Long] =
    sql"""
      UPDATE projection_checkpoints
      SET
        commit_group = $int8,
        global_position = $int8,
        running = $bool,
        error = ${text.opt}
      WHERE projection_name = $text
        AND commit_group = $int8
        AND global_position = $int8
      RETURNING global_position
    """.query(int8)

  val createTableCommand: Command[Void] =
    sql"""
      CREATE TABLE IF NOT EXISTS projection_checkpoints (
        projection_name TEXT   PRIMARY KEY,
        commit_group    BIGINT NOT NULL DEFAULT 0,
        global_position BIGINT NOT NULL,
        running BOOLEAN NOT NULL,
        error TEXT
      )
    """.command

  /** A checkpoint saved before commit groups existed points at an event stored in commit group 0, which is exactly
    * where those events stay.
    */
  val addCommitGroupCommand: Command[Void] =
    sql"""
      ALTER TABLE projection_checkpoints
      ADD COLUMN IF NOT EXISTS commit_group BIGINT NOT NULL DEFAULT 0
    """.command

  def make[F[_]: Async: Logger](pool: Resource[F, Session[F]]): PostgresProjectionCheckpoint[F] =
    new PostgresProjectionCheckpoint(pool)
