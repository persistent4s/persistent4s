/*
 * Copyright 2026 Bastien Jolidon
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
import skunk.*
import skunk.implicits.*
import skunk.codec.all.*

import persistent4s.{ProjectionCheckpoint, ProjectionCheckpointState}

/** A PostgreSQL-backed implementation of ProjectionCheckpoint. Checkpoints are stored in a `projection_checkpoints`
  * table, keyed by projection name. This survives application restarts, allowing projectors to resume from where they
  * left off.
  *
  * @param pool
  *   a resource for obtaining database sessions
  */
final class PostgresProjectionCheckpoint[F[_]: Async] private (
  pool: Resource[F, Session[F]],
) extends ProjectionCheckpoint[F]:

  import PostgresProjectionCheckpoint.*

  override def load(projectionName: String): F[Option[ProjectionCheckpointState]] =
    pool.use(_.option(loadQuery)(projectionName))

  override def save(state: ProjectionCheckpointState): F[Unit] =
    pool
      .use(
        _.execute(upsertCommand)(
          state.projectionName *: state.globalPosition *: state.running *: state.error *: EmptyTuple,
        ),
      )
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

  private val loadQuery: Query[String, ProjectionCheckpointState] =
    sql"""
      SELECT projection_name, global_position, running, error
      FROM projection_checkpoints
      WHERE projection_name = $text
    """.query(text *: int8 *: bool *: text.opt).to[ProjectionCheckpointState]

  private val loadAllQuery: Query[Void, ProjectionCheckpointState] =
    sql"""
      SELECT projection_name, global_position, running, error
      FROM projection_checkpoints
      ORDER BY projection_name ASC
    """.query(text *: int8 *: bool *: text.opt).to[ProjectionCheckpointState]

  private val upsertCommand: Command[String *: Long *: Boolean *: Option[String] *: EmptyTuple] =
    sql"""
      INSERT INTO projection_checkpoints (projection_name, global_position, running, error)
      VALUES ($text, $int8, $bool, ${text.opt})
      ON CONFLICT (projection_name) DO UPDATE SET
        global_position = EXCLUDED.global_position,
        running = EXCLUDED.running,
        error = EXCLUDED.error
    """.command

  val createTableCommand: Command[Void] =
    sql"""
      CREATE TABLE IF NOT EXISTS projection_checkpoints (
        projection_name TEXT   PRIMARY KEY,
        global_position BIGINT NOT NULL,
        running BOOLEAN NOT NULL,
        error TEXT
      )
    """.command

  def make[F[_]: Async](pool: Resource[F, Session[F]]): PostgresProjectionCheckpoint[F] =
    new PostgresProjectionCheckpoint(pool)
