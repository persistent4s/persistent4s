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
import skunk.*
import skunk.implicits.*
import skunk.codec.all.*

import persistent4s.ProjectionCheckpoint

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

  override def load(projectionName: String): F[Option[Long]] =
    pool.use(_.option(loadQuery)(projectionName))

  override def save(projectionName: String, globalPosition: Long): F[Unit] =
    pool.use(_.execute(upsertCommand)(projectionName *: globalPosition *: EmptyTuple)).void

object PostgresProjectionCheckpoint:

  private val loadQuery: Query[String, Long] =
    sql"""
      SELECT global_position
      FROM projection_checkpoints
      WHERE projection_name = $text
    """.query(int8)

  private val upsertCommand: Command[String *: Long *: EmptyTuple] =
    sql"""
      INSERT INTO projection_checkpoints (projection_name, global_position)
      VALUES ($text, $int8)
      ON CONFLICT (projection_name) DO UPDATE SET
        global_position = EXCLUDED.global_position
    """.command

  val createTableCommand: Command[Void] =
    sql"""
      CREATE TABLE IF NOT EXISTS projection_checkpoints (
        projection_name TEXT   PRIMARY KEY,
        global_position BIGINT NOT NULL
      )
    """.command

  def make[F[_]: Async](pool: Resource[F, Session[F]]): PostgresProjectionCheckpoint[F] =
    new PostgresProjectionCheckpoint(pool)
