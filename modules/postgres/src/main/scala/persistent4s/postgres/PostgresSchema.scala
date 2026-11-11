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

import pureconfig.ConfigReader
import pureconfig.error.CannotConvert

/** The PostgreSQL schema holding the tables persistent4s creates and uses: events, tags, outboxes, checkpoints,
  * snapshots, leader leases and sagas. A service can put them in a schema of their own, e.g. `persistent4s`, so they
  * stay apart from its domain tables; queries across schemas remain plain SQL joins.
  *
  * The module creates the schema at startup when it is missing. Every statement persistent4s runs names its tables with
  * this schema, so nothing depends on the connection's `search_path`.
  */
final class PostgresSchema private (val name: String):

  /** `table` qualified with this schema and quoted, ready to splice into SQL. */
  private[postgres] def table(table: String): String = SqlIdentifier.table(s"$name.$table")

  private[postgres] val quotedName: String = SqlIdentifier.schema(name)

  private[postgres] val events: String                = table("events")
  private[postgres] val eventTags: String             = table("event_tags")
  private[postgres] val eventOutbox: String           = table("event_outbox")
  private[postgres] val messageOutbox: String         = table("message_outbox")
  private[postgres] val projectionCheckpoints: String = table("projection_checkpoints")
  private[postgres] val commandSnapshots: String      = table("command_snapshots")
  private[postgres] val leaderLeases: String          = table("leader_leases")
  private[postgres] val sagaInstances: String         = table("saga_instances")

  override def equals(other: Any): Boolean =
    other match
      case that: PostgresSchema => name == that.name
      case _                    => false

  override def hashCode: Int = name.hashCode

  override def toString: String = s"PostgresSchema($name)"

object PostgresSchema:

  /** PostgreSQL's default schema, where persistent4s kept its tables before the schema was configurable. */
  val Public: PostgresSchema = new PostgresSchema("public")

  /** A schema named `name`, or why the name is not a valid PostgreSQL identifier. */
  def from(name: String): Either[String, PostgresSchema] =
    Either.cond(SqlIdentifier.isValid(name), new PostgresSchema(name), s"Invalid PostgreSQL schema name [$name]")

  /** A schema named `name`; throws `IllegalArgumentException` when it is not a valid PostgreSQL identifier. */
  def apply(name: String): PostgresSchema =
    from(name).fold(error => throw IllegalArgumentException(error), identity)

  given ConfigReader[PostgresSchema] =
    ConfigReader[String].emap(name => from(name).left.map(CannotConvert(name, "PostgresSchema", _)))
