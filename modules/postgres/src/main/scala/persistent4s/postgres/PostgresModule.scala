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
import cats.effect.std.Console
import cats.syntax.all.*

import fs2.io.net.Network

import persistent4s.{CommandRuntime, DefaultProjector, Event, EventCodec, ProjectionRuntime}

import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import pureconfig.ConfigSource
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

sealed trait PostgresModuleError extends Throwable

object PostgresModuleError:

  final case class ConfigNotFound(path: String, details: String) extends PostgresModuleError:

    override def getMessage: String =
      s"""PostgreSQL EventStore configuration not found.
         |
         |Please ensure your application.conf contains the database configuration at path '$path'.
         |
         |Example configuration:
         |
         |$path {
         |  host = "localhost"
         |  port = 5432
         |  user = "your_user"
         |  password = "your_password"
         |  database = "eventstore"
         |  max-connections = 10
         |}
         |
         |Configuration error details: $details""".stripMargin

  final case class ConnectionFailed(cause: Throwable) extends PostgresModuleError:

    override def getMessage: String =
      s"""Failed to connect to PostgreSQL database.
         |
         |Please verify that:
         |1. The PostgreSQL server is running
         |2. The connection parameters in application.conf are correct
         |3. The database exists and is accessible
         |
         |Connection error: ${cause.getMessage}""".stripMargin

    override def getCause: Throwable = cause

/** A module that provides a fully-initialized PostgresEventStore. It handles:
  *   - Loading configuration from application.conf
  *   - Establishing database connections
  *   - Creating the event store schema if it doesn't exist
  *   - Providing the PostgresEventStore instance
  */
object PostgresModule:

  /** Fully initialized PostgreSQL components sharing one connection pool. `sessions` is exposed for application read
    * model repositories; command snapshots, event storage and atomic projection commits therefore require no second
    * pool or duplicate configuration.
    */
  final case class Components[F[_], A <: Event](
    eventStore: PostgresEventStore[F, A],
    checkpoint: PostgresProjectionCheckpoint[F],
    snapshotStore: PostgresCommandSnapshotStore[F],
    sessions: Resource[F, Session[F]],
  ):

    val commandRuntime: CommandRuntime[F, A] =
      CommandRuntime(eventStore, Some(snapshotStore))

    def projectionRuntime(using Async[F]): ProjectionRuntime[F, A] =
      ProjectionRuntime(DefaultProjector(eventStore, checkpoint))

    /** Build an atomic projection repository from derived PostgreSQL table metadata using this module's shared session
      * pool.
      */
    def repository[K, S <: Product](table: PostgresTable[K, S])(using Async[F]): DerivedPostgresRepository[F, K, S] =
      DerivedPostgresRepository(sessions, table)

    /** Build a derived repository and replace selected generated callbacks while retaining the shared session pool. */
    def repository[K, S <: Product](
      table: PostgresTable[K, S],
      customize: PostgresRepositoryTable[F, K, S] => PostgresRepositoryTable[F, K, S],
    )(using Async[F]): DerivedPostgresRepository[F, K, S] =
      DerivedPostgresRepository.customized(sessions, table)(customize)

  private val defaultConfigPath = "persistent4s.postgres"

  /** Create a PostgresModule Resource that manages the lifecycle of the database connection and event store.
    *
    * @param codec
    *   the event codec for serializing/deserializing events
    * @param configPath
    *   the path in application.conf where the PostgresConfig is located (default: "persistent4s.postgres")
    * @return
    *   initialized event, command-snapshot, checkpoint and shared-session components, or a clear connection/config
    *   failure
    */
  def make[F[_]: Async: Network: Tracer: Meter: Console, A <: Event](
    codec: EventCodec[A],
    configPath: String = defaultConfigPath,
  ): Resource[F, Components[F, A]] =
    for
      logger <- Resource.eval(Slf4jLogger.create[F])
      config <- Resource.eval(loadConfig[F](configPath))
      _      <- Resource.eval(
             logger.info(
               s"Connecting to PostgreSQL at ${config.host}:${config.port}/${config.database}",
             ),
           )
      pool <- createSessionPool[F](config)
      _    <- Resource.eval(initializeDatabase[F](pool, logger))
    yield components(pool, codec)

  /** Create a PostgresModule Resource using an explicitly provided configuration (bypasses application.conf loading).
    *
    * @param config
    *   the PostgresConfig to use
    * @param codec
    *   the event codec for serializing/deserializing events
    * @return
    *   initialized event, command-snapshot, checkpoint and shared-session components
    */
  def makeWithConfig[F[_]: Async: Network: Tracer: Meter: Console, A <: Event](
    config: PostgresConfig,
    codec: EventCodec[A],
  ): Resource[F, Components[F, A]] =
    for
      logger <- Resource.eval(Slf4jLogger.create[F])
      _      <- Resource.eval(
             logger.info(
               s"Connecting to PostgreSQL at ${config.host}:${config.port}/${config.database}",
             ),
           )
      pool <- createSessionPool[F](config)
      _    <- Resource.eval(initializeDatabase[F](pool, logger))
    yield components(pool, codec)

  private def components[F[_]: Async, A <: Event](
    pool: Resource[F, Session[F]],
    codec: EventCodec[A],
  ): Components[F, A] =
    val eventStore = PostgresEventStore[F, A](pool, codec)
    Components(
      eventStore,
      PostgresProjectionCheckpoint.make[F](pool),
      PostgresCommandSnapshotStore.make[F](pool),
      pool,
    )

  private def loadConfig[F[_]: Sync](configPath: String): F[PostgresConfig] =
    Sync[F].delay {
      ConfigSource.default.at(configPath).load[PostgresConfig]
    }.flatMap {
      case Right(config) => Sync[F].pure(config)
      case Left(errors)  =>
        Sync[F].raiseError(
          PostgresModuleError.ConfigNotFound(configPath, errors.prettyPrint()),
        )
    }

  private def createSessionPool[F[_]: Async: Network: Tracer: Meter: Console](
    config: PostgresConfig,
  ): Resource[F, Resource[F, Session[F]]] =
    Session
      .Builder[F]
      .withHost(config.host)
      .withPort(config.port)
      .withUserAndPassword(config.user, config.password)
      .withDatabase(config.database)
      .pooled(config.maxConnections)
      .adaptError { case e => PostgresModuleError.ConnectionFailed(e) }

  private def initializeDatabase[F[_]: Async](
    pool: Resource[F, Session[F]],
    logger: Logger[F],
  ): F[Unit] =
    pool.use { session =>
      for
        eventsTableExists <- checkTableExists(session, "events")
        _                 <-
          if !eventsTableExists then logger.info("Event store schema not found, creating schema...")
          else logger.info("Event store schema already exists, ensuring required objects are present")
        _ <- createSchema(session)
      yield ()
    }

  private def checkTableExists[F[_]: Sync](session: Session[F], tableName: String): F[Boolean] =
    session.unique(checkTableExistsQuery)(tableName).map(_ > 0)

  private def createSchema[F[_]: Sync](session: Session[F]): F[Unit] =
    session.execute(createTableCommand) *>
      session.execute(addEventVersionCommand) *>
      session.execute(createEventTypeIndexCommand) *>
      session.execute(createEventTagsTableCommand) *>
      session.execute(createEventTagsSequenceIndexCommand) *>
      session.execute(PostgresProjectionCheckpoint.createTableCommand) *>
      session.execute(PostgresCommandSnapshotStore.createTableCommand).void

  private val checkTableExistsQuery: Query[String, Long] =
    sql"""
      SELECT COUNT(*)
      FROM information_schema.tables
      WHERE table_schema = 'public'
        AND table_name = $text
    """.query(int8)

  private val createTableCommand: Command[Void] =
    sql"""
      CREATE TABLE IF NOT EXISTS events (
        sequence_number BIGSERIAL PRIMARY KEY,
        event_id        UUID        NOT NULL DEFAULT gen_random_uuid(),
        event_type      TEXT        NOT NULL,
        event_version   INTEGER     NOT NULL DEFAULT 1,
        tags            JSONB       NOT NULL DEFAULT '[]',
        payload         JSONB       NOT NULL DEFAULT '{}',
        is_external     BOOLEAN     NOT NULL,
        recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

        CONSTRAINT unique_event_id UNIQUE (event_id)
      )
    """.command

  private val addEventVersionCommand: Command[Void] =
    sql"""
      ALTER TABLE events
      ADD COLUMN IF NOT EXISTS event_version INTEGER NOT NULL DEFAULT 1
    """.command

  private val createEventTagsTableCommand: Command[Void] =
    sql"""
      CREATE TABLE IF NOT EXISTS event_tags (
        tag             TEXT   NOT NULL,
        sequence_number BIGINT NOT NULL REFERENCES events(sequence_number) ON DELETE CASCADE,
        PRIMARY KEY (tag, sequence_number)
      )
    """.command

  private val createEventTypeIndexCommand: Command[Void] =
    sql"""
      CREATE INDEX IF NOT EXISTS idx_events_event_type ON events (event_type)
    """.command

  private val createEventTagsSequenceIndexCommand: Command[Void] =
    sql"""
      CREATE INDEX IF NOT EXISTS idx_event_tags_sequence_number ON event_tags (sequence_number)
    """.command
