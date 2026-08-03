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
import cats.effect.std.{Console, SecureRandom}
import cats.syntax.all.*
import fs2.io.net.Network
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.otel4s.metrics.{Histogram, Meter}
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource
import skunk.*
import skunk.implicits.*
import skunk.codec.all.*

import persistent4s.EventCodec
import persistent4s.Event
import persistent4s.EventNotification
import persistent4s.EventStore
import persistent4s.InstrumentedEventStore
import persistent4s.EventStoreNotification
import persistent4s.ProjectionCheckpoint
import persistent4s.InstrumentedProjectionCheckpoint

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

  /** Holds the fully-initialized PostgreSQL event store and projection checkpoint, sharing the same connection pool. */
  final case class Components[F[_], A <: Event](
    eventStore: EventStore[F, A] & EventNotification[F],
    checkpoint: ProjectionCheckpoint[F],
    sendNotification: EventStoreNotification => F[Unit],
  )

  private val defaultConfigPath = "persistent4s.postgres"

  /** Create a PostgresModule Resource that manages the lifecycle of the database connection and event store.
    *
    * @param codec
    *   the event codec for serializing/deserializing events
    * @param configPath
    *   the path in application.conf where the PostgresConfig is located (default: "persistent4s.postgres")
    * @return
    *   a Resource containing the PostgresEventStore and PostgresProjectionCheckpoint, or fails with a clear error
    *   message
    */
  def make[F[_]: Async: Network: Tracer: Meter: Console: SecureRandom, A <: Event](
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
      pool  <- createSessionPool[F](config)
      _     <- Resource.eval(initializeDatabase[F](pool, logger))
      raw    = PostgresEventStore[F, A](pool, codec)
      store <- Resource.eval(
                 InstrumentedEventStore.makeWithNotification[F, A](raw),
               )
      checkpoint <- Resource.eval(InstrumentedProjectionCheckpoint.make[F](PostgresProjectionCheckpoint.make[F](pool)))
    yield Components(
      store,
      checkpoint,
      raw.notify,
    )

  /** Create a PostgresModule Resource using an explicitly provided configuration (bypasses application.conf loading).
    *
    * @param config
    *   the PostgresConfig to use
    * @param codec
    *   the event codec for serializing/deserializing events
    * @return
    *   a Resource containing the PostgresEventStore and PostgresProjectionCheckpoint
    */
  def makeWithConfig[F[_]: Async: Network: Tracer: Meter: Console: SecureRandom, A <: Event](
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
      pool  <- createSessionPool[F](config)
      _     <- Resource.eval(initializeDatabase[F](pool, logger))
      raw    = PostgresEventStore(pool, codec)
      store <- Resource.eval(
                 InstrumentedEventStore.makeWithNotification[F, A](raw),
               )
      checkpoint <- Resource.eval(InstrumentedProjectionCheckpoint.make[F](PostgresProjectionCheckpoint.make[F](pool)))
    yield Components(
      store,
      checkpoint,
      raw.notify,
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
    for
      rawPool <- Session
                   .Builder[F]
                   .withHost(config.host)
                   .withPort(config.port)
                   .withUserAndPassword(config.user, config.password)
                   .withDatabase(config.database)
                   .pooled(config.maxConnections)
                   .adaptError { case e => PostgresModuleError.ConnectionFailed(e) }
      waitDuration <- Resource.eval(
                        Meter[F]
                          .histogram[Double]("persistent4s.postgres.pool.wait_duration")
                          .withDescription("Time spent waiting to acquire a session from the connection pool")
                          .withUnit("ms")
                          .create,
                      )
    yield timedPool(rawPool, waitDuration)

  private def timedPool[F[_]: Async](
    pool: Resource[F, Session[F]],
    waitDuration: Histogram[F, Double],
  ): Resource[F, Session[F]] =
    Resource.suspend(
      Async[F].monotonic.map { start =>
        pool.evalMap { session =>
          Async[F].monotonic.flatMap { end =>
            waitDuration.record((end - start).toNanos.toDouble / 1e6).as(session)
          }
        }
      },
    )

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
      session.execute(createEventTypeIndexCommand) *>
      session.execute(createEventTagsTableCommand) *>
      session.execute(createEventTagsSequenceIndexCommand) *>
      session.execute(PostgresProjectionCheckpoint.createTableCommand).void

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
        tags            JSONB       NOT NULL DEFAULT '[]',
        payload         JSONB       NOT NULL DEFAULT '{}',
        is_external     BOOLEAN     NOT NULL,
        headers         JSONB       NOT NULL DEFAULT '{}',
        recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

        CONSTRAINT unique_event_id UNIQUE (event_id)
      )
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
