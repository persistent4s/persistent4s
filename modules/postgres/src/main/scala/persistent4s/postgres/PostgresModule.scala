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

import scala.concurrent.duration.*

import cats.effect.*
import cats.effect.std.{Console, SecureRandom}
import cats.syntax.all.*

import fs2.io.net.Network
import org.typelevel.otel4s.metrics.Histogram
import org.typelevel.log4cats.Logger
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import pureconfig.ConfigSource
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

import persistent4s.{
  CommandRuntime,
  DefaultProjector,
  Event,
  EventCodec,
  EventNotification,
  EventStore,
  EventStoreNotification,
  InstrumentedEventStore,
  InstrumentedProjectionCheckpoint,
  LeaderElectedProjector,
  ProjectionCheckpoint,
  ProjectionRuntime,
}

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
    *
    * @param eventStore
    *   the instrumented store; use this for ordinary appends and reads
    * @param transactionalStore
    *   the same store, untyped-down to its PostgreSQL implementation, for the dual-write entry points
    *   (`appendWithMessages`/`appendUncheckedWithMessages`) that commit events and outgoing messages in one
    *   transaction. Appends made through it bypass the otel4s instrumentation on [[eventStore]].
    * @param outbox
    *   present only when the module was built with `enableOutbox = true`; otherwise `None` and no outbox table is
    *   created
    * @param messageOutbox
    *   present only when the module was built with `enableMessageOutbox = true`; otherwise `None` and no message outbox
    *   table is created
    */
  final case class Components[F[_], A <: Event](
    eventStore: EventStore[F, A] & EventNotification[F],
    transactionalStore: PostgresEventStore[F, A],
    checkpoint: ProjectionCheckpoint[F],
    snapshotStore: PostgresCommandSnapshotStore[F],
    outbox: Option[PostgresOutbox[F, A]],
    messageOutbox: Option[PostgresMessageOutbox[F]],
    leaderElection: PostgresLeaderElection[F],
    sessions: Resource[F, Session[F]],
    sendNotification: EventStoreNotification => F[Unit],
  ):

    val commandRuntime: CommandRuntime[F, A] =
      CommandRuntime(eventStore, Some(snapshotStore))

    def projectionRuntime(using Async[F], Logger[F], Tracer[F], Meter[F]): ProjectionRuntime[F, A] =
      ProjectionRuntime(DefaultProjector(eventStore, checkpoint))

    /** Like [[projectionRuntime]], but each projection runs under [[leaderElection]] so that several instances of the
      * same service pointed at this database never race a projection's shared checkpoint.
      */
    def leaderElectedProjectionRuntime(using Async[F], Logger[F], Tracer[F], Meter[F]): ProjectionRuntime[F, A] =
      ProjectionRuntime(LeaderElectedProjector(DefaultProjector(eventStore, checkpoint), leaderElection))

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

  private val defaultGapTimeout = 6.seconds

  /** Create a PostgresModule Resource that manages the lifecycle of the database connection and event store.
    *
    * @param codec
    *   the event codec for serializing/deserializing events
    * @param configPath
    *   the path in application.conf where the PostgresConfig is located (default: "persistent4s.postgres")
    * @param enableOutbox
    *   create the event outbox table and enqueue every newly-appended non-external event into it
    * @param enableMessageOutbox
    *   create the outgoing-message outbox table, enabling the dual-write entry points on
    *   [[Components.transactionalStore]]
    * @return
    *   initialized event, command-snapshot, checkpoint and shared-session components, or a clear connection/config
    *   failure
    */
  def make[F[_]: Async: Network: Logger: Tracer: Meter: Console, A <: Event](
    codec: EventCodec[A],
    configPath: String = defaultConfigPath,
    enableOutbox: Boolean = false,
    enableMessageOutbox: Boolean = false,
    gapTimeout: FiniteDuration = defaultGapTimeout,
    appendTimeout: FiniteDuration = defaultGapTimeout / 3,
  ): Resource[F, Components[F, A]] =
    for
      config <- Resource.eval(loadConfig[F](configPath))
      _      <- Resource.eval(
             Logger[F].info(
               s"Connecting to PostgreSQL at ${config.host}:${config.port}/${config.database}",
             ),
           )
      pool       <- createSessionPool[F](config)
      _          <- Resource.eval(initializeDatabase[F](pool, enableOutbox, enableMessageOutbox))
      components <- components[F, A](pool, codec, enableOutbox, enableMessageOutbox, gapTimeout, appendTimeout)
    yield components

  /** Create a PostgresModule Resource using an explicitly provided configuration (bypasses application.conf loading).
    *
    * @param config
    *   the PostgresConfig to use
    * @param codec
    *   the event codec for serializing/deserializing events
    * @return
    *   initialized event, command-snapshot, checkpoint and shared-session components
    */
  def makeWithConfig[F[_]: Async: Network: Logger: Tracer: Meter: Console, A <: Event](
    config: PostgresConfig,
    codec: EventCodec[A],
    enableOutbox: Boolean = false,
    enableMessageOutbox: Boolean = false,
    gapTimeout: FiniteDuration = defaultGapTimeout,
    appendTimeout: FiniteDuration = defaultGapTimeout / 3,
  ): Resource[F, Components[F, A]] =
    for
      _ <- Resource.eval(
             Logger[F].info(
               s"Connecting to PostgreSQL at ${config.host}:${config.port}/${config.database}",
             ),
           )
      pool       <- createSessionPool[F](config)
      _          <- Resource.eval(initializeDatabase[F](pool, enableOutbox, enableMessageOutbox))
      components <- components[F, A](pool, codec, enableOutbox, enableMessageOutbox, gapTimeout, appendTimeout)
    yield components

  /** Wire the raw PostgreSQL store, wrap it with otel4s instrumentation, and expose the shared pool alongside command
    * snapshots so one configuration serves events, snapshots and read models. The raw store is also exposed directly so
    * the dual-write entry points stay reachable, and `SecureRandom` is built here rather than demanded from the caller.
    */
  private def components[F[_]: Async: Logger: Tracer: Meter, A <: Event](
    pool: Resource[F, Session[F]],
    codec: EventCodec[A],
    enableOutbox: Boolean,
    enableMessageOutbox: Boolean,
    gapTimeout: FiniteDuration,
    appendTimeout: FiniteDuration,
  ): Resource[F, Components[F, A]] =
    for
      given SecureRandom[F] <- Resource.eval(SecureRandom.javaSecuritySecureRandom[F])
      raw                    = PostgresEventStore[F, A](pool, codec, outboxEnabled = enableOutbox, gapTimeout = gapTimeout,
              appendTimeout = appendTimeout)
      store      <- Resource.eval(InstrumentedEventStore.makeWithNotification[F, A](raw))
      checkpoint <- Resource.eval(
                      InstrumentedProjectionCheckpoint.make[F](PostgresProjectionCheckpoint.make[F](pool)),
                    )
    yield Components(
      store,
      raw,
      checkpoint,
      PostgresCommandSnapshotStore.make[F](pool),
      if enableOutbox then Some(PostgresOutbox[F, A](pool, codec)) else None,
      if enableMessageOutbox then Some(PostgresMessageOutbox[F](pool)) else None,
      PostgresLeaderElection.make[F](pool),
      pool,
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
                   .withSSL(toSkunkSSL(config.ssl))
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

  private def toSkunkSSL(mode: PostgresSslMode): SSL =
    mode match
      case PostgresSslMode.Disabled => SSL.None
      case PostgresSslMode.System   => SSL.System
      // Skunk's `SSL.Trusted` actually trusts all certs (uses an insecure TLSContext under the hood); the name is
      // misleading. We expose it as `TrustAll` so the config is honest about what it does.
      case PostgresSslMode.TrustAll => SSL.Trusted

  private def initializeDatabase[F[_]: Async: Logger](
    pool: Resource[F, Session[F]],
    enableOutbox: Boolean,
    enableMessageOutbox: Boolean,
  ): F[Unit] =
    pool.use { session =>
      for
        eventsTableExists <- checkTableExists(session, "events")
        _                 <-
          if !eventsTableExists then Logger[F].info("Event store schema not found, creating schema...")
          else Logger[F].info("Event store schema already exists, ensuring required objects are present")
        _ <- createSchema(session, enableOutbox, enableMessageOutbox)
      yield ()
    }

  private def checkTableExists[F[_]: Sync](session: Session[F], tableName: String): F[Boolean] =
    session.unique(checkTableExistsQuery)(tableName).map(_ > 0)

  private def createSchema[F[_]: Sync](
    session: Session[F],
    enableOutbox: Boolean,
    enableMessageOutbox: Boolean,
  ): F[Unit] =
    val outboxDdl =
      if enableOutbox then session.execute(PostgresOutbox.createTableCommand).void
      else Sync[F].unit

    val outboxMessageDdl =
      if enableMessageOutbox then session.execute(PostgresMessageOutbox.createTableCommand).void
      else Sync[F].unit

    session.execute(createTableCommand) *>
      session.execute(addEventVersionCommand) *>
      session.execute(addHeadersCommand) *>
      session.execute(createEventTypeIndexCommand) *>
      session.execute(createEventTagsTableCommand) *>
      session.execute(createEventTagsSequenceIndexCommand) *>
      outboxDdl *>
      outboxMessageDdl *>
      session.execute(PostgresProjectionCheckpoint.createTableCommand) *>
      session.execute(PostgresLeaderElection.createTableCommand) *>
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
        headers         JSONB       NOT NULL DEFAULT '{}',
        recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),

        CONSTRAINT unique_event_id UNIQUE (event_id)
      )
    """.command

  private val addEventVersionCommand: Command[Void] =
    sql"""
      ALTER TABLE events
      ADD COLUMN IF NOT EXISTS event_version INTEGER NOT NULL DEFAULT 1
    """.command

  /** Mirrors [[addEventVersionCommand]]: an events table created before author-supplied headers existed must gain the
    * column in place, or every append would fail against it.
    */
  private val addHeadersCommand: Command[Void] =
    sql"""
      ALTER TABLE events
      ADD COLUMN IF NOT EXISTS headers JSONB NOT NULL DEFAULT '{}'
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
