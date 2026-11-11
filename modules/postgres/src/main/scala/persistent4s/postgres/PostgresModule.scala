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
import io.opentelemetry.instrumentation.api.incubator.semconv.db.{SqlDialect, SqlQueryAnalyzer}
import org.typelevel.otel4s.metrics.Histogram
import org.typelevel.log4cats.Logger
import org.typelevel.otel4s.metrics.{Meter, MeterProvider}
import org.typelevel.otel4s.trace.{Tracer, TracerProvider}
import pureconfig.ConfigSource
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*
import skunk.telemetry.{QueryAnalyzer, TelemetryConfig}

import persistent4s.{
  Event,
  EventCodec,
  EventStoreNotification,
  InstrumentedEventStore,
  InstrumentedProjectionCheckpoint,
  Persistent4sFailure,
  ProjectionCheckpoint,
}

sealed trait PostgresModuleError extends Persistent4sFailure

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

  /** Raised by [[PostgresModule.domain]] when another domain of the same module already enabled the event outbox. */
  case object OutboxAlreadyEnabled extends PostgresModuleError:

    override def getMessage: String =
      "Another domain of this PostgresModule already enabled the event outbox. Its relay decodes every queued event " +
        "with that domain's codec, so only one domain per module may enable it."

/** Shared PostgreSQL infrastructure for one service: a connection pool, the tables, projection checkpoints, command
  * snapshots, leader election, the message outbox, the saga repository and a single notification listener. None of it
  * depends on an event type, so a service builds one module and then one [[PostgresDomain]] per event type with
  * [[domain]]; every domain shares this module's pool and listening session.
  *
  * `sessions` is exposed for application read model repositories, so event storage, snapshots and atomic projection
  * commits need no second pool or duplicate configuration. It carries the [[PostgresSchema]] the module created its
  * tables in, which atomic repositories need to commit their checkpoints.
  *
  * Query spans use sanitized SQL summaries; pool and wire-protocol spans are disabled. Bound parameters are excluded
  * from query attributes. Skunk can still include values in exception details or mixed literal/parameter SQL text, so
  * applications must sanitize those fields before export. Providers must come from the application's telemetry SDK.
  *
  * @param sessions
  *   the shared session pool and the schema of the persistent4s tables
  * @param messageOutbox
  *   present only when the module was built with `enableMessageOutbox = true` or `enableSaga = true`; otherwise `None`
  *   and no message outbox table is created
  * @param sagaRepository
  *   present only when the module was built with `enableSaga = true`
  */
final class PostgresModule[F[_]: Async: Logger: Tracer: Meter: SecureRandom] private (
  val sessions: PostgresSessions[F],
  val checkpoint: ProjectionCheckpoint[F],
  val snapshotStore: PostgresCommandSnapshotStore[F],
  val leaderElection: PostgresLeaderElection[F],
  val messageOutbox: Option[PostgresMessageOutbox[F]],
  val sagaRepository: Option[PostgresSagaRepository[F]],
  notificationHub: PostgresNotificationHub[F],
  outboxEnabled: Ref[F, Boolean],
  runningProjections: Ref[F, Set[String]],
):

  /** Build the store, runtimes and optional event outbox for the events `codec` handles.
    *
    * @param codec
    *   the event codec for serializing/deserializing this domain's events
    * @param outbox
    *   create the event outbox table and enqueue every event appended through this domain into it. Only one domain per
    *   module may enable it, because its relay decodes every queued event with this domain's codec; a second one fails
    *   with [[PostgresModuleError.OutboxAlreadyEnabled]] until the first is released.
    */
  def domain[A <: Event](codec: EventCodec[A], outbox: Boolean = false): Resource[F, PostgresDomain[F, A]] =
    for
      _     <- if outbox then enableOutbox else Resource.unit[F]
      raw    = PostgresEventStore[F, A](sessions, codec, notificationHub, outboxEnabled = outbox)
      store <- Resource.eval(InstrumentedEventStore.makeWithNotification[F, A](raw, PostgresErrorType.sqlState))
    yield PostgresDomain(
      this,
      store,
      raw,
      Option.when(outbox)(PostgresOutbox[F, A](sessions, codec, notificationHub)),
      runningProjections,
    )

  /** This module with `f` applied to its projection checkpoint store, e.g. to stop the service when a projection pauses
    * with an error. Domains built from the returned module run their projections with the wrapped store; everything
    * else, including the listening session and the module-wide rules, is shared with this module.
    *
    * Atomic repositories advance the checkpoint in the same transaction as their rows, without calling `save`; the
    * wrapper still sees every `load`, and every `save` of a pause, a resume or an error.
    */
  def mapCheckpoint(f: ProjectionCheckpoint[F] => ProjectionCheckpoint[F]): PostgresModule[F] =
    new PostgresModule[F](
      sessions, f(checkpoint), snapshotStore, leaderElection, messageOutbox, sagaRepository, notificationHub,
      outboxEnabled, runningProjections,
    )

  /** Send a notification to every projection listening on this database, e.g. to pause or resume one. */
  def sendNotification(notification: EventStoreNotification): F[Unit] =
    sessions.use(_.channel(PostgresEventStore.NotificationChannel).notify(PostgresNotification.encode(notification)))

  /** Build an atomic projection repository from derived PostgreSQL table metadata using this module's shared session
    * pool.
    */
  def repository[K, S <: Product](table: PostgresTable[K, S]): DerivedPostgresRepository[F, K, S] =
    DerivedPostgresRepository(sessions, table)

  /** Build a derived repository and replace selected generated callbacks while retaining the shared session pool. */
  def repository[K, S <: Product](
    table: PostgresTable[K, S],
    customize: PostgresRepositoryTable[F, K, S] => PostgresRepositoryTable[F, K, S],
  ): DerivedPostgresRepository[F, K, S] =
    DerivedPostgresRepository.customized(sessions, table)(customize)

  private def enableOutbox: Resource[F, Unit] =
    Resource.make(
      outboxEnabled.getAndSet(true).flatMap { alreadyEnabled =>
        if alreadyEnabled then PostgresModuleError.OutboxAlreadyEnabled.raiseError[F, Unit]
        else
          sessions
            .use(_.execute(PostgresOutbox.Statements(sessions.schema).createTableCommand))
            .void
            .onError(_ => outboxEnabled.set(false))
      },
    )(_ => outboxEnabled.set(false))

object PostgresModule:

  private val defaultConfigPath = "persistent4s.postgres"

  /** Create a PostgresModule Resource that manages the lifecycle of the database connection and schema.
    *
    * @param configPath
    *   the path in application.conf where the PostgresConfig is located (default: "persistent4s.postgres")
    * @param enableMessageOutbox
    *   create the outgoing-message outbox table, enabling the dual-write entry points on
    *   [[PostgresDomain.transactionalStore]]
    * @param enableSaga
    *   create the saga table, and the message outbox table sagas send their commands through
    * @return
    *   the initialized module, or a clear connection/config failure
    */
  def make[F[_]: Async: Network: Logger: Tracer: Meter: TracerProvider: MeterProvider: Console](
    configPath: String = defaultConfigPath,
    enableMessageOutbox: Boolean = false,
    enableSaga: Boolean = false,
  ): Resource[F, PostgresModule[F]] =
    Resource.eval(loadConfig[F](configPath)).flatMap(makeWithConfig[F](_, enableMessageOutbox, enableSaga))

  /** Create a PostgresModule Resource using an explicitly provided configuration (bypasses application.conf loading).
    *
    * @param config
    *   the PostgresConfig to use
    * @return
    *   the initialized module, or a clear connection failure
    */
  def makeWithConfig[F[_]: Async: Network: Logger: Tracer: Meter: TracerProvider: MeterProvider: Console](
    config: PostgresConfig,
    enableMessageOutbox: Boolean = false,
    enableSaga: Boolean = false,
  ): Resource[F, PostgresModule[F]] =
    for
      _ <- Resource.eval(
             Logger[F].info(
               s"Connecting to PostgreSQL at ${config.host}:${config.port}/${config.database} " +
                 s"(persistent4s schema ${config.schema.name})",
             ),
           )
      pool    <- createSessionPool[F](config)
      sessions = PostgresSessions(pool, config.schema)
      _       <- Resource.eval(initializeDatabase[F](sessions, enableMessageOutbox, enableSaga))
      module  <- module[F](sessions, enableMessageOutbox, enableSaga)
    yield module

  /** Wire the components shared by every domain. `SecureRandom` is built here rather than demanded from the caller, and
    * the store of every domain and both outboxes share one [[PostgresNotificationHub]], so the module listens with a
    * single session.
    */
  private def module[F[_]: Async: Logger: Tracer: Meter](
    sessions: PostgresSessions[F],
    enableMessageOutbox: Boolean,
    enableSaga: Boolean,
  ): Resource[F, PostgresModule[F]] =
    for
      given SecureRandom[F] <- Resource.eval(SecureRandom.javaSecuritySecureRandom[F])
      notificationHub       <- PostgresNotificationHub.make[F](sessions.pool)
      checkpoint            <- Resource.eval(
                      InstrumentedProjectionCheckpoint.make[F](PostgresProjectionCheckpoint.make[F](sessions)),
                    )
      outboxEnabled      <- Resource.eval(Ref.of[F, Boolean](false))
      runningProjections <- Resource.eval(Ref.of[F, Set[String]](Set.empty))
    yield new PostgresModule[F](
      sessions,
      checkpoint,
      PostgresCommandSnapshotStore.make[F](sessions),
      PostgresLeaderElection.make[F](sessions),
      if enableMessageOutbox || enableSaga then Some(PostgresMessageOutbox[F](sessions, notificationHub)) else None,
      if enableSaga then Some(PostgresSagaRepository[F](sessions)) else None,
      notificationHub,
      outboxEnabled,
      runningProjections,
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

  private def createSessionPool[F[_]: Async: Network: TracerProvider: MeterProvider: Meter: Console](
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
                   .withRedactionStrategy(RedactionStrategy.All)
                   .withTelemetryConfig(
                     TelemetryConfig.default
                       .withPoolSpans(TelemetryConfig.PoolSpans.Disabled)
                       .withProtocolSpans(TelemetryConfig.ProtocolSpans.Disabled)
                       .withQueryAnalyzer(queryAnalyzer),
                   )
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

  private val queryAnalyzer: QueryAnalyzer =
    val analyzer = SqlQueryAnalyzer.create(true)
    QueryAnalyzer { sql =>
      Option(analyzer.analyzeWithSummary(sql, SqlDialect.DOUBLE_QUOTES_ARE_IDENTIFIERS)).map { query =>
        QueryAnalyzer.Analysis(
          queryText = Option(query.getQueryText),
          storedProcedureName = Option(query.getStoredProcedureName),
          querySummary = Option(query.getQuerySummary),
        )
      }
    }

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

  /** The schema is created only when missing: `CREATE SCHEMA IF NOT EXISTS` still requires the CREATE privilege on the
    * database, which a role using an existing schema, `public` included, may not have.
    */
  private def initializeDatabase[F[_]: Async: Logger](
    sessions: PostgresSessions[F],
    enableMessageOutbox: Boolean,
    enableSaga: Boolean,
  ): F[Unit] =
    val schema = sessions.schema
    val statements = Statements(schema)
    sessions.use { session =>
      for
        schemaExists <- session.unique(statements.schemaExistsQuery)(schema.name)
        _            <- (Logger[F].info(s"Schema ${schema.name} not found, creating it...") *>
               session.execute(statements.createSchemaCommand)).unlessA(schemaExists)
        eventsTableExists <- session.unique(statements.tableExistsQuery)(schema.name *: "events" *: EmptyTuple)
        _                 <-
          if !eventsTableExists then Logger[F].info(s"Event store tables not found in ${schema.name}, creating them...")
          else
            Logger[F].info(s"Event store tables already exist in ${schema.name}, ensuring required objects are present")
        _ <- createTables(session, schema, enableMessageOutbox, enableSaga)
      yield ()
    }

  private def createTables[F[_]: Sync](
    session: Session[F],
    schema: PostgresSchema,
    enableMessageOutbox: Boolean,
    enableSaga: Boolean,
  ): F[Unit] =
    val statements = Statements(schema)
    val checkpointStatements = PostgresProjectionCheckpoint.Statements(schema)
    val messageOutboxStatements = PostgresMessageOutbox.Statements(schema)
    val sagaStatements = PostgresSagaRepository.Statements(schema)
    import statements.*

    val outboxMessageDdl =
      if enableMessageOutbox || enableSaga then session.execute(messageOutboxStatements.createTableCommand).void
      else Sync[F].unit

    val sagaDdl =
      if enableSaga then
        session.execute(sagaStatements.createTableCommand) *>
          session.execute(sagaStatements.createDueIndexCommand).void
      else Sync[F].unit

    session.execute(createTableCommand) *>
      session.execute(addEventVersionCommand) *>
      session.execute(addHeadersCommand) *>
      session.execute(addCommitGroupCommand) *>
      session.execute(setCommitGroupDefaultCommand) *>
      session.execute(createEventTypeIndexCommand) *>
      session.execute(createCommitOrderIndexCommand) *>
      session.execute(createEventTypeCommitOrderIndexCommand) *>
      session.execute(createEventTagsTableCommand) *>
      session.execute(createEventTagsSequenceIndexCommand) *>
      outboxMessageDdl *>
      session.execute(checkpointStatements.createTableCommand) *>
      session.execute(checkpointStatements.addCommitGroupCommand) *>
      session.execute(PostgresLeaderElection.Statements(schema).createTableCommand) *>
      session.execute(PostgresCommandSnapshotStore.Statements(schema).createTableCommand) *>
      sagaDdl

  final private[postgres] class Statements(schema: PostgresSchema):

    val schemaExistsQuery: Query[String, Boolean] =
      sql"""
        SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $text)
      """.query(bool)

    val createSchemaCommand: Command[Void] =
      sql"""
        CREATE SCHEMA IF NOT EXISTS #${schema.quotedName}
      """.command

    val tableExistsQuery: Query[String *: String *: EmptyTuple, Boolean] =
      sql"""
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_schema = $text
            AND table_name = $text
        )
      """.query(bool)

    val createTableCommand: Command[Void] =
      sql"""
        CREATE TABLE IF NOT EXISTS #${schema.events} (
          sequence_number BIGSERIAL PRIMARY KEY,
          event_id        UUID        NOT NULL DEFAULT gen_random_uuid(),
          event_type      TEXT        NOT NULL,
          event_version   INTEGER     NOT NULL DEFAULT 1,
          tags            JSONB       NOT NULL DEFAULT '[]',
          payload         JSONB       NOT NULL DEFAULT '{}',
          is_external     BOOLEAN     NOT NULL,
          headers         JSONB       NOT NULL DEFAULT '{}',
          recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
          commit_group    BIGINT      NOT NULL DEFAULT (pg_current_xact_id()::text::bigint),

          CONSTRAINT unique_event_id UNIQUE (event_id)
        )
      """.command

    val addEventVersionCommand: Command[Void] =
      sql"""
        ALTER TABLE #${schema.events}
        ADD COLUMN IF NOT EXISTS event_version INTEGER NOT NULL DEFAULT 1
      """.command

    /** Mirrors [[addEventVersionCommand]]: an events table created before author-supplied headers existed must gain the
      * column in place, or every append would fail against it.
      */
    val addHeadersCommand: Command[Void] =
      sql"""
        ALTER TABLE #${schema.events}
        ADD COLUMN IF NOT EXISTS headers JSONB NOT NULL DEFAULT '{}'
      """.command

    /** An events table created before commit groups existed gains the column with 0 for every existing row, which keeps
      * those rows in position order ahead of every later write. The default is set separately so it only applies to new
      * rows: each write is then grouped under the id of the transaction that stored it (see
      * [[persistent4s.ReadCursor]]).
      */
    val addCommitGroupCommand: Command[Void] =
      sql"""
        ALTER TABLE #${schema.events}
        ADD COLUMN IF NOT EXISTS commit_group BIGINT NOT NULL DEFAULT 0
      """.command

    val setCommitGroupDefaultCommand: Command[Void] =
      sql"""
        ALTER TABLE #${schema.events}
        ALTER COLUMN commit_group SET DEFAULT (pg_current_xact_id()::text::bigint)
      """.command

    /** Serves projection reads, which walk the log in (commit_group, sequence_number) order, and the held-back check. */
    val createCommitOrderIndexCommand: Command[Void] =
      sql"""
        CREATE INDEX IF NOT EXISTS idx_events_commit_order ON #${schema.events} (commit_group, sequence_number)
      """.command

    /** Serves projection reads filtered by event type, the filter every projection applies. */
    val createEventTypeCommitOrderIndexCommand: Command[Void] =
      sql"""
        CREATE INDEX IF NOT EXISTS idx_events_event_type_commit_order ON #${schema.events} (event_type, commit_group, sequence_number)
      """.command

    val createEventTagsTableCommand: Command[Void] =
      sql"""
        CREATE TABLE IF NOT EXISTS #${schema.eventTags} (
          tag             TEXT   NOT NULL,
          sequence_number BIGINT NOT NULL REFERENCES #${schema.events}(sequence_number) ON DELETE CASCADE,
          PRIMARY KEY (tag, sequence_number)
        )
      """.command

    val createEventTypeIndexCommand: Command[Void] =
      sql"""
        CREATE INDEX IF NOT EXISTS idx_events_event_type ON #${schema.events} (event_type)
      """.command

    val createEventTagsSequenceIndexCommand: Command[Void] =
      sql"""
        CREATE INDEX IF NOT EXISTS idx_event_tags_sequence_number ON #${schema.eventTags} (sequence_number)
      """.command
