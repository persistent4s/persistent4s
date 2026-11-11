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

import java.time.Instant
import java.util.UUID

import scala.concurrent.duration.*

import cats.effect.{Deferred, IO, Resource}
import cats.syntax.all.*

import io.circe.{Decoder, Encoder}

import org.testcontainers.containers.PostgreSQLContainer
import org.typelevel.log4cats.Logger

import persistent4s.circe.CirceEventCodec
import persistent4s.{
  Event,
  EventFilter,
  EventTypeName,
  OutgoingMessage,
  PendingEvent,
  ProjectionCheckpointState,
  ProjectionCommit,
  ReadCursor,
  SagaId,
  SagaStatus,
  SnapshotId,
  StoredCommandSnapshot,
  Tag,
}

import skunk.*
import skunk.codec.all.*
import skunk.implicits.*

import weaver.IOSuite

/** A module configured with its own schema keeps every persistent4s table there. The connection's `search_path` stays
  * PostgreSQL's default, which does not include that schema, so any statement still naming a table without its schema
  * fails instead of passing unnoticed.
  */
object PostgresCustomSchemaSuite extends IOSuite:

  given Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  override def maxParallelism: Int = 1

  private val Schema = PostgresSchema("persistent4s")

  private val Persistent4sTables: Set[String] = Set(
    "events", "event_tags", "event_outbox", "message_outbox", "projection_checkpoints", "command_snapshots",
    "leader_leases", "saga_instances",
  )

  final case class Fixture(config: PostgresConfig, domain: PostgresDomain[IO, SchemaEvent])

  type Res = Fixture

  override def sharedResource: Resource[IO, Res] =
    for
      container <- postgresContainerResource
      config     = postgresConfig(container)
      _         <- Resource.eval(createDomainTable(config))
      module    <- PostgresModule.makeWithConfig[IO](config, enableMessageOutbox = true, enableSaga = true)
      domain    <- module.domain(eventCodec, outbox = true)
    yield Fixture(config, domain)

  sealed trait SchemaEvent extends Event

  final case class Recorded(value: String) extends SchemaEvent derives Encoder.AsObject, Decoder

  final case class RecordedValue(id: UUID, value: String) derives PostgresRow

  private val eventCodec = CirceEventCodec.derived[SchemaEvent]

  // A domain table, left in `public` while persistent4s lives in its own schema.
  private val valueTable: PostgresTable[UUID, RecordedValue] =
    PostgresTable.derived[RecordedValue]("recorded_values").key(_.id)

  test("the module creates every persistent4s table in the configured schema and none in public") { fixture =>
    for
      inSchema <- tablesIn(fixture, Schema.name)
      inPublic <- tablesIn(fixture, "public")
    yield expect.all(inSchema == Persistent4sTables, inPublic == Set("recorded_values"))
  }

  test("starting the module again on an existing schema keeps its events") { fixture =>
    val tag = Tag("restart", UUID.randomUUID().toString)
    for
      _        <- append(fixture, tag, "before restart")
      restored <- PostgresModule.makeWithConfig[IO](fixture.config).flatMap(_.domain(eventCodec)).use { domain =>
                    domain.eventStore.readFrom(0L, EventFilter(tags = Set(tag))).compile.toList
                  }
    yield expect(restored.map(_.payload) == List(Recorded("before restart")))
  }

  test("the event store, event outbox and message outbox use the configured schema") { fixture =>
    val store = fixture.domain.transactionalStore
    val tag = Tag("course", UUID.randomUUID().toString)
    val filter = EventFilter(Set(EventTypeName.of[Recorded]), Set(tag))
    val message = OutgoingMessage("schema.topic", Some(tag.value), "payload")
    for
      appended  <- store.appendWithMessages(filter, 0L, List(message), List(pending(tag, "first")))
      _         <- store.append(filter, appended.last.metadata.globalPosition, List(pending(tag, "second")))
      revision  <- store.currentRevision(filter)
      byTag     <- store.currentRevision(EventFilter(tags = Set(tag)))
      read      <- store.readFrom(0L, filter).compile.toList
      limited   <- store.readFrom(0L, filter, Some(1)).compile.toList
      projected <- store.readForProjection(ReadCursor.Start, filter, maxEvents = 10)
      cursor    <- store.cursorAt(revision)
      // The outbox stream never ends: stop at this test's last event, which it delivers in position order.
      queued <- fixture.domain.outbox.get
                  .stream(10)
                  .takeThrough(_.metadata.globalPosition < revision)
                  .compile
                  .toList
                  .timeout(10.seconds)
      _       <- fixture.domain.outbox.get.markPublished(queued.map(_.metadata.globalPosition))
      drained <- fixture.domain.module.messageOutbox.get.drainBatch(10)(_ => IO.unit)
    yield expect.all(
      read.map(_.payload) == List(Recorded("first"), Recorded("second")),
      limited.map(_.payload) == List(Recorded("first")),
      projected.events.map(_.payload) == List(Recorded("first"), Recorded("second")),
      revision == read.last.metadata.globalPosition,
      byTag == revision,
      cursor.globalPosition == revision,
      queued.map(_.metadata.globalPosition).takeRight(2) == read.map(_.metadata.globalPosition),
      drained >= 1,
    )
  }

  test("checkpoints, snapshots, leader leases and sagas use the configured schema") { fixture =>
    val module = fixture.domain.module
    val projection = s"projection-${UUID.randomUUID()}"
    val snapshotId = SnapshotId(s"snapshot-${UUID.randomUUID()}")
    val snapshot = StoredCommandSnapshot(globalPosition = 3L, eventCount = 2L, filterFingerprint = "f", payload = "{}")
    val sagaName = s"saga-${UUID.randomUUID()}"
    val sagaId = SagaId.instance(sagaName, "k1")
    val lease = s"lease-${UUID.randomUUID()}"
    for
      _          <- module.checkpoint.save(ProjectionCheckpointState(projection, ReadCursor(0L, 5L), true, None))
      checkpoint <- module.checkpoint.load(projection)
      all        <- module.checkpoint.loadAll()
      _          <- module.snapshotStore.save(snapshotId, "key", 1, snapshot)
      loaded     <- module.snapshotStore.load(snapshotId, "key", 1)
      _          <- module.snapshotStore.delete(snapshotId, "key", 1)
      deleted    <- module.snapshotStore.load(snapshotId, "key", 1)
      leading    <- Deferred[IO, Long]
      leader     <- module.leaderElection
                  .runAsLeader(lease)(leaseCount(module, lease).flatMap(leading.complete) *> IO.never)
                  .start
      leases   <- leading.get.timeout(10.seconds).guarantee(leader.cancel)
      sagas     = module.sagaRepository.get
      started  <- sagas.start(sagaId, sagaName, "k1", "data", Some(Instant.now().minusSeconds(1)), Nil)
      advanced <- sagas.advance(sagaId, 0, SagaStatus.Pending, 1, "data-1", Some(Instant.now().minusSeconds(1)))
      claimed  <- sagas.claimExpired(sagaName, 10)(_ => IO.unit)
      saga     <- sagas.find(sagaId)
    yield expect.all(
      checkpoint.map(_.cursor).contains(ReadCursor(0L, 5L)),
      all.exists(_.projectionName == projection),
      loaded.contains(snapshot),
      deleted.isEmpty,
      leases == 1L,
      started,
      advanced,
      claimed == 1,
      saga.map(_.step).contains(1),
    )
  }

  test("an atomic repository commits its domain rows with the checkpoint in the persistent4s schema") { fixture =>
    val module = fixture.domain.module
    val repository = module.repository(valueTable)
    val projection = s"values-${UUID.randomUUID()}"
    val id = UUID.randomUUID()
    val committed = ProjectionCheckpointState(projection, ReadCursor(0L, 7L), true, None)
    for
      _ <- repository.persistAtomically(
             ProjectionCommit(Map(id -> RecordedValue(id, "v")), Nil, ReadCursor.Start, committed),
           )
      read   <- repository.readWithCursor(projection)(repository.find(_, id))
      stored <- module.checkpoint.load(projection)
    yield expect.all(read == (Some(RecordedValue(id, "v")), ReadCursor(0L, 7L)), stored.contains(committed))
  }

  test("a module refuses to start an empty event store while another schema holds events") { fixture =>
    for
      config  <- freshDatabase(fixture.config)
      _       <- appendInPublic(config, "kept in public")
      started <- PostgresModule.makeWithConfig[IO](config).use_.attempt
      created <- session(config).use(_.unique(schemaExists)(Schema.name))
    yield expect.all(
      started == Left(PostgresModuleError.EventsInOtherSchema(Schema.name, List("public"))),
      !created,
    )
  }

  test("a module starts with the events once the tables moved into the configured schema") { fixture =>
    for
      config   <- freshDatabase(fixture.config)
      _        <- appendInPublic(config, "moved")
      _        <- session(config).use(moveTablesFromPublic)
      restored <- PostgresModule.makeWithConfig[IO](config).flatMap(_.domain(eventCodec)).use { domain =>
                    domain.eventStore.readFrom(0L, EventFilter()).compile.toList
                  }
    yield expect(restored.map(_.payload) == List(Recorded("moved")))
  }

  test("an empty event store in another schema does not block a new one") { fixture =>
    for
      config  <- freshDatabase(fixture.config)
      _       <- PostgresModule.makeWithConfig[IO](config.copy(schema = PostgresSchema.Public)).use_
      started <- PostgresModule.makeWithConfig[IO](config).use_.attempt
    yield expect(started.isRight)
  }

  pureTest("a schema name must be a single PostgreSQL identifier") {
    expect.all(
      PostgresSchema.from("persistent4s").isRight,
      PostgresSchema.from("integration.events").isLeft,
      PostgresSchema.from("bad-name").isLeft,
      PostgresSchema.from("").isLeft,
    )
  }

  private def pending(tag: Tag, value: String): PendingEvent[SchemaEvent] =
    PendingEvent(Recorded(value), Set(tag), EventTypeName.of[Recorded], isExternal = false)

  /** A new database on the suite's server, configured for [[Schema]]. */
  private def freshDatabase(config: PostgresConfig): IO[PostgresConfig] =
    val name = s"guard_${UUID.randomUUID().toString.replace("-", "")}"
    session(config).use(_.execute(sql"CREATE DATABASE #$name".command)).as(config.copy(database = name))

  /** Stores one event the way a service did before it configured a schema. */
  private def appendInPublic(config: PostgresConfig, value: String): IO[Unit] =
    PostgresModule
      .makeWithConfig[IO](config.copy(schema = PostgresSchema.Public))
      .flatMap(_.domain(eventCodec))
      .use(_.eventStore.appendUnchecked(List(pending(Tag("guard", value), value))).void)

  private def moveTablesFromPublic(session: Session[IO]): IO[Unit] =
    session.execute(sql"CREATE SCHEMA #${Schema.quotedName}".command) *>
      Persistent4sTables.toList.traverse_ { table =>
        session.execute(sql"ALTER TABLE IF EXISTS public.#$table SET SCHEMA #${Schema.quotedName}".command)
      }

  private val schemaExists: Query[String, Boolean] =
    sql"SELECT EXISTS (SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = $text)".query(bool)

  private def append(fixture: Fixture, tag: Tag, value: String): IO[Unit] =
    fixture.domain.eventStore.appendUnchecked(List(pending(tag, value))).void

  private def tablesIn(fixture: Fixture, schema: String): IO[Set[String]] =
    val query: Query[String, String] =
      sql"SELECT table_name::text FROM information_schema.tables WHERE table_schema = $text".query(text)
    fixture.domain.module.sessions.use(_.execute(query)(schema)).map(_.toSet)

  private def leaseCount(module: PostgresModule[IO], name: String): IO[Long] =
    val query: Query[String, Long] =
      sql"SELECT COUNT(*) FROM #${Schema.leaderLeases} WHERE name = $text".query(int8)
    module.sessions.use(_.unique(query)(name))

  private def createDomainTable(config: PostgresConfig): IO[Unit] =
    session(config).use(
      _.execute(sql"CREATE TABLE recorded_values (id UUID PRIMARY KEY, value TEXT NOT NULL)".command).void,
    )

  private def session(config: PostgresConfig): Resource[IO, Session[IO]] =
    Session
      .Builder[IO]
      .withHost(config.host)
      .withPort(config.port)
      .withUserAndPassword(config.user, config.password)
      .withDatabase(config.database)
      .single

  private type Container = PostgreSQLContainer[Nothing]

  private def postgresConfig(container: Container): PostgresConfig =
    PostgresConfig(
      host = container.getHost, port = container.getMappedPort(5432), user = container.getUsername,
      password = container.getPassword, database = container.getDatabaseName, maxConnections = 16, schema = Schema,
    )

  private def postgresContainerResource: Resource[IO, Container] =
    Resource.make {
      IO.blocking {
        val container = new PostgreSQLContainer[Nothing]("postgres:16-alpine")
        container.start()
        container
      }
    } { container =>
      IO.blocking(container.stop()).handleErrorWith(_ => IO.unit)
    }
