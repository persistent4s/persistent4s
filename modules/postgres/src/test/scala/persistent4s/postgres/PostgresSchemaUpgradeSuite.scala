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

import java.util.UUID

import scala.concurrent.duration.*

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*

import io.circe.syntax.*
import io.circe.{Decoder, Encoder, Json}

import org.typelevel.log4cats.Logger

import org.testcontainers.containers.PostgreSQLContainer

import persistent4s.circe.CirceEventCodec
import persistent4s.{
  DefaultProjector,
  Event,
  EventEnvelope,
  EventFilter,
  EventTypeName,
  PendingEvent,
  ProjectionCheckpointState,
  ReadCursor,
  StatelessProjection,
}

import skunk.*
import skunk.circe.codec.all.jsonb
import skunk.codec.all.*
import skunk.implicits.*

import weaver.IOSuite

/** A database created before commit groups existed must keep working once the module starts on it: its events stay
  * readable and ahead of every new write, and a projection checkpointed before the upgrade resumes right after the same
  * event. The legacy schema below is frozen on purpose: it is what an existing deployment has, not what the module
  * creates today.
  */
object PostgresSchemaUpgradeSuite extends IOSuite:

  given Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  override def maxParallelism: Int = 1

  private val LegacyValues: List[String] = List("legacy-1", "legacy-2", "legacy-3")

  // Checkpointed right after legacy-2 by a projector running the previous version.
  private val LegacyProjection = "legacy-projection"

  private val LegacyCheckpointPosition = 2L

  final case class Upgraded(config: PostgresConfig, components: PostgresDomain[IO, UpgradeEvent])

  type Res = Upgraded

  override def sharedResource: Resource[IO, Res] =
    for
      container  <- postgresContainerResource
      config      = postgresConfig(container)
      _          <- Resource.eval(createLegacyDatabase(config))
      components <- PostgresModule.makeWithConfig[IO](config).flatMap(_.domain(eventCodec))
    yield Upgraded(config, components)

  sealed trait UpgradeEvent extends Event

  final case class Recorded(value: String) extends UpgradeEvent derives Encoder.AsObject, Decoder

  private type Container = PostgreSQLContainer[Nothing]

  private val eventCodec = CirceEventCodec.derived[UpgradeEvent]

  private def postgresConfig(container: Container): PostgresConfig =
    PostgresConfig(
      host = container.getHost, port = container.getMappedPort(5432), user = container.getUsername,
      password = container.getPassword, database = container.getDatabaseName, maxConnections = 16,
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

  // ----- the schema and data of a deployment that predates commit groups -----

  private val legacyEventsTable: Command[Void] =
    sql"""
      CREATE TABLE events (
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

  private val legacyCheckpointsTable: Command[Void] =
    sql"""
      CREATE TABLE projection_checkpoints (
        projection_name TEXT   PRIMARY KEY,
        global_position BIGINT NOT NULL,
        running BOOLEAN NOT NULL,
        error TEXT
      )
    """.command

  private val insertLegacyEvent: Command[String *: Json *: EmptyTuple] =
    sql"""
      INSERT INTO events (event_type, payload, is_external)
      VALUES ($text, $jsonb, true)
    """.command

  private val insertLegacyCheckpoint: Command[String *: Long *: EmptyTuple] =
    sql"""
      INSERT INTO projection_checkpoints (projection_name, global_position, running, error)
      VALUES ($text, $int8, true, NULL)
    """.command

  private def createLegacyDatabase(config: PostgresConfig): IO[Unit] =
    Session
      .Builder[IO]
      .withHost(config.host)
      .withPort(config.port)
      .withUserAndPassword(config.user, config.password)
      .withDatabase(config.database)
      .single
      .use { session =>
        for
          _ <- session.execute(legacyEventsTable)
          _ <- session.execute(legacyCheckpointsTable)
          _ <- session.prepare(insertLegacyEvent).flatMap { insert =>
                 LegacyValues.traverse_ { value =>
                   val event = Recorded(value)
                   insert.execute(eventCodec.eventType(event).value *: event.asJson *: EmptyTuple)
                 }
               }
          _ <- session.prepare(insertLegacyCheckpoint).flatMap { insert =>
                 insert.execute(LegacyProjection *: LegacyCheckpointPosition *: EmptyTuple)
               }
        yield ()
      }

  // ----- helpers -----

  private def append(components: PostgresDomain[IO, UpgradeEvent], value: String) =
    val event = Recorded(value)
    components.eventStore
      .appendUnchecked(List(PendingEvent(event, Set.empty, eventCodec.eventType(event), true, None, Map.empty)))
      .map(_.head)

  private def waitUntil(cond: IO[Boolean]): IO[Unit] =
    cond.flatMap {
      case true  => IO.unit
      case false => IO.sleep(20.millis) *> waitUntil(cond)
    }

  private val indexNames: Query[Void, String] =
    sql"SELECT indexname FROM pg_indexes WHERE tablename = 'events'".query(name)

  // ----- tests -----

  test("events stored before the upgrade keep commit group 0 and are delivered ahead of every new write") {
    case Upgraded(_, components) =>
      for
        written  <- append(components, s"new-${UUID.randomUUID()}")
        legacy   <- components.eventStore.readFrom(0L, EventFilter(), Some(LegacyValues.size)).compile.toList
        firstRun <- components.eventStore.readForProjection(ReadCursor.Start, EventFilter(), LegacyValues.size)
      yield expect.all(
        clue(legacy.map(_.payload)) == LegacyValues.map(Recorded(_)),
        clue(legacy.map(_.metadata.commitGroup)).forall(_ == 0L),
        clue(written.metadata.commitGroup) > 0L,
        clue(firstRun.events.map(_.payload)) == LegacyValues.map(Recorded(_)),
      )
  }

  test("a projection checkpointed before the upgrade resumes right after the same event") {
    case Upgraded(_, components) =>
      val projection = (seen: Ref[IO, List[UpgradeEvent]]) =>
        new StatelessProjection[IO, UpgradeEvent]:
          def name: String = LegacyProjection
          def filter: Set[EventTypeName] = Set(EventTypeName.of[Recorded])
          def handle(event: EventEnvelope[UpgradeEvent]): IO[Unit] = seen.update(_ :+ event.payload)
      for
        loaded  <- components.module.checkpoint.load(LegacyProjection)
        written <- append(components, "after-upgrade")
        seen    <- Ref.of[IO, List[UpgradeEvent]](Nil)
        _       <- DefaultProjector(components.eventStore, components.module.checkpoint)
               .run(projection(seen))
               .compile
               .drain
               .background
               .use(_ => waitUntil(seen.get.map(_.contains(written.payload))).timeoutTo(5.seconds, IO.unit))
        handled <- seen.get
      yield expect.all(
        clue(loaded) == Some(
          ProjectionCheckpointState(LegacyProjection, ReadCursor(0L, LegacyCheckpointPosition), true, None),
        ),
        // Every other test only appends, so whatever follows legacy-3 was written after the upgrade.
        clue(handled).headOption == Some(Recorded("legacy-3")),
        handled.last == written.payload,
        !handled.contains(Recorded("legacy-1")),
        !handled.contains(Recorded("legacy-2")),
      )
  }

  test("starting the module again on an upgraded database changes nothing") { case Upgraded(config, components) =>
    for
      _       <- PostgresModule.makeWithConfig[IO](config).flatMap(_.domain(eventCodec)).use_
      written <- append(components, s"after-restart-${UUID.randomUUID()}")
      legacy  <- components.eventStore.readFrom(0L, EventFilter(), Some(LegacyValues.size)).compile.toList
      indexes <- components.module.sessions.use(_.execute(indexNames))
    yield expect.all(
      clue(written.metadata.commitGroup) > 0L,
      clue(legacy.map(_.metadata.commitGroup)).forall(_ == 0L),
      clue(indexes.toSet).contains("idx_events_commit_order"),
      indexes.toSet.contains("idx_events_event_type_commit_order"),
    )
  }
