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

import cats.effect.{IO, Resource}

import io.circe.syntax.*
import io.circe.{Decoder, Encoder, Json}

import persistent4s.circe.{CirceEventCodec, JsonEventUpcaster}
import persistent4s.{Event, EventCodec, EventFilter, EventSchema, EventSchemaMismatch, EventTypeName, Tag}

import org.testcontainers.containers.PostgreSQLContainer
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import skunk.*
import skunk.circe.codec.all.jsonb
import skunk.codec.all.*
import skunk.implicits.*
import weaver.IOSuite

object PostgresEventSchemaSuite extends IOSuite:

  override def maxParallelism: Int = 1

  private given Tracer[IO] = Tracer.Implicits.noop

  private given Meter[IO] = Meter.Implicits.noop

  sealed trait VersionedEvent extends Event

  final case class ValueChanged(value: String) extends VersionedEvent derives Encoder.AsObject, Decoder

  object ValueChanged:

    given EventSchema[ValueChanged] = EventSchema(StableEventType, version = 2)

    given JsonEventUpcaster[ValueChanged] =
      JsonEventUpcaster
        .builder[ValueChanged]
        .step(fromVersion = 1)(json =>
          json.hcursor
            .get[String]("oldValue")
            .map(value => Json.obj("value" -> value.asJson)),
        )
        .build

  final case class Resources(
    store: PostgresEventStore[IO, VersionedEvent],
    sessions: Resource[IO, Session[IO]],
  )

  type Res = Resources

  override def sharedResource: Resource[IO, Res] =
    postgresContainerResource.flatMap { container =>
      PostgresModule
        .makeWithConfig[IO, VersionedEvent](postgresConfig(container), eventCodec)
        .map(components => Resources(components.eventStore, components.sessions))
    }

  private type Container = PostgreSQLContainer[Nothing]

  private val StableEventType = "library.value-changed"

  private val stableEventType = EventTypeName.fromString(StableEventType)

  private val eventCodec = CirceEventCodec.derived[VersionedEvent]

  private def postgresConfig(container: Container): PostgresConfig =
    PostgresConfig(
      host = container.getHost, port = container.getMappedPort(5432), user = container.getUsername,
      password = container.getPassword, database = container.getDatabaseName, maxConnections = 8,
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

  test("append persists the stable event id and current schema version") { resources =>
    for
      tag <- IO(Tag("schema-test", UUID.randomUUID().toString))
      _   <- resources.store.appendUnchecked(
             List(
               (
                 None,
                 Set(tag),
                 EventTypeName.of[ValueChanged],
                 false,
                 ValueChanged("current"): VersionedEvent,
               ),
             ),
           )
      events <- resources.store.readFrom(0L, EventFilter(Set(stableEventType), Set(tag))).compile.toList
    yield expect.all(
      events.length == 1,
      events.head.payload == ValueChanged("current"),
      events.head.metadata.eventType == stableEventType,
      events.head.metadata.eventVersion == 2,
    )
  }

  test("readFrom upcasts an older stored payload using its persisted version") { resources =>
    val legacyPayload = Json.obj("oldValue" -> "legacy".asJson)

    for
      position <- resources.sessions.use(
                    _.unique(insertLegacyEvent)(
                      stableEventType.value *: 1 *: legacyPayload *: EmptyTuple,
                    ),
                  )
      events <- resources.store
                  .readFrom(position - 1L, EventFilter(Set(stableEventType), Set.empty), Some(1))
                  .compile
                  .toList
    yield expect.all(
      events.length == 1,
      events.head.payload == ValueChanged("legacy"),
      events.head.metadata.eventType == stableEventType,
      events.head.metadata.eventVersion == 1,
      events.head.metadata.globalPosition == position,
    )
  }

  test("append rejects a caller schema that disagrees with the storage codec") { resources =>
    val wrongType = EventTypeName.fromString("another.value-changed")

    for
      tag    <- IO(Tag("schema-mismatch", UUID.randomUUID().toString))
      result <- resources.store
                  .appendUnchecked(
                    List((None, Set(tag), wrongType, false, ValueChanged("invalid"): VersionedEvent)),
                  )
                  .attempt
      revision <- resources.store.currentRevision(EventFilter(Set.empty, Set(tag)))
    yield result match
      case Left(error: EventSchemaMismatch) =>
        expect(error.declared.eventType == wrongType) and
          expect(error.storage.eventType == stableEventType) and
          expect(revision == 0L)
      case other => failure(s"Expected EventSchemaMismatch, got $other")
  }

  test("append rejects malformed JSON from a custom codec instead of storing an empty object") { resources =>
    val malformedCodec = new EventCodec[VersionedEvent]:
      def encode(event: VersionedEvent): String = "not-json"
      def decode(eventType: EventTypeName, payload: String): Either[Throwable, VersionedEvent] =
        Left(new UnsupportedOperationException)

    val store = PostgresEventStore[IO, VersionedEvent](resources.sessions, malformedCodec)

    for
      tag    <- IO(Tag("malformed-json", UUID.randomUUID().toString))
      result <- store
                  .appendUnchecked(
                    List(
                      (
                        None,
                        Set(tag),
                        EventTypeName.of[ValueChanged],
                        false,
                        ValueChanged("invalid"): VersionedEvent,
                      ),
                    ),
                  )
                  .attempt
      revision <- store.currentRevision(EventFilter(Set.empty, Set(tag)))
    yield expect(result.isLeft) and expect(revision == 0L)
  }

  test("module initialization upgrades a pre-versioning events table in place") { _ =>
    postgresContainerResource.use { container =>
      val config = postgresConfig(container)
      Session
        .Builder[IO]
        .withHost(config.host)
        .withPort(config.port)
        .withUserAndPassword(config.user, config.password)
        .withDatabase(config.database)
        .pooled(4)
        .use { legacyPool =>
          for
            _      <- legacyPool.use(_.execute(createLegacyEventsTable))
            result <- PostgresModule.makeWithConfig[IO, VersionedEvent](config, eventCodec).use { components =>
                        for
                          columnCount <- components.sessions.use(_.unique(eventVersionColumnCount))
                          tag          = Tag("upgraded-schema", UUID.randomUUID().toString)
                          _           <- components.eventStore.appendUnchecked(
                                 List(
                                   (
                                     None,
                                     Set(tag),
                                     EventTypeName.of[ValueChanged],
                                     false,
                                     ValueChanged("after-upgrade"): VersionedEvent,
                                   ),
                                 ),
                               )
                          events <- components.eventStore
                                      .readFrom(0L, EventFilter(Set(stableEventType), Set(tag)))
                                      .compile
                                      .toList
                        yield expect(columnCount == 1L) and expect(
                          events.headOption.exists(_.metadata.eventVersion == 2),
                        )
                      }
          yield result
        }
    }
  }

  private val insertLegacyEvent: Query[String *: Int *: Json *: EmptyTuple, Long] =
    sql"""
      INSERT INTO events (event_type, event_version, tags, payload, is_external)
      VALUES ($text, $int4, '[]'::jsonb, $jsonb, false)
      RETURNING sequence_number
    """.query(int8)

  private val createLegacyEventsTable: Command[Void] =
    sql"""
      CREATE TABLE events (
        sequence_number BIGSERIAL PRIMARY KEY,
        event_id        UUID        NOT NULL DEFAULT gen_random_uuid(),
        event_type      TEXT        NOT NULL,
        tags            JSONB       NOT NULL DEFAULT '[]',
        payload         JSONB       NOT NULL DEFAULT '{}',
        is_external     BOOLEAN     NOT NULL,
        recorded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
        CONSTRAINT unique_event_id UNIQUE (event_id)
      )
    """.command

  private val eventVersionColumnCount: Query[Void, Long] =
    sql"""
      SELECT COUNT(*)
      FROM information_schema.columns
      WHERE table_schema = 'public'
        AND table_name = 'events'
        AND column_name = 'event_version'
    """.query(int8)
