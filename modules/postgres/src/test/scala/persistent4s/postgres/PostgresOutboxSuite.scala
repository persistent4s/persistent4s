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

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.circe.{Decoder, Encoder}
import io.circe.syntax.*
import org.testcontainers.containers.PostgreSQLContainer
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*
import weaver.IOSuite

import persistent4s.circe.CirceEventCodec
import persistent4s.{Event, EventFilter, EventTypeName, PendingEvent, Tag}

object PostgresOutboxSuite extends IOSuite:

  given org.typelevel.log4cats.Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  override def maxParallelism: Int = 1

  final case class Fixture(
    store: PostgresEventStore[IO, TestEvent],
    outbox: PostgresOutbox[IO, TestEvent],
    pool: Resource[IO, Session[IO]],
  )

  type Res = Fixture

  final case class TestEvent(value: String) extends Event derives Encoder, Decoder

  private type Container = PostgreSQLContainer[Nothing]

  private val eventCodec = CirceEventCodec.make[TestEvent](
    encodeEvent = _.asJson,
    decodeEvent = (_, json) => json.as[TestEvent].left.map(error => error: Throwable),
  )

  private def postgresConfig(container: Container): PostgresConfig =
    PostgresConfig(
      host = container.getHost, port = container.getMappedPort(5432), user = container.getUsername,
      password = container.getPassword, database = container.getDatabaseName, maxConnections = 16,
    )

  private def postgresContainerResource: Resource[IO, Container] =
    Resource.make {
      IO.blocking {
        val container = new PostgreSQLContainer[Nothing]("postgres:16-alpine")
        container.withStartupTimeout(java.time.Duration.ofMinutes(2))
        container.start()
        container
      }
    }(container => IO.blocking(container.stop()).handleErrorWith(_ => IO.unit))

  override def sharedResource: Resource[IO, Fixture] =
    postgresContainerResource.flatMap { container =>
      val config = postgresConfig(container)
      for
        components <- PostgresModule.makeWithConfig[IO, TestEvent](config, eventCodec, enableOutbox = true)
        outbox     <- Resource.eval(
                    IO.fromOption(components.outbox)(
                      new IllegalStateException("outbox missing despite enableOutbox = true"),
                    ),
                  )
        pool <- Session
                  .Builder[IO]
                  .withHost(config.host)
                  .withPort(config.port)
                  .withUserAndPassword(config.user, config.password)
                  .withDatabase(config.database)
                  .pooled(4)
      yield Fixture(components.transactionalStore, outbox, pool)
    }

  // ----- helpers -----

  private def truncate(pool: Resource[IO, Session[IO]]): IO[Unit] =
    pool.use(_.execute(sql"TRUNCATE events RESTART IDENTITY CASCADE".command)).void

  private def outboxPositions(pool: Resource[IO, Session[IO]]): IO[List[Long]] =
    pool.use(_.execute(sql"SELECT global_position FROM event_outbox ORDER BY global_position".query(int8)))

  private val deleteOutboxRow: Command[Long] =
    sql"DELETE FROM event_outbox WHERE global_position = $int8".command

  private def appendLocal(
    store: PostgresEventStore[IO, TestEvent],
    tag: Tag,
    value: String,
    id: Option[UUID] = None,
  ): IO[Unit] =
    store
      .appendUnchecked(
        List(
          PendingEvent(TestEvent(value), Set(tag), EventTypeName.of[TestEvent], isExternal = false, id = id),
        ),
      )
      .void

  private def appendExternal(
    store: PostgresEventStore[IO, TestEvent],
    tag: Tag,
    value: String,
    id: UUID,
  ): IO[Unit] =
    store
      .appendUnchecked(
        List(
          PendingEvent(TestEvent(value), Set(tag), EventTypeName.of[TestEvent], isExternal = true, id = Some(id)),
        ),
      )
      .void

  // ----- tests -----

  test("appending a non-external event enqueue one outbox row at its global position") { case Fixture(store, _, pool) =>
    for
      _         <- truncate(pool)
      _         <- appendLocal(store, Tag("course", "c1"), "created")
      positions <- outboxPositions(pool)
    yield expect(positions == List(1L))
  }

  test("markPublished removes the entry from the outbox") { case Fixture(store, outbox, pool) =>
    for
      _      <- truncate(pool)
      _      <- appendLocal(store, Tag("course", "c1"), "created")
      before <- outboxPositions(pool)
      _      <- outbox.markPublished(before.head)
      after  <- outboxPositions(pool)
    yield expect.all(before == List(1L), after.isEmpty)
  }

  test("external events are not enqueud, but local ones are") { case Fixture(store, _, pool) =>
    for
      _         <- truncate(pool)
      _         <- appendExternal(store, Tag("course", "c1"), "imported", UUID.randomUUID())
      _         <- appendLocal(store, Tag("course", "c1"), "local")
      positions <- outboxPositions(pool)
    yield expect(positions == List(2L))
  }

  test("a single batched append enqueues only its new local events") { case Fixture(store, _, pool) =>
    // All events in one append go out as one multi-row upsert, and the outbox rows are selected from that
    // statement's RETURNING set. A mis-paired row would enqueue the wrong positions, so this mixes a fresh
    // local event, an already-seen id, and an external event in a single call.
    val seen = UUID.randomUUID()
    for
      _   <- truncate(pool)
      _   <- appendLocal(store, Tag("course", "c1"), "already-there", Some(seen))
      pre <- outboxPositions(pool)
      _   <- outboxPositions(pool).flatMap(_.traverse_(p => pool.use(_.execute(deleteOutboxRow)(p)).void))
      _   <- store
             .appendUnchecked(
               List(
                 // fresh local -> enqueued
                 PendingEvent(
                   TestEvent("fresh"),
                   Set(Tag("course", "c2")),
                   EventTypeName.of[TestEvent],
                   isExternal = false,
                 ),
                 // already-seen id -> resolves to the existing row, must not be enqueued again
                 PendingEvent(
                   TestEvent("duplicate"),
                   Set(Tag("course", "c1")),
                   EventTypeName.of[TestEvent],
                   isExternal = false,
                   id = Some(seen),
                 ),
                 // external -> never enqueued
                 PendingEvent(
                   TestEvent("imported"),
                   Set(Tag("course", "c3")),
                   EventTypeName.of[TestEvent],
                   isExternal = true,
                   id = Some(UUID.randomUUID()),
                 ),
               ),
             )
             .void
      positions <- outboxPositions(pool)
      stored    <- store.readFrom(0L, EventFilter()).compile.toList
    yield expect.all(
      pre == List(1L),
      // only the fresh local event's position, which is the one written after the pre-existing row
      positions == List(2L),
      // the duplicate did not create a fourth row: 3 distinct events are stored, not 4
      stored.map(_.payload.value) == List("already-there", "fresh", "imported"),
    )
  }

  test("duplicate id while the outbox row is still pending: no crash, no second row") { case Fixture(store, _, pool) =>
    val id = UUID.randomUUID()
    for
      _         <- truncate(pool)
      _         <- appendLocal(store, Tag("course", "c1"), "first", Some(id))
      dup       <- appendLocal(store, Tag("coures", "c1"), "duplicate", Some(id)).attempt
      positions <- outboxPositions(pool)
    yield expect.all(dup.isRight, positions == List(1L))
  }

  test("duplicate id after the row was published is not re-enqueud") { case Fixture(store, outbox, pool) =>
    val id = UUID.randomUUID()
    for
      _      <- truncate(pool)
      _      <- appendLocal(store, Tag("course", "c1"), "first", Some(id))
      first  <- outboxPositions(pool)
      _      <- outbox.markPublished(first.head)
      empty  <- outboxPositions(pool)
      dup    <- appendLocal(store, Tag("course", "c1"), "duplicate", Some(id)).attempt
      final_ <- outboxPositions(pool)
    yield expect.all(first == List(1L), empty.isEmpty, dup.isRight, final_.isEmpty)
  }

  test("stream emits unpublished entries in ascending global_position order") { case Fixture(store, outbox, pool) =>
    for
      _        <- truncate(pool)
      _        <- appendLocal(store, Tag("course", "c1"), "e1")
      _        <- appendLocal(store, Tag("course", "c2"), "e2")
      _        <- appendLocal(store, Tag("course", "c3"), "e3")
      emitted  <- outbox.stream(10).take(3).compile.toList.timeout(30.seconds)
      positions = emitted.map(_.metadata.globalPosition)
      payloads  = emitted.map(_.payload)
    yield expect.all(
      positions == List(1L, 2L, 3L),
      payloads == List(TestEvent("e1"), TestEvent("e2"), TestEvent("e3")),
    )
  }
