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
import persistent4s.{Event, EventTypeName, Tag}

object PostgresOutboxSuite extends IOSuite:

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
      yield Fixture(components.eventStore, outbox, pool)
    }

  // ----- helpers -----

  private def truncate(pool: Resource[IO, Session[IO]]): IO[Unit] =
    pool.use(_.execute(sql"TRUNCATE events RESTART IDENTITY CASCADE".command)).void

  private def outboxPositions(pool: Resource[IO, Session[IO]]): IO[List[Long]] =
    pool.use(_.execute(sql"SELECT global_position FROM event_outbox ORDER BY global_position".query(int8)))

  private def appendLocal(
    store: PostgresEventStore[IO, TestEvent],
    tag: Tag,
    value: String,
    id: Option[UUID] = None,
  ): IO[Unit] =
    store.appendUnchecked(List((id, Set(tag), EventTypeName.of[TestEvent], false, TestEvent(value)))).void

  private def appendExternal(
    store: PostgresEventStore[IO, TestEvent],
    tag: Tag,
    value: String,
    id: UUID,
  ): IO[Unit] =
    store.appendUnchecked(List((Some(id), Set(tag), EventTypeName.of[TestEvent], true, TestEvent(value)))).void

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
