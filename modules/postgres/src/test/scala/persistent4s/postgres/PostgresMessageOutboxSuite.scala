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

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.circe.Encoder
import io.circe.syntax.*
import org.testcontainers.containers.PostgreSQLContainer
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*
import weaver.IOSuite

import persistent4s.circe.CirceEventCodec
import persistent4s.{Event, EventFilter, EventTypeName, MessageCodec, OutgoingMessage, PendingEvent, Tag}
import io.circe.Decoder

object PostgresMessageOutboxSuite extends IOSuite:

  given org.typelevel.log4cats.Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  override def maxParallelism: Int = 1

  final case class Fixture(
    store: PostgresEventStore[IO, TestEvent],
    outbox: PostgresMessageOutbox[IO],
    pool: Resource[IO, Session[IO]],
  )

  type Res = Fixture

  final case class TestEvent(value: String) extends Event derives Encoder, Decoder

  final case class CreateCharge(amount: Int)

  private given MessageCodec[CreateCharge] with

    def encode(message: CreateCharge): Either[Throwable, String] = Right(s"""{"amount":${message.amount}}""")

    def decode(payload: String): Either[Throwable, CreateCharge] = Right(CreateCharge(0))

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
        components <- PostgresModule.makeWithConfig[IO, TestEvent](config, eventCodec, enableMessageOutbox = true)
        outbox     <- Resource.eval(
                    IO.fromOption(components.messageOutbox)(new IllegalStateException("message outbox missing")),
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
    pool.use(_.execute(sql"TRUNCATE events, message_outbox RESTART IDENTITY CASCADE".command)).void

  private def messageRows(pool: Resource[IO, Session[IO]]): IO[List[(String, Option[String], String)]] =
    pool
      .use(
        _.execute(
          sql"SELECT topic, message_key, payload FROM message_outbox ORDER BY id".query(text *: text.opt *: text),
        ),
      )
      .map(_.map { case t *: k *: p *: EmptyTuple => (t, k, p) })

  private def eventCount(pool: Resource[IO, Session[IO]]): IO[Long] =
    pool.use(_.unique(sql"SELECT count(*) FROM events".query(int8)))

  private val tag = Tag("saga", "s1")

  private val filter = EventFilter(Set.empty, Set(tag))

  private def testEvent(v: String): PendingEvent[TestEvent] =
    PendingEvent(TestEvent(v), Set(tag), EventTypeName.of[TestEvent], isExternal = false)

  // ----- tests -----

  test("enqueue inserts a row with topic, key, and payload") { case Fixture(_, outbox, pool) =>
    for
      _    <- truncate(pool)
      _    <- outbox.enqueue(List(OutgoingMessage("cmd.topic", Some("k1"), "payload")))
      rows <- messageRows(pool)
    yield expect(rows == List(("cmd.topic", Some("k1"), "payload")))
  }

  test("send encodes a typed message via its MessageCodec and enqueues it") { case Fixture(store, outbox, pool) =>
    for
      _    <- truncate(pool)
      _    <- outbox.send("billing.commands", Some("acct-1"), CreateCharge(42))
      rows <- messageRows(pool)
    yield expect(rows == List(("billing.commands", Some("acct-1"), """{"amount":42}""")))
  }

  test("drainBatch hands entries to publish in id order and deletes them") { case Fixture(store, outbox, pool) =>
    for
      _ <- truncate(pool)
      _ <- outbox.enqueue(
             List(OutgoingMessage("t", None, "a"), OutgoingMessage("t", None, "b"), OutgoingMessage("t", None, "c")),
           )
      captured  <- IO.ref(List.empty[String])
      count     <- outbox.drainBatch(10)(entries => captured.update(_ ++ entries.map(_._2.payload)))
      published <- captured.get
      remaining <- messageRows(pool)
    yield expect.all(count == 3, published == List("a", "b", "c"), remaining.isEmpty)
  }

  test("drainBatch on an empty outbox returns 0 and does not call publish") { case Fixture(store, outbox, pool) =>
    for
      _         <- truncate(pool)
      called    <- IO.ref(false)
      count     <- outbox.drainBatch(10)(_ => called.set(true))
      wasCalled <- called.get
    yield expect.all(count == 0, !wasCalled)
  }

  test("a publish failure rolls the batch back - rows remain") { case Fixture(store, outbox, pool) =>
    for
      _         <- truncate(pool)
      _         <- outbox.enqueue(List(OutgoingMessage("t", None, "a"), OutgoingMessage("t", None, "b")))
      result    <- outbox.drainBatch(10)(_ => IO.raiseError(new RuntimeException("boom"))).attempt
      remaining <- messageRows(pool)
    yield expect.all(result.isLeft, remaining.size == 2)
  }

  test("appendWithMessages commits events and message together") { case Fixture(store, outbox, pool) =>
    val msg = OutgoingMessage("cmd.topic", Some("s1"), "payload")
    for
      _      <- truncate(pool)
      _      <- store.appendWithMessages(filter, 0L, List(msg), List(testEvent("e1"))).void
      events <- store.readFrom(0L, filter).compile.toList
      rows   <- messageRows(pool)
    yield expect.all(events.map(_.payload) == List(TestEvent("e1")), rows == List(("cmd.topic", Some("s1"), "payload")))
  }

  test("a rolled-back append (index conflict) enqueues a message - the dual-write guarantee") {
    case Fixture(store, outbox, pool) =>
      val msg = OutgoingMessage("cmd.topic", Some("s1"), "payload")
      for
        _      <- truncate(pool)
        _      <- store.append(filter, 0L, List(testEvent("e1"))).void
        result <- store.appendWithMessages(filter, 0L, List(msg), List(testEvent("e2"))).attempt
        rows   <- messageRows(pool)
        events <- eventCount(pool)
      yield expect.all(result.isLeft, rows.isEmpty, events == 1L)
  }
