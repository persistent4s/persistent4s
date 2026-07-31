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

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import fs2.Stream
import io.circe.{Decoder, Encoder}
import org.testcontainers.containers.PostgreSQLContainer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*
import weaver.IOSuite

import persistent4s.*
import persistent4s.circe.{CirceEventCodec, CirceMessageCodec}

/** Integration tests for [[SagaRunner]], driving the real [[PostgresSagaRepository]] and [[PostgresEventStore]].
  *
  * The repository is not faked. Everything interesting about the runner is how it uses the two SQL guards — `start`'s
  * primary-key conflict and `advance`'s pending-at-expected-step condition — so a fake would only prove the fake works.
  * The reply topic *is* faked: [[MessageSubscriber]] exists in core precisely so the runner can be driven without a
  * broker, and a finite fake makes the reply loop terminate on its own instead of needing a timeout to stop it.
  */
object SagaRunnerSuite extends IOSuite:

  override def maxParallelism: Int = 1

  given Logger[IO] = Slf4jLogger.getLogger[IO]

  final case class Fixture(
    store: PostgresEventStore[IO, TestEvent],
    checkpoint: PostgresProjectionCheckpoint[IO],
    repository: PostgresSagaRepository[IO],
    pool: Resource[IO, Session[IO]],
  )

  type Res = Fixture

  // ----- domain under test -----

  sealed trait TestEvent extends Event

  final case class OrderPlaced(orderId: String) extends TestEvent derives Encoder.AsObject, Decoder

  final case class OrderConfirmed(orderId: String) extends TestEvent derives Encoder.AsObject, Decoder

  final case class OrderCancelled(orderId: String, reason: String) extends TestEvent derives Encoder.AsObject, Decoder

  final case class Unrelated(what: String) extends TestEvent derives Encoder.AsObject, Decoder

  final case class ReserveStock(orderId: String) derives Encoder.AsObject, Decoder

  final case class StockReserved(ok: Boolean, reason: Option[String]) derives Encoder.AsObject, Decoder

  final case class OrderState(orderId: String) derives Encoder.AsObject, Decoder

  private val RequestTopic = "stock.commands"

  private val ReplyTopic = "orders.replies"

  private def orderTag(orderId: String): Tag = Tag("order", orderId)

  /** Declines orders keyed "skip", so the trigger loop's "saga ignores an event it was offered" path is reachable. */
  object TestSaga extends Saga[TestEvent, OrderState, ReserveStock, StockReserved]:

    val name = "reserve-stock"

    val triggers = Set(EventTypeName.of[OrderPlaced])

    def start(event: EventEnvelope[TestEvent]): Option[SagaStart[OrderState, ReserveStock]] =
      event.payload match
        case OrderPlaced(orderId) if orderId != "skip" =>
          Some(
            SagaStart(
              key = orderId,
              data = OrderState(orderId),
              request = List(SagaRequest(RequestTopic, Some(orderId), ReserveStock(orderId))),
              timeout = Some(1.hour),
            ),
          )
        case _ => None

    def onReply(
      ctx: SagaContext,
      state: OrderState,
      reply: StockReserved,
    ): SagaDecision[TestEvent, OrderState, ReserveStock] =
      if reply.ok then
        SagaDecision.completed(events = List(Set(orderTag(state.orderId)) -> OrderConfirmed(state.orderId)))
      else
        SagaDecision.compensated(events =
          List(Set(orderTag(state.orderId)) -> OrderCancelled(state.orderId, reply.reason.getOrElse("rejected"))),
        )

    def onTimeout(ctx: SagaContext, state: OrderState): SagaDecision[TestEvent, OrderState, ReserveStock] =
      SagaDecision.compensated(events =
        List(Set(orderTag(state.orderId)) -> OrderCancelled(state.orderId, "timed out")),
      )

    val stateCodec: MessageCodec[OrderState] = CirceMessageCodec.derived[OrderState]

    val requestCodec: MessageCodec[ReserveStock] = CirceMessageCodec.derived[ReserveStock]

    val replyCodec: MessageCodec[StockReserved] = CirceMessageCodec.derived[StockReserved]

  private val eventCodec: EventCodec[TestEvent] = CirceEventCodec.derived[TestEvent]

  // ----- fake reply topic -----

  /** Emits a fixed list and records which messages were acknowledged, so tests can assert that a message the runner
    * chose to drop was still acked rather than left to be redelivered forever.
    */
  final class FakeSubscriber(messages: List[IncomingMessage], acked: Ref[IO, List[IncomingMessage]])
      extends MessageSubscriber[IO]:

    def subscribe(topic: String, fromBeginning: Boolean): Stream[IO, (IncomingMessage, IO[Unit])] =
      Stream.emits(messages).map(message => (message, acked.update(_ :+ message)))

  private def reply(
    id: java.util.UUID,
    payload: String,
    sagaName: String = TestSaga.name,
    withId: Boolean = true,
  ): IncomingMessage =
    val headers = Map(SagaHeaders.Name -> sagaName) ++
      (if withId then Map(SagaHeaders.Id -> id.toString) else Map.empty)
    IncomingMessage(ReplyTopic, None, payload, headers)

  private def accepted: String = """{"ok":true,"reason":null}"""

  private def rejected: String = """{"ok":false,"reason":"out of stock"}"""

  // ----- fixture -----

  private type Container = PostgreSQLContainer[Nothing]

  private def postgresConfig(container: Container): PostgresConfig =
    PostgresConfig(
      host = container.getHost, port = container.getMappedPort(5432), user = container.getUsername,
      password = container.getPassword, database = container.getDatabaseName, maxConnections = 16,
    )

  override def sharedResource: Resource[IO, Fixture] =
    Resource
      .make(
        IO.blocking {
          val container = new PostgreSQLContainer[Nothing]("postgres:16-alpine")
          container.start()
          container
        },
      )(container => IO.blocking(container.stop()).handleErrorWith(_ => IO.unit))
      .flatMap { container =>
        val config = postgresConfig(container)
        for
          components <- PostgresModule.makeWithConfig[IO, TestEvent](config, eventCodec, enableSaga = true)
          repository <- Resource.eval(
                          IO.fromOption(components.sagaRepository)(
                            new IllegalStateException("saga repository missing despite enableSaga = true"),
                          ),
                        )
          pool <- Session
                    .Builder[IO]
                    .withHost(config.host)
                    .withPort(config.port)
                    .withUserAndPassword(config.user, config.password)
                    .withDatabase(config.database)
                    .pooled(4)
        yield Fixture(components.eventStore, components.checkpoint, repository, pool)
      }

  // ----- helpers -----

  private def truncate(fixture: Fixture): IO[Unit] =
    fixture.pool
      .use(
        _.execute(
          sql"""TRUNCATE events, event_tags, message_outbox, saga_instances, projection_checkpoints
                RESTART IDENTITY CASCADE""".command,
        ),
      )
      .void

  private def runner(fixture: Fixture, replies: List[IncomingMessage], acked: Ref[IO, List[IncomingMessage]]) =
    SagaRunner[IO, TestEvent](
      store = fixture.store,
      checkpoint = fixture.checkpoint,
      repository = fixture.repository,
      replies = FakeSubscriber(replies, acked),
      replyTopic = ReplyTopic,
      // Short enough that `.take(1)` on the timer loop is quick rather than a five-second wait.
      timerPollInterval = 100.millis,
    )

  private def noReplies(fixture: Fixture) =
    Ref.of[IO, List[IncomingMessage]](Nil).map(acked => runner(fixture, Nil, acked))

  private def append(fixture: Fixture, events: TestEvent*): IO[Unit] =
    fixture.store
      .appendUnchecked(events.toList.map { event =>
        PendingEvent(event, Set.empty, EventTypeName.fromInstance(event), isExternal = false)
      })
      .void

  private def instanceOf(fixture: Fixture, orderId: String): IO[Option[SagaRecord]] =
    fixture.repository.find(SagaId.instance(TestSaga.name, orderId))

  private def outboxRows(fixture: Fixture): IO[List[(String, Option[String], String, Map[String, String])]] =
    fixture.pool
      .use(
        _.execute(
          sql"SELECT topic, message_key, payload, headers FROM message_outbox ORDER BY id"
            .query(text *: text.opt *: text *: OutboxHeaderCodec.headers),
        ),
      )
      .map(_.map { case t *: k *: p *: h *: EmptyTuple => (t, k, p, h) })

  private def storedEvents(fixture: Fixture): IO[List[String]] =
    fixture.pool.use(_.execute(sql"SELECT event_type FROM events ORDER BY sequence_number".query(text)))

  private def eventCount(fixture: Fixture): IO[Long] =
    fixture.pool.use(_.unique(sql"SELECT count(*) FROM events".query(int8)))

  /** Drive the trigger loop through exactly one work pass. */
  private def runTrigger(fixture: Fixture): IO[Unit] =
    noReplies(fixture).flatMap(_.triggerLoop(TestSaga).take(1).compile.drain)

  // ----- trigger loop -----

  test("a trigger event starts an instance and enqueues its request with the saga headers") { fixture =>
    for
      _       <- truncate(fixture)
      _       <- append(fixture, OrderPlaced("o1"))
      _       <- runTrigger(fixture)
      record  <- instanceOf(fixture, "o1")
      rows    <- outboxRows(fixture)
      expected = SagaId.instance(TestSaga.name, "o1")
    yield
      val (topic, key, payload, headers) = rows.head
      expect.all(
        record.exists(_.status == SagaStatus.Pending),
        record.exists(_.step == 0),
        record.exists(_.key == "o1"),
        rows.size == 1,
        topic == RequestTopic,
        key == Some("o1"),
        payload == """{"orderId":"o1"}""",
        headers.get(SagaHeaders.Name) == Some(TestSaga.name),
        headers.get(SagaHeaders.Id) == Some(expected.toString),
        headers.get(SagaHeaders.ReplyTo) == Some(ReplyTopic),
        headers.get(SagaHeaders.IdempotencyKey) == Some(s"$expected:0:0"),
      )
  }

  test("an event the saga declines starts nothing, and the checkpoint still moves past it") { fixture =>
    for
      _      <- truncate(fixture)
      _      <- append(fixture, OrderPlaced("skip"))
      _      <- runTrigger(fixture)
      record <- instanceOf(fixture, "skip")
      state  <- fixture.checkpoint.load(SagaRunner.loopName(TestSaga.name))
      rows   <- outboxRows(fixture)
    yield expect.all(record.isEmpty, rows.isEmpty, state.exists(_.globalPosition > 0L))
  }

  test("events outside the saga's triggers are never offered to it") { fixture =>
    for
      _    <- truncate(fixture)
      _    <- append(fixture, Unrelated("noise"), OrderConfirmed("o1"))
      _    <- runTrigger(fixture)
      rows <- outboxRows(fixture)
      all  <- fixture.pool.use(_.unique(sql"SELECT count(*) FROM saga_instances".query(int8)))
    yield expect.all(rows.isEmpty, all == 0L)
  }

  test("replaying a trigger event does not enqueue the request twice") { fixture =>
    for
      _ <- truncate(fixture)
      _ <- append(fixture, OrderPlaced("o1"))
      _ <- runTrigger(fixture)
      // Rewind the checkpoint so the same event is offered again, as it would be after a restore or a reset.
      _    <- fixture.checkpoint.save(ProjectionCheckpointState(SagaRunner.loopName(TestSaga.name), -1L, true, None))
      _    <- runTrigger(fixture)
      rows <- outboxRows(fixture)
    yield expect(rows.size == 1)
  }

  // ----- reply loop -----

  test("an accepting reply completes the instance and appends its event") { fixture =>
    val id = SagaId.instance(TestSaga.name, "o1")
    for
      _      <- truncate(fixture)
      _      <- append(fixture, OrderPlaced("o1"))
      _      <- runTrigger(fixture)
      acked  <- Ref.of[IO, List[IncomingMessage]](Nil)
      _      <- runner(fixture, List(reply(id, accepted)), acked).replyLoop(TestSaga).compile.drain
      record <- instanceOf(fixture, "o1")
      events <- storedEvents(fixture)
      seen   <- acked.get
    yield expect.all(
      record.exists(_.status == SagaStatus.Completed),
      events == List("OrderPlaced", "OrderConfirmed"),
      seen.size == 1,
    )
  }

  test("a rejecting reply compensates the instance and appends the compensating event") { fixture =>
    val id = SagaId.instance(TestSaga.name, "o1")
    for
      _      <- truncate(fixture)
      _      <- append(fixture, OrderPlaced("o1"))
      _      <- runTrigger(fixture)
      acked  <- Ref.of[IO, List[IncomingMessage]](Nil)
      _      <- runner(fixture, List(reply(id, rejected)), acked).replyLoop(TestSaga).compile.drain
      record <- instanceOf(fixture, "o1")
      events <- storedEvents(fixture)
    yield expect.all(
      record.exists(_.status == SagaStatus.Compensated),
      events == List("OrderPlaced", "OrderCancelled"),
    )
  }

  test("a reply naming another saga is left untouched") { fixture =>
    val id = SagaId.instance(TestSaga.name, "o1")
    for
      _     <- truncate(fixture)
      _     <- append(fixture, OrderPlaced("o1"))
      _     <- runTrigger(fixture)
      acked <- Ref.of[IO, List[IncomingMessage]](Nil)
      _     <- runner(fixture, List(reply(id, accepted, sagaName = "some-other-saga")), acked)
             .replyLoop(TestSaga)
             .compile
             .drain
      record <- instanceOf(fixture, "o1")
      count  <- eventCount(fixture)
    yield expect.all(record.exists(_.status == SagaStatus.Pending), count == 1L)
  }

  test("a reply with no correlation id is dropped and acknowledged") { fixture =>
    val id = SagaId.instance(TestSaga.name, "o1")
    for
      _      <- truncate(fixture)
      _      <- append(fixture, OrderPlaced("o1"))
      _      <- runTrigger(fixture)
      acked  <- Ref.of[IO, List[IncomingMessage]](Nil)
      _      <- runner(fixture, List(reply(id, accepted, withId = false)), acked).replyLoop(TestSaga).compile.drain
      record <- instanceOf(fixture, "o1")
      seen   <- acked.get
    yield expect.all(record.exists(_.status == SagaStatus.Pending), seen.size == 1)
  }

  test("an undecodable reply is dropped and acknowledged, leaving the instance pending for its deadline") { fixture =>
    val id = SagaId.instance(TestSaga.name, "o1")
    for
      _      <- truncate(fixture)
      _      <- append(fixture, OrderPlaced("o1"))
      _      <- runTrigger(fixture)
      acked  <- Ref.of[IO, List[IncomingMessage]](Nil)
      _      <- runner(fixture, List(reply(id, "not json at all")), acked).replyLoop(TestSaga).compile.drain
      record <- instanceOf(fixture, "o1")
      count  <- eventCount(fixture)
      seen   <- acked.get
    yield expect.all(record.exists(_.status == SagaStatus.Pending), count == 1L, seen.size == 1)
  }

  test("a redelivered reply for a settled instance changes nothing") { fixture =>
    val id = SagaId.instance(TestSaga.name, "o1")
    for
      _     <- truncate(fixture)
      _     <- append(fixture, OrderPlaced("o1"))
      _     <- runTrigger(fixture)
      acked <- Ref.of[IO, List[IncomingMessage]](Nil)
      _     <- runner(fixture, List(reply(id, accepted), reply(id, accepted)), acked)
             .replyLoop(TestSaga)
             .compile
             .drain
      record <- instanceOf(fixture, "o1")
      events <- storedEvents(fixture)
      seen   <- acked.get
    yield expect.all(
      record.exists(_.status == SagaStatus.Completed),
      events == List("OrderPlaced", "OrderConfirmed"),
      seen.size == 2,
    )
  }

  test("a reply for an unknown instance is dropped and acknowledged") { fixture =>
    for
      _     <- truncate(fixture)
      acked <- Ref.of[IO, List[IncomingMessage]](Nil)
      _     <- runner(fixture, List(reply(SagaId.instance(TestSaga.name, "ghost"), accepted)), acked)
             .replyLoop(TestSaga)
             .compile
             .drain
      count <- eventCount(fixture)
      seen  <- acked.get
    yield expect.all(count == 0L, seen.size == 1)
  }

  // ----- timer loop -----

  test("an instance past its deadline is compensated") { fixture =>
    val id = SagaId.instance(TestSaga.name, "o1")
    for
      _      <- truncate(fixture)
      _      <- fixture.repository.start(id, TestSaga.name, "o1", """{"orderId":"o1"}""", Some(1.milli), Nil)
      _      <- IO.sleep(100.millis)
      _      <- noReplies(fixture).flatMap(_.timerLoop(TestSaga).take(1).compile.drain)
      record <- instanceOf(fixture, "o1")
      events <- storedEvents(fixture)
    yield expect.all(record.exists(_.status == SagaStatus.Compensated), events == List("OrderCancelled"))
  }

  test("an instance still within its deadline is left alone") { fixture =>
    val id = SagaId.instance(TestSaga.name, "o1")
    for
      _      <- truncate(fixture)
      _      <- fixture.repository.start(id, TestSaga.name, "o1", """{"orderId":"o1"}""", Some(1.hour), Nil)
      _      <- noReplies(fixture).flatMap(_.timerLoop(TestSaga).take(1).compile.drain)
      record <- instanceOf(fixture, "o1")
      count  <- eventCount(fixture)
    yield expect.all(record.exists(_.status == SagaStatus.Pending), count == 0L)
  }

  test("an instance whose stored state cannot be decoded is marked failed rather than retried forever") { fixture =>
    val id = SagaId.instance(TestSaga.name, "o1")
    for
      _      <- truncate(fixture)
      _      <- fixture.repository.start(id, TestSaga.name, "o1", "not json at all", Some(1.milli), Nil)
      _      <- IO.sleep(100.millis)
      _      <- noReplies(fixture).flatMap(_.timerLoop(TestSaga).take(1).compile.drain)
      record <- instanceOf(fixture, "o1")
      count  <- eventCount(fixture)
    yield expect.all(record.exists(_.status == SagaStatus.Failed), record.exists(_.deadline.isEmpty), count == 0L)
  }

/** The outbox `headers` column is JSONB; this suite reads it back to assert what the runner stamped. */
private object OutboxHeaderCodec:

  import io.circe.Json
  import skunk.circe.codec.all.jsonb

  val headers: Codec[Map[String, String]] =
    jsonb.imap { json =>
      json.asObject.map(_.toMap.flatMap { case (k, v) => v.asString.map(k -> _) }).getOrElse(Map.empty)
    }(map => Json.obj(map.toSeq.map((k, v) => k -> Json.fromString(v))*))
