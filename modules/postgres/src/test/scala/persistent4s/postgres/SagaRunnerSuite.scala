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
      reply: SagaReply[StockReserved],
    ): SagaDecision[TestEvent, OrderState, ReserveStock] =
      if reply.payload.ok then
        SagaDecision.completed(events = List(Set(orderTag(state.orderId)) -> OrderConfirmed(state.orderId)))
      else
        SagaDecision.compensated(events =
          List(
            Set(orderTag(state.orderId)) ->
              OrderCancelled(state.orderId, reply.payload.reason.getOrElse("rejected")),
          ),
        )

    def onTimeout(ctx: SagaContext, state: OrderState): SagaDecision[TestEvent, OrderState, ReserveStock] =
      SagaDecision.compensated(events =
        List(Set(orderTag(state.orderId)) -> OrderCancelled(state.orderId, "timed out")),
      )

    val stateCodec: MessageCodec[OrderState] = CirceMessageCodec.derived[OrderState]

    val requestEncoder: MessageEncoder[ReserveStock] = CirceMessageCodec.derived[ReserveStock]

    val replyDecoder: MessageDecoder[StockReserved] = CirceMessageCodec.derived[StockReserved]

  /** Writes what the runner told it about the reply into the event it appends, so a test can see whether a saga can
    * actually tell which of its requests was answered — the thing a fan-out needs and cannot get from the payload.
    */
  object ObservingSaga extends Saga[TestEvent, OrderState, ReserveStock, StockReserved]:

    val name = "observe-reply"

    val triggers = Set(EventTypeName.of[OrderPlaced])

    def start(event: EventEnvelope[TestEvent]): Option[SagaStart[OrderState, ReserveStock]] =
      event.payload match
        case OrderPlaced(orderId) =>
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
      reply: SagaReply[StockReserved],
    ): SagaDecision[TestEvent, OrderState, ReserveStock] =
      val answered = reply.answering.fold("none")(ref => s"${ref.round}:${ref.ordinal}")
      SagaDecision.completed(events = List(Set(orderTag(state.orderId)) -> Unrelated(answered)))

    def onTimeout(ctx: SagaContext, state: OrderState): SagaDecision[TestEvent, OrderState, ReserveStock] =
      SagaDecision.compensated()

    val stateCodec: MessageCodec[OrderState] = CirceMessageCodec.derived[OrderState]

    val requestEncoder: MessageEncoder[ReserveStock] = CirceMessageCodec.derived[ReserveStock]

    val replyDecoder: MessageDecoder[StockReserved] = CirceMessageCodec.derived[StockReserved]

  // ----- a saga that asks two partners at once -----

  sealed trait FanOutRequest

  final case class ReserveStockFor(orderId: String) extends FanOutRequest derives Encoder.AsObject, Decoder

  final case class ChargeFor(orderId: String, cents: Int) extends FanOutRequest derives Encoder.AsObject, Decoder

  final case class ReleaseStockFor(orderId: String) extends FanOutRequest derives Encoder.AsObject, Decoder

  final case class RefundFor(orderId: String, cents: Int) extends FanOutRequest derives Encoder.AsObject, Decoder

  private val PaymentTopic = "payment.commands"

  /** What an instance has heard back so far. This is what `S` is actually for: a single-request saga can work from
    * [[SagaContext]] alone, but a fan-in has to remember which partners have answered while it waits for the rest.
    */
  final case class FanOutState(orderId: String, stock: Option[Boolean], payment: Option[Boolean])
      derives Encoder.AsObject,
        Decoder

  /** Asks two partners in one round and waits for both.
    *
    * Both partners answer with the same [[StockReserved]] shape on purpose: structurally identical replies are exactly
    * the case that cannot be told apart from the payload, so this saga attributes them by [[SagaReply.answering]]
    * instead — and refuses to decide at all when that is missing.
    */
  object FanOutSaga extends Saga[TestEvent, FanOutState, FanOutRequest, StockReserved]:

    val name = "fan-out"

    val triggers = Set(EventTypeName.of[OrderPlaced])

    /** The ordinal *is* the position in `start`'s request list, so these two constants and that list have to be read
      * together — getting them out of step would attribute stock's answer to payment.
      */
    private val StockOrdinal = 0

    private val PaymentOrdinal = 1

    /** Why this saga gives up on a reply it cannot attribute. Shared with the test that checks it reaches the log: a
      * `Failed` reason is persisted nowhere, so the log is the only place it exists. Safe to pin, unlike the runner's
      * own wording, because this text belongs to the saga — rewording it moves both sides at once.
      */
    val NoAttribution = "reply did not say which request it answered"

    def start(event: EventEnvelope[TestEvent]): Option[SagaStart[FanOutState, FanOutRequest]] =
      event.payload match
        case OrderPlaced(orderId) =>
          Some(
            SagaStart(
              key = orderId,
              data = FanOutState(orderId, stock = None, payment = None),
              request = List(
                SagaRequest(RequestTopic, Some(orderId), ReserveStockFor(orderId)),
                SagaRequest(PaymentTopic, Some(orderId), ChargeFor(orderId, 500)),
              ),
              timeout = Some(1.hour),
            ),
          )
        case _ => None

    def onReply(
      ctx: SagaContext,
      state: FanOutState,
      reply: SagaReply[StockReserved],
    ): SagaDecision[TestEvent, FanOutState, FanOutRequest] =
      reply.answering.map(_.ordinal) match
        case Some(StockOrdinal)   => settle(state.copy(stock = Some(reply.payload.ok)))
        case Some(PaymentOrdinal) => settle(state.copy(payment = Some(reply.payload.ok)))
        case Some(other)          =>
          SagaDecision.failed(s"reply named request ordinal $other, which this saga never sent")
        case None =>
          // Refusing beats guessing: both partners answer in the same shape, so a wrong attribution here would confirm
          // an order whose payment had actually failed.
          SagaDecision.failed(NoAttribution)

    /** Terminal once both have spoken, and until then just remember. */
    private def settle(state: FanOutState): SagaDecision[TestEvent, FanOutState, FanOutRequest] =
      (state.stock, state.payment) match
        case (Some(true), Some(true)) =>
          SagaDecision.completed(events = List(Set(orderTag(state.orderId)) -> OrderConfirmed(state.orderId)))

        case (Some(_), Some(_)) =>
          // Whatever did succeed has to be handed back, and that is a *request*, not an event: undoing it is the
          // partner's business, and this saga cannot reach into the partner's log.
          SagaDecision.compensated(
            events = List(Set(orderTag(state.orderId)) -> OrderCancelled(state.orderId, "a partner declined")),
            messages = undoOf(state),
          )

        case _ => SagaDecision.continue(state, timeout = Some(1.hour))

    /** On a deadline the local event is not enough: a partner may well have done the work and simply not been heard,
      * and once this instance is terminal the runner drops any reply that turns up later. So the undo goes out here
      * too.
      *
      * And it goes to everyone who has not explicitly declined, not just to those known to have succeeded. Silence is
      * not evidence that there is nothing to give back — a partner that never answered is precisely the one most likely
      * to have reserved the stock and lost its reply. Which means an undo for something that never happened has to be a
      * no-op on the partner's side; that is a requirement this saga places on its partners, not an accident.
      */
    def onTimeout(ctx: SagaContext, state: FanOutState): SagaDecision[TestEvent, FanOutState, FanOutRequest] =
      SagaDecision.compensated(
        events = List(Set(orderTag(state.orderId)) -> OrderCancelled(state.orderId, "a partner never answered")),
        messages = undoOf(state),
      )

    /** Undo requests for every partner that did not say no.
      *
      * At decline time that is exactly the ones that succeeded, because a decline is only reached once everybody has
      * answered. On a deadline it also covers the ones that said nothing at all.
      */
    private def undoOf(state: FanOutState): List[SagaRequest[FanOutRequest]] =
      List(
        Option.unless(state.stock.contains(false))(
          SagaRequest(RequestTopic, Some(state.orderId), ReleaseStockFor(state.orderId)),
        ),
        Option.unless(state.payment.contains(false))(
          SagaRequest(PaymentTopic, Some(state.orderId), RefundFor(state.orderId, 500)),
        ),
      ).flatten

    val stateCodec: MessageCodec[FanOutState] = CirceMessageCodec.derived[FanOutState]

    private val stockCodec = CirceMessageCodec.derived[ReserveStockFor]

    private val chargeCodec = CirceMessageCodec.derived[ChargeFor]

    private val releaseCodec = CirceMessageCodec.derived[ReleaseStockFor]

    private val refundCodec = CirceMessageCodec.derived[RefundFor]

    /** Dispatches to each leaf's own codec instead of deriving one for the sealed trait, so what goes on the wire is
      * the shape each partner's own decoder expects rather than circe's `{"ReserveStockFor": {...}}` sum wrapper.
      *
      * Note that there is no honest `decode` for this type — an incoming payload could be either leaf and nothing on it
      * says which. That is exactly why the saga asks for a [[MessageEncoder]] and not a codec: before the split this
      * had to be a stub that threw.
      */
    val requestEncoder: MessageEncoder[FanOutRequest] = new MessageEncoder[FanOutRequest]:
      def encode(request: FanOutRequest): Either[Throwable, String] = request match
        case r: ReserveStockFor => stockCodec.encode(r)
        case c: ChargeFor       => chargeCodec.encode(c)
        case r: ReleaseStockFor => releaseCodec.encode(r)
        case r: RefundFor       => refundCodec.encode(r)

    val replyDecoder: MessageDecoder[StockReserved] = CirceMessageCodec.derived[StockReserved]

  private val eventCodec: EventCodec[TestEvent] = CirceEventCodec.derived[TestEvent]

  // ----- fake reply topic -----

  /** Emits a fixed list and records which messages were acknowledged, so tests can assert that a message the runner
    * chose to drop was still acked rather than left to be redelivered forever.
    */
  final class FakeSubscriber(messages: List[IncomingMessage], acked: Ref[IO, List[IncomingMessage]])
      extends MessageSubscriber[IO]:

    def subscribe(topic: String, fromBeginning: Boolean): Stream[IO, (IncomingMessage, IO[Unit])] =
      Stream.emits(messages).map(message => (message, acked.update(_ :+ message)))

  // ----- captured logging -----

  /** Collects what the runner logs instead of printing it.
    *
    * Several paths here are reached deliberately — an undecodable reply, an unroutable one, unreadable stored state —
    * and each one logs, because for those the log ''is'' the outcome: nothing else tells an operator why an instance
    * was abandoned. Printing them made the suite look like it was failing when it was passing, and asserting them turns
    * three lines of console noise into the only check that message survives a refactor.
    */
  final class CapturingLogger(entries: Ref[IO, List[String]]) extends Logger[IO]:

    private def record(level: String, message: String): IO[Unit] = entries.update(_ :+ s"$level $message")

    def error(message: => String): IO[Unit] = record("ERROR", message)

    def warn(message: => String): IO[Unit] = record("WARN", message)

    def info(message: => String): IO[Unit] = record("INFO", message)

    def debug(message: => String): IO[Unit] = record("DEBUG", message)

    def trace(message: => String): IO[Unit] = record("TRACE", message)

    def error(t: Throwable)(message: => String): IO[Unit] = record("ERROR", s"$message [${t.getMessage}]")

    def warn(t: Throwable)(message: => String): IO[Unit] = record("WARN", s"$message [${t.getMessage}]")

    def info(t: Throwable)(message: => String): IO[Unit] = record("INFO", s"$message [${t.getMessage}]")

    def debug(t: Throwable)(message: => String): IO[Unit] = record("DEBUG", s"$message [${t.getMessage}]")

    def trace(t: Throwable)(message: => String): IO[Unit] = record("TRACE", s"$message [${t.getMessage}]")

  /** Run `use` against a capturing logger and hand back both its result and what was logged. */
  private def capturingLogs[A](use: Logger[IO] => IO[A]): IO[(A, List[String])] =
    for
      entries <- Ref.of[IO, List[String]](Nil)
      result  <- use(CapturingLogger(entries))
      logged  <- entries.get
    yield (result, logged)

  extension (logged: List[String])

    /** Whether `subject` was reported at `level` — an instance id, a header name, something with an identity.
      *
      * Deliberately anchored on an identifier and a severity rather than on the wording. What an operator needs is that
      * the outcome was reported, loudly enough to be seen, naming the thing it happened to; the phrasing is free to
      * improve. Pinning the prose would make every reworded log line look like a regression, and a test that fails for
      * a reason that is not a defect is worse than no test.
      */
    def reported(level: String, subject: String): Boolean =
      logged.exists(entry => entry.startsWith(level) && entry.contains(subject))

  private def reply(
    id: java.util.UUID,
    payload: String,
    sagaName: String = TestSaga.name,
    withId: Boolean = true,
    inReplyTo: Option[String] = None,
  ): IncomingMessage =
    val headers = Map(SagaHeaders.Name -> sagaName) ++
      (if withId then Map(SagaHeaders.Id -> id.toString) else Map.empty) ++
      inReplyTo.map(SagaHeaders.InReplyTo -> _)
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

  /** Takes its logger explicitly so a test that drives a logging path on purpose can hand in [[CapturingLogger]]; every
    * other call resolves the suite's slf4j one and keeps printing to the console.
    */
  private def runner(fixture: Fixture, replies: List[IncomingMessage], acked: Ref[IO, List[IncomingMessage]])(using
    Logger[IO],
  ) =
    SagaRunner[IO, TestEvent](
      store = fixture.store,
      checkpoint = fixture.checkpoint,
      repository = fixture.repository,
      replies = FakeSubscriber(replies, acked),
      replyTopic = ReplyTopic,
      // Short enough that `.take(1)` on the timer loop is quick rather than a five-second wait.
      timerPollInterval = 100.millis,
    )

  private def noReplies(fixture: Fixture)(using Logger[IO]) =
    Ref.of[IO, List[IncomingMessage]](Nil).map(acked => runner(fixture, Nil, acked))

  private def append(fixture: Fixture, events: TestEvent*): IO[Unit] =
    fixture.store
      .appendUnchecked(events.toList.map { event =>
        PendingEvent(event, Set.empty, EventTypeName.fromInstance(event), isExternal = false)
      })
      .void

  private def instanceOf(fixture: Fixture, orderId: String): IO[Option[SagaRecord]] =
    fixture.repository.find(SagaId.instance(TestSaga.name, orderId))

  private def instanceOf(fixture: Fixture, sagaName: String, key: String): IO[Option[SagaRecord]] =
    fixture.repository.find(SagaId.instance(sagaName, key))

  /** A reply attributed to one of [[FanOutSaga]]'s requests, exactly as `SagaHeaders.reply` would have built it. */
  private def fanOutReply(id: java.util.UUID, ordinal: Int, payload: String, round: Int = 0): IncomingMessage =
    reply(
      id,
      payload,
      sagaName = FanOutSaga.name,
      inReplyTo = Some(SagaRequestRef.idempotencyKey(id, round, ordinal)),
    )

  private def runFanOutTrigger(fixture: Fixture, orderId: String): IO[Unit] =
    append(fixture, OrderPlaced(orderId)) *>
      noReplies(fixture).flatMap(_.triggerLoop(FanOutSaga).take(1).compile.drain)

  private def deliverToFanOut(fixture: Fixture, replies: IncomingMessage*)(using Logger[IO]): IO[Unit] =
    Ref.of[IO, List[IncomingMessage]](Nil).flatMap { acked =>
      runner(fixture, replies.toList, acked).replyLoop(FanOutSaga).compile.drain
    }

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

  private def storedPayloads(fixture: Fixture, eventType: String): IO[List[String]] =
    fixture.pool.use(
      _.execute(
        sql"SELECT payload::text FROM events WHERE event_type = $text ORDER BY sequence_number".query(text),
      )(eventType),
    )

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

  test("one round can ask two partners, each in that partner's own format") { fixture =>
    for
      _    <- truncate(fixture)
      _    <- append(fixture, OrderPlaced("o-fan"))
      _    <- noReplies(fixture).flatMap(_.triggerLoop(FanOutSaga).take(1).compile.drain)
      rows <- outboxRows(fixture)
      id    = SagaId.instance(FanOutSaga.name, "o-fan")
    yield expect.all(
      rows.size == 2,
      rows.map(_._1) == List(RequestTopic, PaymentTopic),
      // Bare leaves. A partner decoding its own DTO would choke on the `{"ReserveStockFor": {...}}` wrapper that
      // deriving a codec for the sealed trait would have produced.
      rows.map(_._3) == List("""{"orderId":"o-fan"}""", """{"orderId":"o-fan","cents":500}"""),
      // Distinct keys within the one round, so a partner deduplicating on them cannot mistake one request for the other,
      // and so the ordinal identifies which of the two a reply is answering.
      rows.flatMap(_._4.get(SagaHeaders.IdempotencyKey)) == List(s"$id:0:0", s"$id:0:1"),
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

  test("a reply tells the saga which of its requests was answered") { fixture =>
    val id = SagaId.instance(ObservingSaga.name, "o-ref")
    for
      _     <- truncate(fixture)
      _     <- append(fixture, OrderPlaced("o-ref"))
      _     <- noReplies(fixture).flatMap(_.triggerLoop(ObservingSaga).take(1).compile.drain)
      acked <- Ref.of[IO, List[IncomingMessage]](Nil)
      // What SagaHeaders.reply would have put there: the idempotency key of round 0, ordinal 0.
      answer = reply(
                 id,
                 accepted,
                 sagaName = ObservingSaga.name,
                 inReplyTo = Some(SagaRequestRef.idempotencyKey(id, 0, 0)),
               )
      _        <- runner(fixture, List(answer), acked).replyLoop(ObservingSaga).compile.drain
      observed <- storedPayloads(fixture, "Unrelated")
    yield expect(observed == List("""{"what": "0:0"}"""))
  }

  test("a reply that names no request leaves the saga with nothing to go on") { fixture =>
    val id = SagaId.instance(ObservingSaga.name, "o-ref")
    for
      _        <- truncate(fixture)
      _        <- append(fixture, OrderPlaced("o-ref"))
      _        <- noReplies(fixture).flatMap(_.triggerLoop(ObservingSaga).take(1).compile.drain)
      acked    <- Ref.of[IO, List[IncomingMessage]](Nil)
      answer    = reply(id, accepted, sagaName = ObservingSaga.name)
      _        <- runner(fixture, List(answer), acked).replyLoop(ObservingSaga).compile.drain
      observed <- storedPayloads(fixture, "Unrelated")
    yield expect(observed == List("""{"what": "none"}"""))
  }

  test("a request key belonging to another instance is not read as this one's") { fixture =>
    val id = SagaId.instance(ObservingSaga.name, "o-ref")
    val other = SagaId.instance(ObservingSaga.name, "someone-else")
    for
      _     <- truncate(fixture)
      _     <- append(fixture, OrderPlaced("o-ref"))
      _     <- noReplies(fixture).flatMap(_.triggerLoop(ObservingSaga).take(1).compile.drain)
      acked <- Ref.of[IO, List[IncomingMessage]](Nil)
      answer = reply(
                 id,
                 accepted,
                 sagaName = ObservingSaga.name,
                 inReplyTo = Some(SagaRequestRef.idempotencyKey(other, 3, 7)),
               )
      _        <- runner(fixture, List(answer), acked).replyLoop(ObservingSaga).compile.drain
      observed <- storedPayloads(fixture, "Unrelated")
    yield expect(observed == List("""{"what": "none"}"""))
  }

  // ----- fan-in: waiting for several partners -----

  test("a fan-in completes only once every partner has answered") { fixture =>
    val id = SagaId.instance(FanOutSaga.name, "fan-both")
    for
      _       <- truncate(fixture)
      _       <- runFanOutTrigger(fixture, "fan-both")
      _       <- deliverToFanOut(fixture, fanOutReply(id, ordinal = 0, accepted))
      midway  <- instanceOf(fixture, FanOutSaga.name, "fan-both")
      events  <- storedEvents(fixture)
      _       <- deliverToFanOut(fixture, fanOutReply(id, ordinal = 1, accepted))
      settled <- instanceOf(fixture, FanOutSaga.name, "fan-both")
      after   <- storedEvents(fixture)
    yield expect.all(
      // One answer in: still pending, one step on, and the answer is remembered in the stored state.
      midway.exists(_.status == SagaStatus.Pending),
      midway.exists(_.step == 1),
      midway.exists(_.data.contains(""""stock":true""")),
      midway.exists(_.data.contains(""""payment":null""")),
      events == List("OrderPlaced"),
      // Both in: terminal.
      settled.exists(_.status == SagaStatus.Completed),
      after == List("OrderPlaced", "OrderConfirmed"),
    )
  }

  test("a fan-in attributes structurally identical replies by the request each answers") { fixture =>
    val id = SagaId.instance(FanOutSaga.name, "fan-attr")
    for
      _ <- truncate(fixture)
      _ <- runFanOutTrigger(fixture, "fan-attr")
      // The same payload twice — only the answered ordinal differs, which is the whole point.
      _      <- deliverToFanOut(fixture, fanOutReply(id, ordinal = 1, accepted))
      record <- instanceOf(fixture, FanOutSaga.name, "fan-attr")
    yield expect.all(
      record.exists(_.data.contains(""""payment":true""")),
      record.exists(_.data.contains(""""stock":null""")),
    )
  }

  test("a redelivered reply does not count as the partner that is still missing") { fixture =>
    val id = SagaId.instance(FanOutSaga.name, "fan-dup")
    for
      _      <- truncate(fixture)
      _      <- runFanOutTrigger(fixture, "fan-dup")
      _      <- deliverToFanOut(fixture, fanOutReply(id, 0, accepted), fanOutReply(id, 0, accepted))
      record <- instanceOf(fixture, FanOutSaga.name, "fan-dup")
      events <- storedEvents(fixture)
    yield expect.all(
      // Still waiting on payment, and nothing terminal was written — the saga's own fold is what makes this idempotent,
      // not the runner: it recorded the same answer twice and reached the same conclusion.
      record.exists(_.status == SagaStatus.Pending),
      record.exists(_.data.contains(""""payment":null""")),
      events == List("OrderPlaced"),
      // But each redelivery is still a `Continue`, so the step climbs and the deadline it carries is set afresh. A
      // partner redelivering faster than the timeout would keep postponing the moment this instance gives up.
      record.exists(_.step == 2),
    )
  }

  test("when one partner declines, the compensation goes back out as a request") { fixture =>
    val id = SagaId.instance(FanOutSaga.name, "fan-undo")
    for
      _      <- truncate(fixture)
      _      <- runFanOutTrigger(fixture, "fan-undo")
      _      <- deliverToFanOut(fixture, fanOutReply(id, 0, accepted))
      _      <- deliverToFanOut(fixture, fanOutReply(id, 1, rejected))
      record <- instanceOf(fixture, FanOutSaga.name, "fan-undo")
      events <- storedEvents(fixture)
      rows   <- outboxRows(fixture)
    yield expect.all(
      record.exists(_.status == SagaStatus.Compensated),
      events == List("OrderPlaced", "OrderCancelled"),
      // The two original requests, then one undo for the partner that did succeed — and nothing for the one that
      // declined, since it has nothing to give back.
      rows.size == 3,
      rows.last._1 == RequestTopic,
      // Identical on the wire to the request that reserved it. Only the key says otherwise: round 2, not round 0, which
      // is what stops a partner deduplicating on that key from discarding the undo as a redelivery of the original.
      rows.last._3 == """{"orderId":"fan-undo"}""",
      rows.last._4.get(SagaHeaders.IdempotencyKey) == Some(s"$id:2:0"),
    )
  }

  test("a deadline hands back the work of a partner that did answer, and of the one that never did") { fixture =>
    val id = SagaId.instance(FanOutSaga.name, "fan-late")
    // Stock said yes, payment never spoke. Started straight through the repository so the deadline is already behind us
    // and the outbox holds nothing but the compensation.
    val halfAnswered = """{"orderId":"fan-late","stock":true,"payment":null}"""
    for
      _      <- truncate(fixture)
      _      <- fixture.repository.start(id, FanOutSaga.name, "fan-late", halfAnswered, Some(1.milli), Nil)
      _      <- IO.sleep(100.millis)
      _      <- noReplies(fixture).flatMap(_.timerLoop(FanOutSaga).take(1).compile.drain)
      record <- instanceOf(fixture, FanOutSaga.name, "fan-late")
      events <- storedEvents(fixture)
      rows   <- outboxRows(fixture)
    yield expect.all(
      record.exists(_.status == SagaStatus.Compensated),
      events == List("OrderCancelled"),
      // Both partners are told to undo: the one that reserved, and the silent one that may have reserved without us
      // hearing. Cancelling locally while leaving either holding the goods is the leak this exists to prevent.
      rows.size == 2,
      rows.map(_._1) == List(RequestTopic, PaymentTopic),
      rows.map(_._3) == List("""{"orderId":"fan-late"}""", """{"orderId":"fan-late","cents":500}"""),
      rows.flatMap(_._4.get(SagaHeaders.IdempotencyKey)) == List(s"$id:1:0", s"$id:1:1"),
    )
  }

  test("a deadline leaves a partner that explicitly declined alone") { fixture =>
    val id = SagaId.instance(FanOutSaga.name, "fan-nope")
    val stockDeclined = """{"orderId":"fan-nope","stock":false,"payment":null}"""
    for
      _    <- truncate(fixture)
      _    <- fixture.repository.start(id, FanOutSaga.name, "fan-nope", stockDeclined, Some(1.milli), Nil)
      _    <- IO.sleep(100.millis)
      _    <- noReplies(fixture).flatMap(_.timerLoop(FanOutSaga).take(1).compile.drain)
      rows <- outboxRows(fixture)
    yield expect.all(
      // A partner that said no has nothing to give back, and telling it to release stock it never took would be noise.
      rows.size == 1,
      rows.head._1 == PaymentTopic,
    )
  }

  test("a reply nobody can attribute fails the instance instead of guessing") { fixture =>
    val id = SagaId.instance(FanOutSaga.name, "fan-blind")
    for
      _ <- truncate(fixture)
      _ <- runFanOutTrigger(fixture, "fan-blind")
      // A partner that built its reply by hand and forgot to say what it was answering.
      logged <- capturingLogs { log =>
                  given Logger[IO] = log
                  deliverToFanOut(fixture, reply(id, accepted, sagaName = FanOutSaga.name))
                }.map(_._2)
      record <- instanceOf(fixture, FanOutSaga.name, "fan-blind")
      events <- storedEvents(fixture)
    yield expect.all(
      record.exists(_.status == SagaStatus.Failed),
      events == List("OrderPlaced"),
      // The reason the *saga* gave has to survive as far as the log, because a Failed instance persists no reason at all.
      logged.reported("WARN", FanOutSaga.NoAttribution),
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
      logged <- capturingLogs { log =>
                  given Logger[IO] = log
                  runner(fixture, List(reply(id, accepted, withId = false)), acked).replyLoop(TestSaga).compile.drain
                }.map(_._2)
      record <- instanceOf(fixture, "o1")
      seen   <- acked.get
    yield expect.all(
      record.exists(_.status == SagaStatus.Pending),
      seen.size == 1,
      // Names the header it could not use — a library constant, so this survives any rewording of the sentence.
      logged.reported("WARN", SagaHeaders.Id),
    )
  }

  test("an undecodable reply is dropped and acknowledged, leaving the instance pending for its deadline") { fixture =>
    val id = SagaId.instance(TestSaga.name, "o1")
    for
      _      <- truncate(fixture)
      _      <- append(fixture, OrderPlaced("o1"))
      _      <- runTrigger(fixture)
      acked  <- Ref.of[IO, List[IncomingMessage]](Nil)
      logged <- capturingLogs { log =>
                  given Logger[IO] = log
                  runner(fixture, List(reply(id, "not json at all")), acked).replyLoop(TestSaga).compile.drain
                }.map(_._2)
      record <- instanceOf(fixture, "o1")
      count  <- eventCount(fixture)
      seen   <- acked.get
    yield expect.all(
      record.exists(_.status == SagaStatus.Pending),
      count == 1L,
      seen.size == 1,
      // The instance is left to its deadline in silence otherwise, so the one thing that must be true is that something
      // was reported about *this instance*, at a level an operator sees.
      logged.reported("ERROR", id.toString),
    )
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
    val ghost = SagaId.instance(TestSaga.name, "ghost")
    for
      _      <- truncate(fixture)
      acked  <- Ref.of[IO, List[IncomingMessage]](Nil)
      logged <- capturingLogs { log =>
                  given Logger[IO] = log
                  runner(fixture, List(reply(ghost, accepted)), acked).replyLoop(TestSaga).compile.drain
                }.map(_._2)
      count <- eventCount(fixture)
      seen  <- acked.get
    yield expect.all(count == 0L, seen.size == 1, logged.reported("WARN", ghost.toString))
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
      logged <- capturingLogs { log =>
                  given Logger[IO] = log
                  noReplies(fixture).flatMap(_.timerLoop(TestSaga).take(1).compile.drain)
                }.map(_._2)
      record <- instanceOf(fixture, "o1")
      count  <- eventCount(fixture)
    yield expect.all(
      record.exists(_.status == SagaStatus.Failed),
      record.exists(_.deadline.isEmpty),
      count == 0L,
      // Marked failed precisely so it stops being re-claimed, which means nothing will ever look at it again unless this
      // was reported.
      logged.reported("ERROR", id.toString),
    )
  }

/** The outbox `headers` column is JSONB; this suite reads it back to assert what the runner stamped. */
private object OutboxHeaderCodec:

  import io.circe.Json
  import skunk.circe.codec.all.jsonb

  val headers: Codec[Map[String, String]] =
    jsonb.imap { json =>
      json.asObject.map(_.toMap.flatMap { case (k, v) => v.asString.map(k -> _) }).getOrElse(Map.empty)
    }(map => Json.obj(map.toSeq.map((k, v) => k -> Json.fromString(v))*))
