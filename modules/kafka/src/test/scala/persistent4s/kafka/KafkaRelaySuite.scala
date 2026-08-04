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

package persistent4s.kafka

import java.time.Instant
import java.util.UUID

import scala.concurrent.duration.*

import cats.effect.{IO, Ref}
import cats.effect.testkit.TestControl
import cats.syntax.all.*
import fs2.Stream
import weaver.SimpleIOSuite

import persistent4s.{Event, EventEnvelope, EventMetadata, EventTypeName, Outbox}
import persistent4s.EventPublisher

/** Unit tests for [[KafkaRelay]] using in-memory fakes for the outbox and the publisher.
  *
  * These tests pin down the relay's contract independently of any Kafka broker: every emitted envelope must be
  * published, every successful publish must be acknowledged via [[Outbox.markPublished]], and a publish failure must
  * leave the failing envelope unacked so the next run reprocesses it.
  */
object KafkaRelaySuite extends SimpleIOSuite:

  final case class TestEvent(value: String) extends Event

  private def envelope(globalPosition: Long): EventEnvelope[TestEvent] =
    EventEnvelope(
      EventMetadata(
        globalPosition = globalPosition, id = UUID.randomUUID(), tags = Set.empty,
        eventType = EventTypeName.fromString("TestEvent"), isExternal = false,
        timestamp = Instant.parse("2026-01-01T00:00:00Z"), headers = Map.empty,
      ),
      TestEvent(s"e$globalPosition"),
    )

  /** Records every successful publish call. Fails on positions for which `failOn` returns true, so we can drive the
    * relay through its error path.
    */
  final private class FakePublisher(
    recorded: Ref[IO, Vector[(String, EventEnvelope[TestEvent])]],
    failOn: Long => Boolean = _ => false,
  ) extends EventPublisher[IO, TestEvent]:

    override def publish(topic: String, envelope: EventEnvelope[TestEvent]): IO[Unit] =
      if failOn(envelope.metadata.globalPosition) then
        IO.raiseError(new RuntimeException(s"simulated publish failure at ${envelope.metadata.globalPosition}"))
      else recorded.update(_ :+ (topic, envelope))

    override def publish(topic: String, envelopes: List[EventEnvelope[TestEvent]]): IO[Unit] =
      envelopes.traverse_(publish(topic, _))

  /** Emits the pre-loaded `pending` list, then blocks forever. The relay treats this as a normal infinite outbox; the
    * test cancels the relay fiber once it has observed the expected number of publishes.
    */
  final private class FakeOutbox(
    pending: List[EventEnvelope[TestEvent]],
    publishedRef: Ref[IO, Vector[Long]],
  ) extends Outbox[IO, TestEvent]:

    override def stream(batchSize: Int): Stream[IO, EventEnvelope[TestEvent]] =
      Stream.emits(pending) ++ Stream.never[IO]

    override def markPublished(globalPosition: Long): IO[Unit] =
      publishedRef.update(_ :+ globalPosition)

    override def markPublished(globalPositions: List[Long]): IO[Unit] =
      publishedRef.update(_ ++ globalPositions)

    override def notifications: Stream[IO, Unit] = Stream.empty

  /** Fails the first `failTimes` publish calls (regardless of position), then succeeds. Simulates a Kafka broker that
    * is temporarily unavailable.
    */
  final private class TransientPublisher(
    recorded: Ref[IO, Vector[(String, EventEnvelope[TestEvent])]],
    failTimes: Int,
    callCount: Ref[IO, Int],
  ) extends EventPublisher[IO, TestEvent]:

    override def publish(topic: String, envelope: EventEnvelope[TestEvent]): IO[Unit] =
      callCount.getAndUpdate(_ + 1).flatMap { n =>
        if n < failTimes then IO.raiseError(new RuntimeException(s"transient failure $n"))
        else recorded.update(_ :+ (topic, envelope))
      }

    override def publish(topic: String, envelopes: List[EventEnvelope[TestEvent]]): IO[Unit] =
      envelopes.traverse_(publish(topic, _))

  private val noopPublisher: EventPublisher[IO, TestEvent] =
    new EventPublisher[IO, TestEvent]:
      override def publish(topic: String, envelope: EventEnvelope[TestEvent]): IO[Unit] = IO.unit
      override def publish(topic: String, envelopes: List[EventEnvelope[TestEvent]]): IO[Unit] = IO.unit

  final private class TimedFailingOutbox(
    runDuration: FiniteDuration,
    startTimes: Ref[IO, Vector[FiniteDuration]],
  ) extends Outbox[IO, TestEvent]:

    override def stream(batchSize: Int): Stream[IO, EventEnvelope[TestEvent]] =
      Stream.exec(IO.monotonic.flatMap(t => startTimes.update(_ :+ t))) ++
        Stream.exec(IO.sleep(runDuration)) ++
        Stream.raiseError[IO](new RuntimeException("boom"))

    override def markPublished(globalPosition: Long): IO[Unit] = IO.unit

    override def markPublished(globalPositions: List[Long]): IO[Unit] = IO.unit

    override def notifications: Stream[IO, Unit] = Stream.empty

  /** Poll `ref` until its size reaches `n` or `timeout` elapses. */
  private def waitFor[A](ref: Ref[IO, Vector[A]], n: Int, timeout: FiniteDuration = 5.seconds): IO[Unit] =
    def poll: IO[Unit] = ref.get.flatMap { v =>
      if v.size >= n then IO.unit
      else IO.sleep(20.millis) *> poll
    }
    poll.timeoutTo(timeout, IO.unit)

  test("publishes every envelope from the outbox to the configured topic") {
    val pending = List(envelope(1L), envelope(2L), envelope(3L))
    for
      recorded  <- Ref.of[IO, Vector[(String, EventEnvelope[TestEvent])]](Vector.empty)
      published <- Ref.of[IO, Vector[Long]](Vector.empty)
      publisher  = new FakePublisher(recorded)
      outbox     = new FakeOutbox(pending, published)
      relay      = KafkaRelay[IO, TestEvent](outbox, publisher, topic = "events", batchSize = 10)
      _         <- relay.runOnce.background.use { _ =>
             waitFor(recorded, 3)
           }
      pubs <- recorded.get
    yield expect.all(
      pubs.size == 3,
      pubs.map(_._1).distinct == Vector("events"),
      pubs.map(_._2.metadata.globalPosition) == Vector(1L, 2L, 3L),
    )
  }

  test("acks every successfully-published envelope via markPublished, in order") {
    val pending = List(envelope(10L), envelope(20L), envelope(30L))
    for
      recorded  <- Ref.of[IO, Vector[(String, EventEnvelope[TestEvent])]](Vector.empty)
      published <- Ref.of[IO, Vector[Long]](Vector.empty)
      publisher  = new FakePublisher(recorded)
      outbox     = new FakeOutbox(pending, published)
      relay      = KafkaRelay[IO, TestEvent](outbox, publisher, topic = "events", batchSize = 10)
      _         <- relay.runOnce.background.use { _ =>
             waitFor(published, 3)
           }
      acks <- published.get
    yield expect(acks == Vector(10L, 20L, 30L))
  }

  test("a publish failure leaves the whole batch unacked") {
    val pending = List(envelope(1L), envelope(2L), envelope(3L))
    for
      recorded  <- Ref.of[IO, Vector[(String, EventEnvelope[TestEvent])]](Vector.empty)
      published <- Ref.of[IO, Vector[Long]](Vector.empty)
      publisher  = new FakePublisher(recorded, failOn = _ == 2L)
      outbox     = new FakeOutbox(pending, published)
      relay      = KafkaRelay[IO, TestEvent](outbox, publisher, topic = "events", batchSize = 10)
      result    <- relay.runOnce.attempt
      pubs      <- recorded.get
      acks      <- published.get
    yield expect.all(
      result.isLeft,
      pubs.map(_._2.metadata.globalPosition) == Vector(1L),
      acks.isEmpty,
    )
  }

  test("publishes are sequential — order matches the outbox emission order even under contention") {
    // 50 envelopes; if the relay used parEvalMap, the recorded order could differ.
    val pending = (1L to 50L).map(envelope).toList
    for
      recorded  <- Ref.of[IO, Vector[(String, EventEnvelope[TestEvent])]](Vector.empty)
      published <- Ref.of[IO, Vector[Long]](Vector.empty)
      publisher  = new FakePublisher(recorded)
      outbox     = new FakeOutbox(pending, published)
      relay      = KafkaRelay[IO, TestEvent](outbox, publisher, topic = "events", batchSize = 10)
      _         <- relay.runOnce.background.use { _ =>
             waitFor(recorded, 50)
           }
      positions <- recorded.get.map(_.map(_._2.metadata.globalPosition))
    yield expect(positions == (1L to 50L).toVector)
  }

  test("run retries after a transient failure and eventually publishes and acks all envelopes") {
    val pending = List(envelope(1L), envelope(2L), envelope(3L))
    for
      recorded  <- Ref.of[IO, Vector[(String, EventEnvelope[TestEvent])]](Vector.empty)
      published <- Ref.of[IO, Vector[Long]](Vector.empty)
      callCount <- Ref.of[IO, Int](0)
      publisher  = new TransientPublisher(recorded, failTimes = 1, callCount)
      outbox     = new FakeOutbox(pending, published)
      relay      = KafkaRelay[IO, TestEvent](outbox, publisher, topic = "events", batchSize = 10)
      _         <- relay.run(initialDelay = 1.millis).background.use { _ =>
             waitFor(published, 3)
           }
      acks <- published.get
    yield expect(acks.toSet == Set(1L, 2L, 3L))
  }

  test("run retries multiple times before succeeding") {
    val pending = List(envelope(1L), envelope(2L))
    for
      recorded  <- Ref.of[IO, Vector[(String, EventEnvelope[TestEvent])]](Vector.empty)
      published <- Ref.of[IO, Vector[Long]](Vector.empty)
      callCount <- Ref.of[IO, Int](0)
      publisher  = new TransientPublisher(recorded, failTimes = 3, callCount)
      outbox     = new FakeOutbox(pending, published)
      relay      = KafkaRelay[IO, TestEvent](outbox, publisher, topic = "events", batchSize = 10)
      _         <- relay.run(initialDelay = 1.millis).background.use { _ =>
             waitFor(published, 2)
           }
      acks <- published.get
    yield expect(acks.toSet == Set(1L, 2L))
  }

  test("run escalates the backoff (doubling, capped at maxDelay) when runs keep failing immediately") {
    val initialDelay = 1.second
    val maxDelay = 8.seconds
    for
      startTimes <- IO.ref(Vector.empty[FiniteDuration])
      outbox      = new TimedFailingOutbox(0.seconds, startTimes)
      relay       = KafkaRelay[IO, TestEvent](outbox, noopPublisher, topic = "events", batchSize = 10)
      control    <- TestControl.execute(relay.run(initialDelay, maxDelay))
      _          <- control.tick
      _          <- control.nextInterval.flatMap(control.advanceAndTick).replicateA_(8)
      times      <- startTimes.get
    yield
      val gaps = times.zip(times.tail).map((a, b) => (b - a).toSeconds)
      expect(gaps.take(5) == Vector(1L, 2L, 4L, 8L, 8L))
  }

  test("run resets the backoff to initialDelay after a run that outlasts maxDelay") {
    val initialDelay = 1.second
    val maxDelay = 8.seconds
    val runDuration = 10.seconds
    for
      startTimes <- IO.ref(Vector.empty[FiniteDuration])
      outbox      = new TimedFailingOutbox(runDuration, startTimes)
      relay       = KafkaRelay[IO, TestEvent](outbox, noopPublisher, topic = "events", batchSize = 10)
      control    <- TestControl.execute(relay.run(initialDelay, maxDelay))
      _          <- control.tick
      _          <- control.nextInterval.flatMap(control.advanceAndTick).replicateA_(10)
      times      <- startTimes.get
    yield
      val gaps = times.zip(times.tail).map((a, b) => (b - a).toSeconds)
      expect(gaps.nonEmpty && gaps.forall(_ == (runDuration + initialDelay).toSeconds))
  }
