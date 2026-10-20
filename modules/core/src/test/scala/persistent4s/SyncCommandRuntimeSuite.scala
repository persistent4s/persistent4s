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

package persistent4s

import java.time.Instant
import java.util.UUID
import java.util.concurrent.TimeoutException

import scala.concurrent.duration.*

import cats.effect.{Deferred, IO, Ref}
import cats.effect.std.Queue
import cats.syntax.all.*

import fs2.Stream
import fs2.concurrent.Topic

import weaver.SimpleIOSuite

object SyncCommandRuntimeSuite extends SimpleIOSuite:

  sealed trait TestEvent extends Event

  final case class Created(id: String, index: Int) extends TestEvent

  final case class Audited(id: String) extends TestEvent

  final case class Record(id: String, count: Int = 1, audit: Boolean = false, blocked: Boolean = false)

  enum Rejection:

    case Blocked

  private given EventSchema[Created] = EventSchema[Created]("sync.created")

  private given EventSchema[Audited] = EventSchema[Audited]("sync.audited")

  private val entity = Scope[String]("sync.entity")

  private val createdTypes = Set(summon[EventSchema[Created]].eventType)

  private val commandHandler = new EventSourcedCommandHandler[Record, Unit, TestEvent, Rejection]:
    protected val behavior = handler(()):
      scope(entity)(_.id)
      on[Created].allEvents.ignore
      on[Audited].allEvents.ignore
      reject:
        case (_, command) if command.blocked => Rejection.Blocked
      emitMany: command =>
        List.tabulate(command.count)(Created(command.id, _)) ++
          Option.when(command.audit)(Audited(command.id)).toList

  private type Notification = (UUID, Either[Throwable, Map[String, Option[Int]]])

  private type Updates = Topic[IO, Notification]

  private def publish(topic: Updates, event: EventEnvelope[TestEvent]): IO[Unit] =
    topic.publish1(event.metadata.id -> Right(Map.empty[String, Option[Int]])).void

  private def commandRuntime(
    onAppend: List[EventEnvelope[TestEvent]] => IO[Unit],
  ): IO[CommandRuntime[IO, TestEvent]] =
    Ref.of[IO, Long](0L).map { position =>
      val store = new EventStore[IO, TestEvent]:
        def append(
          eventFilter: EventFilter,
          expectedIndex: Long,
          events: List[PendingEvent[TestEvent]]*,
        ): IO[List[EventEnvelope[TestEvent]]] =
          val pending = events.flatten.toList
          for
            previous <- position.getAndUpdate(_ + pending.size.toLong)
            saved     = pending.zipWithIndex.map { (event, index) =>
                      EventEnvelope(
                        EventMetadata(
                          previous + index.toLong + 1L,
                          event.id.getOrElse(UUID.randomUUID()),
                          event.tags,
                          event.eventType,
                          event.isExternal,
                          Instant.EPOCH,
                          event.headers,
                        ),
                        event.payload,
                      )
                    }
            _ <- onAppend(saved)
          yield saved

        def appendUnchecked(events: List[PendingEvent[TestEvent]]*): IO[List[EventEnvelope[TestEvent]]] =
          append(EventFilter(), 0L, events*)

        def currentRevision(eventFilter: EventFilter): IO[Long] = IO.pure(0L)

        def readFrom(
          fromPosition: Long,
          eventFilter: EventFilter,
          maxEvents: Option[Int],
        ): Stream[IO, EventEnvelope[TestEvent]] = Stream.empty

      CommandRuntime.eventStoreOnly(store)
    }

  private def run(command: Record)(using
    runtime: SyncCommandRuntime[IO, TestEvent],
  ): IO[Either[Rejection, List[EventEnvelope[TestEvent]]]] =
    commandHandler.runSync[IO](command)

  test("subscribes before append and returns every saved envelope after synchronous projection notifications") {
    for
      topic       <- Topic[IO, Notification]
      saved       <- Deferred[IO, List[EventEnvelope[TestEvent]]]
      commands    <- commandRuntime(events => saved.complete(events) *> events.traverse_(publish(topic, _)))
      runtime      = SyncCommandRuntime.fromTopic(commands, topic, createdTypes, 1.second)
      result      <- run(Record("a", count = 2))(using runtime)
      expected    <- saved.get
      subscribers <- topic.subscribers.head.compile.lastOrError
    yield expect(result == Right(expected)) and expect(expected.size == 2) and expect(subscribers == 0)
  }

  test("duplicate and unrelated notifications cannot finish a command before its last event") {
    val failure = new RuntimeException("last event could not be projected")
    for
      topic    <- Topic[IO, Notification]
      commands <- commandRuntime { events =>
                    publish(topic, events.head) *>
                      topic.publish1(UUID.randomUUID() -> Left(new RuntimeException("unrelated failure"))) *>
                      publish(topic, events.head) *>
                      topic.publish1(events.last.metadata.id -> Left(failure)).void
                  }
      runtime = SyncCommandRuntime.fromTopic(commands, topic, createdTypes, 1.second)
      result <- run(Record("a", count = 2))(using runtime).attempt
    yield expect(result == Left(failure))
  }

  test("events outside the projection filter remain in the result without requiring notifications") {
    for
      topic    <- Topic[IO, Notification]
      commands <-
        commandRuntime(events => events.filter(e => createdTypes(e.metadata.eventType)).traverse_(publish(topic, _)))
      runtime = SyncCommandRuntime.fromTopic(commands, topic, createdTypes, 1.second)
      result <- run(Record("a", audit = true))(using runtime)
    yield expect(result.map(_.map(_.payload)) == Right(List(Created("a", 0), Audited("a"))))
  }

  test("a command producing only excluded events does not wait for the projection") {
    for
      topic    <- Topic[IO, Notification]
      commands <- commandRuntime(_ => IO.unit)
      runtime   = SyncCommandRuntime.fromTopic(commands, topic, createdTypes, Duration.Zero)
      result   <- run(Record("a", count = 0, audit = true))(using runtime)
    yield expect(result.map(_.map(_.payload)) == Right(List(Audited("a"))))
  }

  test("an empty projection filter waits for every event type") {
    val failure = new RuntimeException("audit event could not be projected")
    for
      topic    <- Topic[IO, Notification]
      commands <- commandRuntime { events =>
                    publish(topic, events.head) *>
                      topic.publish1(events.last.metadata.id -> Left(failure)).void
                  }
      runtime = SyncCommandRuntime.fromTopic(commands, topic, Set.empty, 1.second)
      result <- run(Record("a", audit = true))(using runtime).attempt
    yield expect(result == Left(failure))
  }

  test("typed rejections and unchanged commands return without saving events or awaiting projection notifications") {
    for
      topic       <- Topic[IO, Notification]
      saved       <- Ref.of[IO, List[EventEnvelope[TestEvent]]](Nil)
      commands    <- commandRuntime(events => saved.update(_ ++ events))
      runtime      = SyncCommandRuntime.fromTopic(commands, topic, createdTypes, Duration.Zero)
      rejected    <- run(Record("a", blocked = true))(using runtime)
      unchanged   <- run(Record("a", count = 0))(using runtime)
      events      <- saved.get
      subscribers <- topic.subscribers.head.compile.lastOrError
    yield expect(rejected == Left(Rejection.Blocked)) and expect(unchanged == Right(Nil)) and
      expect(events.isEmpty) and expect(subscribers == 0)
  }

  test("concurrent commands only observe projection results for their own saved event identifiers") {
    val failure = new RuntimeException("second command projection failed")
    for
      topic        <- Topic[IO, Notification]
      appended     <- Queue.unbounded[IO, List[EventEnvelope[TestEvent]]]
      commands     <- commandRuntime(appended.offer)
      runtime       = SyncCommandRuntime.fromTopic(commands, topic, createdTypes, 1.second)
      first        <- run(Record("first"))(using runtime).attempt.start
      second       <- run(Record("second"))(using runtime).attempt.start
      batchA       <- appended.take
      batchB       <- appended.take
      events        = batchA ++ batchB
      firstEvent    = events.find(_.payload == Created("first", 0)).get
      secondEvent   = events.find(_.payload == Created("second", 0)).get
      _            <- topic.publish1(secondEvent.metadata.id -> Left(failure))
      _            <- publish(topic, firstEvent)
      firstResult  <- first.joinWithNever
      secondResult <- second.joinWithNever
    yield expect(firstResult == Right(Right(List(firstEvent)))) and expect(secondResult == Left(failure))
  }

  test("a missing projection notification raises the default timeout and releases its subscription") {
    for
      topic       <- Topic[IO, Notification]
      commands    <- commandRuntime(_ => IO.unit)
      runtime      = SyncCommandRuntime.fromTopic(commands, topic, createdTypes, 20.millis)
      result      <- run(Record("a"))(using runtime).attempt
      subscribers <- topic.subscribers.head.compile.lastOrError
    yield expect(result.left.exists(_.isInstanceOf[TimeoutException])) and expect(subscribers == 0)
  }

  test("a projection wait timeout uses the configured error without evaluating it on success") {
    val failure = new RuntimeException("saved change is not visible yet")
    for
      topic              <- Topic[IO, Notification]
      commands           <- commandRuntime(_ => IO.unit)
      runtime             = SyncCommandRuntime.fromTopic(commands, topic, createdTypes, 20.millis, timeoutError = failure)
      rejected           <- run(Record("a", blocked = true))(using runtime)
      result             <- run(Record("a"))(using runtime).attempt
      successfulCommands <- commandRuntime(events => events.traverse_(publish(topic, _)))
      successfulRuntime   =
        SyncCommandRuntime.fromTopic(
          successfulCommands, topic, createdTypes, 1.second,
          timeoutError = throw new AssertionError("The timeout error must only be evaluated on timeout"),
        )
      successful <- run(Record("b"))(using successfulRuntime)
    yield expect(rejected == Left(Rejection.Blocked)) and expect(result == Left(failure)) and expect(successful.isRight)
  }

  test("a command timeout keeps its original error instead of becoming a projection wait timeout") {
    val commandFailure = new TimeoutException("database timed out while saving")
    val waitFailure = new RuntimeException("projection timed out")
    for
      topic    <- Topic[IO, Notification]
      commands <- commandRuntime(_ => IO.raiseError(commandFailure))
      runtime   = SyncCommandRuntime.fromTopic(commands, topic, createdTypes, 1.second, timeoutError = waitFailure)
      result   <- run(Record("a"))(using runtime).attempt
    yield expect(result == Left(commandFailure))
  }

  test("a projection's own timeout keeps its original error instead of becoming a wait timeout") {
    val projectionFailure = new TimeoutException("projection database query timed out")
    val waitFailure = new RuntimeException("projection notification timed out")
    for
      topic    <- Topic[IO, Notification]
      commands <- commandRuntime(events => topic.publish1(events.head.metadata.id -> Left(projectionFailure)).void)
      runtime   = SyncCommandRuntime.fromTopic(commands, topic, createdTypes, 1.second, timeoutError = waitFailure)
      result   <- run(Record("a"))(using runtime).attempt
    yield expect(result == Left(projectionFailure))
  }

  test("cancelling the projection wait removes the topic subscriber") {
    for
      topic    <- Topic[IO, Notification]
      appended <- Deferred[IO, Unit]
      commands <- commandRuntime(_ => appended.complete(()).void)
      runtime   = SyncCommandRuntime.fromTopic(commands, topic, createdTypes, 1.minute)
      fiber    <- run(Record("a"))(using runtime).start
      _        <- appended.get
      before   <- topic.subscribers.head.compile.lastOrError
      _        <- fiber.cancel
      after    <- topic.subscribers.head.compile.lastOrError
    yield expect(before == 1) and expect(after == 0)
  }
