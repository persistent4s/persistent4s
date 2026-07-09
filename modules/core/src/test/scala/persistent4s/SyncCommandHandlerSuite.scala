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

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Topic
import weaver.SimpleIOSuite

import scala.concurrent.duration.*
import java.util.UUID

object SyncCommandHandlerSuite extends SimpleIOSuite:

  // ------------------------------------------------------------
  // Test domain
  // ------------------------------------------------------------

  sealed trait TestEvent extends Event

  object TestEvent:

    final case class Created(id: String) extends TestEvent

  final case class CreateMany(ids: List[String])

  private val handler: CommandHandler[CreateMany, Unit, TestEvent] =
    new CommandHandler[CreateMany, Unit, TestEvent]:
      def tags(command: CreateMany): Set[Tag] = command.ids.map(entityTag).toSet
      def initial: Unit = ()
      def evolve(command: CreateMany, state: Unit, event: TestEvent): Unit = ()
      def validate(state: Unit, command: CreateMany): Either[Throwable, Unit] = Right(())
      def decide(state: Unit, command: CreateMany): List[(Set[Tag], TestEvent)] =
        command.ids.map(id => Set(entityTag(id)) -> TestEvent.Created(id))

  private type SyncTopic = Topic[IO, (UUID, Either[Throwable, Map[String, Option[Int]]])]

  // ------------------------------------------------------------
  // Inline infrastructure
  // ------------------------------------------------------------

  final class InMemoryStore private (events: Ref[IO, Vector[EventEnvelope[TestEvent]]])
      extends EventStore[IO, TestEvent]:

    def getAll: IO[Vector[EventEnvelope[TestEvent]]] = events.get

    def append(
      eventFilter: EventFilter,
      expectedIndex: Long,
      evts: List[(Option[UUID], Set[Tag], EventTypeName, Boolean, TestEvent)]*,
    ): IO[List[EventEnvelope[TestEvent]]] =
      events.modify { current =>
        val lastPos = current.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
        val newEvts = evts.flatten.zipWithIndex.map { case ((maybeId, tags, eventType, isExternal, evt), i) =>
          EventEnvelope(
            EventMetadata(
              lastPos + i.toLong + 1L,
              maybeId.getOrElse(UUID.randomUUID()),
              tags,
              eventType,
              isExternal,
              java.time.Instant.now(),
            ),
            evt,
          )
        }
        (current ++ newEvts, newEvts)
      }.map(_.toList)

    def appendUnchecked(
      events: List[(Option[UUID], Set[Tag], EventTypeName, Boolean, TestEvent)]*,
    ): IO[List[EventEnvelope[TestEvent]]] = IO.pure(List.empty)

    def readFrom(
      fromPosition: Long,
      eventFilter: EventFilter,
      maxEvents: Option[Int],
    ): Stream[IO, EventEnvelope[TestEvent]] =
      Stream.eval(events.get).flatMap(Stream.emits).filter(_.metadata.globalPosition > fromPosition)

  object InMemoryStore:

    def make: IO[InMemoryStore] =
      Ref.of[IO, Vector[EventEnvelope[TestEvent]]](Vector.empty).map(new InMemoryStore(_))

  /** Publishes to the topic synchronously as part of append itself, simulating the fastest possible projector - faster
    * than a real on could evet be. Used to prove SyncCommandHandler can't miss it.
    */
  final class InstantPublishingStore(
    events: Ref[IO, Vector[EventEnvelope[TestEvent]]],
    topic: SyncTopic,
  ) extends EventStore[IO, TestEvent]:

    def append(
      eventFilter: EventFilter,
      expectedIndex: Long,
      evts: List[(Option[UUID], Set[Tag], EventTypeName, Boolean, TestEvent)]*,
    ): IO[List[EventEnvelope[TestEvent]]] =
      for
        current <- events.get
        lastPos  = current.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
        newEvts  = evts.flatten.toList.zipWithIndex.map { case ((maybeId, tags, eventType, isExternal, evt), i) =>
                    EventEnvelope(
                      EventMetadata(
                        lastPos + i.toLong + 1L,
                        maybeId.getOrElse(UUID.randomUUID()),
                        tags,
                        eventType,
                        isExternal,
                        java.time.Instant.now(),
                      ),
                      evt,
                    )
                  }
        _ <- events.update(_ ++ newEvts)
        _ <- newEvts.traverse_ { env =>
               val id = env.payload match
                 case TestEvent.Created(id) => id
               topic.publish1((env.metadata.id, Right(Map(id -> Some(1)))))
             }
      yield newEvts

    def appendUnchecked(
      events: List[(Option[UUID], Set[Tag], EventTypeName, Boolean, TestEvent)]*,
    ): IO[List[EventEnvelope[TestEvent]]] = IO.pure(List.empty)

    def readFrom(
      fromPosition: Long,
      eventFilter: EventFilter,
      maxEvents: Option[Int],
    ): Stream[IO, EventEnvelope[TestEvent]] =
      Stream.eval(events.get).flatMap(Stream.emits).filter(_.metadata.globalPosition > fromPosition)

  // ------------------------------------------------------------
  // Helpers
  // ------------------------------------------------------------

  private def entityTag(id: String): Tag = Tag("entity", id)

  private def waitUntil(cond: IO[Boolean]): IO[Unit] =
    cond.flatMap {
      case true  => IO.unit
      case false => IO.sleep(10.millis) *> waitUntil(cond)
    }

  // ------------------------------------------------------------
  // Tests
  // ------------------------------------------------------------

  test("returns the projected state once the projection published for the appended event") {
    for
      store  <- InMemoryStore.make
      topic  <- Topic[IO, (UUID, Either[Throwable, Map[String, Option[Int]]])]
      sync    = SyncCommandHandler(handler, topic, timeout = 2.seconds)
      fiber  <- sync.runSync(CreateMany(List("a")))(using store).start
      _      <- waitUntil(store.getAll.map(_.nonEmpty))
      events <- store.getAll
      _      <- topic.publish1((events.head.metadata.id, Right(Map("a" -> Some(1)))))
      result <- fiber.joinWithNever
    yield expect(result == Map("a" -> Some(1)))
  }

  test("waits for every event produced by the command, merging all their states") {
    for
      store  <- InMemoryStore.make
      topic  <- Topic[IO, (UUID, Either[Throwable, Map[String, Option[Int]]])]
      sync    = SyncCommandHandler(handler, topic, timeout = 2.seconds)
      fiber  <- sync.runSync(CreateMany(List("a", "b")))(using store).start
      _      <- waitUntil(store.getAll.map(_.size == 2))
      events <- store.getAll
      _      <- topic.publish1((events(0).metadata.id, Right(Map("a" -> Some(1)))))
      _      <- topic.publish1((events(1).metadata.id, Right(Map("b" -> Some(2)))))
      result <- fiber.joinWithNever
    yield expect(result == Map("a" -> Some(1), "b" -> Some(2)))
  }

  test("a duplicate publish for an already-seen id does not cause premature completion") {
    for
      store  <- InMemoryStore.make
      topic  <- Topic[IO, (UUID, Either[Throwable, Map[String, Option[Int]]])]
      sync    = SyncCommandHandler(handler, topic, timeout = 2.seconds)
      fiber  <- sync.runSync(CreateMany(List("a", "b")))(using store).start
      _      <- waitUntil(store.getAll.map(_.size == 2))
      events <- store.getAll
      _      <- topic.publish1((events(0).metadata.id, Right(Map("a" -> Some(1)))))
      _      <- topic.publish1((events(0).metadata.id, Right(Map("a" -> Some(1)))))
      _      <- topic.publish1((events(1).metadata.id, Right(Map("b" -> Some(2)))))
      result <- fiber.joinWithNever
    yield expect(result == Map("a" -> Some(1), "b" -> Some(2)))
  }

  test("returns immediately without waiting when the command decides no events") {
    for
      store  <- InMemoryStore.make
      topic  <- Topic[IO, (UUID, Either[Throwable, Map[String, Option[Int]]])]
      sync    = SyncCommandHandler(handler, topic, timeout = 2.seconds)
      result <- sync.runSync(CreateMany(Nil))(using store).timeout(500.millis)
    yield expect(result == Map.empty)
  }

  test("fails with the projection's error when a failure is published for one of the appended events") {
    val boom = new RuntimeException("projection blew up")
    for
      store  <- InMemoryStore.make
      topic  <- Topic[IO, (UUID, Either[Throwable, Map[String, Option[Int]]])]
      sync    = SyncCommandHandler(handler, topic, timeout = 2.seconds)
      fiber  <- sync.runSync(CreateMany(List("a")))(using store).attempt.start
      _      <- waitUntil(store.getAll.map(_.nonEmpty))
      events <- store.getAll
      _      <- topic.publish1((events.head.metadata.id, Left(boom)))
      result <- fiber.joinWithNever
    yield expect(result == Left(boom))
  }

  test("fails with a timeout when the projection never catch up, without undoing the append") {
    for
      store    <- InMemoryStore.make
      topic    <- Topic[IO, (UUID, Either[Throwable, Map[String, Option[Int]]])]
      sync      = SyncCommandHandler(handler, topic, timeout = 100.millis)
      result   <- sync.runSync(CreateMany(List("a")))(using store).attempt
      stored   <- store.getAll
      isTimeout = result match
                    case Left(_: java.util.concurrent.TimeoutException) => true
                    case _                                              => false
    yield expect.all(isTimeout, stored.exists(_.payload == TestEvent.Created("a")))
  }

  test("does not miss a publish that races ahead of the append call returning") {
    for
      events <- Ref.of[IO, Vector[EventEnvelope[TestEvent]]](Vector.empty)
      topic  <- Topic[IO, (UUID, Either[Throwable, Map[String, Option[Int]]])]
      store   = new InstantPublishingStore(events, topic)
      sync    = SyncCommandHandler(handler, topic, timeout = 500.millis)
      result <- sync.runSync(CreateMany(List("a")))(using store)
    yield expect(result == Map("a" -> Some(1)))
  }
