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
import fs2.Stream
import java.time.Instant
import weaver.SimpleIOSuite
import java.util.UUID

object CommandHandlerSnapshotSuite extends SimpleIOSuite:

  // ---------------------------------------------------------------------------
  // Test domain
  // ---------------------------------------------------------------------------

  sealed trait TestEvent extends Event

  final case class Incremented(amount: Int) extends TestEvent

  final case class TestCommand(increment: Int, tags: Set[Tag])

  final case class TestState(total: Int)

  given SnapshotCodec[TestState] with

    def encode(state: TestState): String = state.total.toString

    def decode(payload: String): Either[Throwable, TestState] =
      payload.toIntOption
        .map(TestState(_))
        .toRight(new RuntimeException(s"Cannot decode TestState: $payload"))

  def makeHandler(threshold: Int): CommandHandler[TestCommand, TestState, TestEvent] =
    new CommandHandler[TestCommand, TestState, TestEvent]:
      def tags(command: TestCommand): Set[Tag] = command.tags
      def initial: TestState = TestState(0)
      def evolve(command: TestCommand, state: TestState, event: TestEvent): TestState =
        event match
          case Incremented(n) => TestState(state.total + n)
      def validate(state: TestState, command: TestCommand): Either[Throwable, Unit] = Right(())
      def decide(state: TestState, command: TestCommand): List[(Set[Tag], TestEvent)] =
        List((command.tags, Incremented(command.increment)))
      override def snapshotThreshold: Int = threshold

  // ---------------------------------------------------------------------------
  // Inline infrastructure
  // ---------------------------------------------------------------------------

  final class InMemoryEventStore private (
    eventsRef: Ref[IO, Vector[EventEnvelope[TestEvent]]],
    val readPositions: Ref[IO, List[Long]],
  ) extends EventStore[IO, TestEvent]:

    private def matches(env: EventEnvelope[TestEvent], f: EventFilter): Boolean =
      val byType = f.eventTypes.isEmpty || f.eventTypes.contains(env.metadata.eventType)
      val byTag = f.tags.isEmpty || env.metadata.tags.exists(f.tags.contains)
      byType && byTag

    def append(
      eventFilter: EventFilter,
      expectedIndex: Long,
      evts: List[(Option[UUID], Set[Tag], EventTypeName, Boolean, TestEvent)]*,
    ): IO[List[TestEvent]] =
      eventsRef.modify { current =>
        val relevant = current.filter(matches(_, eventFilter))
        val actualIdx = relevant.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
        if actualIdx != expectedIndex then (current, Left(IndexConflictException(expectedIndex, actualIdx)))
        else
          val lastPos = current.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
          val newEvts = evts.flatten.zipWithIndex.map { case ((id, tags, typeName, isExt, evt), i) =>
            EventEnvelope(
              EventMetadata(
                lastPos + i.toLong + 1L,
                id.getOrElse(UUID.randomUUID()),
                tags,
                typeName,
                isExt,
                Instant.now(),
              ),
              evt,
            )
          }
          (current ++ newEvts, Right(newEvts.map(_.payload).toList))
      }.flatMap {
        case Left(e)     => IO.raiseError(e)
        case Right(evts) => IO.pure(evts)
      }

    def appendUnchecked(
      events: List[(Option[UUID], Set[Tag], EventTypeName, Boolean, TestEvent)]*,
    ): IO[List[TestEvent]] = IO.pure(List.empty)

    def readFrom(
      fromPosition: Long,
      eventFilter: EventFilter,
      maxEvents: Option[Int] = None,
    ): Stream[IO, EventEnvelope[TestEvent]] =
      Stream
        .eval(readPositions.update(fromPosition :: _) *> eventsRef.get)
        .flatMap(Stream.emits)
        .filter(env => matches(env, eventFilter) && env.metadata.globalPosition > fromPosition)
        .take(maxEvents.map(_.toLong).getOrElse(Long.MaxValue))

  object InMemoryEventStore:

    def make: IO[InMemoryEventStore] =
      for
        ref     <- Ref.of[IO, Vector[EventEnvelope[TestEvent]]](Vector.empty)
        readPos <- Ref.of[IO, List[Long]](Nil)
      yield new InMemoryEventStore(ref, readPos)

  final class InMemorySnapshotStore private (
    storeRef: Ref[IO, Map[(String, String), (String, Long)]],
    val saves: Ref[IO, Int],
  ) extends SnapshotStore[IO]:

    private def key(handlerName: String, tags: Set[Tag]): (String, String) =
      (handlerName, tags.toList.map(_.value).sorted.mkString("|"))

    def load[S: SnapshotCodec](handlerName: String, tags: Set[Tag]): IO[Option[Snapshot[S]]] =
      storeRef.get.flatMap {
        _.get(key(handlerName, tags)) match
          case None                 => IO.pure(None)
          case Some((payload, pos)) =>
            IO.fromEither(
              summon[SnapshotCodec[S]]
                .decode(payload)
                .map(s => Some(Snapshot(s, pos)))
                .left
                .map(SnapshotDecodeException(_)),
            )
      }

    def save[S: SnapshotCodec](handlerName: String, tags: Set[Tag], snapshot: Snapshot[S]): IO[Unit] =
      saves.update(_ + 1) *>
        storeRef.update(
          _.updated(key(handlerName, tags), (summon[SnapshotCodec[S]].encode(snapshot.state), snapshot.globalPosition)),
        )

    def injectRaw(handlerName: String, tags: Set[Tag], payload: String, position: Long): IO[Unit] =
      storeRef.update(_.updated(key(handlerName, tags), (payload, position)))

    def getSnapshot[S: SnapshotCodec](handlerName: String, tags: Set[Tag]): IO[Option[Snapshot[S]]] =
      load[S](handlerName, tags).recover { case _: SnapshotDecodeException => None }

  object InMemorySnapshotStore:

    def make: IO[InMemorySnapshotStore] =
      for
        store <- Ref.of[IO, Map[(String, String), (String, Long)]](Map.empty)
        saves <- Ref.of[IO, Int](0)
      yield new InMemorySnapshotStore(store, saves)

  val testTags = Set(Tag("entity", "test-1"))

  val otherTags = Set(Tag("entity", "test-2"))

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  test("snapshot is saved once snapshotThreshold events have been read") {
    InMemoryEventStore.make.flatMap { eventStore =>
      InMemorySnapshotStore.make.flatMap { snapshotStore =>
        given EventStore[IO, TestEvent] = eventStore
        given SnapshotStore[IO] = snapshotStore
        val handler = makeHandler(threshold = 3)
        // 3 commands build up 3 events; each command reads < 3 events, no snapshot yet
        for
          _           <- handler.run[IO](TestCommand(1, testTags))
          _           <- handler.run[IO](TestCommand(1, testTags))
          _           <- handler.run[IO](TestCommand(1, testTags))
          countBefore <- snapshotStore.saves.get
          // 4th command reads 3 events >= threshold -> snapshot saved
          _          <- handler.run[IO](TestCommand(1, testTags))
          countAfter <- snapshotStore.saves.get
          snap       <- snapshotStore.getSnapshot[TestState](handler.handlerId, testTags)
        yield expect(countBefore == 0) &&
          expect(countAfter == 1) &&
          expect(snap.map(_.state) == Some(TestState(3)))
      }
    }
  }

  test("snapshot is not saved when events read are below threshold") {
    InMemoryEventStore.make.flatMap { eventStore =>
      InMemorySnapshotStore.make.flatMap { snapshotStore =>
        given EventStore[IO, TestEvent] = eventStore
        given SnapshotStore[IO] = snapshotStore
        val handler = makeHandler(threshold = 5)
        for
          _     <- handler.run[IO](TestCommand(1, testTags))
          _     <- handler.run[IO](TestCommand(1, testTags))
          _     <- handler.run[IO](TestCommand(1, testTags))
          count <- snapshotStore.saves.get
        yield expect(count == 0)
      }
    }
  }

  test("existing snapshot is used as base state: events before snapshot position are not replayed") {
    InMemoryEventStore.make.flatMap { eventStore =>
      InMemorySnapshotStore.make.flatMap { snapshotStore =>
        given EventStore[IO, TestEvent] = eventStore
        given SnapshotStore[IO] = snapshotStore
        val handler = makeHandler(threshold = 1)
        for
          // Seed 3 events
          _ <- eventStore.append(
                 EventFilter(tags = testTags),
                 0L,
                 List((Some(UUID.randomUUID()), testTags, EventTypeName.of[Incremented], false, Incremented(10))),
               )
          _ <- eventStore.append(
                 EventFilter(tags = testTags),
                 1L,
                 List((Some(UUID.randomUUID()), testTags, EventTypeName.of[Incremented], false, Incremented(10))),
               )
          _ <- eventStore.append(
                 EventFilter(tags = testTags),
                 2L,
                 List((Some(UUID.randomUUID()), testTags, EventTypeName.of[Incremented], false, Incremented(10))),
               )
          // Inject a snapshot with an intentionally wrong state at position 3.
          // Correct total would be 30, but we use 999 to detect if events before the
          // snapshot are replayed (which would give 999+30=1029) or ignored (giving 999).
          _ <- snapshotStore.save[TestState](handler.handlerId, testTags, Snapshot(TestState(999), 3L))
          // Add one event after the snapshot
          _ <- eventStore.append(
                 EventFilter(tags = testTags),
                 3L,
                 List((Some(UUID.randomUUID()), testTags, EventTypeName.of[Incremented], false, Incremented(1))),
               )
          // Run command: load snapshot(999, pos=3), read only the 1 event after pos 3, state=1000
          _ <- handler.run[IO](TestCommand(0, testTags))
          // New snapshot: state = 999 + 1 = 1000 (not 30+1=31, not 999+30+1=1030)
          snap <- snapshotStore.getSnapshot[TestState](handler.handlerId, testTags)
        yield expect(snap.map(_.state.total) == Some(1000))
      }
    }
  }

  test("decode error triggers full replay and a fresh snapshot is saved") {
    InMemoryEventStore.make.flatMap { eventStore =>
      InMemorySnapshotStore.make.flatMap { snapshotStore =>
        given EventStore[IO, TestEvent] = eventStore
        given SnapshotStore[IO] = snapshotStore
        val handler = makeHandler(threshold = 2)
        for
          // Seed 2 events
          _ <- eventStore.append(
                 EventFilter(tags = testTags),
                 0L,
                 List((Some(UUID.randomUUID()), testTags, EventTypeName.of[Incremented], false, Incremented(5))),
               )
          _ <- eventStore.append(
                 EventFilter(tags = testTags),
                 1L,
                 List((Some(UUID.randomUUID()), testTags, EventTypeName.of[Incremented], false, Incremented(5))),
               )
          // Inject a corrupted snapshot (simulates a stale snapshot after a state schema change)
          _ <- snapshotStore.injectRaw(handler.handlerId, testTags, "not-an-int", 2L)
          // Run command: decode fails -> SnapshotDecodeException -> full replay -> state=10,
          // reads 2 events >= threshold -> saves fresh snapshot
          _    <- handler.run[IO](TestCommand(0, testTags))
          snap <- snapshotStore.getSnapshot[TestState](handler.handlerId, testTags)
        yield expect(snap.map(_.state) == Some(TestState(10)))
      }
    }
  }

  test("snapshots are isolated by tag set") {
    InMemoryEventStore.make.flatMap { eventStore =>
      InMemorySnapshotStore.make.flatMap { snapshotStore =>
        given EventStore[IO, TestEvent] = eventStore
        given SnapshotStore[IO] = snapshotStore
        val handler = makeHandler(threshold = 3)
        for
          // 4 commands for testTags: at the 4th, reads 3 events -> snapshot saved
          _ <- handler.run[IO](TestCommand(1, testTags))
          _ <- handler.run[IO](TestCommand(1, testTags))
          _ <- handler.run[IO](TestCommand(1, testTags))
          _ <- handler.run[IO](TestCommand(1, testTags))
          // 2 commands for otherTags: reads at most 1 event -> no snapshot
          _     <- handler.run[IO](TestCommand(1, otherTags))
          _     <- handler.run[IO](TestCommand(1, otherTags))
          snap1 <- snapshotStore.getSnapshot[TestState](handler.handlerId, testTags)
          snap2 <- snapshotStore.getSnapshot[TestState](handler.handlerId, otherTags)
        yield expect(snap1.isDefined) && expect(snap2.isEmpty)
      }
    }
  }
