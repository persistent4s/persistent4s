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

import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Topic
import weaver.SimpleIOSuite

import scala.concurrent.duration.*

object DefaultProjectorSuite extends SimpleIOSuite:

  // ---------------------------------------------------------------------------
  // Test domain
  // ---------------------------------------------------------------------------

  sealed trait TestEvent extends Event

  object TestEvent:

    final case class Created(id: String) extends TestEvent

    final case class Deleted(id: String) extends TestEvent

  // ---------------------------------------------------------------------------
  // Inline infrastructure
  // ---------------------------------------------------------------------------

  final class InMemoryStore[A <: Event] private (
    events: Ref[IO, Vector[EventEnvelope[A]]],
    topic: Topic[IO, Unit],
  ) extends EventStore[IO, A]
      with EventNotification[IO]:

    private def matches(env: EventEnvelope[A], f: EventFilter): Boolean =
      val byType = f.eventTypes.isEmpty || f.eventTypes.contains(env.metadata.eventType)
      val byTag = f.tags.isEmpty || env.metadata.tags.exists(f.tags.contains)
      byType && byTag

    def getAll: IO[Vector[EventEnvelope[A]]] = events.get

    def append(
      eventFilter: EventFilter,
      expectedIndex: Long,
      evts: List[(Set[Tag], EventTypeName, A)]*,
    ): IO[Unit] =
      events.modify { current =>
        val relevant = current.filter(matches(_, eventFilter))
        val actualIdx = relevant.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
        if actualIdx != expectedIndex then (current, Left(new IndexConflictException(expectedIndex, actualIdx)))
        else
          val lastPos = current.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
          val newEvts = evts.flatten.zipWithIndex.map { case ((tags, eventType, evt), i) =>
            EventEnvelope(
              EventMetadata(lastPos + i.toLong + 1L, tags, eventType, java.time.Instant.now()),
              evt,
            )
          }
          (current ++ newEvts, Right(()))
      }.flatMap {
        case Left(e)  => IO.raiseError(e)
        case Right(_) => topic.publish1(()).void
      }

    def readFrom(fromPosition: Long, eventFilter: EventFilter): Stream[IO, EventEnvelope[A]] =
      Stream
        .eval(events.get)
        .flatMap(Stream.emits)
        .filter(env => matches(env, eventFilter) && env.metadata.globalPosition > fromPosition)

    def notification: Stream[IO, Unit] = topic.subscribe(1)

  object InMemoryStore:

    def make[A <: Event]: IO[InMemoryStore[A]] =
      for
        ref   <- Ref.of[IO, Vector[EventEnvelope[A]]](Vector.empty)
        topic <- Topic[IO, Unit]
      yield InMemoryStore(ref, topic)

  final class InMemoryCheckpoint private (state: Ref[IO, Map[String, Long]]) extends ProjectionCheckpoint[IO]:

    def load(projectionName: String): IO[Option[Long]] = state.get.map(_.get(projectionName))

    def save(projectionName: String, globalPosition: Long): IO[Unit] =
      state.update(_.updated(projectionName, globalPosition))

    def getAll: IO[Map[String, Long]] = state.get

  object InMemoryCheckpoint:

    def make: IO[InMemoryCheckpoint] =
      Ref.of[IO, Map[String, Long]](Map.empty).map(InMemoryCheckpoint(_))

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def entityTag(id: String): Tag = Tag("entity", id)

  private def seed(store: InMemoryStore[TestEvent], events: (Set[Tag], TestEvent)*): IO[Unit] =
    events.toList.zipWithIndex.traverse_ { case ((tags, event), i) =>
      store.append(
        EventFilter(),
        i.toLong,
        List((tags, EventTypeName.fromInstance(event), event)),
      )
    }

  private def trackingProjection(
    handled: Ref[IO, List[EventEnvelope[TestEvent]]],
    states: Ref[IO, Map[String, Int]],
    eventFilter: EventFilter = EventFilter(),
    resolveKeysF: EventEnvelope[TestEvent] => List[String] = ev =>
      ev.payload match
        case TestEvent.Created(id) => List(id)
        case TestEvent.Deleted(id) => List(id),
    failOnPosition: Option[Long] = None,
  ): Projection[IO, TestEvent, String] =
    new Projection[IO, TestEvent, String]:
      type State = Int

      def name: String = "tracking"

      def filter: EventFilter = eventFilter

      def resolveKeys(event: EventEnvelope[TestEvent]): List[String] = resolveKeysF(event)

      def fetchStates(keys: List[String]): IO[Map[String, Option[Int]]] = states.get.map { current =>
        keys.map(k => k -> current.get(k)).toMap
      }

      def handle(state: Option[Int], event: EventEnvelope[TestEvent]): IO[Option[Int]] =
        failOnPosition match
          case Some(pos) if event.metadata.globalPosition == pos =>
            IO.raiseError(new RuntimeException(s"simulated failure at position $pos"))
          case _ =>
            handled.update(_ :+ event).as(Some(state.getOrElse(0) + 1))

      def persist(key: String, state: Option[Int]): IO[Unit] =
        state match
          case Some(v) => states.update(_.updated(key, v))
          case None    => states.update(_ - key)

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  test("processes all pre-seeded events in order") {
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      handled    <- Ref.of[IO, List[EventEnvelope[TestEvent]]](Nil)
      states     <- Ref.of[IO, Map[String, Int]](Map.empty)
      projection  = trackingProjection(handled, states)
      _          <- seed(
             store,
             (Set(entityTag("a")), TestEvent.Created("a")),
             (Set(entityTag("b")), TestEvent.Created("b")),
             (Set(entityTag("c")), TestEvent.Created("c")),
           )
      _      <- DefaultProjector(store, checkpoint).run(projection).take(1).compile.drain
      events <- handled.get
    yield expect.all(
      events.length == 3,
      events.map(_.payload) == List(
        TestEvent.Created("a"),
        TestEvent.Created("b"),
        TestEvent.Created("c"),
      ),
    )
  }

  test("resumes from checkpoint and skips already-processed events") {
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      handled    <- Ref.of[IO, List[EventEnvelope[TestEvent]]](Nil)
      states     <- Ref.of[IO, Map[String, Int]](Map.empty)
      projection  = trackingProjection(handled, states)
      _          <- seed(
             store,
             (Set(entityTag("a")), TestEvent.Created("a")),
             (Set(entityTag("b")), TestEvent.Created("b")),
             (Set(entityTag("c")), TestEvent.Created("c")),
           )
      _      <- checkpoint.save("tracking", 1L) // simulate having already processed position 1
      _      <- DefaultProjector(store, checkpoint).run(projection).take(1).compile.drain
      events <- handled.get
    yield expect.all(
      events.length == 2,
      events.map(_.payload) == List(TestEvent.Created("b"), TestEvent.Created("c")),
    )
  }

  test("persists state after a successful batch") {
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      handled    <- Ref.of[IO, List[EventEnvelope[TestEvent]]](Nil)
      states     <- Ref.of[IO, Map[String, Int]](Map.empty)
      projection  = trackingProjection(handled, states)
      _          <- seed(
             store,
             (Set(entityTag("a")), TestEvent.Created("a")),
             (Set(entityTag("a")), TestEvent.Created("a")), // second event for same key
           )
      _         <- DefaultProjector(store, checkpoint).run(projection).take(1).compile.drain
      persisted <- states.get
    yield expect(persisted.get("a") == Some(2)) // handle called twice for key "a"
  }

  test("advances checkpoint to the position of the last event in the batch") {
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      handled    <- Ref.of[IO, List[EventEnvelope[TestEvent]]](Nil)
      states     <- Ref.of[IO, Map[String, Int]](Map.empty)
      projection  = trackingProjection(handled, states)
      _          <- seed(
             store,
             (Set(entityTag("a")), TestEvent.Created("a")),
             (Set(entityTag("b")), TestEvent.Created("b")),
             (Set(entityTag("c")), TestEvent.Created("c")),
           )
      _     <- DefaultProjector(store, checkpoint).run(projection).take(1).compile.drain
      saved <- checkpoint.getAll
    yield expect(saved.get("tracking") == Some(3L))
  }

  test("fetches state once per unique key regardless of how many events reference it") {
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      handled    <- Ref.of[IO, List[EventEnvelope[TestEvent]]](Nil)
      states     <- Ref.of[IO, Map[String, Int]](Map.empty)
      fetchCount <- Ref.of[IO, Int](0)
      projection  = new Projection[IO, TestEvent, String]:
                     type State = Int
                     def name: String = "tracking"
                     def filter: EventFilter = EventFilter()
                     def resolveKeys(event: EventEnvelope[TestEvent]): List[String] =
                       event.payload match
                         case TestEvent.Created(id) => List(id)
                         case TestEvent.Deleted(id) => List(id)
                     def fetchStates(keys: List[String]): IO[Map[String, Option[Int]]] =
                       fetchCount.update(_ + keys.size) *> states.get.map { current =>
                         keys.map(k => k -> current.get(k)).toMap
                       }
                     def handle(state: Option[Int], event: EventEnvelope[TestEvent]): IO[Option[Int]] =
                       handled.update(_ :+ event).as(Some(state.getOrElse(0) + 1))
                     def persist(key: String, state: Option[Int]): IO[Unit] =
                       state match
                         case Some(v) => states.update(_.updated(key, v))
                         case None    => states.update(_ - key)
      _ <- seed(
             store,
             (Set(entityTag("a")), TestEvent.Created("a")),
             (Set(entityTag("a")), TestEvent.Created("a")),
             (Set(entityTag("a")), TestEvent.Created("a")),
           )
      _      <- DefaultProjector(store, checkpoint).run(projection).take(1).compile.drain
      count  <- fetchCount.get
      events <- handled.get
    yield expect.all(
      count == 1,        // fetchState called once for key "a" across the whole batch
      events.length == 3, // handle called once per event
    )
  }

  test("calls handle for each resolved key when an event maps to multiple keys") {
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      handled    <- Ref.of[IO, List[EventEnvelope[TestEvent]]](Nil)
      states     <- Ref.of[IO, Map[String, Int]](Map.empty)
      projection  = trackingProjection(
                     handled,
                     states,
                     resolveKeysF = _ => List("k1", "k2"), // every event maps to two keys
                   )
      _         <- seed(store, (Set(entityTag("x")), TestEvent.Created("x")))
      _         <- DefaultProjector(store, checkpoint).run(projection).take(1).compile.drain
      persisted <- states.get
    yield expect.all(
      persisted.contains("k1"),
      persisted.contains("k2"),
      persisted("k1") == 1,
      persisted("k2") == 1,
    )
  }

  test("on partial batch failure: persists progress up to the failed event and propagates the error") {
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      handled    <- Ref.of[IO, List[EventEnvelope[TestEvent]]](Nil)
      states     <- Ref.of[IO, Map[String, Int]](Map.empty)
      projection  = trackingProjection(handled, states, failOnPosition = Some(2L))
      _          <- seed(
             store,
             (Set(entityTag("a")), TestEvent.Created("a")), // pos 1 — succeeds
             (Set(entityTag("b")), TestEvent.Created("b")), // pos 2 — fails
             (Set(entityTag("c")), TestEvent.Created("c")), // pos 3 — never reached
           )
      result    <- DefaultProjector(store, checkpoint).run(projection).compile.drain.attempt
      persisted <- states.get
      saved     <- checkpoint.getAll
    yield expect.all(
      result.isLeft,
      persisted.contains("a"),          // state for pos 1 was saved
      !persisted.contains("b"),         // state for pos 2 was not saved (it failed)
      !persisted.contains("c"),         // state for pos 3 was never processed
      saved.get("tracking") == Some(1L), // checkpoint advanced only to pos 1
    )
  }

  test("on first event failure: no state persisted and no checkpoint advance") {
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      handled    <- Ref.of[IO, List[EventEnvelope[TestEvent]]](Nil)
      states     <- Ref.of[IO, Map[String, Int]](Map.empty)
      projection  = trackingProjection(handled, states, failOnPosition = Some(1L))
      _          <- seed(
             store,
             (Set(entityTag("a")), TestEvent.Created("a")), // pos 1 — fails immediately
             (Set(entityTag("b")), TestEvent.Created("b")), // pos 2 — never reached
           )
      result    <- DefaultProjector(store, checkpoint).run(projection).compile.drain.attempt
      persisted <- states.get
      saved     <- checkpoint.getAll
    yield expect.all(
      result.isLeft,
      persisted.isEmpty,            // nothing persisted
      saved.get("tracking").isEmpty, // checkpoint not advanced
    )
  }

  test("processes events appended after a notification without polling") {
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      processed  <- Deferred[IO, EventEnvelope[TestEvent]]
      projection  = new StatelessProjection[IO, TestEvent]:
                     def name: String = "notif-test"
                     def filter: EventFilter = EventFilter()
                     def handle(ev: EventEnvelope[TestEvent]): IO[Unit] =
                       processed.complete(ev).void
      projector = DefaultProjector(store, checkpoint)
      ev       <- projector.run(projection).compile.drain.background.use { _ =>
              IO.sleep(50.millis) *>
                store.append(
                  EventFilter(),
                  0L,
                  List(
                    (
                      Set(entityTag("1")),
                      EventTypeName.fromString("Created"),
                      TestEvent.Created("1"),
                    ),
                  ),
                ) *>
                processed.get.timeout(2.seconds)
            }
    yield expect(ev.payload == TestEvent.Created("1"))
  }

  test("respects the projection's EventFilter and ignores non-matching events") {
    val targetType = EventTypeName.of[TestEvent.Created]
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      handled    <- Ref.of[IO, List[EventEnvelope[TestEvent]]](Nil)
      states     <- Ref.of[IO, Map[String, Int]](Map.empty)
      projection  = trackingProjection(
                     handled,
                     states,
                     eventFilter = EventFilter(eventTypes = Set(targetType)),
                   )
      _ <- seed(
             store,
             (Set(entityTag("a")), TestEvent.Created("a")), // matches filter
             (Set(entityTag("b")), TestEvent.Deleted("b")), // does NOT match filter
             (Set(entityTag("c")), TestEvent.Created("c")), // matches filter
           )
      _      <- DefaultProjector(store, checkpoint).run(projection).take(1).compile.drain
      events <- handled.get
    yield expect.all(
      events.length == 2,
      events.forall(_.payload.isInstanceOf[TestEvent.Created]),
    )
  }
