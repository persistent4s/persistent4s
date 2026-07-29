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

import cats.effect.std.Queue
import cats.effect.{Deferred, IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import persistent4s.EventStoreNotification.*
import weaver.SimpleIOSuite

import scala.concurrent.duration.*
import java.util.UUID

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
    queue: Queue[IO, EventStoreNotification],
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
      evts: List[PendingEvent[A]]*,
    ): IO[List[A]] =
      events.modify { current =>
        val relevant = current.filter(matches(_, eventFilter))
        val actualIdx = relevant.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
        if actualIdx != expectedIndex then (current, Left(new IndexConflictException(expectedIndex, actualIdx)))
        else
          val lastPos = current.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
          val newEvts = evts.flatten.zipWithIndex.map { case (pending, i) =>
            EventEnvelope(
              EventMetadata(
                lastPos + i.toLong + 1L,
                pending.id.getOrElse(UUID.randomUUID()),
                pending.tags,
                pending.eventType,
                pending.isExternal,
                java.time.Instant.now(),
                pending.headers,
              ),
              pending.payload,
            )
          }
          (current ++ newEvts, Right(evts.flatten.map(_.payload)))
      }.flatMap {
        case Left(e)       => IO.raiseError(e)
        case Right(result) => queue.offer(EventsAppended).as(result.toList)
      }

    def appendUnchecked(events: List[PendingEvent[A]]*): IO[List[A]] =
      IO.pure(List.empty) // not needed for these tests

    def readFrom(
      fromPosition: Long,
      eventFilter: EventFilter,
      maxEvents: Option[Int] = None,
    ): Stream[IO, EventEnvelope[A]] =
      Stream
        .eval(events.get)
        .flatMap(Stream.emits)
        .filter(env => matches(env, eventFilter) && env.metadata.globalPosition > fromPosition)
        .take(maxEvents.map(_.toLong).getOrElse(Long.MaxValue))

    def notification(projectionName: String): Stream[IO, EventStoreNotification] =
      Stream.fromQueueUnterminated(queue)

    def sendNotification(notif: EventStoreNotification): IO[Unit] =
      queue.offer(notif)

  object InMemoryStore:

    def make[A <: Event]: IO[InMemoryStore[A]] =
      for
        ref   <- Ref.of[IO, Vector[EventEnvelope[A]]](Vector.empty)
        queue <- Queue.unbounded[IO, EventStoreNotification]
      yield InMemoryStore(ref, queue)

  final class InMemoryCheckpoint private (state: Ref[IO, Map[String, ProjectionCheckpointState]])
      extends ProjectionCheckpoint[IO]:

    def load(projectionName: String): IO[Option[ProjectionCheckpointState]] =
      state.get.map(_.get(projectionName))

    def save(projectionCheckpointState: ProjectionCheckpointState): IO[Unit] =
      state.update(_.updated(projectionCheckpointState.projectionName, projectionCheckpointState))

    def loadAll(): IO[List[ProjectionCheckpointState]] = state.get.map(_.values.toList)

    def getAll: IO[Map[String, ProjectionCheckpointState]] = state.get

  object InMemoryCheckpoint:

    def make: IO[InMemoryCheckpoint] =
      Ref.of[IO, Map[String, ProjectionCheckpointState]](Map.empty).map(InMemoryCheckpoint(_))

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  private def entityTag(id: String): Tag = Tag("entity", id)

  private def seed(store: InMemoryStore[TestEvent], events: (Set[Tag], TestEvent)*): IO[Unit] =
    events.toList.zipWithIndex.traverse_ { case ((tags, event), i) =>
      store.append(
        EventFilter(),
        i.toLong,
        List(PendingEvent(event, tags, EventTypeName.fromInstance(event), false, None, Map.empty)),
      )
    }

  private def waitUntil(cond: IO[Boolean]): IO[Unit] =
    cond.flatMap {
      case true  => IO.unit
      case false => IO.sleep(10.millis) *> waitUntil(cond)
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
  ): Projection[IO, TestEvent, String, Int] =
    new Projection[IO, TestEvent, String, Int]:

      protected val repository: Repository[IO, String, Int] = new Repository[IO, String, Int] {
        def findMany(keys: List[String]): IO[Map[String, Option[Int]]] = states.get.map { current =>
          keys.map(k => k -> current.get(k)).toMap
        }

        def upsertMany(repoStates: Map[String, Int]): IO[Unit] =
          repoStates.toList.traverse_ { case (key, state) =>
            states.update(_.updated(key, state))
          }

        def deleteMany(keys: List[String]): IO[Unit] =
          keys.traverse_(key => states.update(_ - key))
      }

      def name: String = "tracking"

      def filter: Set[EventTypeName] = eventFilter.eventTypes

      def resolveKeys(event: EventEnvelope[TestEvent]): List[String] = resolveKeysF(event)

      def handle(state: Option[Int], event: EventEnvelope[TestEvent]): IO[Option[Int]] =
        failOnPosition match
          case Some(pos) if event.metadata.globalPosition == pos =>
            IO.raiseError(new RuntimeException(s"simulated failure at position $pos"))
          case _ =>
            handled.update(_ :+ event).as(Some(state.getOrElse(0) + 1))

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
      _      <- checkpoint.save(ProjectionCheckpointState("tracking", 1L, true, None))
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
    yield expect(saved.get("tracking").map(_.globalPosition) == Some(3L))
  }

  test("fetches state once per unique key regardless of how many events reference it") {
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      handled    <- Ref.of[IO, List[EventEnvelope[TestEvent]]](Nil)
      states     <- Ref.of[IO, Map[String, Int]](Map.empty)
      fetchCount <- Ref.of[IO, Int](0)
      projection  = new Projection[IO, TestEvent, String, Int]:

                     protected val repository: Repository[IO, String, Int] = new Repository[IO, String, Int] {
                       def findMany(keys: List[String]): IO[Map[String, Option[Int]]] =
                         fetchCount.update(_ + keys.size) *> states.get.map { current =>
                           keys.map(k => k -> current.get(k)).toMap
                         }

                       def upsertMany(repoStates: Map[String, Int]): IO[Unit] =
                         repoStates.toList.traverse_ { case (key, state) =>
                           states.update(_.updated(key, state))
                         }

                       def deleteMany(keys: List[String]): IO[Unit] =
                         keys.traverse_(key => states.update(_ - key))
                     }

                     def name: String = "tracking"
                     def filter: Set[EventTypeName] = Set.empty
                     def resolveKeys(event: EventEnvelope[TestEvent]): List[String] =
                       event.payload match
                         case TestEvent.Created(id) => List(id)
                         case TestEvent.Deleted(id) => List(id)
                     def handle(state: Option[Int], event: EventEnvelope[TestEvent]): IO[Option[Int]] =
                       handled.update(_ :+ event).as(Some(state.getOrElse(0) + 1))

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
      // count == 1, // fetchState called once for key "a" across the whole batch
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

  test("on partial batch failure: persists progress up to the failed event, pauses the projector with an error") {
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
      result <- DefaultProjector(store, checkpoint).run(projection).compile.drain.background.use { _ =>
                  waitUntil(checkpoint.load("tracking").map(_.exists(_.error.isDefined))).timeout(2.seconds) *>
                    (states.get, checkpoint.getAll).tupled
                }
      (persisted, saved) = result
    yield expect.all(
      persisted.contains("a"),                                               // state for pos 1 was saved
      !persisted.contains("b"),                                              // state for pos 2 was not saved (it failed)
      !persisted.contains("c"),                                              // state for pos 3 was never processed
      saved.get("tracking").map(_.globalPosition) == Some(1L),               // checkpoint advanced only to pos 1
      saved.get("tracking").exists(!_.running),                              // projector is paused
      saved.get("tracking").flatMap(_.error).exists(_.contains("simulated")), // error recorded in checkpoint
    )
  }

  test("on first event failure: no state persisted, checkpoint saved with error at initial position, projector paused") {
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
      result <- DefaultProjector(store, checkpoint).run(projection).compile.drain.background.use { _ =>
                  waitUntil(checkpoint.load("tracking").map(_.exists(_.error.isDefined))).timeout(2.seconds) *>
                    (states.get, checkpoint.getAll).tupled
                }
      (persisted, saved) = result
    yield expect.all(
      persisted.isEmpty,                                                     // nothing persisted
      saved.get("tracking").map(_.globalPosition) == Some(-1L),              // position not advanced from initial
      saved.get("tracking").exists(!_.running),                              // projector is paused
      saved.get("tracking").flatMap(_.error).exists(_.contains("simulated")), // error recorded in checkpoint
    )
  }

  test("processes events appended after a notification without polling") {
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      processed  <- Deferred[IO, EventEnvelope[TestEvent]]
      projection  = new StatelessProjection[IO, TestEvent]:
                     def name: String = "notif-test"
                     def filter: Set[EventTypeName] = Set.empty
                     def handle(ev: EventEnvelope[TestEvent]): IO[Unit] =
                       processed.complete(ev).void
      projector = DefaultProjector(store, checkpoint)
      ev       <- projector.run(projection).compile.drain.background.use { _ =>
              IO.sleep(50.millis) *>
                store.append(
                  EventFilter(),
                  0L,
                  List(
                    PendingEvent(TestEvent.Created("1"), Set(entityTag("1")), EventTypeName.fromString("Created"),
                      false, None, Map.empty),
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

  test("pause notification stops event processing") {
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      count      <- Ref.of[IO, Int](0)
      projection  = new StatelessProjection[IO, TestEvent]:
                     def name = "pause-test"
                     def filter = Set.empty
                     def handle(ev: EventEnvelope[TestEvent]): IO[Unit] =
                       count.update(_ + 1)
      result <- DefaultProjector(store, checkpoint).run(projection).compile.drain.background.use { _ =>
                  for
                    _ <- IO.sleep(50.millis)
                    _ <- store.sendNotification(PauseProjection("pause-test"))
                    _ <- waitUntil(checkpoint.load("pause-test").map(_.exists(!_.running)))
                    _ <- seed(
                           store,
                           (Set(entityTag("a")), TestEvent.Created("a")),
                           (Set(entityTag("b")), TestEvent.Created("b")),
                         )
                    _ <- IO.sleep(200.millis)
                    n <- count.get
                  yield n
                }
    yield expect(result == 0)
  }

  test("resume notification restarts processing after pause") {
    for
      store      <- InMemoryStore.make[TestEvent]
      checkpoint <- InMemoryCheckpoint.make
      count      <- Ref.of[IO, Int](0)
      done       <- Deferred[IO, Unit]
      projection  = new StatelessProjection[IO, TestEvent]:
                     def name = "resume-test"
                     def filter = Set.empty
                     def handle(ev: EventEnvelope[TestEvent]): IO[Unit] =
                       count.updateAndGet(_ + 1).flatMap(n => if n == 2 then done.complete(()).void else IO.unit)
      result <- DefaultProjector(store, checkpoint).run(projection).compile.drain.background.use { _ =>
                  for
                    _ <- IO.sleep(50.millis)
                    _ <- store.sendNotification(PauseProjection("resume-test"))
                    _ <- waitUntil(checkpoint.load("resume-test").map(_.exists(!_.running)))
                    _ <- seed(
                           store,
                           (Set(entityTag("a")), TestEvent.Created("a")),
                           (Set(entityTag("b")), TestEvent.Created("b")),
                         )
                    _                <- IO.sleep(100.millis)
                    countWhilePaused <- count.get
                    _                <- store.sendNotification(ResumeProjection("resume-test"))
                    _                <- done.get.timeout(2.seconds)
                    countAfterResume <- count.get
                  yield (countWhilePaused, countAfterResume)
                }
    yield expect.all(
      result._1 == 0, // not processed while paused
      result._2 == 2, // processed after resume
    )
  }

  test("UpdateCheckpointIndex causes reprocessing of events from the new position") {
    for
      store         <- InMemoryStore.make[TestEvent]
      checkpoint    <- InMemoryCheckpoint.make
      count         <- Ref.of[IO, Int](0)
      firstPassDone <- Deferred[IO, Unit]
      allDone       <- Deferred[IO, Unit]
      projection     = new StatelessProjection[IO, TestEvent]:
                     def name = "reindex-test"
                     def filter = Set.empty
                     def handle(ev: EventEnvelope[TestEvent]): IO[Unit] =
                       count.updateAndGet(_ + 1).flatMap {
                         case 2 => firstPassDone.complete(()).void
                         case 4 => allDone.complete(()).void
                         case _ => IO.unit
                       }
      _ <- seed(
             store,
             (Set(entityTag("a")), TestEvent.Created("a")), // pos 1
             (Set(entityTag("b")), TestEvent.Created("b")), // pos 2
           )
      result <- DefaultProjector(store, checkpoint).run(projection).compile.drain.background.use { _ =>
                  for
                    _ <- firstPassDone.get.timeout(2.seconds)
                    _ <- store.sendNotification(UpdateCheckpointIndex("reindex-test", 0L))
                    _ <- allDone.get.timeout(2.seconds)
                    n <- count.get
                  yield n
                }
    yield expect(result == 4) // 2 from first pass + 2 reprocessed after index reset
  }
