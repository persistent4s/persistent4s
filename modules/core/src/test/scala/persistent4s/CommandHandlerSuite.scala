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

import cats.effect.IO
import cats.effect.Ref
import fs2.Stream
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import weaver.SimpleIOSuite

object CommandHandlerSuite extends SimpleIOSuite:

  given Tracer[IO] = Tracer.Implicits.noop

  given Meter[IO] = Meter.Implicits.noop

  // ---------------------------------------------------------------------------
  // Test domain
  // ---------------------------------------------------------------------------

  sealed trait TestCmd

  final case class CreateItem(id: String) extends TestCmd

  final case class RenameItem(id: String, newName: String) extends TestCmd

  sealed trait TestEvent extends Event

  final case class ItemCreated(id: String) extends TestEvent

  final case class ItemRenamed(id: String, name: String) extends TestEvent

  object TestHandler extends CommandHandler[TestCmd, Option[String], TestEvent]:

    def tags(cmd: TestCmd): Set[Tag] = cmd match
      case CreateItem(id)    => Set(Tag("item", id))
      case RenameItem(id, _) => Set(Tag("item", id))

    def initial: Option[String] = None

    def evolve(cmd: TestCmd, state: Option[String], event: TestEvent): Option[String] = event match
      case ItemCreated(id)      => Some(id)
      case ItemRenamed(_, name) => Some(name)

    def validate(state: Option[String], cmd: TestCmd): Either[Throwable, Unit] = Right(())

    def decide(state: Option[String], cmd: TestCmd): List[(Set[Tag], TestEvent)] = cmd match
      case CreateItem(id)       => List((Set(Tag("item", id)), ItemCreated(id)))
      case RenameItem(id, name) => List((Set(Tag("item", id)), ItemRenamed(id, name)))

  // ---------------------------------------------------------------------------
  // Minimal in-memory store
  // ---------------------------------------------------------------------------

  final class InMemoryStore[A <: Event](ref: Ref[IO, Vector[EventEnvelope[A]]]) extends EventStore[IO, A]:

    def append(
      eventFilter: EventFilter,
      expectedIndex: Long,
      evts: List[(Set[Tag], EventTypeName, A)]*,
    ): IO[Unit] =
      ref.modify { current =>
        val relevant = current.filter { env =>
          (eventFilter.tags.isEmpty || env.metadata.tags.exists(eventFilter.tags.contains)) &&
          (eventFilter.eventTypes.isEmpty || eventFilter.eventTypes.contains(env.metadata.eventType))
        }
        val actualIdx = relevant.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
        if actualIdx != expectedIndex then (current, Left(IndexConflictException(expectedIndex, actualIdx)))
        else
          val last = current.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
          val newEvt = evts.flatten.zipWithIndex.map { case ((tags, et, ev), i) =>
            EventEnvelope(EventMetadata(last + i.toLong + 1L, tags, et, java.time.Instant.now()), ev)
          }
          (current ++ newEvt, Right(()))
      }.flatMap(_.fold(IO.raiseError, IO.pure))

    def readFrom(fromPosition: Long, eventFilter: EventFilter): Stream[IO, EventEnvelope[A]] =
      Stream.eval(ref.get).flatMap(Stream.emits).filter { env =>
        env.metadata.globalPosition > fromPosition &&
        (eventFilter.tags.isEmpty || env.metadata.tags.exists(eventFilter.tags.contains)) &&
        (eventFilter.eventTypes.isEmpty || eventFilter.eventTypes.contains(env.metadata.eventType))
      }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  test("run appends the decided events") {
    for
      ref                            <- Ref.of[IO, Vector[EventEnvelope[TestEvent]]](Vector.empty)
      given EventStore[IO, TestEvent] = InMemoryStore(ref)
      _                              <- TestHandler.run[IO](CreateItem("x"))
      stored                         <- ref.get
    yield expect(stored.size == 1 && stored.head.payload == ItemCreated("x"))
  }

  test("run reads current state before deciding") {
    for
      ref                            <- Ref.of[IO, Vector[EventEnvelope[TestEvent]]](Vector.empty)
      given EventStore[IO, TestEvent] = InMemoryStore(ref)
      _                              <- TestHandler.run[IO](CreateItem("x"))
      _                              <- TestHandler.run[IO](RenameItem("x", "renamed"))
      stored                         <- ref.get
    yield expect(stored.size == 2 && stored.last.payload == ItemRenamed("x", "renamed"))
  }

  test("run retries on IndexConflictException up to maxRetries") {
    for
      attemptsRef  <- Ref.of[IO, Int](0)
      storeRef     <- Ref.of[IO, Vector[EventEnvelope[TestEvent]]](Vector.empty)
      conflictStore = new EventStore[IO, TestEvent]:
                        def append(
                          ef: EventFilter,
                          ei: Long,
                          evts: List[(Set[Tag], EventTypeName, TestEvent)]*,
                        ): IO[Unit] =
                          attemptsRef.update(_ + 1) *> IO.raiseError(IndexConflictException(0L, 1L))
                        def readFrom(fp: Long, ef: EventFilter): Stream[IO, EventEnvelope[TestEvent]] =
                          Stream.eval(storeRef.get).flatMap(Stream.emits)
      given EventStore[IO, TestEvent] = conflictStore
      retryHandler                    = new CommandHandler[TestCmd, Option[String], TestEvent]:
                       def tags(cmd: TestCmd) = Set(Tag("item", "x"))
                       def initial = None
                       def evolve(cmd: TestCmd, state: Option[String], ev: TestEvent) = state
                       def validate(state: Option[String], cmd: TestCmd) = Right(())
                       def decide(state: Option[String], cmd: TestCmd) = List((Set.empty, ItemCreated("x")))
                       override def maxRetries = 2
      result   <- retryHandler.run[IO](CreateItem("x")).attempt
      attempts <- attemptsRef.get
    yield expect(result.isLeft && attempts == 3) // 1 initial + 2 retries
  }
