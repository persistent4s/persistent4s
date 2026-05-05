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

object InstrumentedEventStoreSuite extends SimpleIOSuite:

  given Tracer[IO] = Tracer.Implicits.noop
  given Meter[IO]  = Meter.Implicits.noop

  sealed trait TestEvent extends Event
  object TestEvent:
    final case class Created(id: String) extends TestEvent

  final class FakeStore(ref: Ref[IO, Vector[EventEnvelope[TestEvent]]]) extends EventStore[IO, TestEvent]:
    def append(
      eventFilter: EventFilter,
      expectedIndex: Long,
      evts: List[(Set[Tag], EventTypeName, TestEvent)]*,
    ): IO[Unit] =
      ref.update { current =>
        val last = current.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
        current ++ evts.flatten.zipWithIndex.map { case ((tags, et, ev), i) =>
          EventEnvelope(EventMetadata(last + i.toLong + 1L, tags, et, java.time.Instant.now()), ev)
        }
      }
    def readFrom(fromPosition: Long, eventFilter: EventFilter): Stream[IO, EventEnvelope[TestEvent]] =
      Stream.eval(ref.get).flatMap(Stream.emits).filter(_.metadata.globalPosition > fromPosition)

  final class ConflictingStore extends EventStore[IO, TestEvent]:
    def append(
      eventFilter: EventFilter,
      expectedIndex: Long,
      evts: List[(Set[Tag], EventTypeName, TestEvent)]*,
    ): IO[Unit] = IO.raiseError(IndexConflictException(0L, 1L))
    def readFrom(fromPosition: Long, eventFilter: EventFilter): Stream[IO, EventEnvelope[TestEvent]] =
      Stream.empty

  test("append delegates to inner store") {
    for
      ref          <- Ref.of[IO, Vector[EventEnvelope[TestEvent]]](Vector.empty)
      inner         = FakeStore(ref)
      instrumented <- InstrumentedEventStore.make[IO, TestEvent](inner)
      _            <- instrumented.append(
                        EventFilter(),
                        0L,
                        List((Set.empty, EventTypeName.of[TestEvent.Created], TestEvent.Created("x"))),
                      )
      stored <- ref.get
    yield expect(stored.size == 1)
  }

  test("append propagates IndexConflictException") {
    for
      instrumented <- InstrumentedEventStore.make[IO, TestEvent](new ConflictingStore)
      result       <- instrumented
                        .append(
                          EventFilter(),
                          0L,
                          List((Set.empty, EventTypeName.of[TestEvent.Created], TestEvent.Created("x"))),
                        )
                        .attempt
    yield expect(result.left.exists(_.isInstanceOf[IndexConflictException]))
  }

  test("readFrom delegates and emits events from inner store") {
    for
      ref          <- Ref.of[IO, Vector[EventEnvelope[TestEvent]]](Vector.empty)
      inner         = FakeStore(ref)
      instrumented <- InstrumentedEventStore.make[IO, TestEvent](inner)
      _            <- inner.append(
                        EventFilter(),
                        0L,
                        List((Set.empty, EventTypeName.of[TestEvent.Created], TestEvent.Created("a"))),
                      )
      events <- instrumented.readFrom(0L, EventFilter()).compile.toList
    yield expect(events.size == 1)
  }

  test("readFrom with fromPosition filters earlier events") {
    for
      ref          <- Ref.of[IO, Vector[EventEnvelope[TestEvent]]](Vector.empty)
      inner         = FakeStore(ref)
      instrumented <- InstrumentedEventStore.make[IO, TestEvent](inner)
      _            <- inner.append(EventFilter(), 0L, List((Set.empty, EventTypeName.of[TestEvent.Created], TestEvent.Created("a"))))
      _            <- inner.append(EventFilter(), 1L, List((Set.empty, EventTypeName.of[TestEvent.Created], TestEvent.Created("b"))))
      events       <- instrumented.readFrom(1L, EventFilter()).compile.toList
    yield expect(events.size == 1 && events.head.payload == TestEvent.Created("b"))
  }
