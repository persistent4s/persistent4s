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

import java.util.UUID

object InstrumentedEventStoreSuite extends SimpleIOSuite:

  given Tracer[IO] = Tracer.Implicits.noop

  given Meter[IO] = Meter.Implicits.noop

  sealed trait TestEvent extends Event

  object TestEvent:

    final case class Created(id: String) extends TestEvent

  final class FakeStore(ref: Ref[IO, Vector[EventEnvelope[TestEvent]]]) extends EventStore[IO, TestEvent]:

    private def matches(env: EventEnvelope[TestEvent], f: EventFilter): Boolean =
      (f.eventTypes.isEmpty || f.eventTypes.contains(env.metadata.eventType)) &&
        (f.tags.isEmpty || env.metadata.tags.exists(f.tags.contains))

    def append(
      eventFilter: EventFilter,
      expectedIndex: Long,
      evts: List[PendingEvent[TestEvent]]*,
    ): IO[List[EventEnvelope[TestEvent]]] =
      ref.modify { current =>
        val relevant = current.filter(matches(_, eventFilter))
        val actualIdx = relevant.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
        if actualIdx != expectedIndex then (current, Left(IndexConflictException(expectedIndex, actualIdx)))
        else
          val last = current.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
          val newEvts = evts.flatten.zipWithIndex.map { case (pending, i) =>
            EventEnvelope(
              EventMetadata(
                last + i.toLong + 1L,
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
          (current ++ newEvts, Right(newEvts.toList))
      }.flatMap(_.fold(IO.raiseError, IO.pure))

    def appendUnchecked(evts: List[PendingEvent[TestEvent]]*): IO[List[EventEnvelope[TestEvent]]] =
      IO.pure(List.empty) // not needed for these tests

    def currentRevision(eventFilter: EventFilter): IO[Long] =
      readFrom(0L, eventFilter, None).compile.toList
        .map(_.lastOption.map(_.metadata.globalPosition).getOrElse(0L))

    def readFrom(
      fromPosition: Long,
      eventFilter: EventFilter,
      maxEvents: Option[Int] = None,
    ): Stream[IO, EventEnvelope[TestEvent]] =
      Stream
        .eval(ref.get)
        .flatMap(Stream.emits)
        .filter(env => matches(env, eventFilter) && env.metadata.globalPosition > fromPosition)
        .take(maxEvents.map(_.toLong).getOrElse(Long.MaxValue))

  final class ConflictingStore extends EventStore[IO, TestEvent]:

    def append(
      eventFilter: EventFilter,
      expectedIndex: Long,
      evts: List[PendingEvent[TestEvent]]*,
    ): IO[List[EventEnvelope[TestEvent]]] = IO.raiseError(IndexConflictException(0L, 1L))

    def appendUnchecked(evts: List[PendingEvent[TestEvent]]*): IO[List[EventEnvelope[TestEvent]]] =
      IO.pure(List.empty) // not needed for these tests

    def currentRevision(eventFilter: EventFilter): IO[Long] =
      readFrom(0L, eventFilter, None).compile.toList
        .map(_.lastOption.map(_.metadata.globalPosition).getOrElse(0L))

    def readFrom(
      fromPosition: Long,
      eventFilter: EventFilter,
      maxEvents: Option[Int] = None,
    ): Stream[IO, EventEnvelope[TestEvent]] =
      Stream.empty

  test("append delegates to inner store") {
    for
      ref          <- Ref.of[IO, Vector[EventEnvelope[TestEvent]]](Vector.empty)
      inner         = FakeStore(ref)
      instrumented <- InstrumentedEventStore.make[IO, TestEvent](inner)
      _            <- instrumented.append(
             EventFilter(),
             0L,
             List(
               PendingEvent(TestEvent.Created("x"), Set.empty, EventTypeName.of[TestEvent.Created], isExternal = false),
             ),
           )
      stored <- ref.get
    yield expect(stored.size == 1)
  }

  test("append propagates IndexConflictException") {
    for
      instrumented <- InstrumentedEventStore.make[IO, TestEvent](new ConflictingStore)
      result       <-
        instrumented
          .append(
            EventFilter(),
            0L,
            List(
              PendingEvent(TestEvent.Created("x"), Set.empty, EventTypeName.of[TestEvent.Created], isExternal = false),
            ),
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
             List(
               PendingEvent(TestEvent.Created("a"), Set.empty, EventTypeName.of[TestEvent.Created], isExternal = false),
             ),
           )
      events <- instrumented.readFrom(0L, EventFilter()).compile.toList
    yield expect(events.size == 1 && events.head.payload == TestEvent.Created("a"))
  }

  test("readFrom with fromPosition filters earlier events") {
    for
      ref          <- Ref.of[IO, Vector[EventEnvelope[TestEvent]]](Vector.empty)
      inner         = FakeStore(ref)
      instrumented <- InstrumentedEventStore.make[IO, TestEvent](inner)
      _            <-
        inner.append(
          EventFilter(),
          0L,
          List(PendingEvent(TestEvent.Created("a"), Set.empty, EventTypeName.of[TestEvent.Created], isExternal = false)),
        )
      _ <-
        inner.append(
          EventFilter(),
          1L,
          List(PendingEvent(TestEvent.Created("b"), Set.empty, EventTypeName.of[TestEvent.Created], isExternal = false)),
        )
      events <- instrumented.readFrom(1L, EventFilter()).compile.toList
    yield expect(events.size == 1 && events.head.payload == TestEvent.Created("b"))
  }
