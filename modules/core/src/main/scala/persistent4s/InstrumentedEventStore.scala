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

import cats.effect.Async
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.{Counter, Histogram, Meter}
import org.typelevel.otel4s.trace.Tracer

/** Decorates any [[EventStore]] with otel4s spans and metrics.
  *
  * Emits:
  *   - span `persistent4s.eventstore.append` per append call
  *   - span `persistent4s.eventstore.read_from` per readFrom stream (open for the stream's lifetime)
  *   - counter `persistent4s.events.appended`
  *   - counter `persistent4s.events.read`
  *   - histogram `persistent4s.append.duration` (ms)
  *   - counter `persistent4s.conflicts` on [[IndexConflictException]]
  */
final class InstrumentedEventStore[F[_]: Async: Tracer, A <: Event] private (
  inner: EventStore[F, A],
  eventsAppended: Counter[F, Long],
  eventsRead: Counter[F, Long],
  appendDuration: Histogram[F, Double],
  conflicts: Counter[F, Long],
) extends EventStore[F, A]:

  override def append(
    eventFilter: EventFilter,
    expectedIndex: Long,
    events: List[(Set[Tag], EventTypeName, A)]*,
  ): F[Unit] =
    val eventCount  = events.flatten.size.toLong
    val filterAttrs = filterAttributes(eventFilter)
    Tracer[F]
      .spanBuilder("persistent4s.eventstore.append")
      .addAttributes(filterAttrs*)
      .addAttribute(Attribute("event.count", eventCount))
      .build
      .surround(
        for
          start  <- Async[F].monotonic
          result <- inner.append(eventFilter, expectedIndex, events*).attempt
          end    <- Async[F].monotonic
          _      <- appendDuration.record((end - start).toNanos.toDouble / 1e6, filterAttrs*)
          _      <- result match
                      case Right(_) =>
                        eventsAppended.add(eventCount, filterAttrs*)
                      case Left(e: IndexConflictException) =>
                        conflicts.add(1L, filterAttrs*) *> Async[F].raiseError(e)
                      case Left(e) =>
                        Async[F].raiseError(e)
        yield ()
      )

  override def readFrom(
    fromPosition: Long,
    eventFilter: EventFilter,
  ): Stream[F, EventEnvelope[A]] =
    val filterAttrs = filterAttributes(eventFilter)
    Stream
      .resource(
        Tracer[F]
          .spanBuilder("persistent4s.eventstore.read_from")
          .addAttribute(Attribute("from_position", fromPosition))
          .addAttributes(filterAttrs*)
          .build
          .resource,
      )
      .flatMap(_ => inner.readFrom(fromPosition, eventFilter).evalTap(_ => eventsRead.add(1L, filterAttrs*)))

  private def filterAttributes(f: EventFilter): List[Attribute[String]] =
    List(
      Attribute("filter.tags", f.tags.map(_.value).mkString(",")),
      Attribute("filter.event_types", f.eventTypes.map(_.value).mkString(",")),
    )

object InstrumentedEventStore:

  def make[F[_]: Async: Tracer: Meter, A <: Event](
    inner: EventStore[F, A],
  ): F[InstrumentedEventStore[F, A]] =
    for
      eventsAppended <- Meter[F]
                          .counter[Long]("persistent4s.events.appended")
                          .withDescription("Number of events written to the event store")
                          .withUnit("{events}")
                          .create
      eventsRead     <- Meter[F]
                          .counter[Long]("persistent4s.events.read")
                          .withDescription("Number of events emitted by readFrom streams")
                          .withUnit("{events}")
                          .create
      appendDuration <- Meter[F]
                          .histogram[Double]("persistent4s.append.duration")
                          .withDescription("End-to-end append latency")
                          .withUnit("ms")
                          .create
      conflicts      <- Meter[F]
                          .counter[Long]("persistent4s.conflicts")
                          .withDescription("Number of optimistic concurrency conflicts")
                          .withUnit("{conflicts}")
                          .create
    yield new InstrumentedEventStore(inner, eventsAppended, eventsRead, appendDuration, conflicts)

  /** Like [[make]], but preserves [[EventNotification]] from the inner store. */
  def makeWithNotification[F[_]: Async: Tracer: Meter, A <: Event](
    inner: EventStore[F, A] & EventNotification[F],
  ): F[EventStore[F, A] & EventNotification[F]] =
    make(inner).map { instrumented =>
      new EventStore[F, A] with EventNotification[F]:
        export instrumented.{append, readFrom}
        def notification: fs2.Stream[F, Unit] = inner.notification
    }
