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

package persistent4s.postgres

import java.util.UUID

import cats.effect.{IO, Resource}
import cats.syntax.all.*

import io.circe.syntax.*
import io.circe.{Decoder, Encoder}

import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer

import org.testcontainers.containers.PostgreSQLContainer
import persistent4s.circe.CirceEventCodec
import persistent4s.{EventFilter, EventStoreNotification, IndexConflictException, Tag}
import weaver.IOSuite
import persistent4s.EventTypeName
import persistent4s.Event
import persistent4s.EventStore
import persistent4s.PendingEvent
import org.typelevel.log4cats.Logger

given Tracer[IO] = Tracer.Implicits.noop

given Meter[IO] = Meter.Implicits.noop

object PostgresEventStoreSuite extends IOSuite:

  given Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  override def maxParallelism: Int = 1

  type Res = PostgresModule.Components[IO, TestEvent]

  override def sharedResource: Resource[IO, Res] =
    postgresContainerResource.flatMap { container =>
      PostgresModule.makeWithConfig[IO, TestEvent](postgresConfig(container), eventCodec)
    }

  final case class TestEvent(value: String) extends Event derives Encoder.AsObject, Decoder

  private type Container = PostgreSQLContainer[Nothing]

  private val eventCodec = CirceEventCodec.make[TestEvent](
    encodeEvent = _.asJson,
    decodeEvent = (_, json) => json.as[TestEvent].left.map(error => error: Throwable),
  )

  private def postgresConfig(container: Container): PostgresConfig =
    PostgresConfig(
      host = container.getHost, port = container.getMappedPort(5432), user = container.getUsername,
      password = container.getPassword, database = container.getDatabaseName, maxConnections = 16,
    )

  private def postgresContainerResource: Resource[IO, Container] =
    Resource.make {
      IO.blocking {
        val container = new PostgreSQLContainer[Nothing]("postgres:16-alpine")
        container.start()
        container
      }
    } { container =>
      IO.blocking(container.stop()).handleErrorWith(_ => IO.unit)
    }

  private def appendOne(
    store: EventStore[IO, TestEvent],
    expectedIndex: Long,
    tags: Set[Tag],
    value: String,
  ): IO[Unit] =
    store
      .append(
        EventFilter(Set.empty, tags),
        expectedIndex,
        List(PendingEvent(TestEvent(value), tags, EventTypeName.of[TestEvent], false, None, Map.empty)),
      )
      .void

  private def appendUncheckedOne(
    store: EventStore[IO, TestEvent],
    tags: Set[Tag],
    value: String,
    eventId: Option[UUID] = None,
  ): IO[Unit] =
    store
      .appendUnchecked(
        List(PendingEvent(TestEvent(value), tags, EventTypeName.of[TestEvent], true, eventId, Map.empty)),
      )
      .void

  private def isConflict(result: Either[Throwable, Unit]): Boolean =
    result match
      case Left(_: IndexConflictException) => true
      case _                               => false

  private def freshId(prefix: String): IO[String] =
    IO(s"$prefix-${UUID.randomUUID().toString}")

  test("append stores event with correct payload, tags, and event type") {
    case PostgresModule.Components(store, _, _, _, _) =>
      for
        id     <- freshId("student")
        tag     = Tag("student", id)
        _      <- appendOne(store, 0L, Set(tag), "hello")
        events <- store.readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(tag))).compile.toList
      yield expect.all(
        events.length == 1,
        events.head.payload == TestEvent("hello"),
        events.head.metadata.tags == Set(tag),
        events.head.metadata.eventType == EventTypeName.of[TestEvent],
        events.head.metadata.globalPosition >= 1L,
      )
  }

  test("append round-trips non-empty headers through the store") { case PostgresModule.Components(store, _, _, _, _) =>
    val headers = Map("correlationId" -> "abc-123", "userId" -> "42")
    for
      id <- freshId("student")
      tag = Tag("student", id)
      _  <- store
             .append(
               EventFilter(Set.empty, Set(tag)),
               0L,
               List(
                 PendingEvent(
                   TestEvent("hello"),
                   Set(tag),
                   EventTypeName.of[TestEvent],
                   isExternal = false,
                   headers = headers,
                 ),
               ),
             )
             .void
      events <- store.readFrom(0L, EventFilter(Set.empty, Set(tag))).compile.toList
    yield expect.all(
      events.length == 1,
      events.head.metadata.headers == headers,
    )
  }

  test("append defaults to empty headers when none are provided") { case PostgresModule.Components(store, _, _, _, _) =>
    for
      id     <- freshId("student")
      tag     = Tag("student", id)
      _      <- appendOne(store, 0L, Set(tag), "hello")
      events <- store.readFrom(0L, EventFilter(Set.empty, Set(tag))).compile.toList
    yield expect.all(
      events.length == 1,
      events.head.metadata.headers.isEmpty,
    )
  }

  test("a multi-event append keeps each event's own headers, tags and isExternal") {
    case PostgresModule.Components(store, _, _, _, _) =>
      // Multiple events go out as a single multi-row INSERT, so a misaligned bind parameter would
      // silently attach one event's headers to another. Distinct values per row catch that.
      for
        id     <- freshId("batch")
        tagA    = Tag("batch", s"$id-a")
        tagB    = Tag("batch", s"$id-b")
        tagC    = Tag("batch", s"$id-c")
        readAll = EventFilter(Set.empty, Set(tagA, tagB, tagC))
        pending = List(
                    PendingEvent(
                      TestEvent("one"),
                      Set(tagA),
                      EventTypeName.of[TestEvent],
                      isExternal = false,
                      headers = Map("k" -> "1"),
                    ),
                    PendingEvent(
                      TestEvent("two"),
                      Set(tagB),
                      EventTypeName.of[TestEvent],
                      isExternal = true,
                      headers = Map("k" -> "2", "extra" -> "x"),
                    ),
                    PendingEvent(
                      TestEvent("three"),
                      Set(tagC),
                      EventTypeName.of[TestEvent],
                      isExternal = false,
                      headers = Map.empty,
                    ),
                  )
        returned <- store.append(readAll, 0L, pending)
        readBack <- store.readFrom(0L, readAll).compile.toList
      yield expect.all(
        // returned envelopes come back in the order the events were supplied
        returned.map(_.payload) == List(TestEvent("one"), TestEvent("two"), TestEvent("three")),
        returned.map(_.metadata.headers) == List(Map("k" -> "1"), Map("k" -> "2", "extra" -> "x"), Map.empty),
        returned.map(_.metadata.isExternal) == List(false, true, false),
        returned.map(_.metadata.tags) == List(Set(tagA), Set(tagB), Set(tagC)),
        // positions are distinct and ascend with that same order
        returned.map(_.metadata.globalPosition).distinct.length == 3,
        returned.map(_.metadata.globalPosition) == returned.map(_.metadata.globalPosition).sorted,
        // and the envelopes agree with what was actually persisted
        readBack.map(_.payload) == returned.map(_.payload),
        readBack.map(_.metadata.headers) == returned.map(_.metadata.headers),
        readBack.map(_.metadata.isExternal) == returned.map(_.metadata.isExternal),
        readBack.map(_.metadata.globalPosition) == returned.map(_.metadata.globalPosition),
        readBack.map(_.metadata.id) == returned.map(_.metadata.id),
        readBack.map(_.metadata.timestamp) == returned.map(_.metadata.timestamp),
      )
  }

  test("a duplicate UUID within a single append is written once and shares the same metadata") {
    case PostgresModule.Components(store, _, _, _, _) =>
      for
        id     <- freshId("batchdedup")
        tag     = Tag("batchdedup", id)
        uuid    = UUID.randomUUID()
        readAll = EventFilter(Set.empty, Set(tag))
        pending = List(
                    PendingEvent(
                      TestEvent("dup"),
                      Set(tag),
                      EventTypeName.of[TestEvent],
                      isExternal = true,
                      id = Some(uuid),
                      headers = Map("h" -> "v"),
                    ),
                    PendingEvent(
                      TestEvent("dup"),
                      Set(tag),
                      EventTypeName.of[TestEvent],
                      isExternal = true,
                      id = Some(uuid),
                      headers = Map("h" -> "v"),
                    ),
                  )
        returned <- store.append(readAll, 0L, pending)
        readBack <- store.readFrom(0L, readAll).compile.toList
      yield expect.all(
        // one envelope per supplied event, but both describe the single row that was written
        returned.length == 2,
        returned.map(_.metadata.globalPosition).distinct.length == 1,
        returned.map(_.metadata.id).distinct == List(uuid),
        readBack.length == 1,
        readBack.head.metadata.globalPosition == returned.head.metadata.globalPosition,
        readBack.head.metadata.headers == Map("h" -> "v"),
      )
  }

  test("readFrom skips events at or before the given position") { case PostgresModule.Components(store, _, _, _, _) =>
    // In Postgres the expectedIndex is the actual globalPosition of the last matching event,
    // not a per-tag counter — so we read it back after each append rather than hardcoding.
    for
      id        <- freshId("student")
      tag        = Tag("student", id)
      readFilter = EventFilter(Set(EventTypeName.of[TestEvent]), Set(tag))
      _         <- appendOne(store, 0L, Set(tag), "e1")
      pos1      <- store.readFrom(0L, readFilter).compile.toList.map(_.head.metadata.globalPosition)
      _         <- appendOne(store, pos1, Set(tag), "e2")
      pos2      <- store.readFrom(0L, readFilter).compile.toList.map(_.last.metadata.globalPosition)
      _         <- appendOne(store, pos2, Set(tag), "e3")
      all       <- store.readFrom(0L, readFilter).compile.toList
      after     <- store.readFrom(pos1, readFilter).compile.toList
    yield expect.all(
      all.length == 3,
      after.length == 2,
      after.map(_.payload) == List(TestEvent("e2"), TestEvent("e3")),
    )
  }

  test("readFrom with empty event type filter matches all event types") {
    case PostgresModule.Components(store, _, _, _, _) =>
      for
        id     <- freshId("student")
        tag     = Tag("student", id)
        _      <- appendOne(store, 0L, Set(tag), "hello")
        events <- store.readFrom(0L, EventFilter(Set.empty, Set(tag))).compile.toList
      yield expect.all(
        events.length == 1,
        events.head.payload == TestEvent("hello"),
      )
  }

  test("concurrent appends with the same tag allow only one success") {
    case PostgresModule.Components(store, _, _, _, _) =>
      for
        id             <- freshId("student")
        tag             = Tag("student", id)
        first           = appendOne(store, 0L, Set(tag), "first").attempt
        second          = appendOne(store, 0L, Set(tag), "second").attempt
        results        <- (first, second).parTupled
        matchingEvents <- store
                            .readFrom(
                              0L,
                              EventFilter(
                                Set(
                                  EventTypeName
                                    .of[TestEvent],
                                ),
                                Set(tag),
                              ),
                            )
                            .compile
                            .toList
        successCount  = List(results._1, results._2).count(_.isRight)
        conflictCount = List(results._1, results._2).count(isConflict)
      yield expect.all(
        successCount == 1,
        conflictCount == 1,
        matchingEvents.length == 1,
      )
  }

  test("concurrent appends with overlapping tag sets allow only one success") {
    case PostgresModule.Components(store, _, _, _, _) =>
      for
        studentId      <- freshId("student")
        courseId       <- freshId("course")
        studentTag      = Tag("student", studentId)
        courseTag       = Tag("course", courseId)
        first           = appendOne(store, 0L, Set(studentTag, courseTag), "enrollment").attempt
        second          = appendOne(store, 0L, Set(studentTag), "student-update").attempt
        results        <- (first, second).parTupled
        matchingEvents <-
          store.readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(studentTag))).compile.toList
        successCount  = List(results._1, results._2).count(_.isRight)
        conflictCount = List(results._1, results._2).count(isConflict)
      yield expect.all(
        successCount == 1,
        conflictCount == 1,
        matchingEvents.length == 1,
      )
  }

  test("many concurrent appends with distinct tags all succeed") { case PostgresModule.Components(store, _, _, _, _) =>
    val numberOfEvents = 50

    for
      runId   <- freshId("distinct")
      tags     = (1 to numberOfEvents).map(index => Tag("student", s"$runId-$index")).toList
      results <- tags.parTraverse { tag =>
                   appendOne(store, 0L, Set(tag), tag.id).attempt
                 }
      events   <- store.readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), tags.toSet)).compile.toList
      positions = events.map(_.metadata.globalPosition)
    yield expect.all(
      results.forall(_.isRight),
      events.length == numberOfEvents,
      positions.distinct.length == numberOfEvents,
    )
  }

  test("appends with fresh tags can start from expected index zero after prior events") {
    case PostgresModule.Components(store, _, _, _, _) =>
      for
        firstTag  <- freshId("first").map(id => Tag("student", id))
        secondTag <- freshId("second").map(id => Tag("student", id))
        _         <- appendOne(store, 0L, Set(firstTag), "before")
        result    <- appendOne(store, 0L, Set(secondTag), "after").attempt
        events    <- store.readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(secondTag))).compile.toList
      yield expect.all(
        events.length == 1,
        result.isRight,
        events.head.payload == TestEvent("after"),
      )
  }

  test("append rejects an expected index ahead of the scoped revision") {
    case PostgresModule.Components(store, _, _, _, _) =>
      for
        id     <- freshId("future-index")
        tag     = Tag("student", id)
        result <- appendOne(store, 99L, Set(tag), "invalid").attempt
      yield result match
        case Left(error: IndexConflictException) =>
          expect(error.expectedIndex == 99L) and expect(error.actualIndex == 0L)
        case other => failure(s"Expected IndexConflictException, got $other")
  }

  test("append with empty tags uses all tags for conflict detection") {
    case PostgresModule.Components(store, _, _, _, _) =>
      for
        firstTag  <- freshId("first").map(id => Tag("student", id))
        secondTag <- freshId("second").map(id => Tag("student", id))
        _         <- appendOne(store, 0L, Set(firstTag), "before")
        result    <- store
                    .append(
                      EventFilter(Set.empty, Set.empty),
                      0L,
                      List(
                        PendingEvent(
                          TestEvent("after"),
                          Set(secondTag),
                          EventTypeName.of[TestEvent],
                          false,
                          None,
                          Map.empty,
                        ),
                      ),
                    )
                    .void
                    .attempt
      yield expect(isConflict(result))
  }

  test("notify completes without error") { case PostgresModule.Components(_, _, _, _, sendNotification) =>
    sendNotification(EventStoreNotification.PauseProjection("test-proj")).as(success)
  }

  test("appending with a provided UUID sets isExternal to true") { case PostgresModule.Components(store, _, _, _, _) =>
    for
      id  <- freshId("external")
      tag  = Tag("external", id)
      uuid = UUID.randomUUID()
      _   <- store.append(
             EventFilter(Set.empty, Set(tag)),
             0L,
             List(
               PendingEvent(
                 TestEvent("external-event"),
                 Set(tag),
                 EventTypeName.of[TestEvent],
                 true,
                 Some(uuid),
                 Map.empty,
               ),
             ),
           )
      events <- store.readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(tag))).compile.toList
    yield expect.all(
      events.length == 1,
      events.head.metadata.isExternal == true,
    )
  }

  test("appending without a provided UUID sets isExternal to false") {
    case PostgresModule.Components(store, _, _, _, _) =>
      for
        id     <- freshId("internal")
        tag     = Tag("internal", id)
        _      <- appendOne(store, 0L, Set(tag), "internal-event")
        events <- store.readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(tag))).compile.toList
      yield expect.all(
        events.length == 1,
        events.head.metadata.isExternal == false,
      )
  }

  test("appending a duplicate UUID is a no-op and returns the original global position") {
    case PostgresModule.Components(store, _, _, _, _) =>
      for
        id  <- freshId("dedup")
        tag  = Tag("dedup", id)
        uuid = UUID.randomUUID()
        _   <- store.append(
               EventFilter(Set.empty, Set(tag)),
               0L,
               List(
                 PendingEvent(TestEvent("first"), Set(tag), EventTypeName.of[TestEvent], true, Some(uuid), Map.empty),
               ),
             )
        pos1 <- store
                  .readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(tag)))
                  .compile
                  .toList
                  .map(_.head.metadata.globalPosition)
        _ <-
          store.append(
            EventFilter(Set.empty, Set(tag)),
            pos1,
            List(
              PendingEvent(TestEvent("duplicate"), Set(tag), EventTypeName.of[TestEvent], true, Some(uuid), Map.empty),
            ),
          )
        events <- store.readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(tag))).compile.toList
      yield expect.all(
        events.length == 1,
        events.head.metadata.globalPosition == pos1,
        events.head.payload == TestEvent("first"),
      )
  }

  test("appendUnchecked stores event with correct payload, tags, event type, and isExternal") {
    case PostgresModule.Components(store, _, _, _, _) =>
      for
        id     <- freshId("imported")
        tag     = Tag("imported", id)
        _      <- appendUncheckedOne(store, Set(tag), "imported-event")
        events <- store.readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(tag))).compile.toList
      yield expect.all(
        events.length == 1,
        events.head.payload == TestEvent("imported-event"),
        events.head.metadata.tags == Set(tag),
        events.head.metadata.eventType == EventTypeName.of[TestEvent],
        events.head.metadata.isExternal == true,
        events.head.metadata.globalPosition >= 1L,
      )
  }

  test("concurrent appendUnchecked calls with the same tag all succeed (no OCC)") {
    case PostgresModule.Components(store, _, _, _, _) =>
      // Unlike append, appendUnchecked performs no conflict check, so parallel writes with overlapping tags
      // do not race — both events land in the store.
      for
        id      <- freshId("concurrent")
        tag      = Tag("imported", id)
        first    = appendUncheckedOne(store, Set(tag), "first").attempt
        second   = appendUncheckedOne(store, Set(tag), "second").attempt
        results <- (first, second).parTupled
        events  <- store.readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(tag))).compile.toList
      yield expect.all(
        results._1.isRight,
        results._2.isRight,
        events.length == 2,
      )
  }

  test("appendUnchecked with a duplicate UUID is a no-op and returns the original global position") {
    case PostgresModule.Components(store, _, _, _, _) =>
      for
        id   <- freshId("dedup-unchecked")
        tag   = Tag("imported", id)
        uuid  = UUID.randomUUID()
        _    <- appendUncheckedOne(store, Set(tag), "first", Some(uuid))
        pos1 <- store
                  .readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(tag)))
                  .compile
                  .toList
                  .map(_.head.metadata.globalPosition)
        _      <- appendUncheckedOne(store, Set(tag), "duplicate", Some(uuid))
        events <- store.readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(tag))).compile.toList
      yield expect.all(
        events.length == 1,
        events.head.metadata.globalPosition == pos1,
        events.head.payload == TestEvent("first"),
      )
  }

  test("appendUnchecked with no events is a no-op") { case PostgresModule.Components(store, _, _, _, _) =>
    for
      before <- store.readFrom(0L, EventFilter(Set.empty, Set.empty)).compile.toList.map(_.length)
      _      <- store.appendUnchecked()
      after  <- store.readFrom(0L, EventFilter(Set.empty, Set.empty)).compile.toList.map(_.length)
    yield expect(before == after)
  }
