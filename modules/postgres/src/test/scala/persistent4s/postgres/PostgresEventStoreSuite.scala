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
import io.circe.{Decoder, Encoder}
import io.circe.syntax.*
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger

given Tracer[IO] = Tracer.Implicits.noop

given Meter[IO] = Meter.Implicits.noop

given Logger[IO] = Slf4jLogger.getLogger[IO]

import org.testcontainers.containers.PostgreSQLContainer
import persistent4s.circe.CirceEventCodec
import persistent4s.{EventFilter, EventStoreNotification, IndexConflictException, Tag}
import weaver.IOSuite
import persistent4s.EventTypeName
import persistent4s.Event

object PostgresEventStoreSuite extends IOSuite:

  override def maxParallelism: Int = 1

  type Res = PostgresEventStore[IO, TestEvent]

  override def sharedResource: Resource[IO, Res] =
    postgresContainerResource.flatMap { container =>
      PostgresModule.makeWithConfig[IO, TestEvent](postgresConfig(container), eventCodec).map(_.eventStore)
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
    store: PostgresEventStore[IO, TestEvent],
    expectedIndex: Long,
    tags: Set[Tag],
    value: String,
  ): IO[Unit] =
    store.append(
      EventFilter(Set.empty, tags),
      expectedIndex,
      List((None, tags, EventTypeName.of[TestEvent], TestEvent(value))),
    )

  private def isConflict(result: Either[Throwable, Unit]): Boolean =
    result match
      case Left(_: IndexConflictException) => true
      case _                               => false

  private def freshId(prefix: String): IO[String] =
    IO(s"$prefix-${UUID.randomUUID().toString}")

  test("append stores event with correct payload, tags, and event type") { store =>
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

  test("readFrom skips events at or before the given position") { store =>
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

  test("readFrom with empty event type filter matches all event types") { store =>
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

  test("concurrent appends with the same tag allow only one success") { store =>
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

  test("concurrent appends with overlapping tag sets allow only one success") { store =>
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

  test("many concurrent appends with distinct tags all succeed") { store =>
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

  test("appends with fresh tags can start from expected index zero after prior events") { store =>
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

  test("append with empty tags uses all tags for conflict detection") { store =>
    for
      firstTag  <- freshId("first").map(id => Tag("student", id))
      secondTag <- freshId("second").map(id => Tag("student", id))
      _         <- appendOne(store, 0L, Set(firstTag), "before")
      result    <- store
                  .append(
                    EventFilter(Set.empty, Set.empty),
                    0L,
                    List((None, Set(secondTag), EventTypeName.of[TestEvent], TestEvent("after"))),
                  )
                  .attempt
    yield expect(isConflict(result))
  }

  test("notify completes without error") { eventStore =>
    eventStore.notify(EventStoreNotification.PauseProjection("test-proj")).as(success)
  }

  test("appending with a provided UUID sets isExternal to true") { store =>
    for
      id  <- freshId("external")
      tag  = Tag("external", id)
      uuid = UUID.randomUUID()
      _   <- store.append(
             EventFilter(Set.empty, Set(tag)),
             0L,
             List((Some(uuid), Set(tag), EventTypeName.of[TestEvent], TestEvent("external-event"))),
           )
      events <- store.readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(tag))).compile.toList
    yield expect.all(
      events.length == 1,
      events.head.metadata.isExternal == true,
    )
  }

  test("appending without a provided UUID sets isExternal to false") { store =>
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

  test("appending a duplicate UUID is a no-op and returns the original global position") { store =>
    for
      id  <- freshId("dedup")
      tag  = Tag("dedup", id)
      uuid = UUID.randomUUID()
      _   <- store.append(
             EventFilter(Set.empty, Set(tag)),
             0L,
             List((Some(uuid), Set(tag), EventTypeName.of[TestEvent], TestEvent("first"))),
           )
      pos1 <- store
                .readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(tag)))
                .compile
                .toList
                .map(_.head.metadata.globalPosition)
      _ <- store.append(
             EventFilter(Set.empty, Set(tag)),
             pos1,
             List((Some(uuid), Set(tag), EventTypeName.of[TestEvent], TestEvent("duplicate"))),
           )
      events <- store.readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent]), Set(tag))).compile.toList
    yield expect.all(
      events.length == 1,
      events.head.metadata.globalPosition == pos1,
      events.head.payload == TestEvent("first"),
    )
  }
