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
import natchez.Trace.Implicits.noop
import org.testcontainers.containers.PostgreSQLContainer
import persistent4s.circe.CirceEventCodec
import persistent4s.{EventFilter, IndexConflictException, Tag}
import weaver.IOSuite

object PostgresEventStoreSuite extends IOSuite:

  override def maxParallelism: Int = 1

  type Res = PostgresEventStore[IO, TestEvent]

  override def sharedResource: Resource[IO, Res] =
    postgresContainerResource.flatMap { container =>
      PostgresModule.makeWithConfig[IO, TestEvent](postgresConfig(container), eventCodec)
    }

  final case class TestEvent(value: String) derives Encoder.AsObject, Decoder

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
    store.append(expectedIndex, List((tags, "TestEvent", TestEvent(value))))

  private def reset(store: PostgresEventStore[IO, TestEvent]): IO[Unit] =
    store.truncateAll

  private def isConflict(result: Either[Throwable, Unit]): Boolean =
    result match
      case Left(_: IndexConflictException) => true
      case _                               => false

  private def freshId(prefix: String): IO[String] =
    IO(s"$prefix-${UUID.randomUUID().toString}")

  test("concurrent appends with the same tag allow only one success") { store =>
    for
      _              <- reset(store)
      id             <- freshId("student")
      tag             = Tag("student", id)
      first           = appendOne(store, 0L, Set(tag), "first").attempt
      second          = appendOne(store, 0L, Set(tag), "second").attempt
      results        <- (first, second).parTupled
      matchingEvents <- store.readFrom(0L, EventFilter(Set("TestEvent"), Set(tag))).compile.toList
      successCount    = List(results._1, results._2).count(_.isRight)
      conflictCount   = List(results._1, results._2).count(isConflict)
    yield expect.all(
      successCount == 1,
      conflictCount == 1,
      matchingEvents.length == 1,
    )
  }

  test("concurrent appends with overlapping tag sets allow only one success") { store =>
    for
      _              <- reset(store)
      studentId      <- freshId("student")
      courseId       <- freshId("course")
      studentTag      = Tag("student", studentId)
      courseTag       = Tag("course", courseId)
      first           = appendOne(store, 0L, Set(studentTag, courseTag), "enrollment").attempt
      second          = appendOne(store, 0L, Set(studentTag), "student-update").attempt
      results        <- (first, second).parTupled
      matchingEvents <- store.readFrom(0L, EventFilter(Set("TestEvent"), Set(studentTag))).compile.toList
      successCount    = List(results._1, results._2).count(_.isRight)
      conflictCount   = List(results._1, results._2).count(isConflict)
    yield expect.all(
      successCount == 1,
      conflictCount == 1,
      matchingEvents.length == 1,
    )
  }

  test("many concurrent appends with distinct tags all succeed") { store =>
    val numberOfEvents = 50

    for
      _       <- reset(store)
      runId   <- freshId("distinct")
      tags     = (1 to numberOfEvents).map(index => Tag("student", s"$runId-$index")).toList
      results <- tags.parTraverse { tag =>
                   appendOne(store, 0L, Set(tag), tag.id).attempt
                 }
      events   <- store.readFrom(0L, EventFilter(Set("TestEvent"), tags.toSet)).compile.toList
      positions = events.map(_.metadata.globalPosition)
    yield expect.all(
      results.forall(_.isRight),
      events.length == numberOfEvents,
      positions.distinct.length == numberOfEvents,
    )
  }

  test("truncateAll removes stored events and resets sequence numbers") { store =>
    for
      _        <- reset(store)
      tag      <- freshId("reset").map(id => Tag("student", id))
      _        <- appendOne(store, 0L, Set(tag), "before-truncate")
      _        <- store.truncateAll
      _        <- appendOne(store, 0L, Set(tag), "after-truncate")
      events   <- store.readFrom(0L, EventFilter(Set("TestEvent"), Set(tag))).compile.toList
      positions = events.map(_.metadata.globalPosition)
    yield expect.all(
      events.length == 1,
      positions == List(1L),
    )
  }
