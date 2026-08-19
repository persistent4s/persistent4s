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

import scala.concurrent.duration.*

import cats.effect.{IO, Resource}
import cats.syntax.all.*

import io.circe.syntax.*
import io.circe.{Decoder, Encoder}

import org.testcontainers.containers.PostgreSQLContainer

import persistent4s.circe.CirceEventCodec
import persistent4s.{Event, EventEnvelope, EventFilter, EventStore, EventTypeName, PendingEvent, Tag}

import skunk.*
import skunk.implicits.*

import weaver.IOSuite

/** Covers the gap-tolerant readFrom mechanism (PostgresEventStore's safe-boundary computation): a fresh sequence_number
  * gap withholds anything past it, and a stale one (older than gapTimeout) gets bridged transparently. Kept as its own
  * suite so it can use a short gapTimeout without affecting PostgresEventStoreSuite's unrelated CRUD/OCC coverage.
  */
object PostgresGapToleranceSuite extends IOSuite:

  given org.typelevel.log4cats.Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  override def maxParallelism: Int = 1

  // Deliberately generous even beyond what the local benchmark called for (observed worst case ~180ms): CI runners
  // are typically slower and noisier than a dev machine, and flaky tests cost more than a slightly slower suite.
  private val GapTimeout: FiniteDuration = 3.seconds

  private val AppendTimeout: FiniteDuration = 1.second

  type Res = PostgresModule.Components[IO, TestEvent]

  override def sharedResource: Resource[IO, Res] =
    postgresContainerResource.flatMap { container =>
      PostgresModule.makeWithConfig[IO, TestEvent](
        postgresConfig(container),
        eventCodec,
        gapTimeout = GapTimeout,
        appendTimeout = AppendTimeout,
      )
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

  private def truncate(sessions: Resource[IO, Session[IO]]): IO[Unit] =
    sessions.use(_.execute(sql"TRUNCATE events RESTART IDENTITY CASCADE".command)).void

  private def appendOne(
    store: EventStore[IO, TestEvent],
    tags: Set[Tag],
    value: String,
    id: Option[UUID] = None,
  ): IO[EventEnvelope[TestEvent]] =
    store
      .appendUnchecked(List(PendingEvent(TestEvent(value), tags, EventTypeName.of[TestEvent], true, id, Map.empty)))
      .map(_.head)

  // Re-appending an id that's already stored burns the sequence_number Postgres allocates for the discarded
  // candidate row (ON CONFLICT DO UPDATE), leaving a permanent hole one position past `existing`.
  private def burnOneSequenceNumber(store: EventStore[IO, TestEvent], tags: Set[Tag], existingId: UUID): IO[Unit] =
    appendOne(store, tags, "discarded-duplicate", Some(existingId)).void

  test("readFrom withholds events past a fresh gap, then bridges it once the gap goes stale") {
    case PostgresModule.Components(store, _, _, _, _, _, _, sessions, _) =>
      val tag = Tag("gap-test", UUID.randomUUID().toString)
      val firstId = UUID.randomUUID()
      for
        _         <- truncate(sessions)
        first     <- appendOne(store, Set(tag), "first", Some(firstId))
        _         <- burnOneSequenceNumber(store, Set(tag), firstId)
        second    <- appendOne(store, Set(tag), "second")
        immediate <- store.readFrom(0L, EventFilter(Set.empty, Set(tag))).compile.toList
        _         <- IO.sleep(GapTimeout + 500.millis)
        bridged   <- store.readFrom(0L, EventFilter(Set.empty, Set(tag))).compile.toList
      yield expect.all(
        second.metadata.globalPosition == first.metadata.globalPosition + 2, // +1 was burned by the duplicate
        immediate.map(_.payload) == List(TestEvent("first")),
        bridged.map(_.payload) == List(TestEvent("first"), TestEvent("second")),
      )
  }

  test("readFrom(-1, ...) does not misdetect a gap before the very first event ever stored") {
    case PostgresModule.Components(store, _, _, _, _, _, _, sessions, _) =>
      // -1 is DefaultProjector's real initial checkpoint sentinel (ProjectionCheckpointState's default), not 0 — the
      // very first safe-boundary check a fresh projection ever does uses it.
      val tag = Tag("gap-test", UUID.randomUUID().toString)
      for
        _         <- truncate(sessions)
        first     <- appendOne(store, Set(tag), "first")
        immediate <- store.readFrom(-1L, EventFilter(Set.empty, Set(tag))).compile.toList
      yield expect(immediate.map(_.payload) == List(TestEvent("first")))
  }

  test(
    "an append exceeding appendTimeout is rolled back cleanly, raising AppendTimeoutException, without " +
      "leaving the store stuck",
  ) { case PostgresModule.Components(store, _, _, _, _, _, _, sessions, _) =>
    val tag = Tag("timeout-test", UUID.randomUUID().toString)
    // A timeout this tight can't complete even the first round-trip, so it deterministically triggers
    // AppendTimeoutException regardless of real DB/network timing — no pg_sleep or held-open transaction needed.
    val tightStore =
      PostgresEventStore[IO, TestEvent](sessions, eventCodec, gapTimeout = GapTimeout, appendTimeout = 1.nanosecond)
    for
      _        <- truncate(sessions)
      timedOut <-
        tightStore
          .append(
            EventFilter(Set.empty, Set(tag)),
            0L,
            List(PendingEvent(TestEvent("late"), Set(tag), EventTypeName.of[TestEvent], true, None, Map.empty)),
          )
          .attempt
      afterTimeout <- store.readFrom(0L, EventFilter(Set.empty, Set(tag))).compile.toList
      // Same tag, same expectedIndex=0: only succeeds if the rollback truly reverted the conflict check's view of
      // this tag's revision, and only completes promptly if the timed-out attempt's advisory lock was released
      // rather than leaked (a lock leak would make this hang until the store's own appendTimeout, or worse).
      onTime <-
        store
          .append(
            EventFilter(Set.empty, Set(tag)),
            0L,
            List(PendingEvent(TestEvent("on-time"), Set(tag), EventTypeName.of[TestEvent], true, None, Map.empty)),
          )
          .timeout(5.seconds)
    yield expect.all(
      timedOut.left.toOption.exists(_.isInstanceOf[AppendTimeoutException]),
      afterTimeout.isEmpty,
      onTime.map(_.payload) == List(TestEvent("on-time")),
    )
  }
