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

import java.time.Instant
import java.time.temporal.ChronoUnit

import scala.concurrent.duration.*

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*
import io.circe.{Decoder, Encoder}
import io.circe.syntax.*
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import weaver.IOSuite

import persistent4s.circe.CirceEventCodec
import persistent4s.{Event, OutgoingMessage, SagaId, SagaRecord, SagaStatus}

/** Integration tests for [[PostgresSagaRepository]] against a real PostgreSQL instance via testcontainers.
  *
  * The deadline is now the caller's to compute — the repository stores the instant it is handed, so expiry here is
  * produced by passing one that has already gone by rather than by asking for a short timeout and then sleeping. That
  * makes these tests both faster and exact: the instant asserted on is the instant written.
  *
  * What that trades away is the guarantee the old shape had. `claimExpired` still compares against `clock_timestamp()`,
  * so a deadline set from this JVM is now judged by the database's clock. Skew between the two moves only *when* the
  * sweeper fires, never what any party was told, and under testcontainers both clocks are the host's anyway.
  */
object PostgresSagaRepositorySuite extends IOSuite:

  override def maxParallelism: Int = 1

  given Logger[IO] = NoOpLogger[IO]

  final case class Fixture(
    repository: PostgresSagaRepository[IO],
    pool: Resource[IO, Session[IO]],
  )

  type Res = Fixture

  final case class TestEvent(value: String) extends Event derives Encoder, Decoder

  /** Short enough that a test can watch a claim expire, long enough that a claim survives a handler doing real work. */
  private val ClaimTtl: FiniteDuration = 2.seconds

  private val eventCodec = CirceEventCodec.make[TestEvent](
    encodeEvent = _.asJson,
    decodeEvent = (_, json) => json.as[TestEvent].left.map(error => error: Throwable),
  )

  override def sharedResource: Resource[IO, Fixture] =
    PostgresContainer.resource().flatMap { config =>
      for
        // Built for its DDL: enableSaga creates saga_instances and, because start() enqueues through it, message_outbox.
        _    <- PostgresModule.makeWithConfig[IO, TestEvent](config, eventCodec, enableSaga = true)
        pool <- Session
                  .Builder[IO]
                  .withHost(config.host)
                  .withPort(config.port)
                  .withUserAndPassword(config.user, config.password)
                  .withDatabase(config.database)
                  .pooled(4)
      yield Fixture(PostgresSagaRepository[IO](pool, claimTtl = ClaimTtl), pool)
    }

  // ----- schema -----

  test("enabling the saga still creates every other table the module owns") { fixture =>
    // This fixture is built with `enableSaga = true`, and the DDL for all of these runs as one chain. Each table is
    // otherwise only ever exercised by the suite that owns it, so a statement dropped from the middle of that chain —
    // the sort of thing a merge does — would be caught by nobody: the saga tests would still pass, and the suites that
    // would notice build their own modules without the saga.
    val expected = List(
      "events", "event_tags", "projection_checkpoints", "leader_leases", "command_snapshots", "message_outbox",
      "saga_instances",
    )
    fixture.pool.use { session =>
      expected.traverse(name => session.unique(tableExistsQuery)(name).map(name -> _))
    }
      .map(found => expect(found.filterNot(_._2).map(_._1) == Nil))
  }

  private val tableExistsQuery: Query[String, Boolean] =
    sql"""
      SELECT EXISTS (
        SELECT 1 FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name = $text
      )
    """.query(bool)

  // ----- helpers -----

  private def truncate(pool: Resource[IO, Session[IO]]): IO[Unit] =
    pool.use(_.execute(sql"TRUNCATE saga_instances, message_outbox RESTART IDENTITY CASCADE".command)).void

  private def messagePayloads(pool: Resource[IO, Session[IO]]): IO[List[String]] =
    pool.use(_.execute(sql"SELECT payload FROM message_outbox ORDER BY id".query(text)))

  private def request(payload: String): OutgoingMessage =
    OutgoingMessage("catalog.commands", Some("c-1"), payload)

  private def sagaId(sagaName: String, key: String): java.util.UUID = SagaId.instance(sagaName, key)

  /** Start an instance of `sagaName` keyed by `key`; its id is always `sagaId(sagaName, key)`.
    *
    * Takes the deadline as a duration and resolves it here, which is what the runner does too. A negative one is a
    * deadline already in the past — the whole of [[startExpired]].
    */
  private def start(
    fixture: Fixture,
    sagaName: String,
    key: String,
    expiresIn: Option[FiniteDuration],
    data: String = "state-0",
    messages: List[OutgoingMessage] = Nil,
  ): IO[Boolean] =
    fixture.repository.start(
      sagaId(sagaName, key),
      sagaName,
      key,
      data,
      expiresIn.map(d => Instant.now().plusMillis(d.toMillis)),
      messages,
    )

  /** Start an instance whose deadline has already passed. No sleep: the deadline is a value now, so it can simply be
    * written in the past.
    */
  private def startExpired(fixture: Fixture, sagaName: String, key: String): IO[java.util.UUID] =
    start(fixture, sagaName, key, expiresIn = Some(-1.second)).as(sagaId(sagaName, key))

  private def claimIds(fixture: Fixture, sagaName: String, limit: Int = 10): IO[List[java.util.UUID]] =
    Ref.of[IO, List[SagaRecord]](Nil).flatMap { claimed =>
      fixture.repository.claimExpired(sagaName, limit)(records => claimed.set(records)) *>
        claimed.get.map(_.map(_.id))
    }

  // ----- start -----

  test("start inserts a pending instance at step 0 and enqueues its request") { fixture =>
    for
      _        <- truncate(fixture.pool)
      ok       <- start(fixture, "reserve", "k1", Some(1.hour), messages = List(request("req-1")))
      record   <- fixture.repository.find(sagaId("reserve", "k1"))
      payloads <- messagePayloads(fixture.pool)
    yield expect.all(
      ok,
      record.exists(_.status == SagaStatus.Pending),
      record.exists(_.step == 0),
      record.exists(_.key == "k1"),
      record.exists(_.sagaName == "reserve"),
      record.exists(_.data == "state-0"),
      record.exists(_.deadline.isDefined),
      payloads == List("req-1"),
    )
  }

  test("start with no deadline leaves the column unset") { fixture =>
    for
      _      <- truncate(fixture.pool)
      ok     <- start(fixture, "reserve", "k1", expiresIn = None)
      record <- fixture.repository.find(sagaId("reserve", "k1"))
    yield expect.all(ok, record.exists(_.deadline.isEmpty))
  }

  test("start stores the exact instant it is given") { fixture =>
    // The point of taking an instant rather than a duration: this same value is stamped on the requests enqueued in
    // this very transaction, so anything the repository did to it on the way in would put the partner and the sweeper
    // back on separate opinions. Truncated to microseconds because that is all a timestamptz column holds.
    val deadline = Instant.now().plusSeconds(3600).truncatedTo(ChronoUnit.MICROS)
    for
      _      <- truncate(fixture.pool)
      _      <- fixture.repository.start(sagaId("reserve", "k1"), "reserve", "k1", "state-0", Some(deadline), Nil)
      record <- fixture.repository.find(sagaId("reserve", "k1"))
    yield expect(record.flatMap(_.deadline) == Some(deadline))
  }

  test("advance stores the exact instant it is given, and clears it when given none") { fixture =>
    val moved = Instant.now().plusSeconds(7200).truncatedTo(ChronoUnit.MICROS)
    val id = sagaId("reserve", "k1")
    for
      _      <- truncate(fixture.pool)
      _      <- start(fixture, "reserve", "k1", Some(1.hour))
      _      <- fixture.repository.advance(id, 0, SagaStatus.Pending, 1, "state-1", Some(moved))
      pushed <- fixture.repository.find(id)
      _      <- fixture.repository.advance(id, 1, SagaStatus.Pending, 2, "state-2", None)
      unset  <- fixture.repository.find(id)
    yield expect.all(
      pushed.flatMap(_.deadline) == Some(moved),
      // `None` means no deadline, not "leave it as it was" — the runner resolves SagaDeadline.Keep to the record's own
      // value before it gets here, so by this point the argument is always the deadline the row should end up with.
      unset.exists(_.deadline.isEmpty),
    )
  }

  test("start on an existing instance returns false and does not enqueue the request again") { fixture =>
    for
      _        <- truncate(fixture.pool)
      ok1      <- start(fixture, "reserve", "k1", Some(1.hour), messages = List(request("req-1")))
      ok2      <- start(fixture, "reserve", "k1", Some(1.hour), messages = List(request("req-1")))
      payloads <- messagePayloads(fixture.pool)
    yield expect.all(ok1, !ok2, payloads == List("req-1"))
  }

  test("start distinguishes instances by key within the same saga") { fixture =>
    for
      _     <- truncate(fixture.pool)
      _     <- start(fixture, "reserve", "k1", Some(1.hour))
      ok    <- start(fixture, "reserve", "k2", Some(1.hour))
      count <- fixture.pool.use(_.unique(sql"SELECT count(*) FROM saga_instances".query(int8)))
    yield expect.all(ok, sagaId("reserve", "k1") != sagaId("reserve", "k2"), count == 2L)
  }

  // ----- advance -----

  test("advance from pending at the expected step writes status, step and data") { fixture =>
    for
      _      <- truncate(fixture.pool)
      id      = sagaId("reserve", "k1")
      _      <- start(fixture, "reserve", "k1", Some(1.hour))
      moved  <- fixture.repository.advance(id, 0, SagaStatus.Completed, 1, "state-1", None)
      record <- fixture.repository.find(id)
    yield expect.all(
      moved,
      record.exists(_.status == SagaStatus.Completed),
      record.exists(_.step == 1),
      record.exists(_.data == "state-1"),
      record.exists(_.deadline.isEmpty),
    )
  }

  test("advance with a stale expected step returns false and changes nothing") { fixture =>
    for
      _      <- truncate(fixture.pool)
      id      = sagaId("reserve", "k1")
      _      <- start(fixture, "reserve", "k1", Some(1.hour))
      moved  <- fixture.repository.advance(id, 7, SagaStatus.Completed, 8, "state-8", None)
      record <- fixture.repository.find(id)
    yield expect.all(
      !moved,
      record.exists(_.status == SagaStatus.Pending),
      record.exists(_.step == 0),
      record.exists(_.data == "state-0"),
    )
  }

  test("advance on a terminal instance returns false even when the step still matches") { fixture =>
    for
      _ <- truncate(fixture.pool)
      id = sagaId("reserve", "k1")
      _ <- start(fixture, "reserve", "k1", Some(1.hour))
      // Complete without bumping the step, exactly as a terminal outcome does.
      first  <- fixture.repository.advance(id, 0, SagaStatus.Completed, 0, "done", None)
      second <- fixture.repository.advance(id, 0, SagaStatus.Compensated, 0, "compensated", None)
      record <- fixture.repository.find(id)
    yield expect.all(
      first,
      !second,
      record.exists(_.status == SagaStatus.Completed),
      record.exists(_.data == "done"),
    )
  }

  test("advance on an unknown instance returns false") { fixture =>
    for
      _     <- truncate(fixture.pool)
      moved <- fixture.repository.advance(SagaId.instance("reserve", "ghost"), 0, SagaStatus.Completed, 0, "x", None)
    yield expect(!moved)
  }

  // ----- claimExpired -----

  test("claimExpired hands over only instances of the named saga whose deadline has passed") { fixture =>
    for
      _       <- truncate(fixture.pool)
      expired <- startExpired(fixture, "alpha", "k1")
      _       <- start(fixture, "alpha", "k2", Some(1.hour))
      _       <- start(fixture, "alpha", "k3", expiresIn = None)
      _       <- startExpired(fixture, "beta", "k4")
      claimed <- claimIds(fixture, "alpha")
    yield expect(claimed == List(expired))
  }

  test("claimExpired ignores instances that are no longer pending") { fixture =>
    for
      _       <- truncate(fixture.pool)
      id      <- startExpired(fixture, "alpha", "k1")
      _       <- fixture.repository.advance(id, 0, SagaStatus.Compensated, 0, "compensated", None)
      claimed <- claimIds(fixture, "alpha")
    yield expect(claimed.isEmpty)
  }

  test("claimExpired returns the number of instances handed over") { fixture =>
    for
      _     <- truncate(fixture.pool)
      _     <- startExpired(fixture, "alpha", "k1")
      _     <- startExpired(fixture, "alpha", "k2")
      count <- fixture.repository.claimExpired("alpha", 10)(_ => IO.unit)
    yield expect(count == 2)
  }

  test("claimExpired respects its limit") { fixture =>
    for
      _     <- truncate(fixture.pool)
      _     <- startExpired(fixture, "alpha", "k1")
      _     <- startExpired(fixture, "alpha", "k2")
      count <- fixture.repository.claimExpired("alpha", 1)(_ => IO.unit)
    yield expect(count == 1)
  }

  test("a claim hides the instance from an immediate second claim, and releases it once the ttl passes") { fixture =>
    for
      _      <- truncate(fixture.pool)
      id     <- startExpired(fixture, "alpha", "k1")
      first  <- claimIds(fixture, "alpha")
      second <- claimIds(fixture, "alpha")
      _      <- IO.sleep(ClaimTtl + 500.millis)
      third  <- claimIds(fixture, "alpha")
    yield expect.all(first == List(id), second.isEmpty, third == List(id))
  }

  test("a handler can advance the instance it was handed without deadlocking against the claim") { fixture =>
    for
      _  <- truncate(fixture.pool)
      id <- startExpired(fixture, "alpha", "k1")
      // The claim must have committed before `handle` runs: this advance touches the very row the claim selected FOR
      // UPDATE, so holding that lock across the handler would wedge until the timeout below fired.
      moved <- Ref.of[IO, Boolean](false).flatMap { advanced =>
                 fixture.repository
                   .claimExpired("alpha", 10) { records =>
                     records.traverse_ { record =>
                       fixture.repository
                         .advance(record.id, record.step, SagaStatus.Compensated, record.step, "compensated", None)
                         .flatMap(advanced.set)
                     }
                   }
                   .timeout(15.seconds) *> advanced.get
               }
      record <- fixture.repository.find(id)
    yield expect.all(moved, record.exists(_.status == SagaStatus.Compensated))
  }

  test("two concurrent claims never hand the same instance to both") { fixture =>
    for
      _         <- truncate(fixture.pool)
      id        <- startExpired(fixture, "alpha", "k1")
      collected <- Ref.of[IO, List[java.util.UUID]](Nil).flatMap { seen =>
                     val claim = fixture.repository
                       .claimExpired("alpha", 10)(records => seen.update(_ ++ records.map(_.id)))
                     IO.both(claim, claim) *> seen.get
                   }
    yield expect(collected == List(id))
  }
