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

import cats.effect.{Deferred, IO, Ref, Resource}

import io.circe.syntax.*
import io.circe.{Decoder, Encoder, Json}

import org.typelevel.log4cats.Logger

import org.testcontainers.containers.PostgreSQLContainer

import persistent4s.circe.CirceEventCodec
import persistent4s.{
  DefaultProjector,
  Event,
  EventEnvelope,
  EventFilter,
  EventStoreNotification,
  EventTypeName,
  ParallelProjector,
  PendingEvent,
  Projector,
  StatelessProjection,
}

import skunk.*
import skunk.circe.codec.all.jsonb
import skunk.codec.all.*
import skunk.implicits.*

import weaver.IOSuite

/** Postgres allocates a sequence_number when a row is inserted, not when its transaction commits, so a lower position
  * can become visible after a higher one. A projection that has already moved its checkpoint past the higher position
  * must still receive the late one rather than skip it, and must not stall once the transaction holding the lower
  * position is gone. The write side reads per consistency boundary and must never wait on unrelated transactions.
  */
object PostgresProjectionGapSuite extends IOSuite:

  given Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  override def maxParallelism: Int = 1

  // How long a projector gets to (wrongly) race ahead of the open transaction before the test resolves it.
  private val RaceWindow: FiniteDuration = 1.second

  private val Within: FiniteDuration = 5.seconds

  type Res = PostgresModule.Components[IO, GapEvent]

  override def sharedResource: Resource[IO, Res] =
    postgresContainerResource.flatMap { container =>
      PostgresModule.makeWithConfig[IO, GapEvent](postgresConfig(container), eventCodec)
    }

  sealed trait GapEvent extends Event

  final case class Recorded(value: String) extends GapEvent derives Encoder.AsObject, Decoder

  private type Container = PostgreSQLContainer[Nothing]

  private val eventCodec = CirceEventCodec.derived[GapEvent]

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

  private def append(components: Res, value: String): IO[Long] =
    val event = Recorded(value)
    components.eventStore
      .appendUnchecked(List(PendingEvent(event, Set.empty, eventCodec.eventType(event), true, None, Map.empty)))
      .map(_.head.metadata.globalPosition)

  // ----- a slow append -----

  private val insertRecorded: Query[String *: Json *: EmptyTuple, Long] =
    sql"""
      INSERT INTO events (event_type, payload, is_external)
      VALUES ($text, $jsonb, true)
      RETURNING sequence_number
    """.query(int8)

  private case object RolledBack extends RuntimeException("slow append rolled back on purpose")

  /** An append whose transaction is still open: its row has a sequence_number but is not visible to anyone else. */
  final private case class SlowAppend(position: Long, commit: IO[Unit], rollback: IO[Unit])

  /** Inserts `value` and sends the same NOTIFY a real append sends, then holds the transaction open until the returned
    * handle resolves it. Postgres only delivers a NOTIFY on commit, so a rollback wakes no projector, exactly as when a
    * real append fails after reserving its position.
    */
  private def openSlowAppend(components: Res, value: String): IO[SlowAppend] =
    val event = Recorded(value)
    for
      inserted <- Deferred[IO, Long]
      outcome  <- Deferred[IO, Boolean]
      done     <- Deferred[IO, Either[Throwable, Unit]]
      _        <- components.sessions
             .use { session =>
               session.transaction.use { _ =>
                 for
                   position <- session
                                 .prepare(insertRecorded)
                                 .flatMap(_.unique(eventCodec.eventType(event).value *: event.asJson *: EmptyTuple))
                   _ <- session
                          .channel(PostgresEventStore.NotificationChannel)
                          .notify(PostgresNotification.encode(EventStoreNotification.EventsAppended))
                   _      <- inserted.complete(position)
                   commit <- outcome.get
                   _      <- IO.raiseUnless(commit)(RolledBack)
                 yield ()
               }
             }
             .recover { case RolledBack => () }
             .attempt
             .flatMap(done.complete)
             .start
      position <- inserted.get
    yield SlowAppend(
      position,
      commit = outcome.complete(true) *> done.get.rethrow,
      rollback = outcome.complete(false) *> done.get.rethrow,
    )

  // ----- projector side -----

  private def recording(seen: Ref[IO, List[GapEvent]]) =
    new StatelessProjection[IO, GapEvent]:
      val name: String = s"gap-${UUID.randomUUID()}"
      def filter: Set[EventTypeName] = Set(EventTypeName.of[Recorded])
      def handle(event: EventEnvelope[GapEvent]): IO[Unit] = seen.update(_ :+ event.payload)

  private val ProjectorKinds: List[String] = List("DefaultProjector", "ParallelProjector")

  private def projector(kind: String, components: Res): Projector[IO, GapEvent] =
    kind match
      case "DefaultProjector"  => DefaultProjector(components.eventStore, components.checkpoint)
      case _                   => ParallelProjector(components.eventStore, components.checkpoint)

  private def waitUntil(cond: IO[Boolean]): IO[Unit] =
    cond.flatMap {
      case true  => IO.unit
      case false => IO.sleep(20.millis) *> waitUntil(cond)
    }

  /** Runs `body` while the projector runs in the background, then returns everything it handled. */
  private def whileProjecting(kind: String, components: Res)(
    body: Ref[IO, List[GapEvent]] => IO[Unit],
  ): IO[List[GapEvent]] =
    for
      seen    <- Ref.of[IO, List[GapEvent]](Nil)
      _       <- projector(kind, components).run(recording(seen)).compile.drain.background.use(_ => body(seen))
      handled <- seen.get
    yield handled

  ProjectorKinds.foreach { kind =>

    test(s"$kind does not skip an event whose transaction commits after a higher position became visible") {
      components =>
        for
          _    <- truncate(components.sessions)
          slow <- openSlowAppend(components, "allocated-first")
          fast <- append(components, "committed-first")
          handled <- whileProjecting(kind, components) { seen =>
                       // Give the projector every chance to read past the open transaction before it resolves.
                       waitUntil(seen.get.map(_.nonEmpty)).timeoutTo(RaceWindow, IO.unit) *>
                         slow.commit *>
                         waitUntil(seen.get.map(_.size >= 2)).timeoutTo(Within, IO.unit)
                     }
        yield expect.all(
          slow.position < fast,
          clue(handled) == List(Recorded("allocated-first"), Recorded("committed-first")),
        )
    }

    test(s"$kind moves on promptly when the transaction holding a lower position rolls back") { components =>
      for
        _    <- truncate(components.sessions)
        slow <- openSlowAppend(components, "rolled-back")
        fast <- append(components, "committed")
        handled <- whileProjecting(kind, components) { seen =>
                     // A rollback sends no notification, so nothing but the projector itself can notice the gap is
                     // now permanent.
                     waitUntil(seen.get.map(_.nonEmpty)).timeoutTo(RaceWindow, IO.unit) *>
                       slow.rollback *>
                       waitUntil(seen.get.map(_.nonEmpty)).timeoutTo(Within, IO.unit)
                   }
      yield expect.all(
        slow.position < fast,
        clue(handled) == List(Recorded("committed")),
      )
    }
  }

  // ----- write side -----

  test("readFrom returns committed events at once while an unrelated transaction holds a lower position open") {
    components =>
      for
        _    <- truncate(components.sessions)
        slow <- openSlowAppend(components, "still-open")
        _    <- append(components, "committed")
        read <- components.eventStore
                  .readFrom(0L, EventFilter())
                  .compile
                  .toList
                  .timeout(RaceWindow)
                  .attempt
        _ <- slow.commit
      yield expect(clue(read.map(_.map(_.payload))) == Right(List(Recorded("committed"))))
  }
