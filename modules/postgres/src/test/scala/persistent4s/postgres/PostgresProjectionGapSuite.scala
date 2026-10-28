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

import cats.effect.std.Queue
import cats.effect.{Deferred, IO, Ref, Resource}
import cats.syntax.all.*

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
  ProjectionRead,
  Projector,
  ReadCursor,
  StatelessProjection,
  Tag,
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

  type Res = PostgresDomain[IO, GapEvent]

  override def sharedResource: Resource[IO, Res] =
    postgresContainerResource.flatMap { container =>
      PostgresModule.makeWithConfig[IO](postgresConfig(container)).flatMap(_.domain(eventCodec))
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

  private def pending(value: String, tags: Set[Tag] = Set.empty, id: Option[UUID] = None): PendingEvent[GapEvent] =
    val event = Recorded(value)
    PendingEvent(event, tags, eventCodec.eventType(event), true, id, Map.empty)

  private def appendEnvelope(
    components: Res,
    value: String,
    tags: Set[Tag] = Set.empty,
    id: Option[UUID] = None,
  ): IO[EventEnvelope[GapEvent]] =
    components.eventStore.appendUnchecked(List(pending(value, tags, id))).map(_.head)

  private def append(components: Res, value: String): IO[Long] =
    appendEnvelope(components, value).map(_.metadata.globalPosition)

  private val RecordedOnly: EventFilter = EventFilter(Set(EventTypeName.of[Recorded]), Set.empty)

  private def readForProjection(
    components: Res,
    after: ReadCursor = ReadCursor.Start,
    filter: EventFilter = RecordedOnly,
    maxEvents: Int = 100,
  ): IO[ProjectionRead[GapEvent]] =
    components.eventStore.readForProjection(after, filter, maxEvents)

  // ----- a slow append -----

  private val insertRecorded: Query[String *: Json *: EmptyTuple, Long] =
    sql"""
      INSERT INTO events (event_type, payload, is_external)
      VALUES ($text, $jsonb, true)
      RETURNING sequence_number
    """.query(int8)

  private case object RolledBack extends RuntimeException("slow append rolled back on purpose")

  private enum SlowStep:

    case Insert(value: String, inserted: Deferred[IO, Long])

    case Finish(commit: Boolean)

  /** An append whose transaction is still open: its rows have sequence_numbers but are not visible to anyone else.
    * `insert` adds another row to the same transaction, as a multi-statement write would.
    */
  final private case class SlowAppend(
    position: Long,
    insert: String => IO[Long],
    commit: IO[Unit],
    rollback: IO[Unit],
  )

  /** Inserts `value` and sends the same NOTIFY a real append sends, then holds the transaction open until the returned
    * handle resolves it. Postgres only delivers a NOTIFY on commit, so a rollback wakes no projector, exactly as when a
    * real append fails after reserving its position.
    */
  private def openSlowAppend(components: Res, value: String): IO[SlowAppend] =
    for
      steps <- Queue.unbounded[IO, SlowStep]
      done  <- Deferred[IO, Either[Throwable, Unit]]
      _     <- components.module.sessions.use { session =>
             session.transaction.use { _ =>
               def run: IO[Unit] =
                 steps.take.flatMap {
                   case SlowStep.Insert(value, inserted) =>
                     val event = Recorded(value)
                     session
                       .prepare(insertRecorded)
                       .flatMap(_.unique(eventCodec.eventType(event).value *: event.asJson *: EmptyTuple))
                       .flatMap(inserted.complete) *> run
                   case SlowStep.Finish(commit) =>
                     IO.raiseUnless(commit)(RolledBack)
                 }
               session
                 .channel(PostgresEventStore.NotificationChannel)
                 .notify(PostgresNotification.encode(EventStoreNotification.EventsAppended)) *> run
             }
           }.recover { case RolledBack => () }.attempt
             .flatMap(done.complete)
             .start
      // Fails instead of hanging if the transaction died before inserting.
      insert = (value: String) =>
                 Deferred[IO, Long].flatMap { inserted =>
                   steps.offer(SlowStep.Insert(value, inserted)) *>
                     IO.race(inserted.get, done.get).flatMap {
                       case Left(position) => IO.pure(position)
                       case Right(ended)   =>
                         IO.raiseError(ended.left.getOrElse(new IllegalStateException("slow append already ended")))
                     }
                 }
      position <- insert(value)
    yield SlowAppend(
      position,
      insert,
      commit = steps.offer(SlowStep.Finish(true)) *> done.get.rethrow,
      rollback = steps.offer(SlowStep.Finish(false)) *> done.get.rethrow,
    )

  // ----- projector side -----

  private def recording(seen: Ref[IO, List[GapEvent]], projectionName: String) =
    new StatelessProjection[IO, GapEvent]:
      val name: String = projectionName
      def filter: Set[EventTypeName] = Set(EventTypeName.of[Recorded])
      def handle(event: EventEnvelope[GapEvent]): IO[Unit] = seen.update(_ :+ event.payload)

  private val ProjectorKinds: List[String] = List("DefaultProjector", "ParallelProjector")

  private def projector(kind: String, components: Res): Projector[IO, GapEvent] =
    kind match
      case "DefaultProjector" => DefaultProjector(components.eventStore, components.module.checkpoint)
      case _                  => ParallelProjector(components.eventStore, components.module.checkpoint)

  private def waitUntil(cond: IO[Boolean]): IO[Unit] =
    cond.flatMap {
      case true  => IO.unit
      case false => IO.sleep(20.millis) *> waitUntil(cond)
    }

  /** Runs `body` while the projector runs in the background, then returns everything it handled. */
  private def whileProjecting(kind: String, components: Res, projectionName: String = s"gap-${UUID.randomUUID()}")(
    body: Ref[IO, List[GapEvent]] => IO[Unit],
  ): IO[List[GapEvent]] =
    for
      seen <- Ref.of[IO, List[GapEvent]](Nil)
      _    <- projector(kind, components)
             .run(recording(seen, projectionName))
             .compile
             .drain
             .background
             .use(_ => body(seen))
      handled <- seen.get
    yield handled

  ProjectorKinds.foreach { kind =>
    test(s"$kind does not skip an event whose transaction commits after a higher position became visible") {
      components =>
        for
          _       <- truncate(components.module.sessions)
          slow    <- openSlowAppend(components, "allocated-first")
          fast    <- append(components, "committed-first")
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
        _       <- truncate(components.module.sessions)
        slow    <- openSlowAppend(components, "rolled-back")
        fast    <- append(components, "committed")
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

    test(s"$kind resumes right after the event an operator moves its checkpoint to") { components =>
      val name = s"gap-${UUID.randomUUID()}"
      for
        _       <- truncate(components.module.sessions)
        _       <- List("one", "two", "three", "four").traverse_(append(components, _))
        handled <- whileProjecting(kind, components, name) { seen =>
                     waitUntil(seen.get.map(_.size >= 4)).timeoutTo(Within, IO.unit) *>
                       components.module.sendNotification(EventStoreNotification.UpdateCheckpointIndex(name, 2L)) *>
                       waitUntil(seen.get.map(_.size >= 6)).timeoutTo(Within, IO.unit)
                   }
      yield expect(
        clue(handled) == List("one", "two", "three", "four", "three", "four").map(Recorded(_)),
      )
    }
  }

  // ----- projection read -----

  test("readForProjection delivers a transaction's events together, ahead of a later write positioned between them") {
    components =>
      for
        _       <- truncate(components.module.sessions)
        slow    <- openSlowAppend(components, "slow-first")
        between <- append(components, "between")
        last    <- slow.insert("slow-second")
        _       <- slow.commit
        first   <- readForProjection(components, maxEvents = 2)
        rest    <- readForProjection(components, after = first.events.last.metadata.cursor, maxEvents = 2)
        groups   = (first.events ++ rest.events).map(_.metadata.commitGroup)
      yield expect.all(
        slow.position < between,
        between < last,
        clue(first.events.map(_.payload)) == List(Recorded("slow-first"), Recorded("slow-second")),
        clue(rest.events.map(_.payload)) == List(Recorded("between")),
        clue(groups).distinct.size == 2,
        groups(0) == groups(1),
        groups(1) < groups(2),
      )
  }

  test("readForProjection reports events held back by an open transaction until that transaction resolves") {
    components =>
      for
        _      <- truncate(components.module.sessions)
        quiet  <- readForProjection(components)
        slow   <- openSlowAppend(components, "open")
        _      <- append(components, "visible")
        during <- readForProjection(components)
        _      <- slow.commit
        after  <- readForProjection(components)
      yield expect.all(
        !quiet.heldBack,
        clue(during.events.map(_.payload)).isEmpty,
        during.heldBack,
        clue(after.events.map(_.payload)) == List(Recorded("open"), Recorded("visible")),
        !after.heldBack,
      )
  }

  test("readForProjection applies tag filters, alone and combined with event types") { components =>
    val wanted = Tag("gap", UUID.randomUUID().toString)
    val other = Tag("gap", UUID.randomUUID().toString)
    for
      _        <- truncate(components.module.sessions)
      _        <- appendEnvelope(components, "wanted-1", Set(wanted))
      _        <- appendEnvelope(components, "other", Set(other))
      _        <- appendEnvelope(components, "untagged")
      _        <- appendEnvelope(components, "wanted-2", Set(wanted, other))
      byTag    <- readForProjection(components, filter = EventFilter(Set.empty, Set(wanted)))
      combined <- readForProjection(components, filter = EventFilter(Set(EventTypeName.of[Recorded]), Set(wanted)))
    yield expect.all(
      clue(byTag.events.map(_.payload)) == List(Recorded("wanted-1"), Recorded("wanted-2")),
      clue(combined.events.map(_.payload)) == List(Recorded("wanted-1"), Recorded("wanted-2")),
    )
  }

  test("every read path reports the commit group of the transaction that stored an event") { components =>
    for
      _         <- truncate(components.module.sessions)
      together  <- components.eventStore.appendUnchecked(List(pending("first"), pending("second")))
      later     <- appendEnvelope(components, "later")
      appended   = together :+ later
      readBack  <- components.eventStore.readFrom(0L, EventFilter()).compile.toList
      projected <- readForProjection(components)
      groupsOf   = (events: List[EventEnvelope[GapEvent]]) => events.map(_.metadata.commitGroup)
    yield expect.all(
      groupsOf(together).forall(_ > 0L),
      together.map(_.metadata.commitGroup).distinct.size == 1,
      together.head.metadata.commitGroup < later.metadata.commitGroup,
      clue(groupsOf(readBack)) == groupsOf(appended),
      clue(groupsOf(projected.events)) == groupsOf(appended),
    )
  }

  test("cursorAt resolves a position to the event at or before it, and to the start before every event") { components =>
    val duplicateId = UUID.randomUUID()
    for
      _       <- truncate(components.module.sessions)
      onEmpty <- components.eventStore.cursorAt(5L)
      first   <- appendEnvelope(components, "first", id = Some(duplicateId))
      // Re-appending a stored id burns the next sequence_number, leaving a gap right after `first`.
      _      <- appendEnvelope(components, "first", id = Some(duplicateId))
      third  <- appendEnvelope(components, "third")
      cursor  = (position: Long) => components.eventStore.cursorAt(position)
      zero   <- cursor(0L)
      below  <- cursor(-3L)
      exact  <- cursor(first.metadata.globalPosition)
      gap    <- cursor(first.metadata.globalPosition + 1L)
      latest <- cursor(third.metadata.globalPosition)
      beyond <- cursor(third.metadata.globalPosition + 100L)
    yield expect.all(
      third.metadata.globalPosition == first.metadata.globalPosition + 2L,
      onEmpty == ReadCursor.Start,
      zero == ReadCursor.Start,
      below == ReadCursor.Start,
      clue(exact) == first.metadata.cursor,
      clue(gap) == first.metadata.cursor,
      clue(latest) == third.metadata.cursor,
      clue(beyond) == third.metadata.cursor,
    )
  }

  // ----- write side -----

  test("readFrom returns committed events at once while an unrelated transaction holds a lower position open") {
    components =>
      for
        _    <- truncate(components.module.sessions)
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
