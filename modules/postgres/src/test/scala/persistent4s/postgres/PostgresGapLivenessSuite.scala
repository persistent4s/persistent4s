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

import cats.effect.{IO, Ref, Resource}
import cats.syntax.all.*

import io.circe.parser.parse as parseJson
import io.circe.syntax.*
import io.circe.{Decoder, Encoder}

import org.typelevel.log4cats.Logger
import org.typelevel.otel4s.metrics.Counter

import org.testcontainers.containers.PostgreSQLContainer

import persistent4s.circe.CirceEventCodec
import persistent4s.{
  CommandHandler,
  CommandHandlerMetrics,
  CommandRuntime,
  DefaultProjector,
  Event,
  EventEnvelope,
  EventSchema,
  EventSourcedCommandHandler,
  EventStore,
  EventTypeName,
  ParallelProjector,
  PendingEvent,
  Projector,
  Scope,
  SnapshotCodec,
  SnapshotFailureMode,
  SnapshotId,
  StatelessProjection,
  Tag,
}

import skunk.*
import skunk.implicits.*

import weaver.IOSuite

/** Every reader of the event log must keep making progress and see complete history, whatever ordering guarantee the
  * projection read path adds on top. A projection whose filter matches only a few events must still reach them
  * promptly, and the write side, which never needs that guarantee, must neither be starved nor handed a truncated
  * history. Projections filter on event type, so the suite needs a family of event classes rather than a single one.
  */
object PostgresGapLivenessSuite extends IOSuite:

  given Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  given CommandHandlerMetrics[IO] = CommandHandlerMetrics(Counter.noop[IO, Long])

  override def maxParallelism: Int = 1

  // Kept tiny so a handful of events is enough to span several read passes.
  private val BatchSize: Int = 2

  private val MaxBatchPerPass: Int = 5

  private val PassLimit: Int = BatchSize * MaxBatchPerPass

  // Large enough to exceed any bounded scan window a read path might use when the caller sets no limit.
  private val LongHistory: Int = 8192

  type Res = PostgresModule.Components[IO, LivenessEvent]

  override def sharedResource: Resource[IO, Res] =
    postgresContainerResource.flatMap { container =>
      PostgresModule.makeWithConfig[IO, LivenessEvent](postgresConfig(container), eventCodec)
    }

  sealed trait LivenessEvent extends Event

  final case class Noise(n: Int) extends LivenessEvent derives Encoder.AsObject, Decoder

  final case class Target(value: String) extends LivenessEvent derives Encoder.AsObject, Decoder

  /** An account-scoped entry, the only event the command handlers under test rebuild their state from. */
  final case class Ledger(account: String, entry: String) extends LivenessEvent derives Encoder.AsObject, Decoder

  object Ledger:
    given EventSchema[Ledger] = EventSchema[Ledger]("liveness.ledger").scopedBy(AccountScope)(_.account)

  private val AccountScope = Scope[String]("liveness.account")

  private type Container = PostgreSQLContainer[Nothing]

  private val eventCodec = CirceEventCodec.derived[LivenessEvent]

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

  private def pending(event: LivenessEvent, tags: Set[Tag] = Set.empty, id: Option[UUID] = None): PendingEvent[
    LivenessEvent,
  ] =
    PendingEvent(event, tags, eventCodec.eventType(event), true, id, Map.empty)

  private def appendNoise(store: EventStore[IO, LivenessEvent], count: Int): IO[Unit] =
    store.appendUnchecked(List.tabulate(count)(n => pending(Noise(n)))).void

  private def appendTarget(store: EventStore[IO, LivenessEvent], value: String): IO[Unit] =
    store.appendUnchecked(List(pending(Target(value)))).void

  private def appendLedger(
    store: EventStore[IO, LivenessEvent],
    account: String,
    entry: String,
    id: Option[UUID] = None,
  ): IO[Unit] =
    store.appendUnchecked(List(pending(Ledger(account, entry), Set(AccountScope(account).toTag), id))).void

  private def waitUntil(cond: IO[Boolean]): IO[Unit] =
    cond.flatMap {
      case true  => IO.unit
      case false => IO.sleep(20.millis) *> waitUntil(cond)
    }

  // ----- projector side -----

  private def targetsOnly(projectionName: String, seen: Ref[IO, List[LivenessEvent]]) =
    new StatelessProjection[IO, LivenessEvent]:
      def name: String = projectionName
      def filter: Set[EventTypeName] = Set(EventTypeName.of[Target])
      def handle(event: EventEnvelope[LivenessEvent]): IO[Unit] = seen.update(_ :+ event.payload)

  private val ProjectorKinds: List[String] = List("DefaultProjector", "ParallelProjector")

  private def projector(kind: String, components: Res): Projector[IO, LivenessEvent] =
    kind match
      case "DefaultProjector" =>
        DefaultProjector(
          components.eventStore,
          components.checkpoint,
          batchSize = BatchSize,
          maxBatchPerPass = MaxBatchPerPass,
        )
      case _ =>
        ParallelProjector(
          components.eventStore,
          components.checkpoint,
          batchSize = BatchSize,
          maxBatchPerPass = MaxBatchPerPass,
        )

  /** Runs the projection until `expected` targets were handled or `within` elapses, then returns what it handled. */
  private def project(
    projector: Projector[IO, LivenessEvent],
    expected: Int,
    within: FiniteDuration,
  ): IO[List[LivenessEvent]] =
    for
      seen <- Ref.of[IO, List[LivenessEvent]](Nil)
      _    <- projector
             .run(targetsOnly(s"liveness-${UUID.randomUUID()}", seen))
             .compile
             .drain
             .background
             .use(_ => waitUntil(seen.get.map(_.size >= expected)).timeoutTo(within, IO.unit))
      handled <- seen.get
    yield handled

  ProjectorKinds.foreach { kind =>

    test(s"$kind reaches a matching event that sits behind more than one pass of non-matching events") { components =>
      for
        _       <- truncate(components.sessions)
        _       <- appendNoise(components.eventStore, PassLimit * 3)
        _       <- appendTarget(components.eventStore, "after-noise")
        handled <- project(projector(kind, components), expected = 1, within = 5.seconds)
      yield expect(clue(handled) == List(Target("after-noise")))
    }

    test(s"$kind catches up across several pass-sized stretches without waiting for a notification") { components =>
      // Every pass-sized stretch of the log holds exactly one matching event. The events are all stored before the
      // projector starts listening, so no notification will arrive to drive the catch-up.
      val stretches = 4
      for
        _ <- truncate(components.sessions)
        _ <- (1 to stretches).toList.traverse_ { stretch =>
               appendNoise(components.eventStore, PassLimit - 1) *>
                 appendTarget(components.eventStore, s"stretch-$stretch")
             }
        handled <- project(projector(kind, components), expected = stretches, within = 5.seconds)
      yield expect(clue(handled) == (1 to stretches).toList.map(stretch => Target(s"stretch-$stretch")))
    }
  }

  // ----- command side -----

  final case class Withdraw(account: String)

  case object NotFunded extends RuntimeException("account has not been funded")

  /** Accepts a withdrawal only once the account history contains a "funded" entry. */
  private object Withdrawals extends CommandHandler[Withdraw, List[String], LivenessEvent]:
    def tags(command: Withdraw): Set[Tag] = Set(AccountScope(command.account).toTag)
    def initial: List[String] = Nil
    def evolve(command: Withdraw, state: List[String], event: LivenessEvent): List[String] =
      event match
        case Ledger(_, entry) => state :+ entry
        case _                => state
    def validate(state: List[String], command: Withdraw): Either[Throwable, Unit] =
      Either.cond(state.contains("funded"), (), NotFunded)
    def decide(state: List[String], command: Withdraw): List[(Set[Tag], LivenessEvent)] =
      List(tags(command) -> Ledger(command.account, "withdrawn"))

  /** The same rules as [[Withdrawals]], declared through the scope-based handler DSL. */
  private object EventSourcedWithdrawals
      extends EventSourcedCommandHandler[Withdraw, List[String], LivenessEvent, NotFunded.type]:
    protected val behavior = handler(List.empty[String]):
      scope(AccountScope)(_.account)
      on[Ledger].evolve((state, event) => state :+ event.entry)
      reject:
        case (state, _) if !state.contains("funded") => NotFunded
      emit(command => Ledger(command.account, "withdrawn"))

  private val WithdrawalSnapshot = SnapshotId("liveness-withdrawals")

  private given SnapshotCodec[List[String]] with
    def encode(state: List[String]): String = state.asJson.noSpaces
    def decode(payload: String): Either[Throwable, List[String]] = parseJson(payload).flatMap(_.as[List[String]])

  /** [[EventSourcedWithdrawals]] with a snapshot cached after every replayed event, keyed by account so a test can
    * check it exists. Strict so a snapshot failure surfaces instead of silently falling back to a cold replay.
    */
  private object SnapshottedWithdrawals
      extends EventSourcedCommandHandler[Withdraw, List[String], LivenessEvent, NotFunded.type]:
    protected val behavior = handler(List.empty[String]):
      scope(AccountScope)(_.account)
      on[Ledger].evolve((state, event) => state :+ event.entry)
      reject:
        case (state, _) if !state.contains("funded") => NotFunded
      snapshotBy(WithdrawalSnapshot, every = 1, failureMode = SnapshotFailureMode.Strict)(_.account)
      emit(command => Ledger(command.account, "withdrawn"))

  /** Every write-side entry point that rebuilds state through readFrom, normalised so a failed effect, a returned
    * rejection and a successful append can all be compared against the same expectation.
    */
  private val CommandRunners: List[(String, (Res, String) => IO[Either[String, List[LivenessEvent]]])] =
    List(
      "CommandHandler.run" -> { (components, account) =>
        given EventStore[IO, LivenessEvent] = components.eventStore
        Withdrawals.run[IO](Withdraw(account)).attempt.map(_.bimap(_.toString, _.map(_.payload)))
      },
      "CommandHandler.runWithMessages" -> { (components, account) =>
        given PostgresEventStore[IO, LivenessEvent] = components.transactionalStore
        Withdrawals
          .runWithMessages[IO](Withdraw(account))
          .attempt
          .map(_.flatten.bimap(_.toString, _.map(_.payload)))
      },
      "EventSourcedCommandHandler.run" -> { (components, account) =>
        given CommandRuntime[IO, LivenessEvent] = CommandRuntime(components.eventStore)
        EventSourcedWithdrawals
          .run[IO](Withdraw(account))
          .attempt
          .map(_.leftMap(_.toString).flatMap(_.bimap(rejection => s"rejected: $rejection", _.map(_.payload))))
      },
    )

  CommandRunners.foreach { (label, withdraw) =>

    test(s"$label can still write to an aggregate whose latest event follows a long stretch of other events") {
      components =>
        val account = UUID.randomUUID().toString
        for
          _      <- truncate(components.sessions)
          _      <- appendLedger(components.eventStore, account, "opened")
          _      <- appendLedger(components.eventStore, account, "funded")
          _      <- appendNoise(components.eventStore, LongHistory)
          _      <- appendLedger(components.eventStore, account, "deposited")
          result <- withdraw(components, account)
        yield expect(clue(result) == Right(List(Ledger(account, "withdrawn"))))
    }

    test(s"$label is not blinded by a sequence_number gap left by an unrelated account") { components =>
      val account = UUID.randomUUID().toString
      val unrelated = UUID.randomUUID().toString
      val unrelatedId = UUID.randomUUID()
      for
        _ <- truncate(components.sessions)
        _ <- appendLedger(components.eventStore, account, "opened")
        _ <- appendLedger(components.eventStore, unrelated, "elsewhere", Some(unrelatedId))
        // Re-appending a stored id burns the sequence_number allocated for the discarded row, leaving a gap.
        _      <- appendLedger(components.eventStore, unrelated, "elsewhere", Some(unrelatedId))
        _      <- appendLedger(components.eventStore, account, "funded")
        result <- withdraw(components, account)
      yield expect(clue(result) == Right(List(Ledger(account, "withdrawn"))))
    }
  }

  test(
    "EventSourcedCommandHandler.run resuming from a snapshot can still write once the aggregate's latest event " +
      "follows a long stretch of other events",
  ) { components =>
    given CommandRuntime[IO, LivenessEvent] = components.commandRuntime
    val account = UUID.randomUUID().toString
    def withdraw: IO[Either[String, List[LivenessEvent]]] =
      SnapshottedWithdrawals
        .run[IO](Withdraw(account))
        .attempt
        .map(_.leftMap(_.toString).flatMap(_.bimap(rejection => s"rejected: $rejection", _.map(_.payload))))
    for
      _     <- truncate(components.sessions)
      _     <- appendLedger(components.eventStore, account, "opened")
      _     <- appendLedger(components.eventStore, account, "funded")
      first <- withdraw
      // The first run replayed both entries and cached them, so the next run replays only what follows position 2.
      snapshot <- components.snapshotStore.load(WithdrawalSnapshot, account, 1)
      _        <- appendNoise(components.eventStore, LongHistory)
      _        <- appendLedger(components.eventStore, account, "deposited")
      second   <- withdraw
    yield expect.all(
      first == Right(List(Ledger(account, "withdrawn"))),
      clue(snapshot.map(_.globalPosition)) == Some(2L),
      clue(second) == Right(List(Ledger(account, "withdrawn"))),
    )
  }
