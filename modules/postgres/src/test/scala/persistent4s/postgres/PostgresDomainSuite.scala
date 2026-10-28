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

import cats.effect.{Deferred, IO, Resource}
import cats.syntax.all.*

import io.circe.syntax.*
import io.circe.{Decoder, Encoder}

import org.typelevel.log4cats.Logger

import org.testcontainers.containers.PostgreSQLContainer
import persistent4s.circe.CirceEventCodec
import persistent4s.{
  DuplicateProjectionNames,
  Event,
  EventEnvelope,
  EventTypeName,
  PendingEvent,
  ProjectionExecutionFailed,
  StatelessProjection,
  Tag,
}
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*
import weaver.IOSuite

object PostgresDomainSuite extends IOSuite:

  given Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  override def maxParallelism: Int = 1

  type Res = PostgresModule[IO]

  override def sharedResource: Resource[IO, Res] =
    Resource.make {
      IO.blocking {
        val container = new PostgreSQLContainer[Nothing]("postgres:16-alpine")
        container.start()
        container
      }
    }(container => IO.blocking(container.stop()).handleErrorWith(_ => IO.unit)).flatMap { container =>
      PostgresModule.makeWithConfig[IO](
        PostgresConfig(
          host = container.getHost, port = container.getMappedPort(5432), user = container.getUsername,
          password = container.getPassword, database = container.getDatabaseName, maxConnections = 4,
        ),
      )
    }

  final case class AlphaEvent(value: String) extends Event derives Encoder.AsObject, Decoder

  final case class BetaEvent(value: String) extends Event derives Encoder.AsObject, Decoder

  private val alphaCodec = CirceEventCodec.make[AlphaEvent](
    encodeEvent = _.asJson,
    decodeEvent = (_, json) => json.as[AlphaEvent].left.map(error => error: Throwable),
  )

  private val betaCodec = CirceEventCodec.make[BetaEvent](
    encodeEvent = _.asJson,
    decodeEvent = (_, json) => json.as[BetaEvent].left.map(error => error: Throwable),
  )

  private val listeningBackends: Query[Void, Int] =
    sql"""
      SELECT pid FROM pg_stat_activity
      WHERE datname = current_database() AND pid <> pg_backend_pid() AND query LIKE 'LISTEN%'
    """.query(int4)

  private def append[A <: Event](domain: PostgresDomain[IO, A], event: A, eventType: EventTypeName): IO[Unit] =
    domain.eventStore
      .appendUnchecked(List(PendingEvent(event, Set(Tag("domain", "suite")), eventType, true, None, Map.empty)))
      .void

  /** A projection of `A` events that completes `seen` when it handles one whose value is `value`. */
  private def awaiting[A <: Event](
    projectionName: String,
    eventType: EventTypeName,
    value: A => String,
    expected: String,
    seen: Deferred[IO, Unit],
  ): StatelessProjection[IO, A] =
    new StatelessProjection[IO, A]:
      def name: String = projectionName
      def filter: Set[EventTypeName] = Set(eventType)
      def handle(event: EventEnvelope[A]): IO[Unit] = seen.complete(()).void.whenA(value(event.payload) == expected)

  private def freshId(prefix: String): IO[String] =
    IO(s"$prefix-${UUID.randomUUID().toString}")

  test("two domains of one module project their own events over a single listening session") { module =>
    (module.domain(alphaCodec), module.domain(betaCodec)).tupled.use { case (alpha, beta) =>
      for
        id        <- freshId("domains")
        alphaSeen <- Deferred[IO, Unit]
        betaSeen  <- Deferred[IO, Unit]
        alphaType  = EventTypeName.of[AlphaEvent]
        betaType   = EventTypeName.of[BetaEvent]
        listeners <- (
                       alpha.projectionRuntime.startAll(
                         _.run(awaiting[AlphaEvent](s"$id-alpha", alphaType, _.value, id, alphaSeen)),
                       ),
                       beta.projectionRuntime.startAll(
                         _.run(awaiting[BetaEvent](s"$id-beta", betaType, _.value, id, betaSeen)),
                       ),
                     ).tupled.use { _ =>
                       append(alpha, AlphaEvent(id), alphaType) *> append(beta, BetaEvent(id), betaType) *>
                         alphaSeen.get *> betaSeen.get *> module.sessions.use(_.execute(listeningBackends))
                     }.timeout(10.seconds)
      yield expect(listeners.size == 1)
    }
  }

  test("only one domain at a time may enable the event outbox") { module =>
    for
      second <- module.domain(alphaCodec, outbox = true).use { first =>
                  module.domain(betaCodec, outbox = true).use_.attempt.map(first.outbox.isDefined -> _)
                }
      afterRelease  <- module.domain(betaCodec, outbox = true).use(domain => IO.pure(domain.outbox.isDefined))
      withoutOutbox <- module.domain(alphaCodec, outbox = true).use_ *>
                         module.domain(alphaCodec).use(domain => IO.pure(domain.outbox.isEmpty))
    yield expect(second._1) and
      expect(second._2 == Left(PostgresModuleError.OutboxAlreadyEnabled)) and
      expect(afterRelease) and
      expect(withoutOutbox)
  }

  test("a projection name already running in another domain fails to start") { module =>
    (module.domain(alphaCodec), module.domain(betaCodec)).tupled.use { case (alpha, beta) =>
      for
        id       <- freshId("duplicate")
        started  <- Deferred[IO, Unit]
        ignored  <- Deferred[IO, Unit]
        alphaType = EventTypeName.of[AlphaEvent]
        result   <-
          alpha.projectionRuntime
            .startAll(_.run(awaiting[AlphaEvent](id, alphaType, _.value, id, started)))
            .use { _ =>
              // Once the alpha projection handled an event, it holds the name.
              append(alpha, AlphaEvent(id), alphaType) *> started.get *>
                beta.projectionRuntime
                  .startAll(_.run(awaiting[BetaEvent](id, EventTypeName.of[BetaEvent], _.value, id, ignored)))
                  .use(_.await)
                  .attempt
            }
            .timeout(10.seconds)
      yield result match
        case Left(ProjectionExecutionFailed(name, DuplicateProjectionNames(names))) =>
          expect(name == id) and expect(names == List(id))
        case other => failure(s"Expected a duplicate projection name failure, got $other")
    }
  }

  test("a projection name is free again once its projection stops") { module =>
    (module.domain(alphaCodec), module.domain(betaCodec)).tupled.use { case (alpha, beta) =>
      for
        id       <- freshId("reused")
        ignored  <- Deferred[IO, Unit]
        betaSeen <- Deferred[IO, Unit]
        betaType  = EventTypeName.of[BetaEvent]
        _        <- alpha.projectionRuntime
               .startAll(_.run(awaiting[AlphaEvent](id, EventTypeName.of[AlphaEvent], _.value, id, ignored)))
               .use_
        _ <- beta.projectionRuntime
               .startAll(_.run(awaiting[BetaEvent](id, betaType, _.value, id, betaSeen)))
               .use(_ => append(beta, BetaEvent(id), betaType) *> betaSeen.get)
               .timeout(10.seconds)
      yield success
    }
  }
