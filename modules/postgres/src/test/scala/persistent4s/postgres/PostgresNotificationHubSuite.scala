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

import fs2.Stream
import io.circe.syntax.*
import io.circe.{Decoder, Encoder}

import org.typelevel.log4cats.Logger

import org.testcontainers.containers.PostgreSQLContainer
import persistent4s.circe.CirceEventCodec
import persistent4s.{Event, EventEnvelope, EventTypeName, PendingEvent, StatelessProjection, Tag}
import skunk.*
import skunk.codec.all.*
import skunk.data.Identifier
import skunk.implicits.*
import weaver.IOSuite

object PostgresNotificationHubSuite extends IOSuite:

  given Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  override def maxParallelism: Int = 1

  private type Container = PostgreSQLContainer[Nothing]

  type Res = Container

  override def sharedResource: Resource[IO, Res] =
    Resource.make {
      IO.blocking {
        val container = new PostgreSQLContainer[Nothing]("postgres:16-alpine")
        container.start()
        container
      }
    }(container => IO.blocking(container.stop()).handleErrorWith(_ => IO.unit))

  final case class HubEvent(value: String) extends Event derives Encoder.AsObject, Decoder

  private val eventCodec = CirceEventCodec.make[HubEvent](
    encodeEvent = _.asJson,
    decodeEvent = (_, json) => json.as[HubEvent].left.map(error => error: Throwable),
  )

  private val channel = PostgresEventStore.NotificationChannel

  private def postgresConfig(container: Container, maxConnections: Int): PostgresConfig =
    PostgresConfig(
      host = container.getHost, port = container.getMappedPort(5432), user = container.getUsername,
      password = container.getPassword, database = container.getDatabaseName, maxConnections = maxConnections,
    )

  private def module(container: Container, maxConnections: Int): Resource[IO, PostgresModule.Components[IO, HubEvent]] =
    PostgresModule.makeWithConfig[IO, HubEvent](postgresConfig(container, maxConnections), eventCodec)

  private def hub(sessions: Resource[IO, Session[IO]]): Resource[IO, PostgresNotificationHub[IO]] =
    PostgresNotificationHub.make[IO](sessions, retryInitialDelay = 50.millis, retryMaxDelay = 200.millis)

  /** Backends whose last statement was a LISTEN, i.e. sessions currently listening on behalf of this database. */
  private val listeningBackends: Query[Void, Int] =
    sql"""
      SELECT pid FROM pg_stat_activity
      WHERE datname = current_database() AND pid <> pg_backend_pid() AND query LIKE 'LISTEN%'
    """.query(int4)

  private val terminateListeners: Query[Void, Boolean] =
    sql"""
      SELECT pg_terminate_backend(pid) FROM pg_stat_activity
      WHERE datname = current_database() AND pid <> pg_backend_pid() AND query LIKE 'LISTEN%'
    """.query(bool)

  private def appendOne(components: PostgresModule.Components[IO, HubEvent], value: String): IO[Unit] =
    components.eventStore
      .appendUnchecked(
        List(PendingEvent(HubEvent(value), Set(Tag("hub", value)), EventTypeName.of[HubEvent], true, None, Map.empty)),
      )
      .void

  private def freshId(prefix: String): IO[String] =
    IO(s"$prefix-${UUID.randomUUID().toString}")

  test("projections outnumbering the pool's sessions all project a new event") { container =>
    module(container, maxConnections = 2).use { components =>
      for
        id        <- freshId("outnumbered")
        projected <- List.range(0, 4).traverse(_ => Deferred[IO, Unit])
        projections = projected.zipWithIndex.map { case (done, index) =>
                        new StatelessProjection[IO, HubEvent]:
                          def name: String                          = s"$id-$index"
                          def filter: Set[EventTypeName]            = Set(EventTypeName.of[HubEvent])
                          def handle(event: EventEnvelope[HubEvent]): IO[Unit] =
                            done.complete(()).void.whenA(event.payload.value == id)
                      }
        _ <- components.projectionRuntime
               .startAll(registrations => projections.foreach(registrations.run(_)))
               .use(_ => appendOne(components, id) *> projected.traverse_(_.get))
               .timeout(20.seconds)
      yield success
    }
  }

  test("every subscriber shares a single listening session") { container =>
    module(container, maxConnections = 8).use { components =>
      val listeners   = components.sessions.use(_.execute(listeningBackends))
      val subscribers =
        Stream
          .emits(List.range(0, 5))
          .map(index => components.eventStore.notification(s"shared-$index"))
          .parJoinUnbounded
          .compile
          .drain
      subscribers.background
        .use(_ => (IO.sleep(50.millis) *> listeners).iterateUntil(_.nonEmpty) *> IO.sleep(500.millis) *> listeners)
        .timeout(10.seconds)
        .map(pids => expect(pids.size == 1))
    }
  }

  test("a subscriber receives notifications sent once LISTEN is registered") { container =>
    module(container, maxConnections = 4).use { components =>
      hub(components.sessions).use { hub =>
        hub.listenR(channel).use { messages =>
          for
            id       <- freshId("payload")
            received <- messages
                          .evalTap {
                            case PostgresNotificationHub.Message.CatchUp =>
                              components.sessions.use(_.channel(channel).notify(id))
                            case _ => IO.unit
                          }
                          .collectFirst { case PostgresNotificationHub.Message.Payload(value) if value == id => value }
                          .compile
                          .lastOrError
                          .timeout(5.seconds)
          yield expect(received == id)
        }
      }
    }
  }

  test("the hub reconnects after its session is terminated and signals a catch-up") { container =>
    module(container, maxConnections = 4).use { components =>
      hub(components.sessions).use { hub =>
        hub.listenR(channel).use { messages =>
          for
            id         <- freshId("reconnect")
            terminated <- Deferred[IO, List[Boolean]]
            // The first catch-up means LISTEN is registered: kill that session once, then notify until the hub has
            // registered LISTEN again on a new one.
            terminateOnce = terminated.tryGet.flatMap {
                              case None    => components.sessions.use(_.execute(terminateListeners)).flatMap(terminated.complete).void
                              case Some(_) => IO.unit
                            }
            notifyAfterTermination = Stream.eval(terminated.get) >>
                                       Stream
                                         .awakeEvery[IO](100.millis)
                                         .evalMap(_ => components.sessions.use(_.channel(channel).notify(id)))
            received <- messages
                          .evalTap {
                            case PostgresNotificationHub.Message.CatchUp => terminateOnce
                            case _                                       => IO.unit
                          }
                          .takeThrough {
                            case PostgresNotificationHub.Message.Payload(value) => value != id
                            case PostgresNotificationHub.Message.CatchUp        => true
                          }
                          .concurrently(notifyAfterTermination)
                          .compile
                          .toList
                          .timeout(10.seconds)
            killed <- terminated.get
          yield expect(killed.nonEmpty) and
            expect(received.count(_ == PostgresNotificationHub.Message.CatchUp) >= 2) and
            expect(received.lastOption.contains(PostgresNotificationHub.Message.Payload(id)))
        }
      }
    }
  }

  test("a projection catches up on an event appended while the listener was down") { container =>
    module(container, maxConnections = 4).use { components =>
      for
        id        <- freshId("outage")
        started   <- Deferred[IO, Unit]
        projected <- Deferred[IO, Unit]
        projection = new StatelessProjection[IO, HubEvent]:
                       def name: String               = id
                       def filter: Set[EventTypeName] = Set(EventTypeName.of[HubEvent])
                       def handle(event: EventEnvelope[HubEvent]): IO[Unit] =
                         if event.payload.value == s"$id-before" then started.complete(()).void
                         else projected.complete(()).void.whenA(event.payload.value == id)
        _ <- components.projectionRuntime
               .startAll(_.run(projection))
               .use { _ =>
                 appendOne(components, s"$id-before") *> started.get *>
                   // Kill the listener and append before it can register LISTEN again: no notification is delivered
                   // for this event, so only the hub's catch-up can make the projection read it.
                   components.sessions.use(session => session.execute(terminateListeners) *> appendOne(components, id)) *>
                   projected.get
               }
               .timeout(10.seconds)
      yield success
    }
  }

  test("subscribing to a channel the hub does not listen on fails") { container =>
    module(container, maxConnections = 2).use { components =>
      val unknown = Identifier.fromValue("not_listened").fold(error => sys.error(error), identity)
      hub(components.sessions).use { hub =>
        hub.listenR(unknown).use_.attempt.map(result => expect(result.left.exists(_.isInstanceOf[IllegalArgumentException])))
      }
    }
  }
