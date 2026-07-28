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
import cats.effect.std.SecureRandom
import cats.syntax.all.*
import org.testcontainers.containers.PostgreSQLContainer
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*
import weaver.IOSuite

object PostgresLeaderElectionSuite extends IOSuite:

  override def maxParallelism: Int = 1

  type Res = Resource[IO, Session[IO]]

  override def sharedResource: Resource[IO, Res] =
    postgresContainerResource.flatMap { container =>
      val cfg = postgresConfig(container)
      Session
        .Builder[IO]
        .withHost(cfg.host)
        .withPort(cfg.port)
        .withUserAndPassword(cfg.user, cfg.password)
        .withDatabase(cfg.database)
        .pooled(cfg.maxConnections)
        .evalTap(pool => pool.use(_.execute(PostgresLeaderElection.createTableCommand).void))
    }

  // Test-fast timings. renewTimeout (400ms) < ttl/3 (667ms), as `make` requires.
  private def election(pool: Resource[IO, Session[IO]]): IO[PostgresLeaderElection[IO]] =
    SecureRandom.javaSecuritySecureRandom[IO].map { implicit sr =>
      PostgresLeaderElection.make[IO](pool, ttl = 2.seconds, pollInterval = 300.millis, renewTimeout = 400.millis)
    }

  /** A task that records itself as active while it runs (until cancelled). */
  private def markingTask(active: Ref[IO, Set[String]], tag: String): IO[Unit] =
    active.update(_ + tag).bracket(_ => IO.never[Unit])(_ => active.update(_ - tag))

  private def waitUntil(cond: IO[Boolean], attempts: Int = 120, every: FiniteDuration = 50.millis): IO[Boolean] =
    cond.flatMap {
      case true                   => IO.pure(true)
      case false if attempts <= 0 => IO.pure(false)
      case false                  => IO.sleep(every) *> waitUntil(cond, attempts - 1, every)
    }

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  test("only one contender runs the task at a time") { pool =>
    for
      le      <- election(pool)
      name    <- freshName("mutex")
      active  <- IO.ref(0)
      maxSeen <- IO.ref(0)
      task     = active
               .updateAndGet(_ + 1)
               .flatMap(n => maxSeen.update(_ max n))
               .bracket(_ => IO.never[Unit])(_ => active.update(_ - 1))
      f1   <- le.runAsLeader(name)(task).start
      f2   <- le.runAsLeader(name)(task).start
      _    <- IO.sleep(1500.millis)
      peak <- maxSeen.get
      _    <- f1.cancel *> f2.cancel
    yield expect(peak == 1)
  }

  test("a standby takes over quickly after the leader releases") { pool =>
    for
      le     <- election(pool)
      name   <- freshName("failover")
      active <- IO.ref(Set.empty[String])
      f1     <- le.runAsLeader(name)(markingTask(active, "A")).start
      aLeads <- waitUntil(active.get.map(_.contains("A")))
      f2     <- le.runAsLeader(name)(markingTask(active, "B")).start
      _      <- IO.sleep(700.millis)
      bIdle  <- active.get.map(!_.contains("B"))
      _      <- f1.cancel
      bLeads <- waitUntil(active.get.map(_.contains("B")))
      _      <- f2.cancel
    yield expect.all(aLeads, bIdle, bLeads)
  }

  test("renewal keeps the leader's task alive across multiple TTLs") { pool =>
    for
      le         <- election(pool)
      name       <- freshName("renew")
      active     <- IO.ref(Set.empty[String])
      f1         <- le.runAsLeader(name)(markingTask(active, "A")).start
      _          <- waitUntil(active.get.map(_.contains("A")))
      _          <- IO.sleep(5.seconds)
      stillLeads <- active.get.map(_.contains("A"))
      _          <- f1.cancel
    yield expect(stillLeads)
  }

  test("different names elect leaders independently") { pool =>
    for
      le     <- election(pool)
      nameA  <- freshName("indep-a")
      nameB  <- freshName("indep-b")
      active <- IO.ref(Set.empty[String])
      f1     <- le.runAsLeader(nameA)(markingTask(active, "A")).start
      f2     <- le.runAsLeader(nameB)(markingTask(active, "B")).start
      both   <- waitUntil(active.get.map(s => s.contains("A") && s.contains("B")))
      _      <- f1.cancel *> f2.cancel
    yield expect(both)
  }

  test("a contender steals a lease only after it expires (crash failover)") { pool =>
    for
      le       <- election(pool)
      name     <- freshName("crash")
      _        <- pool.use(_.execute(orphanLeaseCommand)(name))
      active   <- IO.ref(Set.empty[String])
      f1       <- le.runAsLeader(name)(markingTask(active, "A")).start
      tooEarly <- IO.sleep(400.millis) *> active.get.map(_.contains("A"))
      acquired <- waitUntil(active.get.map(_.contains("A")))
      _        <- f1.cancel
    yield expect.all(!tooEarly, acquired)
  }

  private val orphanLeaseCommand: Command[String] =
    sql"""
      INSERT INTO leader_leases (name, holder, expires_at)
      VALUES ($text, gen_random_uuid(), clock_timestamp() + interval '1 second')
    """.command

  // --- container boilerplate ---

  private type Container = PostgreSQLContainer[Nothing]

  private def postgresConfig(container: Container): PostgresConfig =
    PostgresConfig(
      host = container.getHost, port = container.getMappedPort(5432), user = container.getUsername,
      password = container.getPassword, database = container.getDatabaseName, maxConnections = 6,
    )

  private def postgresContainerResource: Resource[IO, Container] =
    Resource.make {
      IO.blocking {
        val container = new PostgreSQLContainer[Nothing]("postgres:16-alpine")
        container.start()
        container
      }
    }(container => IO.blocking(container.stop()).handleErrorWith(_ => IO.unit))

  private def freshName(prefix: String): IO[String] =
    IO(s"$prefix-${UUID.randomUUID()}")
