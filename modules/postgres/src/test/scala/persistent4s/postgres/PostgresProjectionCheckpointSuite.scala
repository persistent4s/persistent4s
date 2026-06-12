/*
 * Copyright 2026 Bastien Jolidon
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
import org.testcontainers.containers.PostgreSQLContainer
import persistent4s.ProjectionCheckpointState
import skunk.Session
import weaver.IOSuite

object PostgresProjectionCheckpointSuite extends IOSuite:

  override def maxParallelism: Int = 1

  type Res = PostgresProjectionCheckpoint[IO]

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
        .flatMap { pool =>
          for _ <- Resource.eval(pool.use(_.execute(PostgresProjectionCheckpoint.createTableCommand).void))
          yield PostgresProjectionCheckpoint.make[IO](pool)
        }
    }

  private type Container = PostgreSQLContainer[Nothing]

  private def postgresConfig(container: Container): PostgresConfig =
    PostgresConfig(
      host = container.getHost, port = container.getMappedPort(5432), user = container.getUsername,
      password = container.getPassword, database = container.getDatabaseName, maxConnections = 4,
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

  private def freshName(prefix: String): IO[String] =
    IO(s"$prefix-${UUID.randomUUID()}")

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  test("load returns None for a projection that has never been saved") { checkpoint =>
    for
      name   <- freshName("proj")
      result <- checkpoint.load(name)
    yield expect(result.isEmpty)
  }

  test("save and load round-trip: running=true, no error") { checkpoint =>
    for
      name   <- freshName("proj")
      state   = ProjectionCheckpointState(name, 42L, true, None)
      _      <- checkpoint.save(state)
      loaded <- checkpoint.load(name)
    yield expect(loaded == Some(state))
  }

  test("save and load round-trip: running=false with error message") { checkpoint =>
    for
      name   <- freshName("proj")
      state   = ProjectionCheckpointState(name, 7L, false, Some("RuntimeException: something went wrong"))
      _      <- checkpoint.save(state)
      loaded <- checkpoint.load(name)
    yield expect(loaded == Some(state))
  }

  test("save and load round-trip: globalPosition zero") { checkpoint =>
    for
      name   <- freshName("proj")
      state   = ProjectionCheckpointState(name, 0L, true, None)
      _      <- checkpoint.save(state)
      loaded <- checkpoint.load(name)
    yield expect(loaded == Some(state))
  }

  test("save and load round-trip: negative globalPosition (initial sentinel -1)") { checkpoint =>
    for
      name   <- freshName("proj")
      state   = ProjectionCheckpointState(name, -1L, true, None)
      _      <- checkpoint.save(state)
      loaded <- checkpoint.load(name)
    yield expect(loaded == Some(state))
  }

  test("second save overwrites the first (upsert semantics)") { checkpoint =>
    for
      name   <- freshName("proj")
      first   = ProjectionCheckpointState(name, 1L, true, None)
      second  = ProjectionCheckpointState(name, 99L, false, Some("error"))
      _      <- checkpoint.save(first)
      _      <- checkpoint.save(second)
      loaded <- checkpoint.load(name)
    yield expect(loaded == Some(second))
  }

  test("error field can be cleared by saving a new state with error=None") { checkpoint =>
    for
      name     <- freshName("proj")
      withError = ProjectionCheckpointState(name, 5L, false, Some("something failed"))
      cleared   = ProjectionCheckpointState(name, 5L, true, None)
      _        <- checkpoint.save(withError)
      _        <- checkpoint.save(cleared)
      loaded   <- checkpoint.load(name)
    yield expect(loaded == Some(cleared))
  }

  test("checkpoints for distinct projections are stored independently") { checkpoint =>
    for
      nameA   <- freshName("projA")
      nameB   <- freshName("projB")
      stateA   = ProjectionCheckpointState(nameA, 10L, true, None)
      stateB   = ProjectionCheckpointState(nameB, 20L, false, Some("err"))
      _       <- checkpoint.save(stateA)
      _       <- checkpoint.save(stateB)
      loadedA <- checkpoint.load(nameA)
      loadedB <- checkpoint.load(nameB)
    yield expect.all(
      loadedA == Some(stateA),
      loadedB == Some(stateB),
    )
  }

  test("loadAll returns all saved checkpoints including recently saved ones") { checkpoint =>
    for
      nameA <- freshName("projA")
      nameB <- freshName("projB")
      stateA = ProjectionCheckpointState(nameA, 10L, true, None)
      stateB = ProjectionCheckpointState(nameB, 20L, false, Some("err"))
      _     <- checkpoint.save(stateA)
      _     <- checkpoint.save(stateB)
      all   <- checkpoint.loadAll()
    yield expect.all(
      all.exists(_ == stateA),
      all.exists(_ == stateB),
    )
  }
