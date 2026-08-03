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

import persistent4s.{SnapshotId, StoredCommandSnapshot}

import org.testcontainers.containers.PostgreSQLContainer
import skunk.Session
import weaver.IOSuite

object PostgresCommandSnapshotStoreSuite extends IOSuite:

  override def maxParallelism: Int = 1

  type Res = PostgresCommandSnapshotStore[IO]

  override def sharedResource: Resource[IO, Res] =
    postgresContainerResource.flatMap { container =>
      val config = postgresConfig(container)
      Session
        .Builder[IO]
        .withHost(config.host)
        .withPort(config.port)
        .withUserAndPassword(config.user, config.password)
        .withDatabase(config.database)
        .pooled(config.maxConnections)
        .flatMap { pool =>
          Resource
            .eval(pool.use(_.execute(PostgresCommandSnapshotStore.createTableCommand).void))
            .as(PostgresCommandSnapshotStore.make(pool))
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

  private def freshSnapshotId(prefix: String): IO[SnapshotId] =
    IO(SnapshotId(s"$prefix-${UUID.randomUUID()}"))

  private def freshKey(prefix: String): IO[String] =
    IO(s"$prefix-${UUID.randomUUID()}")

  test("save and load round-trip every snapshot field") { snapshots =>
    for
      snapshotId <- freshSnapshotId("round-trip")
      key        <- freshKey("entity")
      expected    = StoredCommandSnapshot(
                      globalPosition = 42L,
                      eventCount = 17L,
                      filterFingerprint = "book-added,book-borrowed",
                      payload = "{\"exists\":true}",
                    )
      _      <- snapshots.save(snapshotId, key, version = 3, expected)
      loaded <- snapshots.load(snapshotId, key, version = 3)
    yield expect(loaded.contains(expected))
  }

  test("an older save cannot regress a newer snapshot") { snapshots =>
    for
      snapshotId <- freshSnapshotId("monotonic")
      key        <- freshKey("entity")
      newer       = StoredCommandSnapshot(
                      globalPosition = 100L,
                      eventCount = 25L,
                      filterFingerprint = "new-filter",
                      payload = "new-state",
                    )
      older       = StoredCommandSnapshot(
                      globalPosition = 99L,
                      eventCount = 99L,
                      filterFingerprint = "stale-filter",
                      payload = "stale-state",
                    )
      _      <- snapshots.save(snapshotId, key, version = 1, newer)
      _      <- snapshots.save(snapshotId, key, version = 1, older)
      loaded <- snapshots.load(snapshotId, key, version = 1)
    yield expect(loaded.contains(newer))
  }

  test("delete removes the selected snapshot") { snapshots =>
    for
      snapshotId <- freshSnapshotId("delete")
      key        <- freshKey("entity")
      snapshot    = StoredCommandSnapshot(12L, 4L, "filter", "state")
      _      <- snapshots.save(snapshotId, key, version = 2, snapshot)
      before <- snapshots.load(snapshotId, key, version = 2)
      _      <- snapshots.delete(snapshotId, key, version = 2)
      after  <- snapshots.load(snapshotId, key, version = 2)
    yield expect.all(before.contains(snapshot), after.isEmpty)
  }
