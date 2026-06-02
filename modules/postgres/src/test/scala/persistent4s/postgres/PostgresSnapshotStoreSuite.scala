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
import io.circe.{Decoder, Encoder}
import org.testcontainers.containers.PostgreSQLContainer
import persistent4s.*
import persistent4s.circe.CirceSnapshotCodec.given
import skunk.Session
import weaver.IOSuite

object PostgresSnapshotStoreSuite extends IOSuite:

  override def maxParallelism: Int = 1

  final case class TestState(value: Int) derives Encoder, Decoder

  type Res = PostgresSnapshotStore[IO]

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
          for _ <- Resource.eval(pool.use(_.execute(PostgresSnapshotStore.createTableCommand).void))
          yield PostgresSnapshotStore[IO](pool)
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

  private def freshTags: IO[Set[Tag]] =
    IO(Set(Tag("entity", UUID.randomUUID().toString)))

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  test("load returns None for a snapshot that has never been saved") { store =>
    for
      tags   <- freshTags
      result <- store.load[TestState]("handler", tags)
    yield expect(result.isEmpty)
  }

  test("save and load round-trip") { store =>
    for
      tags     <- freshTags
      snapshot  = Snapshot(TestState(42), 10L)
      _        <- store.save[TestState]("handler", tags, snapshot)
      result   <- store.load[TestState]("handler", tags)
    yield expect(result == Some(snapshot))
  }

  test("save overwrites existing snapshot (upsert semantics)") { store =>
    for
      tags   <- freshTags
      snap1   = Snapshot(TestState(1), 5L)
      snap2   = Snapshot(TestState(2), 10L)
      _      <- store.save[TestState]("handler", tags, snap1)
      _      <- store.save[TestState]("handler", tags, snap2)
      result <- store.load[TestState]("handler", tags)
    yield expect(result == Some(snap2))
  }

  test("snapshots for distinct handler names are stored independently") { store =>
    for
      tags  <- freshTags
      snap1  = Snapshot(TestState(10), 1L)
      snap2  = Snapshot(TestState(20), 2L)
      _     <- store.save[TestState]("handler-a", tags, snap1)
      _     <- store.save[TestState]("handler-b", tags, snap2)
      r1    <- store.load[TestState]("handler-a", tags)
      r2    <- store.load[TestState]("handler-b", tags)
    yield expect(r1 == Some(snap1)) && expect(r2 == Some(snap2))
  }

  test("snapshots for distinct tag sets are stored independently") { store =>
    for
      tags1  <- freshTags
      tags2  <- freshTags
      snap1   = Snapshot(TestState(10), 1L)
      snap2   = Snapshot(TestState(20), 2L)
      _      <- store.save[TestState]("handler", tags1, snap1)
      _      <- store.save[TestState]("handler", tags2, snap2)
      r1     <- store.load[TestState]("handler", tags1)
      r2     <- store.load[TestState]("handler", tags2)
    yield expect(r1 == Some(snap1)) && expect(r2 == Some(snap2))
  }

  test("load raises SnapshotDecodeException on a corrupted payload") { store =>
    given badCodec: SnapshotCodec[TestState] with
      def encode(s: TestState): String                       = "corrupted"
      def decode(p: String): Either[Throwable, TestState]   = Left(RuntimeException("bad payload"))
    for
      tags   <- freshTags
      _      <- store.save[TestState]("handler", tags, Snapshot(TestState(1), 1L))
      result <- store.load[TestState]("handler", tags).attempt
    yield expect(result.left.exists(_.isInstanceOf[SnapshotDecodeException]))
  }
