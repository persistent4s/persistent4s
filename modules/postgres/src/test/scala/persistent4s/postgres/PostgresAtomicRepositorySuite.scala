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

import persistent4s.{ProjectionCheckpointConflict, ProjectionCheckpointState, ProjectionCommit}

import org.testcontainers.containers.PostgreSQLContainer
import org.typelevel.log4cats.Logger
import skunk.*
import skunk.codec.all.*
import skunk.implicits.*
import weaver.IOSuite

object PostgresAtomicRepositorySuite extends IOSuite:

  given Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  override def maxParallelism: Int = 1

  final case class Resources(
    pool: Resource[IO, Session[IO]],
    checkpoint: PostgresProjectionCheckpoint[IO],
  )

  type Res = Resources

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
            .eval(
              pool.use { session =>
                session.execute(PostgresProjectionCheckpoint.createTableCommand).void *>
                  session.execute(createStateTable).void
              },
            )
            .as(Resources(pool, PostgresProjectionCheckpoint.make(pool)))
        }
    }

  final private class TestRepository(
    pool: Resource[IO, Session[IO]],
    failAfterWrite: Boolean = false,
  ) extends PostgresAtomicRepository[IO, String, Int](pool):

    protected val table: PostgresRepositoryTable[IO, String, Int] =
      PostgresRepositoryTable(
        fetch = (session, keys) =>
          keys
            .traverse(key => session.option(findQuery)(key).map(_.map(key -> _)))
            .map(_.flatten),
        delete = (session, keys) => keys.traverse_(key => session.execute(deleteCommand)(key).void),
        upsert = (session, entries) =>
          entries.traverse_ { case (key, value) =>
            session.execute(upsertCommand)(key *: value *: EmptyTuple).void
          } *> (if failAfterWrite then IO.raiseError(new RuntimeException("write failed")) else IO.unit),
      )

  final private class RecordingRepository(
    pool: Resource[IO, Session[IO]],
    existing: Map[String, Int] = Map.empty,
    batchSize: Int = 2,
  ) extends PostgresAtomicRepository[IO, String, Int](pool):

    var fetchCalls: List[List[String]] = Nil

    var deleteCalls: List[List[String]] = Nil

    var upsertCalls: List[List[(String, Int)]] = Nil

    var operations: List[String] = Nil

    protected val table: PostgresRepositoryTable[IO, String, Int] =
      PostgresRepositoryTable(
        fetch = (_, keys) =>
          IO {
            fetchCalls = fetchCalls :+ keys
            keys.flatMap(key => existing.get(key).map(key -> _))
          },
        delete = (_, keys) =>
          IO {
            deleteCalls = deleteCalls :+ keys
            operations = operations :+ "delete"
          },
        upsert = (_, entries) =>
          IO {
            upsertCalls = upsertCalls :+ entries
            operations = operations :+ "upsert"
          },
        batchSize = batchSize,
      )

  final private case class EmbeddedState(id: String, value: Int)

  final private class RowsRepository(
    pool: Resource[IO, Session[IO]],
    existing: List[EmbeddedState],
  ) extends PostgresAtomicRepository[IO, String, EmbeddedState](pool):

    var fetchCalls: List[List[String]] = Nil

    var upsertCalls: List[List[EmbeddedState]] = Nil

    protected val table: PostgresRepositoryTable[IO, String, EmbeddedState] =
      PostgresRepositoryTable.rows(
        keyOf = _.id,
        fetch = (_, keys) =>
          IO {
            fetchCalls = fetchCalls :+ keys
            existing.filter(state => keys.contains(state.id))
          },
        delete = (_, _) => IO.unit,
        upsert = (_, states) => IO { upsertCalls = upsertCalls :+ states },
        batchSize = 2,
      )

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

  private def fresh(prefix: String): IO[String] =
    IO(s"$prefix-${UUID.randomUUID()}")

  test("empty reads and writes do not invoke table callbacks") { resources =>
    val repository = RecordingRepository(resources.pool)

    for
      found <- repository.findMany(Nil)
      _     <- repository.persist(Map.empty, Nil)
    yield expect.all(
      found.isEmpty,
      repository.fetchCalls.isEmpty,
      repository.deleteCalls.isEmpty,
      repository.upsertCalls.isEmpty,
    )
  }

  test("findMany deduplicates and chunks keys, reconstructs missing entries, and find reuses the batch query") {
    resources =>
      val repository = RecordingRepository(resources.pool, Map("a" -> 1, "b" -> 2))

      for
        found <- repository.findMany(List("a", "a", "missing", "b"))
        one   <- repository.find("a")
      yield expect.all(
        found == Map("a" -> Some(1), "missing" -> None, "b" -> Some(2)),
        one.contains(1),
        repository.fetchCalls == List(List("a", "missing"), List("b"), List("a")),
      )
  }

  test("rows derives fetched keys and unwraps keyed upserts for state-containing-key tables") { resources =>
    val a = EmbeddedState("a", 1)
    val b = EmbeddedState("b", 2)
    val repository = RowsRepository(resources.pool, List(a, b))

    for
      found <- repository.findMany(List("a", "missing", "b"))
      _     <- repository.persist(Map("a" -> a.copy(value = 10), "b" -> b.copy(value = 20)), Nil)
    yield expect.all(
      found == Map("a" -> Some(a), "missing" -> None, "b" -> Some(b)),
      repository.fetchCalls == List(List("a", "missing"), List("b")),
      repository.upsertCalls.flatten.toSet == Set(a.copy(value = 10), b.copy(value = 20)),
    )
  }

  test("rows rejects an upsert whose map key differs from its state key before invoking SQL") { resources =>
    val repository = RowsRepository(resources.pool, Nil)
    val state = EmbeddedState("state-key", 1)

    for
      name      <- fresh("projection")
      checkpoint = ProjectionCheckpointState(name, 1L, true, None)
      result    <- repository
                  .persistAtomically(ProjectionCommit(Map("map-key" -> state), Nil, -1L, checkpoint))
                  .attempt
      saved <- resources.checkpoint.load(name)
    yield expect.all(
      result.left.exists(
        _.getMessage ==
          "PostgresRepositoryTable.rows key mismatch: upsert key [map-key] does not match state key [state-key]",
      ),
      repository.upsertCalls.isEmpty,
      saved.isEmpty,
    )
  }

  test("writes deduplicate and chunk deletes before chunking upserts") { resources =>
    val repository = RecordingRepository(resources.pool, batchSize = 2)

    repository
      .persist(
        Map("a" -> 1, "b" -> 2, "c" -> 3),
        List("d", "d", "e", "f"),
      )
      .map { _ =>
        expect.all(
          repository.deleteCalls == List(List("d", "e"), List("f")),
          repository.deleteCalls.flatten.distinct == List("d", "e", "f"),
          repository.upsertCalls.map(_.size) == List(2, 1),
          repository.upsertCalls.flatten.toMap == Map("a" -> 1, "b" -> 2, "c" -> 3),
          repository.operations == List("delete", "delete", "upsert", "upsert"),
        )
      }
  }

  test("commits projection state and checkpoint together") { resources =>
    val repository = TestRepository(resources.pool)
    for
      key   <- fresh("state")
      name  <- fresh("projection")
      next   = ProjectionCheckpointState(name, 7L, true, None)
      _     <- repository.persistAtomically(ProjectionCommit(Map(key -> 42), Nil, -1L, next))
      state <- repository.findMany(key :: Nil)
      saved <- resources.checkpoint.load(name)
    yield expect.all(
      state.get(key).flatten.contains(42),
      saved.contains(next),
    )
  }

  test("a stale checkpoint rolls back the state write") { resources =>
    val repository = TestRepository(resources.pool)
    for
      key       <- fresh("state")
      name      <- fresh("projection")
      current    = ProjectionCheckpointState(name, 5L, true, None)
      next       = current.copy(globalPosition = 6L)
      _         <- resources.checkpoint.save(current)
      result    <- repository.persistAtomically(ProjectionCommit(Map(key -> 42), Nil, 4L, next)).attempt
      state     <- repository.findMany(key :: Nil)
      persisted <- resources.checkpoint.load(name)
    yield expect.all(
      result.left.exists(_.isInstanceOf[ProjectionCheckpointConflict]),
      state.get(key).flatten.isEmpty,
      persisted.contains(current),
    )
  }

  test("a non-initial expected position cannot create a missing checkpoint") { resources =>
    val repository = TestRepository(resources.pool)
    for
      key        <- fresh("state")
      name       <- fresh("projection")
      next        = ProjectionCheckpointState(name, 6L, true, None)
      result     <- repository.persistAtomically(ProjectionCommit(Map(key -> 42), Nil, 5L, next)).attempt
      state      <- repository.findMany(key :: Nil)
      checkpoint <- resources.checkpoint.load(name)
    yield expect.all(
      result.left.exists(_.isInstanceOf[ProjectionCheckpointConflict]),
      state.get(key).flatten.isEmpty,
      checkpoint.isEmpty,
    )
  }

  test("a state write failure rolls back the checkpoint and state") { resources =>
    val repository = TestRepository(resources.pool, failAfterWrite = true)
    for
      key        <- fresh("state")
      name       <- fresh("projection")
      next        = ProjectionCheckpointState(name, 1L, true, None)
      result     <- repository.persistAtomically(ProjectionCommit(Map(key -> 42), Nil, -1L, next)).attempt
      state      <- repository.findMany(key :: Nil)
      checkpoint <- resources.checkpoint.load(name)
    yield expect.all(
      result.isLeft,
      state.get(key).flatten.isEmpty,
      checkpoint.isEmpty,
    )
  }

  private val createStateTable: Command[Void] =
    sql"""
      CREATE TABLE IF NOT EXISTS atomic_projection_test (
        state_key TEXT PRIMARY KEY,
        value INT NOT NULL
      )
    """.command

  private val findQuery: Query[String, Int] =
    sql"SELECT value FROM atomic_projection_test WHERE state_key = $text".query(int4)

  private val upsertCommand: Command[String *: Int *: EmptyTuple] =
    sql"""
      INSERT INTO atomic_projection_test (state_key, value)
      VALUES ($text, $int4)
      ON CONFLICT (state_key) DO UPDATE SET value = EXCLUDED.value
    """.command

  private val deleteCommand: Command[String] =
    sql"DELETE FROM atomic_projection_test WHERE state_key = $text".command
