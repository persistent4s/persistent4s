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

import scala.compiletime.testing.typeCheckErrors

import cats.effect.{IO, Resource}
import cats.syntax.all.*

import org.testcontainers.containers.PostgreSQLContainer
import org.typelevel.log4cats.Logger
import skunk.codec.all.*
import skunk.exception.PostgresErrorException
import skunk.implicits.*
import skunk.{Command, Session, Void}
import weaver.IOSuite

object PostgresGeneratedWriteSuite extends IOSuite:

  given Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  override def maxParallelism: Int = 1

  final case class State(
    rowId: UUID,
    displayName: String,
    note: Option[String],
    revision: Int,
    active: Boolean,
  ) derives PostgresRow

  final case class CompositeState(tenantId: UUID, itemId: UUID, displayName: String, revision: Int)
      derives PostgresRow

  final case class Resources(
    pool: Resource[IO, Session[IO]],
    repository: DerivedPostgresRepository[IO, UUID, State],
    composite: DerivedPostgresRepository[IO, (UUID, UUID), CompositeState],
  )

  type Res = Resources

  override def sharedResource: Resource[IO, Res] =
    postgresContainerResource.flatMap { container =>
      Session
        .Builder[IO]
        .withHost(container.getHost)
        .withPort(container.getMappedPort(5432))
        .withUserAndPassword(container.getUsername, container.getPassword)
        .withDatabase(container.getDatabaseName)
        .pooled(4)
        .evalTap(pool =>
          pool.use(session => List(createStateTable, createCompositeTable, createReceiptsTable).traverse_(session.execute)),
        )
        .map(pool => Resources(pool, DerivedPostgresRepository(pool, stateTable), DerivedPostgresRepository(pool, compositeTable)))
    }

  test("insert reports primary-key conflicts without replacing saved values") { resources =>
    val initial = freshState()
    val conflicting = initial.copy(displayName = s"replacement-${initial.rowId}", note = None, revision = 99)

    for
      inserted <- resources.pool.use(resources.repository.insert(_, initial))
      duplicate <- resources.pool.use(resources.repository.insert(_, conflicting))
      saved <- resources.repository.find(initial.rowId)
    yield expect.all(inserted, !duplicate, saved.contains(initial))
  }

  test("insert does not hide unrelated unique-constraint violations") { resources =>
    val initial = freshState()
    val conflicting = initial.copy(rowId = UUID.randomUUID())

    for
      _ <- resources.pool.use(resources.repository.insert(_, initial))
      result <- resources.pool.use(resources.repository.insert(_, conflicting)).attempt
      existing <- resources.repository.find(initial.rowId)
      rejected <- resources.repository.find(conflicting.rowId)
    yield expect.all(
      result.left.toOption.collect { case error: PostgresErrorException => error.code }.contains("23505"),
      existing.contains(initial),
      rejected.isEmpty,
    )
  }

  test("conditional updates reject missing rows and stale or mismatched revisions") { resources =>
    val initial = freshState().copy(revision = 7)
    val repository = resources.repository

    for
      _ <- resources.pool.use(repository.insert(_, initial))
      missing <- resources.pool.use(repository.update(UUID.randomUUID()).set(_.revision)(8).run)
      stale <- resources.pool.use(repository.update(initial.rowId).set(_.revision)(7).whereLessThan(_.revision)(7).run)
      mismatch <- resources.pool.use(repository.update(initial.rowId).set(_.revision)(8).whereEqual(_.revision)(6).run)
      updated <- resources.pool.use(
        repository.update(initial.rowId).set(_.revision)(8).whereLessThan(_.revision)(8).whereEqual(_.active)(true).run,
      )
      saved <- repository.find(initial.rowId)
    yield expect.all(!missing, !stale, !mismatch, updated, saved.contains(initial.copy(revision = 8)))
  }

  test("partial updates preserve omitted fields and distinguish keeping, clearing, and setting a nullable field") { resources =>
    val initial = freshState()
    val repository = resources.repository

    for
      _ <- resources.pool.use(repository.insert(_, initial))
      kept <- resources.pool.use(
        repository.update(initial.rowId)
          .setWhenDefined(_.displayName)(Option.empty[String])
          .setWhenDefined(_.note)(Option.empty[Option[String]])
          .set(_.revision)(1)
          .run,
      )
      afterKeep <- repository.find(initial.rowId)
      cleared <- resources.pool.use(
        repository.update(initial.rowId).setWhenDefined(_.note)(Some(None)).whereEqual(_.note)(initial.note).run,
      )
      afterClear <- repository.find(initial.rowId)
      set <- resources.pool.use(
        repository.update(initial.rowId).setWhenDefined(_.note)(Some(Some("new note"))).whereEqual(_.note)(None).run,
      )
      afterSet <- repository.find(initial.rowId)
      _ <- resources.pool.use(repository.update(initial.rowId).set(_.note)(None).run)
      afterDirectClear <- repository.find(initial.rowId)
    yield expect.all(
      kept,
      cleared,
      set,
      afterKeep.contains(initial.copy(revision = 1)),
      afterClear.contains(initial.copy(revision = 1, note = None)),
      afterSet.contains(initial.copy(revision = 1, note = Some("new note"))),
      afterDirectClear == afterClear,
    )
  }

  test("generated writes honor renamed columns and every part of a composite key") { resources =>
    val tenant = UUID.randomUUID()
    val item = UUID.randomUUID()
    val initial = CompositeState(tenant, item, "initial", 3)
    val sameTenant = CompositeState(tenant, UUID.randomUUID(), "same tenant", 3)
    val sameItem = CompositeState(UUID.randomUUID(), item, "same item", 3)
    val repository = resources.composite

    for
      inserted <- resources.pool.use(session => List(initial, sameTenant, sameItem).traverse(repository.insert(session, _)))
      changed <- resources.pool.use(
        repository.update((tenant, item))
          .set(_.displayName)("renamed")
          .set(_.revision)(4)
          .whereLessThan(_.revision)(4)
          .run,
      )
      saved <- repository.find((tenant, item))
      siblingTenant <- repository.find((sameTenant.tenantId, sameTenant.itemId))
      siblingItem <- repository.find((sameItem.tenantId, sameItem.itemId))
    yield expect.all(
      inserted.forall(identity),
      changed,
      saved.contains(initial.copy(displayName = "renamed", revision = 4)),
      siblingTenant.contains(sameTenant),
      siblingItem.contains(sameItem),
    )
  }

  test("SQL-like strings are bound as values in inserts, assignments, and conditions") { resources =>
    val initial = freshState().copy(displayName = s"'; DROP TABLE generated_write_state; -- ${UUID.randomUUID()}")
    val replacement = "name = 'changed', active = false WHERE true; --"
    val repository = resources.repository

    for
      inserted <- resources.pool.use(repository.insert(_, initial))
      changed <- resources.pool.use(
        repository.update(initial.rowId)
          .set(_.displayName)(replacement)
          .whereEqual(_.displayName)(initial.displayName)
          .run,
      )
      saved <- repository.find(initial.rowId)
    yield expect.all(inserted, changed, saved.contains(initial.copy(displayName = replacement)))
  }

  test("concurrent conditional updates allow exactly one writer to claim a revision") { resources =>
    val initial = freshState()
    val repository = resources.repository

    for
      inserted <- repository.insert(initial)
      results <- List("first writer", "second writer").parTraverse { note =>
        repository.update(initial.rowId)
          .whereEqual(_.revision)(0)
          .set(_.revision)(1)
          .set(_.note)(Some(note))
          .run
          .map(note -> _)
      }
      saved <- repository.find(initial.rowId)
    yield
      val winners = results.collect { case (note, true) => note }
      expect.all(
        inserted,
        winners.size == 1,
        saved.contains(initial.copy(revision = 1, note = winners.headOption)),
      )
  }

  test("session-bound writes see uncommitted rows and never acquire a repository connection") { resources =>
    val initial = freshState()
    val unavailablePool = Resource.eval(IO.raiseError[Session[IO]](new AssertionError("Unexpected repository connection")))
    val repository = DerivedPostgresRepository(unavailablePool, stateTable)

    for
      result <- resources.pool.use { session =>
        session.transaction.use { _ =>
          for
            inserted <- repository.insert(session, initial)
            changed <- repository.update(initial.rowId).set(_.revision)(1).whereEqual(_.revision)(0).run(session)
            visible <- session.unique(stateTable.findManyQuery(1))(List(initial.rowId))
            _ <- session.execute(insertReceipt)(initial.rowId)
          yield (inserted, changed, visible)
        }
      }
      saved <- resources.repository.find(initial.rowId)
      receipt <- resources.pool.use(_.unique(receiptExists)(initial.rowId))
    yield expect.all(result._1, result._2, result._3 == initial.copy(revision = 1), saved.contains(result._3), receipt)
  }

  test("the caller's rollback undoes inserts, updates, and their event receipt together") { resources =>
    val initial = freshState()
    val inserted = freshState()
    val failure = new IllegalStateException("receipt processing failed")
    val repository = resources.repository

    for
      _ <- resources.pool.use(repository.insert(_, initial))
      result <- resources.pool.use { session =>
        session.transaction.use { _ =>
          repository.insert(session, inserted) *>
            repository.update(initial.rowId).set(_.revision)(1).run(session) *>
            session.execute(insertReceipt)(inserted.rowId) *>
            IO.raiseError[Unit](failure)
        }
      }.attempt
      existing <- repository.find(initial.rowId)
      absent <- repository.find(inserted.rowId)
      receipt <- resources.pool.use(_.unique(receiptExists)(inserted.rowId))
    yield expect.all(result == Left(failure), existing.contains(initial), absent.isEmpty, !receipt)
  }

  test("empty updates, key assignments, and duplicate assignments are rejected without changing rows") { resources =>
    val initial = freshState()
    val repository = resources.repository

    for
      _ <- resources.pool.use(repository.insert(_, initial))
      empty <- resources.pool.use(repository.update(initial.rowId).run).attempt
      omitted <- resources.pool.use(repository.update(initial.rowId).setWhenDefined(_.note)(None).run).attempt
      key <- IO(repository.update(initial.rowId).set(_.rowId)(UUID.randomUUID())).attempt
      duplicate <- IO(repository.update(initial.rowId).set(_.revision)(1).set(_.revision)(2)).attempt
      saved <- repository.find(initial.rowId)
    yield expect.all(
      empty.left.exists(_.isInstanceOf[IllegalArgumentException]),
      omitted.left.exists(_.isInstanceOf[IllegalArgumentException]),
      key.left.exists(_.isInstanceOf[IllegalArgumentException]),
      duplicate.left.exists(_.isInstanceOf[IllegalArgumentException]),
      saved.contains(initial),
    )
  }

  test("update selectors reject computed fields and values of the wrong type") { _ =>
    val computed = typeCheckErrors("""
      import cats.effect.IO
      import persistent4s.postgres.*
      final case class State(id: java.util.UUID, name: String) derives PostgresRow
      def invalid(repository: DerivedPostgresRepository[IO, java.util.UUID, State]) =
        repository.update(java.util.UUID.randomUUID()).set(_.name.toUpperCase)("NAME")
    """)
    val wrongType = typeCheckErrors("""
      import cats.effect.IO
      import persistent4s.postgres.*
      final case class State(id: java.util.UUID, revision: Int) derives PostgresRow
      def invalid(repository: DerivedPostgresRepository[IO, java.util.UUID, State]) =
        repository.update(java.util.UUID.randomUUID()).set(_.revision)("invalid")
    """)

    IO.pure(expect.all(computed.exists(_.message.contains("directly")), wrongType.nonEmpty))
  }

  private def freshState(): State =
    val id = UUID.randomUUID()
    State(id, s"name-$id", Some("original note"), 0, active = true)

  private val stateTable = PostgresTable.derived[State]("generated_write_state").key(_.rowId)

  private val compositeTable = PostgresTable
    .derived[CompositeState]("generated_write_composite")
    .key(state => (state.tenantId, state.itemId))
    .column(_.tenantId, "tenant")
    .column(_.itemId, "item")
    .column(_.displayName, "label")
    .column(_.revision, "version")

  private val createStateTable: Command[Void] = sql"""
    CREATE TABLE generated_write_state (
      row_id UUID PRIMARY KEY,
      display_name TEXT NOT NULL UNIQUE,
      note TEXT,
      revision INT NOT NULL,
      active BOOLEAN NOT NULL
    )
  """.command

  private val createCompositeTable: Command[Void] = sql"""
    CREATE TABLE generated_write_composite (
      tenant UUID NOT NULL,
      item UUID NOT NULL,
      label TEXT NOT NULL,
      version INT NOT NULL,
      PRIMARY KEY (tenant, item)
    )
  """.command

  private val createReceiptsTable: Command[Void] =
    sql"CREATE TABLE generated_write_receipts (event_id UUID PRIMARY KEY)".command

  private val insertReceipt = sql"INSERT INTO generated_write_receipts (event_id) VALUES ($uuid)".command

  private val receiptExists =
    sql"SELECT EXISTS (SELECT 1 FROM generated_write_receipts WHERE event_id = $uuid)".query(bool)

  private type Container = PostgreSQLContainer[Nothing]

  private def postgresContainerResource: Resource[IO, Container] =
    Resource.make {
      IO.blocking {
        val container = new PostgreSQLContainer[Nothing]("postgres:16-alpine")
        container.start()
        container
      }
    }(container => IO.blocking(container.stop()).handleErrorWith(_ => IO.unit))
