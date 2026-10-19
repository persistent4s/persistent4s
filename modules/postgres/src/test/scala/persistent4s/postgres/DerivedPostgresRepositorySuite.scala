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
import skunk.implicits.*
import skunk.{Command, Session, Void}
import weaver.IOSuite

object DerivedPostgresRepositorySuite extends IOSuite:

  given Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

  override def maxParallelism: Int = 1

  final case class ScalarState(
    rowId: UUID,
    displayName: String,
    note: Option[String],
    revision: Int,
    active: Boolean,
  ) derives PostgresRow

  final case class CompositeState(
    tenantId: UUID,
    itemId: UUID,
    payload: String,
    note: Option[String],
  ) derives PostgresRow

  final case class VarcharState(id: Long, value: String)

  final case class Resources(
    pool: Resource[IO, Session[IO]],
    scalar: DerivedPostgresRepository[IO, UUID, ScalarState],
    composite: DerivedPostgresRepository[IO, (UUID, UUID), CompositeState],
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
                  session.execute(createScalarTable).void *>
                  session.execute(createCompositeTable).void
              },
            )
            .as(
              Resources(
                pool,
                DerivedPostgresRepository(pool, scalarTable),
                DerivedPostgresRepository(pool, compositeTable),
                PostgresProjectionCheckpoint.make(pool),
              ),
            )
        }
    }

  test("derived scalar repository supports batched CRUD, all, nullable fields, and full-row updates") { resources =>
    val ids = List.fill(5)(UUID.randomUUID())
    val initial = ids.zipWithIndex.map { case (id, index) =>
      val note = if index % 2 == 0 then None else Some(s"note-$index")
      id -> ScalarState(id, s"name-$index", note, index, active = index % 2 == 0)
    }.toMap
    val missing = UUID.randomUUID()
    val updated = initial(ids(1)).copy(displayName = "renamed", note = None, revision = 99, active = true)

    for
      _        <- resources.scalar.persist(initial, Nil)
      found    <- resources.scalar.findMany(ids ++ List(ids.head, missing))
      all      <- resources.scalar.all
      _        <- resources.scalar.persist(Map(ids(1) -> updated), List(ids(0), ids(0), ids(4)))
      after    <- resources.scalar.findMany(List(ids(0), ids(1), ids(2), ids(4)))
      afterAll <- resources.scalar.all
    yield
      val relevantAll = all.filter(state => ids.contains(state.rowId)).map(state => state.rowId -> state).toMap
      val relevantAfterAll =
        afterAll.filter(state => ids.contains(state.rowId)).map(state => state.rowId -> state).toMap

      expect.all(
        found == initial.view.mapValues(Option(_)).toMap.updated(missing, None),
        relevantAll == initial,
        found(ids.head).exists(_.note.isEmpty),
        found(ids(1)).flatMap(_.note).contains("note-1"),
        after(ids(0)).isEmpty,
        after(ids(1)).contains(updated),
        after(ids(2)).contains(initial(ids(2))),
        after(ids(4)).isEmpty,
        relevantAfterAll == initial.removed(ids(0)).removed(ids(4)).updated(ids(1), updated),
      )
  }

  test("derived filters support equality, booleans, and chunked IN values") { resources =>
    val marker = UUID.randomUUID().toString
    val ids = List.fill(5)(UUID.randomUUID())
    val states = ids.zipWithIndex.map { case (id, index) =>
      id -> ScalarState(
        id,
        displayName = s"filter-$marker-$index",
        note = Some(s"equality-$marker-$index"),
        revision = index,
        active = index % 2 == 0,
      )
    }.toMap
    val missing = UUID.randomUUID()

    for
      _          <- resources.scalar.persist(states, Nil)
      byName     <- resources.scalar.filterBy(_.displayName).is(s"filter-$marker-3").run
      active     <- resources.scalar.filterBy(_.active).is(true).run
      inactive   <- resources.scalar.filterBy(_.active).is(false).run
      selected   <- resources.scalar.filterBy(_.rowId).in((ids ++ List(ids.head, missing, ids(1))).iterator).run
      emptyInput <- resources.scalar.filterBy(_.rowId).in(Iterator.empty).run
    yield
      val activeIds = active.iterator.map(_.rowId).filter(ids.contains).toSet
      val inactiveIds = inactive.iterator.map(_.rowId).filter(ids.contains).toSet
      val selectedById = selected.iterator.map(state => state.rowId -> state).toMap

      expect.all(
        byName == List(states(ids(3))),
        activeIds == Set(ids(0), ids(2), ids(4)),
        inactiveIds == Set(ids(1), ids(3)),
        selectedById == states,
        selected.size == ids.size,
        emptyInput.isEmpty,
      )
  }

  test("derived filters distinguish nullable equality, null, not-null, and mixed IN values") { resources =>
    val marker = UUID.randomUUID().toString
    val ids = List.fill(5)(UUID.randomUUID())
    val notes = List(
      None,
      Some(s"nullable-$marker-1"),
      Some(s"nullable-$marker-2"),
      Some(s"nullable-$marker-3"),
      Some(s"nullable-$marker-other"),
    )
    val states = ids
      .zip(notes)
      .zipWithIndex
      .map { case ((id, note), index) =>
        id -> ScalarState(id, s"nullable-filter-$marker-$index", note, index, active = false)
      }
      .toMap
    val mixedValues = List(None, notes(1), notes(2), notes(3), None)

    for
      _       <- resources.scalar.persist(states, Nil)
      oneNote <- resources.scalar.filterBy(_.note).is(notes(1)).run
      nulls   <- resources.scalar.filterBy(_.note).isNull.run
      present <- resources.scalar.filterBy(_.note).isNotNull.run
      mixed   <- resources.scalar.filterBy(_.note).in(mixedValues).run
    yield
      def relevantIds(rows: List[ScalarState]): Set[UUID] =
        rows.iterator.map(_.rowId).filter(ids.contains).toSet

      expect.all(
        oneNote == List(states(ids(1))),
        relevantIds(nulls) == Set(ids(0)),
        relevantIds(present) == Set(ids(1), ids(2), ids(3), ids(4)),
        relevantIds(mixed) == Set(ids(0), ids(1), ids(2), ids(3)),
        mixed.iterator.map(_.rowId).toList.distinct.size == mixed.size,
      )
  }

  test("lazy filters combine predicates and distinguish absent, empty, false, and NULL inputs") { resources =>
    val marker = UUID.randomUUID().toString
    val ids = List.fill(6)(UUID.randomUUID())
    val states = ids.zipWithIndex.map { case (id, index) =>
      id -> ScalarState(
        id,
        displayName = s"chain-$marker-$index",
        note = Option.when(index % 2 == 1)(s"chain-note-$marker-$index"),
        revision = index,
        active = index % 2 == 0,
      )
    }.toMap
    val names = ids.map(states(_).displayName)

    for
      _        <- resources.scalar.persist(states, Nil)
      inactive <- resources.scalar
                    .filterBy(_.displayName)
                    .in(Some(names))
                    .and(_.active)
                    .is(Some(false))
                    .run
      withoutActive <- resources.scalar
                         .filterBy(_.rowId)
                         .in(ids)
                         .and(_.active)
                         .is(Option.empty[Boolean])
                         .run
      withoutNames <- resources.scalar
                        .filterBy(_.rowId)
                        .in(ids)
                        .and(_.displayName)
                        .in(Option.empty[List[String]])
                        .run
      expectedAll <- resources.scalar.all
      allOmitted  <- resources.scalar
                      .filterBy(_.displayName)
                      .in(Option.empty[List[String]])
                      .and(_.active)
                      .is(Option.empty[Boolean])
                      .run
      emptyNames <- resources.scalar
                      .filterBy(_.rowId)
                      .in(ids)
                      .and(_.displayName)
                      .in(Some(List.empty[String]))
                      .run
      directEmpty <- resources.scalar
                       .filterBy(_.rowId)
                       .in(ids)
                       .and(_.displayName)
                       .in(List.empty[String])
                       .run
      multiIn <- resources.scalar
                   .filterBy(_.displayName)
                   .in(names.take(5))
                   .and(_.revision)
                   .in(List(2, 3, 4, 5, 100))
                   .run
      nullNotes <- resources.scalar
                     .filterBy(_.rowId)
                     .in(ids)
                     .and(_.note)
                     .is(None)
                     .run
    yield
      def relevantIds(rows: List[ScalarState]): Set[UUID] =
        rows.iterator.map(_.rowId).filter(ids.contains).toSet

      expect.all(
        relevantIds(inactive) == Set(ids(1), ids(3), ids(5)),
        relevantIds(withoutActive) == ids.toSet,
        relevantIds(withoutNames) == ids.toSet,
        allOmitted.toSet == expectedAll.toSet,
        emptyNames.isEmpty,
        directEmpty.isEmpty,
        relevantIds(multiIn) == Set(ids(2), ids(3), ids(4)),
        relevantIds(nullNotes) == Set(ids(0), ids(2), ids(4)),
      )
  }

  test("derived composite repository treats key pairs atomically for find, delete, and upsert") { resources =>
    val tenantA = UUID.randomUUID()
    val tenantB = UUID.randomUUID()
    val itemA = UUID.randomUUID()
    val itemB = UUID.randomUUID()

    val keyA = tenantA     -> itemA
    val keyB = tenantB     -> itemB
    val crossedA = tenantA -> itemB
    val crossedB = tenantB -> itemA
    val stateA = CompositeState(tenantA, itemA, "a", None)
    val stateB = CompositeState(tenantB, itemB, "b", Some("before"))
    val updatedB = stateB.copy(payload = "updated", note = None)

    for
      _        <- resources.composite.persist(Map(keyA -> stateA, keyB -> stateB), Nil)
      found    <- resources.composite.findMany(List(keyA, crossedA, keyB, crossedB))
      _        <- resources.composite.persist(Map(keyB -> updatedB), List(crossedA, crossedB))
      retained <- resources.composite.findMany(List(keyA, keyB))
      _        <- resources.composite.persist(Map.empty, List(keyA))
      deleted  <- resources.composite.findMany(List(keyA, keyB))
    yield expect.all(
      found == Map(keyA -> Some(stateA), crossedA -> None, keyB -> Some(stateB), crossedB -> None),
      retained == Map(keyA -> Some(stateA), keyB -> Some(updatedB)),
      deleted == Map(keyA -> None, keyB -> Some(updatedB)),
    )
  }

  test("ordering and pagination apply globally across shuffled IN batches") { resources =>
    isolatedScalar(resources).use { repository =>
      val states = paginationStates(8)
      val shuffled = List(7, 1, 5, 0, 6, 2, 4, 3, 7).map(states(_).rowId)

      for
        _    <- repository.persist(states.map(state => state.rowId -> state).toMap, Nil)
        page <- repository
                  .filterBy(_.rowId)
                  .in(shuffled)
                  .and(_.revision)
                  .in(List(7, 5, 3, 1, 6, 4, 2, 0))
                  .orderBy(_.revision)
                  .withLimit(3)
                  .withOffset(2L)
                  .run
      yield expect(page == states.slice(2, 5))
    }
  }

  test("descending ordering appends an ascending tie-breaker before pagination") { resources =>
    isolatedScalar(resources).use { repository =>
      val states = paginationStates(8).zipWithIndex.map { case (state, index) =>
        state.copy(revision = index / 2)
      }

      for
        _    <- repository.persist(states.map(state => state.rowId -> state).toMap, Nil)
        page <- repository.query
                  .orderByDescending(_.revision)
                  .orderBy(_.displayName)
                  .withOffset(1L)
                  .withLimit(4)
                  .run
      yield expect(page == List(7, 4, 5, 2).map(states(_)))
    }
  }

  test("omitting every optional filter still applies ordering and a row limit") { resources =>
    isolatedScalar(resources).use { repository =>
      val states = paginationStates(5)

      for
        _    <- repository.persist(states.map(state => state.rowId -> state).toMap, Nil)
        page <- repository
                  .filterBy(_.displayName)
                  .is(Option.empty[String])
                  .and(_.active)
                  .is(Option.empty[Boolean])
                  .orderBy(_.revision)
                  .withLimit(2)
                  .run
      yield expect(page == states.take(2))
    }
  }

  test("greater-than cursors return consecutive bounded pages") { resources =>
    isolatedScalar(resources).use { repository =>
      val states = paginationStates(7)

      for
        _     <- repository.persist(states.map(state => state.rowId -> state).toMap, Nil)
        first <- repository
                   .filterBy(_.revision)
                   .greaterThan(Option.empty[Int])
                   .orderBy(_.revision)
                   .withLimit(3)
                   .run
        second <- repository
                    .filterBy(_.revision)
                    .greaterThan(Some(first.last.revision))
                    .orderBy(_.revision)
                    .withLimit(3)
                    .run
        last <- repository
                  .filterBy(_.revision)
                  .greaterThan(second.last.revision)
                  .orderBy(_.revision)
                  .withLimit(3)
                  .run
      yield expect.all(
        first == states.take(3),
        second == states.slice(3, 6),
        last == states.drop(6),
      )
    }
  }

  test("a zero limit returns no rows") { resources =>
    isolatedScalar(resources).use { repository =>
      val states = paginationStates(3)

      for
        _    <- repository.persist(states.map(state => state.rowId -> state).toMap, Nil)
        page <- repository.query.orderBy(_.revision).withLimit(0).run
      yield expect(page.isEmpty)
    }
  }

  test("PostgreSQL applies default and custom limit caps after ordering and offset") { resources =>
    isolatedScalar(resources).use { repository =>
      val states = paginationStates(350)
      val base = repository.filterBy(_.revision).greaterThan(4).orderBy(_.revision)

      for
        _           <- repository.persist(states.map(state => state.rowId -> state).toMap, Nil)
        defaultPage <- base.withLimit(5000).withOffset(10L).run
        customPage  <- base.withLimit(5000, maxLimit = 300).withOffset(20L).run
        smallerPage <- base.withLimit(50, maxLimit = 300).withOffset(30L).run
        aliasPage   <- base.limit(Int.MaxValue, maxLimit = 300).offset(20L).run
        empty       <- base.withLimit(5000, maxLimit = 0).run
      yield expect.all(
        defaultPage == states.slice(15, 215),
        customPage == states.slice(25, 325),
        smallerPage == states.slice(35, 85),
        aliasPage == customPage,
        empty.isEmpty,
      )
    }
  }

  test("run rejects negative bounds before opening a connection or returning an empty page") { _ =>
    val pool = Resource.eval(IO.raiseError[Session[IO]](new AssertionError("Database connection must not be opened")))
    val repository = DerivedPostgresRepository(pool, scalarTable)
    val queries = List(
      repository.query.withLimit(-1)                                    -> PaginationError.NegativeLimit(-1),
      repository.query.withLimit(Int.MinValue, maxLimit = 0)            -> PaginationError.NegativeLimit(Int.MinValue),
      repository.query.withOffset(-1L)                                  -> PaginationError.NegativeOffset(-1L),
      repository.query.withOffset(Long.MinValue)                        -> PaginationError.NegativeOffset(Long.MinValue),
      repository.query.withLimit(0).withOffset(-1L)                     -> PaginationError.NegativeOffset(-1L),
      repository.filterBy(_.rowId).in(List.empty[UUID]).withLimit(-1)   -> PaginationError.NegativeLimit(-1),
      repository.filterBy(_.rowId).in(List.empty[UUID]).withOffset(-1L) -> PaginationError.NegativeOffset(-1L),
    )
    queries.traverse { case (query, expected) =>
      query.run.attempt.map(_.left.exists(_ == expected))
    }.map(results => expect(results.forall(identity)))
  }

  test("derived state and checkpoint commit together and a stale checkpoint rolls both back") { resources =>
    val id = UUID.randomUUID()
    val name = s"derived-${UUID.randomUUID()}"
    val initial = ScalarState(id, "committed", Some("initial"), 1, active = true)
    val rolledBack = initial.copy(displayName = "must-not-persist", revision = 2)
    val committed = ProjectionCheckpointState(name, 7L, running = true, None)
    val staleNext = committed.copy(globalPosition = 8L)

    for
      _          <- resources.scalar.persistAtomically(ProjectionCommit(Map(id -> initial), Nil, -1L, committed))
      state      <- resources.scalar.find(id)
      checkpoint <- resources.checkpoint.load(name)
      stale      <- resources.scalar
                 .persistAtomically(ProjectionCommit(Map(id -> rolledBack), Nil, 6L, staleNext))
                 .attempt
      finalState      <- resources.scalar.find(id)
      finalCheckpoint <- resources.checkpoint.load(name)
    yield expect.all(
      state.contains(initial),
      checkpoint.contains(committed),
      stale.left.exists(_.isInstanceOf[ProjectionCheckpointConflict]),
      finalState.contains(initial),
      finalCheckpoint.contains(committed),
    )
  }

  test("ranges, optional nullable inputs, and counts execute over the complete matching set") { resources =>
    isolatedScalar(resources).use { repository =>
      val states = paginationStates(5).zipWithIndex.map { (state, index) =>
        state.copy(note = Option.when(index > 0)(s"note-$index"), active = index % 2 == 0)
      }
      val ranged = repository.filterBy(_.revision).between(1, 3).and(_.revision).notEqualTo(2)
      for
        _       <- repository.persist(states.map(state => state.rowId -> state).toMap, Nil)
        rows    <- ranged.orderBy(_.revision).run
        total   <- ranged.withLimit(0).withOffset(100).count
        grouped <- repository.query.withLimit(1).withOffset(100).countBy(_.active)
        omitted <- repository.filterBy(_.note).inValues(None: Option[List[String]]).count
        empty   <- repository.filterBy(_.note).inValues(Some(List.empty[String])).count
        present <- repository.filterBy(_.note).inValues(Some(List("note-1", "note-3"))).orderBy(_.revision).run
        nulls   <- repository.filterBy(_.note).is(None).count
        others  <- repository.filterBy(_.note).notEqualTo(None).count
      yield expect.all(
        rows.map(_.revision) == List(1, 3),
        total == 2L,
        grouped == Map(true -> 3L, false -> 2L),
        omitted == 5L,
        empty == 0L,
        present.map(_.revision) == List(1, 3),
        nulls == 1L,
        others == 4L,
      )
    }
  }

  test("SQL subqueries require every child condition on the same row without duplicating parents") { resources =>
    isolatedScalar(resources).use { repository =>
      val states = paginationStates(3)
      val children = List(
        CompositeState(UUID.randomUUID(), states(0).rowId, "wanted", Some("other")),
        CompositeState(UUID.randomUUID(), states(0).rowId, "other", Some("wanted")),
        CompositeState(UUID.randomUUID(), states(1).rowId, "wanted", Some("wanted")),
        CompositeState(UUID.randomUUID(), states(1).rowId, "wanted", Some("wanted")),
      )
      val childQuery = resources.composite.filterBy(_.payload).is("wanted").and(_.note).is(Some("wanted"))
      val matches = repository.filterBy(_.rowId).in(childQuery.select(_.itemId))
      val emptyChildren = resources.composite.filterBy(_.itemId).in(List.empty[UUID]).select(_.itemId)
      for
        _     <- repository.persist(states.map(state => state.rowId -> state).toMap, Nil)
        _     <- resources.composite.persist(children.map(state => (state.tenantId, state.itemId) -> state).toMap, Nil)
        found <- matches.orderBy(_.revision).withLimit(200).run
        count <- matches.withLimit(0).count
        empty <- repository.filterBy(_.rowId).in(emptyChildren).count
      yield expect.all(found == List(states(1)), count == 1L, empty == 0L)
    }
  }

  test("counts and nested selections validate pagination before acquiring a connection") { _ =>
    val unavailable =
      Resource.eval[IO, Session[IO]](IO.raiseError(new AssertionError("Unexpected database connection")))
    val repository = DerivedPostgresRepository(unavailable, scalarTable)
    val inner = repository.query.withOffset(-7L).select(_.rowId)
    for
      count  <- repository.query.withLimit(-1).count.attempt
      group  <- repository.query.withOffset(-2L).countBy(_.active).attempt
      nested <- repository.filterBy(_.rowId).in(inner).run.attempt
    yield expect.all(
      count == Left(PaginationError.NegativeLimit(-1)),
      group == Left(PaginationError.NegativeOffset(-2L)),
      nested == Left(PaginationError.NegativeOffset(-7L)),
    )
  }

  test("generated table operations and filtered deletes share the caller's transaction") { resources =>
    val state = paginationStates(1).head
    val child = CompositeState(UUID.randomUUID(), state.rowId, "transaction", None)
    val failure = new AssertionError("Roll back both generated writes")
    for
      rolledBack <- resources.pool.use { session =>
                      session.transaction.use { _ =>
                        scalarTable.upsert(session, List(state)) *>
                          compositeTable.upsert(session, List(child)) *>
                          IO.raiseError[Unit](failure)
                      }
                    }.attempt
      absent      <- resources.scalar.find(state.rowId)
      childAbsent <- resources.composite.find((child.tenantId, child.itemId))
      read        <- resources.pool.use { session =>
                session.transaction.use { _ =>
                  for
                    _       <- scalarTable.upsert(session, List(state))
                    _       <- compositeTable.upsert(session, List(child))
                    rows    <- resources.scalar.filterBy(_.rowId).is(state.rowId).run(session)
                    fetched <- scalarTable.fetch(session, List(state.rowId, UUID.randomUUID()))
                    _       <- resources.composite.filterBy(_.itemId).is(state.rowId).delete(session)
                    _       <- scalarTable.delete(session, List(state.rowId))
                  yield rows -> fetched
                }
              }
      deleted      <- resources.scalar.find(state.rowId)
      childDeleted <- resources.composite.find((child.tenantId, child.itemId))
    yield expect.all(
      rolledBack == Left(failure),
      absent.isEmpty,
      childAbsent.isEmpty,
      read == (List(state) -> List(state)),
      deleted.isEmpty,
      childDeleted.isEmpty,
    )
  }

  test("related-row enrichment applies to find, all, and generated filters") { resources =>
    val state = paginationStates(1).head
    val repository = new DerivedPostgresRepository[IO, UUID, ScalarState](resources.pool, scalarTable):
      override protected def enrich(
        @scala.annotation.unused session: Session[IO],
        rows: List[ScalarState],
      ): IO[List[ScalarState]] =
        IO.pure(rows.map(row => row.copy(displayName = s"enriched-${row.displayName}")))
    val expected = state.copy(displayName = s"enriched-${state.displayName}")
    for
      _        <- repository.persist(Map(state.rowId -> state), Nil)
      found    <- repository.find(state.rowId)
      all      <- repository.all
      filtered <- repository.filterBy(_.rowId).is(state.rowId).run
      paged    <- repository.filterBy(_.rowId).is(state.rowId).withLimit(1).run
    yield expect.all(
      found.contains(expected),
      all.contains(expected),
      filtered == List(expected),
      paged == List(expected),
    )
  }

  test("text field filters and grouping respect varchar column overrides") { resources =>
    given PostgresRow[VarcharState] = PostgresRow
      .flattened[VarcharState](identity, PostgresRow.columnCodec[VarcharState](_.value)(skunk.codec.all.varchar(10)))
    val table = PostgresTable.derived[VarcharState]("derived_varchar_state").key(_.id)
    val repository = DerivedPostgresRepository(resources.pool, table)
    val setup = resources.pool
      .use(
        _.execute(sql"CREATE TABLE derived_varchar_state (id BIGINT PRIMARY KEY, value VARCHAR(10) NOT NULL)".command),
      )
      .void
    Resource.make(setup)(_ => resources.pool.use(_.execute(sql"DROP TABLE derived_varchar_state".command)).void).use {
      _ =>
        for
          _ <- repository.persist(
                 Map(1L -> VarcharState(1L, "FR"), 2L -> VarcharState(2L, "FR"), 3L -> VarcharState(3L, "DE")),
                 Nil,
               )
          rows   <- repository.filterBy(_.value).is("FR").orderBy(_.id).run
          counts <- repository.query.countBy(_.value)
        yield expect.all(rows.map(_.id) == List(1L, 2L), counts == Map("FR" -> 2L, "DE" -> 1L))
    }
  }

  private val scalarTable: PostgresTable[UUID, ScalarState] =
    PostgresTable
      .derived[ScalarState]("derived_scalar_state")
      .key(_.rowId)
      .withBatchSize(2)

  private def paginationStates(count: Int): List[ScalarState] =
    List.tabulate(count) { index =>
      ScalarState(UUID.randomUUID(), s"name-$index", None, index, active = true)
    }

  private def isolatedScalar(resources: Resources): Resource[IO, DerivedPostgresRepository[IO, UUID, ScalarState]] =
    val table = PostgresTable.derived[ScalarState]("derived_paged_state").key(_.rowId).withBatchSize(2)
    Resource
      .make(
        resources.pool
          .use(_.execute(sql"CREATE TABLE derived_paged_state (LIKE derived_scalar_state INCLUDING ALL)".command))
          .void,
      ) { _ =>
        resources.pool.use(_.execute(sql"DROP TABLE derived_paged_state".command)).void
      }
      .as(DerivedPostgresRepository(resources.pool, table))

  private val compositeTable: PostgresTable[(UUID, UUID), CompositeState] =
    PostgresTable
      .derived[CompositeState]("derived_composite_state")
      .key(state => (state.tenantId, state.itemId))
      .withBatchSize(2)

  private val createScalarTable: Command[Void] =
    sql"""
      CREATE TABLE derived_scalar_state (
        row_id UUID PRIMARY KEY,
        display_name TEXT NOT NULL,
        note TEXT,
        revision INT NOT NULL,
        active BOOLEAN NOT NULL
      )
    """.command

  private val createCompositeTable: Command[Void] =
    sql"""
      CREATE TABLE derived_composite_state (
        tenant_id UUID NOT NULL,
        item_id UUID NOT NULL,
        payload TEXT NOT NULL,
        note TEXT,
        PRIMARY KEY (tenant_id, item_id)
      )
    """.command

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
