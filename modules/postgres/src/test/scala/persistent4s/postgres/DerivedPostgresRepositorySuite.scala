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

  final case class Resources(
    pool: Resource[IO, Session[IO]],
    scalar: DerivedPostgresRepository[IO, UUID, ScalarState],
    composite: DerivedPostgresRepository[IO, (UUID, UUID), CompositeState],
    checkpoint: PostgresProjectionCheckpoint[IO],
  )

  type Res = Resources

  override def sharedResource: Resource[IO, Res] =
    PostgresContainer.resource(maxConnections = 4).flatMap { config =>
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

  private val scalarTable: PostgresTable[UUID, ScalarState] =
    PostgresTable
      .derived[ScalarState]("derived_scalar_state")
      .key(_.rowId)
      .withBatchSize(2)

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

