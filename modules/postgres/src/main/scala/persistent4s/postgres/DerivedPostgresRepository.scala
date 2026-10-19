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

import cats.effect.{Async, Resource}
import cats.syntax.all.*

import skunk.{AppliedFragment, Codec, Session}
import skunk.codec.all.{bool, int8}

/** An atomic PostgreSQL projection repository whose standard persistence operations come from a [[PostgresTable]].
  *
  * Extend this class when a read model also needs domain-specific Skunk queries. [[all]], [[find]], generated writes,
  * and atomic checkpoint persistence need no application SQL.
  */
class DerivedPostgresRepository[F[_]: Async, K, S <: Product](
  pool: Resource[F, Session[F]],
  final protected val mapping: PostgresTable[K, S],
  customize: PostgresRepositoryTable[F, K, S] => PostgresRepositoryTable[F, K, S] =
    (table: PostgresRepositoryTable[F, K, S]) => table,
) extends PostgresAtomicRepository[F, K, S](pool):

  final override protected val table: PostgresRepositoryTable[F, K, S] =
    val base = customize(mapping.repositoryTable[F])
    base.withFetch { (session, keys) =>
      base
        .fetchRows(session, keys)
        .flatMap(rows => enrich(session, rows.map(_._2)))
        .map(_.map(state => mapping.keyOf(state) -> state))
    }

  /** Load related rows using the same connection. Override when a state includes data from other tables.
    *
    * @param session
    *   connection that loaded the rows; use it for related queries
    * @param rows
    *   decoded rows, already filtered, ordered, and paged
    * @return
    *   enriched rows in the same order, keeping their keys
    */
  protected def enrich(@scala.annotation.unused session: Session[F], rows: List[S]): F[List[S]] = Async[F].pure(rows)

  /** Load every row. No ordering is implied. */
  def all: F[List[S]] =
    useSession(session => session.execute(mapping.allQuery).flatMap(enrich(session, _)))

  /** Insert one row using a pooled connection. An existing key is left unchanged.
    *
    * @param state
    *   row to insert
    * @return
    *   true when inserted, or false when its key already exists
    */
  final def insert(state: S): F[Boolean] = useSession(insert(_, state))

  /** Insert one row using the caller's connection and transaction. Other unique or constraint violations fail normally.
    *
    * @param session
    *   caller's database connection
    * @param state
    *   row to insert
    * @return
    *   true when inserted, or false when its key already exists
    */
  final def insert(session: Session[F], state: S): F[Boolean] =
    Async[F].defer(session.option(mapping.insertQuery)(state).map(_.nonEmpty))

  /** Start an update for one key. Only explicitly assigned fields change.
    *
    * @param key
    *   row to update
    * @return
    *   an update builder with optional conditions
    */
  final def update(key: K): PostgresUpdate[F, K, S] =
    new PostgresUpdate(this, key, Nil, Nil)

  private[postgres] def updateAssignment[A](name: String, value: A, field: PostgresField[A]): AppliedFragment =
    mapping.updateAssignment(name, field.codec, value)

  private[postgres] def updateEquality[A](name: String, value: A, field: PostgresField[A]): AppliedFragment =
    mapping.updateEquality(name, field.codec, value)

  private[postgres] def updateLessThan[A](name: String, value: A, field: PostgresField[A]): AppliedFragment =
    mapping.updateLessThan(name, field.codec, value)

  private[postgres] def runUpdate(
    key: K,
    assignments: List[AppliedFragment],
    conditions: List[AppliedFragment],
  ): F[Boolean] =
    Async[F].defer {
      val statement = mapping.updateQuery(key, assignments, conditions)
      useSession(session => session.option(statement.fragment.query(bool))(statement.argument).map(_.nonEmpty))
    }

  private[postgres] def runUpdate(
    session: Session[F],
    key: K,
    assignments: List[AppliedFragment],
    conditions: List[AppliedFragment],
  ): F[Boolean] =
    Async[F].defer {
      val statement = mapping.updateQuery(key, assignments, conditions)
      session.option(statement.fragment.query(bool))(statement.argument).map(_.nonEmpty)
    }

  /** Start a query without filters. Add ordering and a limit before loading a large table.
    *
    * @return
    *   an immutable query over this repository
    */
  final def query: PostgresFilterQuery[F, K, S] =
    new PostgresFilterQuery(this, PostgresFilterPlan())

  /** Select one scalar state field to filter. Flattened nested fields may use direct selection or
    * `Option.map`/`flatMap`, such as `_.address.flatMap(_.city)`.
    *
    * Example: `repository.filterBy(_.active).is(true).run`, or
    * `repository.filterBy(_.author).in(authors).and(_.availableCopies).is(Some(1)).run`.
    */
  final inline def filterBy[A](inline field: S => A)(using
    postgresField: PostgresField[A],
  ): PostgresFieldFilter[F, K, S, A] =
    ${ DerivedPostgresRepositoryMacros.filterBy[F, K, S, A]('this, 'field, 'postgresField) }

  /** Low-level filter constructor used by the inline field-selector API. */
  final def filterField[A](
    scalaFieldName: String,
    postgresField: PostgresField[A],
    optional: Boolean,
  ): PostgresFieldFilter[F, K, S, A] =
    query.filterField(scalaFieldName, postgresField, optional)

  private[postgres] def addValueFilter[A](
    plan: PostgresFilterPlan,
    scalaFieldName: String,
    value: A,
    postgresField: PostgresField[A],
    optional: Boolean,
  ): PostgresFilterPlan =
    val predicate =
      if optional && value == None then mapping.filterNullPredicate(scalaFieldName)
      else mapping.filterValuePredicate(scalaFieldName, postgresField.codec, value)

    plan.append(PostgresFilterClause(predicate :: Nil))

  private[postgres] def addGreaterThanFilter[A](
    plan: PostgresFilterPlan,
    scalaFieldName: String,
    value: A,
    postgresField: PostgresField[A],
  ): PostgresFilterPlan =
    plan.append(
      PostgresFilterClause(mapping.filterGreaterThanPredicate(scalaFieldName, postgresField.codec, value) :: Nil),
    )

  private[postgres] def addNotEqualFilter[A](
    plan: PostgresFilterPlan,
    name: String,
    value: A,
    field: PostgresField[A],
  ): PostgresFilterPlan =
    plan.append(PostgresFilterClause(mapping.filterNotEqualPredicate(name, field.codec, value) :: Nil))

  private[postgres] def addBetweenFilter[A](
    plan: PostgresFilterPlan,
    name: String,
    from: A,
    to: A,
    field: PostgresField[A],
  ): PostgresFilterPlan =
    plan.append(PostgresFilterClause(mapping.filterBetweenPredicate(name, field.codec, from, to) :: Nil))

  private[postgres] def addSelectionFilter[A](
    plan: PostgresFilterPlan,
    name: String,
    selection: PostgresSelection[F, A],
  ): PostgresFilterPlan =
    plan
      .append(PostgresFilterClause(mapping.filterSelectionPredicate(name, selection) :: Nil))
      .copy(validations = plan.validations :+ selection.validate)

  private[postgres] def addValuesFilter[A](
    plan: PostgresFilterPlan,
    scalaFieldName: String,
    values: IterableOnce[A],
    postgresField: PostgresField[A],
    optional: Boolean,
  ): PostgresFilterPlan =
    val allValues = values.iterator.toList.distinct

    if allValues.isEmpty then plan.noMatches
    else
      val (includeNull, presentValues) =
        if optional then
          val (nullValues, nonNullValues) = allValues.partition(_ == None)
          nullValues.nonEmpty -> nonNullValues
        else false -> allValues

      val alternatives =
        if presentValues.isEmpty then mapping.filterNullPredicate(scalaFieldName) :: Nil
        else
          presentValues
            .grouped(mapping.batchSize)
            .zipWithIndex
            .map { case (chunk, index) =>
              mapping.filterValuesPredicate(
                scalaFieldName,
                postgresField.codec,
                chunk,
                includeNull = includeNull && index == 0,
              )
            }
            .toList

      plan.append(PostgresFilterClause(alternatives))

  private[postgres] def addNullFilter(plan: PostgresFilterPlan, scalaFieldName: String): PostgresFilterPlan =
    plan.append(PostgresFilterClause(mapping.filterNullPredicate(scalaFieldName) :: Nil))

  private[postgres] def addNotNullFilter(plan: PostgresFilterPlan, scalaFieldName: String): PostgresFilterPlan =
    plan.append(PostgresFilterClause(mapping.filterNotNullPredicate(scalaFieldName) :: Nil))

  private[postgres] def validateFilter(plan: PostgresFilterPlan): F[Unit] = Async[F].delay(plan.validate())

  private[postgres] def selectFilter[A](
    plan: PostgresFilterPlan,
    name: String,
    field: PostgresField[A],
  ): PostgresSelection[F, A] =
    new PostgresSelection(field.codec, mapping.selectionQuery(name, field.codec, plan), () => plan.validate())

  private[postgres] def countFilter(plan: PostgresFilterPlan): F[Long] =
    validateFilter(plan) *> Async[F].defer {
      if plan.matchesNothing then Async[F].pure(0L)
      else
        useSession { session =>
          val applied = mapping.countQuery(plan)
          session.unique(applied.fragment.query(int8))(applied.argument)
        }
    }

  private[postgres] def countFilterBy[A](
    plan: PostgresFilterPlan,
    name: String,
    field: PostgresField[A],
  ): F[Map[A, Long]] =
    validateFilter(plan) *> Async[F].defer {
      if plan.matchesNothing then Async[F].pure(Map.empty)
      else
        useSession { session =>
          val applied = mapping.countByQuery(name, field.codec, plan)
          session
            .execute(applied.fragment.query(mapping.mappedFieldCodec(name, field.codec) *: int8))(applied.argument)
            .map(_.toMap)
        }
    }

  private[postgres] def deleteFilter(session: Session[F], plan: PostgresFilterPlan): F[Unit] =
    validateFilter(plan) *> Async[F].defer {
      val applied = mapping.deleteQuery(plan)
      session.execute(applied.fragment.command)(applied.argument).void
    }

  private[postgres] def runFilter(plan: PostgresFilterPlan): F[List[S]] =
    validateFilter(plan) *> Async[F].defer {
      if plan.matchesNothing || plan.limit.contains(0) then Async[F].pure(List.empty)
      else useSession(session => runFilter(session, plan))
    }

  private[postgres] def runFilter(session: Session[F], plan: PostgresFilterPlan): F[List[S]] =
    validateFilter(plan) *> Async[F].defer {
      if plan.matchesNothing || plan.limit.contains(0) then Async[F].pure(List.empty)
      else if plan.hasQueryOptions then
        val applied = mapping.filterQuery(plan)
        session.execute(applied.fragment.query(mapping.row.codec))(applied.argument).flatMap(enrich(session, _))
      else if plan.clauses.isEmpty then session.execute(mapping.allQuery).flatMap(enrich(session, _))
      else
        val predicateCombinations =
          plan.clauses.foldLeft(List(List.empty[skunk.AppliedFragment])) { (combinations, clause) =>
            for
              combination <- combinations
              alternative <- clause.alternatives
            yield combination :+ alternative
          }

        predicateCombinations.traverse { predicates =>
          val applied = mapping.filterQuery(predicates)
          session.execute(applied.fragment.query(mapping.row.codec))(applied.argument)
        }
          .map(_.flatten.distinctBy(mapping.keyOf))
          .flatMap(enrich(session, _))
    }

  /** The derived row codec, for additional typed Skunk queries in concrete repositories. */
  final protected val rowCodec: Codec[S] = mapping.row.codec

object DerivedPostgresRepository:

  def apply[F[_]: Async, K, S <: Product](
    pool: Resource[F, Session[F]],
    table: PostgresTable[K, S],
  ): DerivedPostgresRepository[F, K, S] =
    new DerivedPostgresRepository(pool, table)

  /** Build a derived repository while replacing one or more generated session-bound callbacks. */
  def customized[F[_]: Async, K, S <: Product](
    pool: Resource[F, Session[F]],
    table: PostgresTable[K, S],
  )(
    customize: PostgresRepositoryTable[F, K, S] => PostgresRepositoryTable[F, K, S],
  ): DerivedPostgresRepository[F, K, S] =
    new DerivedPostgresRepository(pool, table, customize)
