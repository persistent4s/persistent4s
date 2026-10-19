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

import skunk.{AppliedFragment, Session}

/** An immutable, parameterized `AND` query over a derived PostgreSQL state.
  *
  * Build one with [[DerivedPostgresRepository.query]] or [[DerivedPostgresRepository.filterBy]], then call [[run]].
  * Filtering, ordering, and pagination run in PostgreSQL. Without [[withLimit]], `run` loads every matching row.
  */
final class PostgresFilterQuery[F[_], K, S <: Product] private[postgres] (
  private[postgres] val repository: DerivedPostgresRepository[F, K, S],
  private[postgres] val plan: PostgresFilterPlan,
):

  /** Select another scalar field, including a flattened nested field, and append its condition with SQL `AND`. */
  final inline def and[A](inline field: S => A)(using
    postgresField: PostgresField[A],
  ): PostgresFieldFilter[F, K, S, A] =
    ${ DerivedPostgresRepositoryMacros.and[F, K, S, A]('this, 'field, 'postgresField) }

  /** Append an ascending SQL ordering. Add a unique field last for stable pagination.
    *
    * @param field
    *   scalar field to order by, including a flattened nested field
    * @return
    *   the query with this ordering appended
    */
  final inline def orderBy[A](inline field: S => A): PostgresFilterQuery[F, K, S] =
    ${ DerivedPostgresRepositoryMacros.orderBy[F, K, S, A]('this, 'field, '{ false }) }

  /** Append a descending SQL ordering. PostgreSQL's default null ordering applies.
    *
    * @param field
    *   scalar field to order by, including a flattened nested field
    * @return
    *   the query with this ordering appended
    */
  final inline def orderByDescending[A](inline field: S => A): PostgresFilterQuery[F, K, S] =
    ${ DerivedPostgresRepositoryMacros.orderBy[F, K, S, A]('this, 'field, '{ true }) }

  /** Limit the rows returned by PostgreSQL to at most 200. [[run]] rejects a negative count.
    *
    * @param count
    *   maximum row count; zero returns no rows
    * @return
    *   the query with this limit
    */
  def withLimit(count: Int): PostgresFilterQuery[F, K, S] =
    withLimit(count, maxLimit = 200)

  /** Limit the rows returned by PostgreSQL, capped at the supplied maximum. [[run]] rejects a negative count or
    * maximum.
    *
    * @param count
    *   requested row count; zero returns no rows
    * @param maxLimit
    *   maximum permitted row count; zero returns no rows
    * @return
    *   the query with the smaller of the requested count and maximum
    */
  def withLimit(count: Int, maxLimit: Int): PostgresFilterQuery[F, K, S] =
    copy(plan = plan.copy(limit = Some(PostgresPagination.cap(count, maxLimit)), maxLimit = maxLimit))

  /** Skip rows in PostgreSQL. [[run]] rejects a negative count. For deep pages, prefer a filter on the last returned
    * key.
    *
    * @param count
    *   non-negative number of rows to skip
    * @return
    *   the query with this offset
    */
  def withOffset(count: Long): PostgresFilterQuery[F, K, S] =
    copy(plan = plan.copy(offset = Some(count)))

  /** Alias for [[withLimit]].
    *
    * @param count
    *   maximum row count; zero returns no rows
    * @return
    *   the query with this limit
    */
  def limit(count: Int): PostgresFilterQuery[F, K, S] = withLimit(count)

  /** Alias for [[withLimit]] with a configurable maximum.
    *
    * @param count
    *   requested row count; zero returns no rows
    * @param maxLimit
    *   maximum permitted row count; zero returns no rows
    * @return
    *   the query with the smaller of the requested count and maximum
    */
  def limit(count: Int, maxLimit: Int): PostgresFilterQuery[F, K, S] = withLimit(count, maxLimit)

  /** Alias for [[withOffset]].
    *
    * @param count
    *   number of rows to skip
    * @return
    *   the query with this offset
    */
  def offset(count: Long): PostgresFilterQuery[F, K, S] = withOffset(count)

  /** Execute this query and load its results. No ordering is implied unless explicitly supplied. Ordered or paged
    * queries use one SQL statement and must fit PostgreSQL's parameter limit. Negative limits and offsets fail with
    * [[PaginationError]] before opening a database connection. A negative maximum fails with
    * [[IllegalArgumentException]].
    *
    * @return
    *   matching rows, ordered and bounded as requested
    */
  def run: F[List[S]] = repository.runFilter(plan)

  /** Execute this query using the caller's connection and transaction.
    *
    * @param session
    *   caller's database connection
    * @return
    *   matching rows, ordered and bounded as requested
    */
  def run(session: Session[F]): F[List[S]] = repository.runFilter(session, plan)

  /** Check pagination without opening a database connection.
    *
    * @return
    *   `Unit` when valid; otherwise [[PaginationError]], or an `IllegalArgumentException` for a negative maximum
    */
  def validate: F[Unit] = repository.validateFilter(plan)

  /** Count every matching row in PostgreSQL, ignoring this query's ordering and pagination. Pagination must still be
    * valid. Limits inside an `IN` subquery remain part of its filter.
    *
    * @return
    *   total matching rows, including when the requested page is empty
    */
  def count: F[Long] = repository.countFilter(plan)

  /** Count matching rows by one field in PostgreSQL, ignoring this query's ordering and pagination. Pagination must
    * still be valid. Null values form one group for optional fields.
    *
    * @param field
    *   field whose values form the groups
    * @return
    *   row count for each distinct field value
    */
  inline def countBy[A](inline field: S => A)(using postgresField: PostgresField[A]): F[Map[A, Long]] =
    ${ DerivedPostgresRepositoryMacros.countBy[F, K, S, A]('this, 'field, 'postgresField) }

  /** Select one column for use in another query's SQL `IN` condition. This retains the filters, ordering, and
    * pagination and does not load rows into memory.
    *
    * @param field
    *   scalar field to select
    * @return
    *   a selection that can be passed to [[PostgresFieldFilter.in]]
    */
  inline def select[A](inline field: S => A)(using postgresField: PostgresField[A]): PostgresSelection[F, A] =
    ${ DerivedPostgresRepositoryMacros.select[F, K, S, A]('this, 'field, 'postgresField) }

  /** Apply additional conditions only when an input is present.
    *
    * @param value
    *   optional input
    * @param f
    *   query changes to apply when the input is present
    * @return
    *   the changed query, or this query when absent
    */
  def whenDefined[A](value: Option[A])(
    f: (PostgresFilterQuery[F, K, S], A) => PostgresFilterQuery[F, K, S],
  ): PostgresFilterQuery[F, K, S] =
    value.fold(this)(f(this, _))

  /** Delete matching rows using the caller's transaction. Ordering and pagination are not allowed. An unfiltered query
    * deletes every row, and an empty `IN` condition deletes none.
    *
    * @param session
    *   caller's database connection
    * @return
    *   `Unit` when matching rows are removed
    */
  def delete(session: Session[F]): F[Unit] = repository.deleteFilter(session, plan)

  /** Low-level scalar selection used by [[select]]. */
  def selectField[A](name: String, field: PostgresField[A]): PostgresSelection[F, A] =
    repository.selectFilter(plan, name, field)

  /** Low-level grouping used by [[countBy]]. */
  def countField[A](name: String, field: PostgresField[A]): F[Map[A, Long]] =
    repository.countFilterBy(plan, name, field)

  /** Low-level ordering constructor used by the inline field-selector API. */
  final def orderField(scalaFieldName: String, descending: Boolean): PostgresFilterQuery[F, K, S] =
    copy(plan = plan.copy(ordering = plan.ordering :+ PostgresOrdering(scalaFieldName, descending)))

  /** Low-level field selector used by the inline [[and]] API. */
  final def filterField[A](
    scalaFieldName: String,
    postgresField: PostgresField[A],
    optional: Boolean,
  ): PostgresFieldFilter[F, K, S, A] =
    new PostgresFieldFilter(this, scalaFieldName, postgresField, optional)

  private[postgres] def equal[A](
    scalaFieldName: String,
    value: A,
    postgresField: PostgresField[A],
    optional: Boolean,
  ): PostgresFilterQuery[F, K, S] =
    copy(plan = repository.addValueFilter(plan, scalaFieldName, value, postgresField, optional))

  private[postgres] def within[A](
    scalaFieldName: String,
    values: IterableOnce[A],
    postgresField: PostgresField[A],
    optional: Boolean,
  ): PostgresFilterQuery[F, K, S] =
    copy(plan = repository.addValuesFilter(plan, scalaFieldName, values, postgresField, optional))

  private[postgres] def greaterThan[A](
    scalaFieldName: String,
    value: A,
    postgresField: PostgresField[A],
  ): PostgresFilterQuery[F, K, S] =
    copy(plan = repository.addGreaterThanFilter(plan, scalaFieldName, value, postgresField))

  private[postgres] def notEqualTo[A](name: String, value: A, field: PostgresField[A]): PostgresFilterQuery[F, K, S] =
    copy(plan = repository.addNotEqualFilter(plan, name, value, field))

  private[postgres] def between[A](
    name: String,
    from: A,
    to: A,
    field: PostgresField[A],
  ): PostgresFilterQuery[F, K, S] =
    copy(plan = repository.addBetweenFilter(plan, name, from, to, field))

  private[postgres] def withinSelection[A](
    name: String,
    selection: PostgresSelection[F, A],
  ): PostgresFilterQuery[F, K, S] =
    copy(plan = repository.addSelectionFilter(plan, name, selection))

  private[postgres] def isNull(scalaFieldName: String): PostgresFilterQuery[F, K, S] =
    copy(plan = repository.addNullFilter(plan, scalaFieldName))

  private[postgres] def isNotNull(scalaFieldName: String): PostgresFilterQuery[F, K, S] =
    copy(plan = repository.addNotNullFilter(plan, scalaFieldName))

  private def copy(plan: PostgresFilterPlan): PostgresFilterQuery[F, K, S] =
    new PostgresFilterQuery(repository, plan)

final private[postgres] case class PostgresFilterPlan(
  clauses: List[PostgresFilterClause] = Nil,
  matchesNothing: Boolean = false,
  ordering: List[PostgresOrdering] = Nil,
  limit: Option[Int] = None,
  offset: Option[Long] = None,
  maxLimit: Int = 200,
  validations: List[() => Unit] = Nil,
):

  def validate(): Unit =
    PostgresPagination.validate(limit, offset, maxLimit)
    validations.foreach(_())

  def hasQueryOptions: Boolean = ordering.nonEmpty || limit.nonEmpty || offset.nonEmpty

  def append(clause: PostgresFilterClause): PostgresFilterPlan =
    copy(clauses = clauses :+ clause)

  def noMatches: PostgresFilterPlan =
    copy(matchesNothing = true)

final private[postgres] case class PostgresFilterClause(alternatives: List[AppliedFragment]):

  require(alternatives.nonEmpty, "A PostgreSQL filter clause must have at least one predicate alternative")

final private[postgres] case class PostgresOrdering(scalaFieldName: String, descending: Boolean)
