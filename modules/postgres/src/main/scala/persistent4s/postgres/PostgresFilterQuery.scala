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

import skunk.AppliedFragment

/** An immutable, parameterized `AND` query over a derived PostgreSQL state.
  *
  * Build one with [[DerivedPostgresRepository.query]] or [[DerivedPostgresRepository.filterBy]], then call [[run]].
  * Filtering, ordering, and pagination run in PostgreSQL. Without [[withLimit]], `run` loads every matching row.
  */
final class PostgresFilterQuery[F[_], K, S <: Product] private[postgres] (
  private[postgres] val repository: DerivedPostgresRepository[F, K, S],
  private[postgres] val plan: PostgresFilterPlan,
):

  /** Select another direct state field and append its completed condition with SQL `AND`. */
  final inline def and[A](inline field: S => A)(using
    postgresField: PostgresField[A],
  ): PostgresFieldFilter[F, K, S, A] =
    ${ DerivedPostgresRepositoryMacros.and[F, K, S, A]('this, 'field, 'postgresField) }

  /** Append an ascending SQL ordering. Add a unique field last for stable pagination.
    *
    * @param field
    *   direct state field to order by
    * @return
    *   the query with this ordering appended
    */
  final inline def orderBy[A](inline field: S => A): PostgresFilterQuery[F, K, S] =
    ${ DerivedPostgresRepositoryMacros.orderBy[F, K, S, A]('this, 'field, '{ false }) }

  /** Append a descending SQL ordering. PostgreSQL's default null ordering applies.
    *
    * @param field
    *   direct state field to order by
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
):

  def hasQueryOptions: Boolean = ordering.nonEmpty || limit.nonEmpty || offset.nonEmpty

  def append(clause: PostgresFilterClause): PostgresFilterPlan =
    copy(clauses = clauses :+ clause)

  def noMatches: PostgresFilterPlan =
    copy(matchesNothing = true)

final private[postgres] case class PostgresFilterClause(alternatives: List[AppliedFragment]):

  require(alternatives.nonEmpty, "A PostgreSQL filter clause must have at least one predicate alternative")

final private[postgres] case class PostgresOrdering(scalaFieldName: String, descending: Boolean)
