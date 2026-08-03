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
  * Build one with [[DerivedPostgresRepository.filterBy]], optionally append conditions with [[and]], then call
  * [[run]]. Conditions supplied as absent frontend inputs are omitted; when every condition is omitted, `run` loads
  * every row.
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

  /** Execute this query. No ordering is implied. */
  def run: F[List[S]] = repository.runFilter(plan)

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

  private[postgres] def isNull(scalaFieldName: String): PostgresFilterQuery[F, K, S] =
    copy(plan = repository.addNullFilter(plan, scalaFieldName))

  private[postgres] def isNotNull(scalaFieldName: String): PostgresFilterQuery[F, K, S] =
    copy(plan = repository.addNotNullFilter(plan, scalaFieldName))

  private def copy(plan: PostgresFilterPlan): PostgresFilterQuery[F, K, S] =
    new PostgresFilterQuery(repository, plan)

private[postgres] final case class PostgresFilterPlan(
  clauses: List[PostgresFilterClause] = Nil,
  matchesNothing: Boolean = false,
):

  def append(clause: PostgresFilterClause): PostgresFilterPlan =
    copy(clauses = clauses :+ clause)

  def noMatches: PostgresFilterPlan =
    copy(matchesNothing = true)

private[postgres] final case class PostgresFilterClause(alternatives: List[AppliedFragment]):
  require(alternatives.nonEmpty, "A PostgreSQL filter clause must have at least one predicate alternative")
