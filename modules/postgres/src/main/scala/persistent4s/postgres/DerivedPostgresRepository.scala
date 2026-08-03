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

import skunk.{Codec, Session}

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
    customize(mapping.repositoryTable[F])

  /** Load every row. No ordering is implied. */
  def all: F[List[S]] =
    useSession(_.execute(mapping.allQuery))

  /** Select one state field to filter. The selector must reference the field directly.
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
    new PostgresFilterQuery(this, PostgresFilterPlan()).filterField(scalaFieldName, postgresField, optional)

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

  private[postgres] def runFilter(plan: PostgresFilterPlan): F[List[S]] =
    if plan.matchesNothing then Async[F].pure(List.empty)
    else if plan.clauses.isEmpty then all
    else
      val predicateCombinations =
        plan.clauses.foldLeft(List(List.empty[skunk.AppliedFragment])) { (combinations, clause) =>
          for
            combination <- combinations
            alternative <- clause.alternatives
          yield combination :+ alternative
        }

      useSession { session =>
        predicateCombinations
          .traverse { predicates =>
            val applied = mapping.filterQuery(predicates)
            session.execute(applied.fragment.query(mapping.row.codec))(applied.argument)
          }
          .map(_.flatten.distinctBy(mapping.keyOf))
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
