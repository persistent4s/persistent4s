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

import scala.annotation.targetName
import scala.util.NotGiven

/** A typed condition for one field of a derived PostgreSQL state.
  *
  * A condition does not access PostgreSQL immediately. Complete it with [[is]], [[in]], [[isNull]], or [[isNotNull]],
  * add further conditions with [[PostgresFilterQuery.and]], and execute the immutable query with
  * [[PostgresFilterQuery.run]].
  */
final class PostgresFieldFilter[F[_], K, S <: Product, A] private[postgres] (
  query: PostgresFilterQuery[F, K, S],
  scalaFieldName: String,
  postgresField: PostgresField[A],
  optionalField: Boolean,
):

  /** Match one field value. For an `Option` state field, `None` means SQL `IS NULL`. */
  def is(value: A): PostgresFilterQuery[F, K, S] =
    query.equal(scalaFieldName, value, postgresField, optionalField)

  /** Conditionally match a non-optional state field. `None` omits this condition; `Some(value)` matches the value.
    *
    * This overload is intended for optional API/search inputs such as `Option[Int]`. SQL-null matching remains explicit
    * through [[isNull]].
    */
  @targetName("isOptionalInput")
  def is(value: Option[A])(using NotGiven[A <:< Option[Any]]): PostgresFilterQuery[F, K, S] =
    value.fold(query)(is)

  /** Treat a literal `None` for an optional state field as SQL `NULL`, without ambiguity with an omitted API input. */
  @targetName("isNoneFieldValue")
  def is(value: None.type)(using A <:< Option[Any]): PostgresFilterQuery[F, K, S] =
    query.isNull(scalaFieldName)

  /** More explicit alias for [[is]]. */
  def equalTo(value: A): PostgresFilterQuery[F, K, S] = is(value)

  /** Match a different non-null value with SQL `<>`. `None` matches SQL `IS NOT NULL`.
    *
    * @param value
    *   value to exclude
    * @return
    *   the query with this condition
    */
  def notEqualTo(value: A): PostgresFilterQuery[F, K, S] =
    query.notEqualTo(scalaFieldName, value, postgresField)

  /** Match both bounds inclusively with SQL `BETWEEN`.
    *
    * @param from
    *   inclusive lower bound
    * @param to
    *   inclusive upper bound
    * @return
    *   the query with this range
    */
  def between(from: A, to: A): PostgresFilterQuery[F, K, S] =
    query.between(scalaFieldName, from, to, postgresField)

  /** Omit a range when its input is absent.
    *
    * @param bounds
    *   inclusive lower and upper bounds, or `None` to omit the condition
    * @return
    *   the query with the supplied range
    */
  def between(bounds: Option[(A, A)]): PostgresFilterQuery[F, K, S] =
    bounds.fold(query)((from, to) => between(from, to))

  /** Match values greater than the supplied value using SQL `>`.
    *
    * @param value
    *   exclusive lower bound
    * @return
    *   the query with this condition
    */
  def greaterThan(value: A): PostgresFilterQuery[F, K, S] =
    query.greaterThan(scalaFieldName, value, postgresField)

  /** Omit the lower bound when an optional API input is absent.
    *
    * @param value
    *   optional exclusive lower bound for a required state field
    * @return
    *   the query with this condition, or unchanged when absent
    */
  @targetName("greaterThanOptionalInput")
  def greaterThan(value: Option[A])(using NotGiven[A <:< Option[Any]]): PostgresFilterQuery[F, K, S] =
    value.fold(query)(greaterThan)

  /** Match any supplied value with SQL `IN`. An empty collection makes the complete query match no rows. */
  def in(values: IterableOnce[A]): PostgresFilterQuery[F, K, S] =
    query.within(scalaFieldName, values, postgresField, optionalField)

  /** Match values selected by another query. The subquery executes in the same PostgreSQL statement.
    *
    * @param selection
    *   one column selected from a filtered query in the same database
    * @return
    *   the query with an SQL `IN` subquery
    */
  def in(selection: PostgresSelection[F, A]): PostgresFilterQuery[F, K, S] =
    query.withinSelection(scalaFieldName, selection)

  /** Filter a nullable field by present values. `None` omits the condition and an empty collection matches nothing.
    *
    * @param values
    *   optional collection of non-null values to match
    * @return
    *   the query with this condition, or unchanged when absent
    */
  def inValues[B](values: Option[IterableOnce[B]])(using evidence: A =:= Option[B]): PostgresFilterQuery[F, K, S] =
    values.fold(query)(items => in(items.iterator.map(value => evidence.flip(Some(value)))))

  /** Conditionally add an `IN` condition. `None` omits it; `Some(values)` retains the ordinary [[in]] semantics. */
  @targetName("inOptionalInput")
  def in(values: Option[IterableOnce[A]])(using NotGiven[A <:< Option[Any]]): PostgresFilterQuery[F, K, S] =
    values.fold(query)(in)

  /** Match SQL `NULL`. This operation is available only for a state field represented by `Option`. */
  def isNull[B](using A =:= Option[B]): PostgresFilterQuery[F, K, S] =
    query.isNull(scalaFieldName)

  /** Match SQL `NOT NULL`. This operation is available only for a state field represented by `Option`. */
  def isNotNull[B](using A =:= Option[B]): PostgresFilterQuery[F, K, S] =
    query.isNotNull(scalaFieldName)
