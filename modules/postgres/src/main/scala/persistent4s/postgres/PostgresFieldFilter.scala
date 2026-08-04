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

  /** Match any supplied value with SQL `IN`. An empty collection makes the complete query match no rows. */
  def in(values: IterableOnce[A]): PostgresFilterQuery[F, K, S] =
    query.within(scalaFieldName, values, postgresField, optionalField)

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
