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

/** An immutable update for one repository key. Assignments and conditions run together in one SQL statement. Keys
  * cannot be changed, fields cannot be assigned twice, and an update must contain at least one assignment. Use
  * [[DerivedPostgresRepository.update]] to create one.
  */
final class PostgresUpdate[F[_], K, S <: Product] private[postgres] (
  repository: DerivedPostgresRepository[F, K, S],
  key: K,
  assignments: List[(String, AppliedFragment)],
  conditions: List[AppliedFragment],
):

  /** Assign a field, including `None` to clear a nullable field.
    *
    * @param field
    *   direct state field to change
    * @param value
    *   new field value
    * @return
    *   the update with this assignment
    */
  inline def set[A](inline field: S => A)(value: A)(using postgresField: PostgresField[A]): PostgresUpdate[F, K, S] =
    ${ DerivedPostgresRepositoryMacros.set[F, K, S, A]('this, 'field, 'value, 'postgresField) }

  /** Assign a field only when supplied. For nullable fields, `Some(None)` clears the value and `None` keeps it.
    *
    * @param field
    *   direct state field to change
    * @param value
    *   optional assignment
    * @return
    *   the update with the assignment, or unchanged when omitted
    */
  inline def setWhenDefined[A](inline field: S => A)(value: Option[A])(using
    postgresField: PostgresField[A],
  ): PostgresUpdate[F, K, S] =
    ${ DerivedPostgresRepositoryMacros.setWhenDefined[F, K, S, A]('this, 'field, 'value, 'postgresField) }

  /** Require a field to equal a value before updating. `None` matches SQL NULL.
    *
    * @param field
    *   direct state field to check
    * @param value
    *   required value
    * @return
    *   the update with this additional condition
    */
  inline def whereEqual[A](inline field: S => A)(value: A)(using
    postgresField: PostgresField[A],
  ): PostgresUpdate[F, K, S] =
    ${ DerivedPostgresRepositoryMacros.updateCondition[F, K, S, A]('this, 'field, 'value, 'postgresField, false) }

  /** Require a field to be less than a value before updating. Conditions are combined with SQL AND.
    *
    * @param field
    *   direct state field to check
    * @param value
    *   exclusive upper bound
    * @return
    *   the update with this additional condition
    */
  inline def whereLessThan[A](inline field: S => A)(value: A)(using
    postgresField: PostgresField[A],
  ): PostgresUpdate[F, K, S] =
    ${ DerivedPostgresRepositoryMacros.updateCondition[F, K, S, A]('this, 'field, 'value, 'postgresField, true) }

  /** Execute using a pooled connection. An empty update fails before acquiring a connection.
    *
    * @return
    *   true when updated, or false when the key is absent or a condition does not match
    */
  def run: F[Boolean] = repository.runUpdate(key, assignments.map(_._2), conditions)

  /** Execute using the caller's connection and transaction. No connection or transaction is opened or closed here.
    *
    * @param session
    *   caller's database connection
    * @return
    *   true when updated, or false when the key is absent or a condition does not match
    */
  def run(session: Session[F]): F[Boolean] = repository.runUpdate(session, key, assignments.map(_._2), conditions)

  /** Low-level assignment used by the field-selector API. */
  def setField[A](name: String, value: A, field: PostgresField[A]): PostgresUpdate[F, K, S] =
    require(!assignments.exists(_._1 == name), s"A PostgreSQL update cannot assign field [$name] twice")
    val assignment = repository.updateAssignment(name, value, field)
    new PostgresUpdate(repository, key, assignments :+ (name -> assignment), conditions)

  /** Low-level optional assignment used by the field-selector API. */
  def setFieldWhenDefined[A](name: String, value: Option[A], field: PostgresField[A]): PostgresUpdate[F, K, S] =
    value.fold(this)(setField(name, _, field))

  /** Low-level equality condition used by the field-selector API. */
  def whereFieldEqual[A](name: String, value: A, field: PostgresField[A]): PostgresUpdate[F, K, S] =
    new PostgresUpdate(repository, key, assignments, conditions :+ repository.updateEquality(name, value, field))

  /** Low-level upper-bound condition used by the field-selector API. */
  def whereFieldLessThan[A](name: String, value: A, field: PostgresField[A]): PostgresUpdate[F, K, S] =
    new PostgresUpdate(repository, key, assignments, conditions :+ repository.updateLessThan(name, value, field))
