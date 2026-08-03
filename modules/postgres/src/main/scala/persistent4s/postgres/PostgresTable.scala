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

import cats.MonadThrow
import cats.syntax.all.*

import skunk.util.Origin
import skunk.{AppliedFragment, Codec, Command, Fragment, Query, Void}

/** Convention-based PostgreSQL persistence for one projection-state table.
  *
  * A table keeps its durable SQL name explicit while deriving columns and row encoding from [[PostgresRow]]. The key
  * selector must reference one state field directly or return a tuple of direct state fields. Generated persistence
  * uses full-row upserts and can be converted to the session-bound [[PostgresRepositoryTable]] used by
  * [[PostgresAtomicRepository]].
  */
final class PostgresTable[K, S <: Product] private (
  val name: String,
  val row: PostgresRow[S],
  val keyOf: S => K,
  private val keyFieldNames: List[String],
  private val keyCodec: Codec[K],
  val batchSize: Int,
  private val columnOverrides: Map[String, String],
):

  require(batchSize > 0, "PostgreSQL repository batch size must be greater than zero")

  private val maxBatchSize = Short.MaxValue.toInt / math.max(row.codec.types.size, keyCodec.types.size)

  require(
    batchSize <= maxBatchSize,
    s"PostgreSQL repository batch size [$batchSize] exceeds the maximum [$maxBatchSize] for this row/key arity",
  )

  private val quotedTable = SqlIdentifier.table(name)

  val columns: List[String] =
    row.fields.map(field => columnOverrides.getOrElse(field.scalaName, field.columnName))

  val keyColumns: List[String] =
    keyFieldNames.map { fieldName =>
      val field = row.fields.find(_.scalaName == fieldName).getOrElse {
        throw new IllegalArgumentException(s"PostgreSQL key references unknown state field [$fieldName]")
      }
      columnOverrides.getOrElse(field.scalaName, field.columnName)
    }

  private val keyFieldIndexes = keyFieldNames.map { fieldName =>
    row.fields.indexWhere(_.scalaName == fieldName)
  }

  private val rowKeyTypes = keyFieldIndexes.map(row.codec.types)

  private val duplicateColumns = columns.groupBy(identity).collect {
    case (column, occurrences) if occurrences.size > 1 =>
      column
  }

  require(duplicateColumns.isEmpty, s"PostgreSQL columns must be unique: ${duplicateColumns.mkString(", ")}")
  require(keyColumns.nonEmpty, "A PostgreSQL projection table must have at least one key column")
  require(
    keyColumns.distinct.size == keyColumns.size,
    s"PostgreSQL key columns must be unique: ${keyColumns.mkString(", ")}",
  )
  require(
    keyCodec.types.size == keyColumns.size,
    s"PostgreSQL key has ${keyColumns.size} columns but its codec represents ${keyCodec.types.size}",
  )
  require(
    keyCodec.types == rowKeyTypes,
    s"PostgreSQL key codecs [${keyCodec.types.mkString(", ")}] do not match the row codecs " +
      s"[${rowKeyTypes.mkString(", ")}] for fields [${keyFieldNames.mkString(", ")}]",
  )

  columns.foreach(SqlIdentifier.column)

  /** Override a conventionally derived column name using a direct field selector. */
  inline def column[A](inline field: S => A, columnName: String): PostgresTable[K, S] =
    ${ PostgresTableMacros.renameColumn[K, S, A]('this, 'field, 'columnName) }

  /** Change the maximum number of rows encoded by each generated statement. */
  def withBatchSize(value: Int): PostgresTable[K, S] =
    copy(batchSize = value)

  /** Low-level column override used by the selector macro. Prefer [[column]]. */
  def renameColumn(scalaFieldName: String, columnName: String): PostgresTable[K, S] =
    require(row.fields.exists(_.scalaName == scalaFieldName), s"Unknown state field [$scalaFieldName]")
    SqlIdentifier.column(columnName)
    copy(columnOverrides = columnOverrides.updated(scalaFieldName, columnName))

  /** Build the generated session-bound CRUD callbacks. Individual callbacks can then be replaced with
    * [[PostgresRepositoryTable.withFetch]], [[PostgresRepositoryTable.withDelete]], or
    * [[PostgresRepositoryTable.withUpsert]].
    */
  def repositoryTable[F[_]: MonadThrow]: PostgresRepositoryTable[F, K, S] =
    PostgresRepositoryTable.rows(
      keyOf = keyOf,
      fetch = (session, keys) => session.execute(findManyQuery(keys.size))(keys),
      delete = (session, keys) => session.execute(deleteManyCommand(keys.size))(keys).void,
      upsert = (session, states) => session.execute(upsertManyCommand(states.size))(states).void,
      batchSize = batchSize,
    )

  private[postgres] def allQuery: Query[Void, S] =
    Query(
      s"SELECT ${quotedColumns.mkString(", ")} FROM $quotedTable",
      Origin.unknown,
      Void.codec,
      row.codec,
    )

  private[postgres] def filterValueQuery[A](
    scalaFieldName: String,
    codec: Codec[A],
  ): Query[A, S] =
    val column      = filterColumn(scalaFieldName, codec)
    val placeholder = codec.sql.runA(1).value

    Query(
      s"SELECT ${quotedColumns.mkString(", ")} FROM $quotedTable WHERE $column = $placeholder",
      Origin.unknown,
      codec,
      row.codec,
    )

  private[postgres] def filterValuesQuery[A](
    scalaFieldName: String,
    codec: Codec[A],
    size: Int,
    includeNull: Boolean,
  ): Query[List[A], S] =
    require(size > 0, "A generated PostgreSQL IN filter requires at least one value")
    require(
      size <= Short.MaxValue.toInt / codec.types.size,
      s"PostgreSQL IN filter contains [$size] values, exceeding the parameter limit for this field codec",
    )

    val column       = filterColumn(scalaFieldName, codec)
    val encoder      = codec.list(size)
    val placeholders = encoder.sql.runA(1).value
    val predicate    =
      if includeNull then s"($column IN ($placeholders) OR $column IS NULL)"
      else s"$column IN ($placeholders)"

    Query(
      s"SELECT ${quotedColumns.mkString(", ")} FROM $quotedTable WHERE $predicate",
      Origin.unknown,
      encoder,
      row.codec,
    )

  private[postgres] def filterNullQuery(scalaFieldName: String): Query[Void, S] =
    val column = filterColumn(scalaFieldName)

    Query(
      s"SELECT ${quotedColumns.mkString(", ")} FROM $quotedTable WHERE $column IS NULL",
      Origin.unknown,
      Void.codec,
      row.codec,
    )

  private[postgres] def filterNotNullQuery(scalaFieldName: String): Query[Void, S] =
    val column = filterColumn(scalaFieldName)

    Query(
      s"SELECT ${quotedColumns.mkString(", ")} FROM $quotedTable WHERE $column IS NOT NULL",
      Origin.unknown,
      Void.codec,
      row.codec,
    )

  private[postgres] def filterValuePredicate[A](
    scalaFieldName: String,
    codec: Codec[A],
    value: A,
  ): AppliedFragment =
    val column = filterColumn(scalaFieldName, codec)
    Fragment[A](
      List(Left(s"$column = "), Right(codec.sql)),
      codec,
      Origin.unknown,
    )(value)

  private[postgres] def filterValuesPredicate[A](
    scalaFieldName: String,
    codec: Codec[A],
    values: List[A],
    includeNull: Boolean,
  ): AppliedFragment =
    require(values.nonEmpty, "A generated PostgreSQL IN filter requires at least one value")

    val column    = filterColumn(scalaFieldName, codec)
    val encoder   = codec.list(values.size)
    val open      = if includeNull then s"($column IN (" else s"$column IN ("
    val close     = if includeNull then s") OR $column IS NULL)" else ")"
    val predicate = Fragment[List[A]](
      List(Left(open), Right(encoder.sql), Left(close)),
      encoder,
      Origin.unknown,
    )

    predicate(values)

  private[postgres] def filterNullPredicate(scalaFieldName: String): AppliedFragment =
    sqlFragment(s"${filterColumn(scalaFieldName)} IS NULL")

  private[postgres] def filterNotNullPredicate(scalaFieldName: String): AppliedFragment =
    sqlFragment(s"${filterColumn(scalaFieldName)} IS NOT NULL")

  private[postgres] def filterQuery(predicates: List[AppliedFragment]): AppliedFragment =
    require(predicates.nonEmpty, "A generated PostgreSQL filter query requires at least one predicate")

    val conjunction = predicates.tail.foldLeft(predicates.head) { (combined, predicate) =>
      combined |+| sqlFragment(" AND ") |+| predicate
    }
    val applied =
      sqlFragment(s"SELECT ${quotedColumns.mkString(", ")} FROM $quotedTable WHERE ") |+| conjunction

    require(
      applied.fragment.encoder.types.size <= Short.MaxValue.toInt,
      s"PostgreSQL filter query contains [${applied.fragment.encoder.types.size}] parameters, exceeding the protocol limit",
    )

    applied

  private[postgres] def findManyQuery(size: Int): Query[List[K], S] =
    require(size > 0, "A generated PostgreSQL find query requires at least one key")

    val encoder = if keyColumns.size == 1 then keyCodec.list(size) else keyCodec.values.list(size)
    val placeholders = encoder.sql.runA(1).value
    val predicate =
      if keyColumns.size == 1 then s"${quotedKeyColumns.head} IN ($placeholders)"
      else s"(${quotedKeyColumns.mkString(", ")}) IN ($placeholders)"

    Query(
      s"SELECT ${quotedColumns.mkString(", ")} FROM $quotedTable WHERE $predicate",
      Origin.unknown,
      encoder,
      row.codec,
    )

  private[postgres] def deleteManyCommand(size: Int): Command[List[K]] =
    require(size > 0, "A generated PostgreSQL delete requires at least one key")

    val encoder = if keyColumns.size == 1 then keyCodec.list(size) else keyCodec.values.list(size)
    val placeholders = encoder.sql.runA(1).value
    val predicate =
      if keyColumns.size == 1 then s"${quotedKeyColumns.head} IN ($placeholders)"
      else s"(${quotedKeyColumns.mkString(", ")}) IN ($placeholders)"

    Command(s"DELETE FROM $quotedTable WHERE $predicate", Origin.unknown, encoder)

  private[postgres] def upsertManyCommand(size: Int): Command[List[S]] =
    require(size > 0, "A generated PostgreSQL upsert requires at least one state")

    val encoder = row.codec.values.list(size)
    val placeholders = encoder.sql.runA(1).value
    val nonKeyColumns = columns.filterNot(keyColumns.toSet)
    val conflictAction =
      if nonKeyColumns.isEmpty then "DO NOTHING"
      else
        nonKeyColumns
          .map(column => s"${SqlIdentifier.column(column)} = EXCLUDED.${SqlIdentifier.column(column)}")
          .mkString("DO UPDATE SET ", ", ", "")

    Command(
      s"INSERT INTO $quotedTable (${quotedColumns.mkString(", ")}) VALUES $placeholders " +
        s"ON CONFLICT (${quotedKeyColumns.mkString(", ")}) $conflictAction",
      Origin.unknown,
      encoder,
    )

  private val quotedColumns: List[String] = columns.map(SqlIdentifier.column)

  private val quotedKeyColumns: List[String] = keyColumns.map(SqlIdentifier.column)

  private def sqlFragment(sql: String): AppliedFragment =
    Fragment[Void](List(Left(sql)), Void.codec, Origin.unknown)(Void)

  private def filterColumn(scalaFieldName: String): String =
    val index = row.fields.indexWhere(_.scalaName == scalaFieldName)
    require(index >= 0, s"PostgreSQL filter references unknown state field [$scalaFieldName]")
    SqlIdentifier.column(columns(index))

  private def filterColumn[A](scalaFieldName: String, codec: Codec[A]): String =
    val index = row.fields.indexWhere(_.scalaName == scalaFieldName)
    require(index >= 0, s"PostgreSQL filter references unknown state field [$scalaFieldName]")
    require(
      codec.types == row.codec.types(index) :: Nil,
      s"PostgreSQL filter codec [${codec.types.mkString(", ")}] does not match row codec " +
        s"[${row.codec.types(index)}] for field [$scalaFieldName]",
    )
    SqlIdentifier.column(columns(index))

  private def copy(
    batchSize: Int = batchSize,
    columnOverrides: Map[String, String] = columnOverrides,
  ): PostgresTable[K, S] =
    new PostgresTable(name, row, keyOf, keyFieldNames, keyCodec, batchSize, columnOverrides)

object PostgresTable:

  /** Start a derived table definition. The table name remains explicit so refactoring a Scala state type cannot rename
    * durable storage accidentally.
    */
  def derived[S <: Product: PostgresRow](tableName: String): Builder[S] =
    new Builder(tableName, PostgresRow[S])

  final class Builder[S <: Product] private[postgres] (
    val tableName: String,
    val row: PostgresRow[S],
  ):

    /** Select one direct state field or a tuple of direct state fields as the durable key. */
    inline def key[K](inline selector: S => K): PostgresTable[K, S] =
      ${ PostgresTableMacros.key[S, K]('this, 'selector) }

    /** Low-level constructor used by the selector macro. Prefer [[key]]. */
    def keyFromFields[K](
      selector: S => K,
      scalaFieldNames: List[String],
      codec: Codec[K],
    ): PostgresTable[K, S] =
      val safeDefaultBatchSize =
        math.min(500, Short.MaxValue.toInt / math.max(row.codec.types.size, codec.types.size))
      new PostgresTable(tableName, row, selector, scalaFieldNames, codec, safeDefaultBatchSize, Map.empty)
