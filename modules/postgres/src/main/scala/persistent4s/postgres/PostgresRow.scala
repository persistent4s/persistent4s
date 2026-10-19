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

import scala.deriving.Mirror

import cats.data.State
import cats.syntax.all.*

import skunk.data.{Encoded, Type}
import skunk.{Codec, Decoder}

/** Skunk row metadata for a projection state product.
  *
  * [[PostgresTable.derived]] automatically uses constructor-field order, converts Scala field names to `snake_case`,
  * and summons a [[PostgresField]] for every field. `derives PostgresRow` is also available when an instance is useful
  * independently. Use [[PostgresRow.flattened]] for nested products stored in separate columns, or
  * [[PostgresRow.apply]] for an entirely custom codec and column mapping.
  */
trait PostgresRow[S]:

  def fields: List[PostgresRow.Field]

  def codec: Codec[S]

object PostgresRow:

  final case class Field(scalaName: String, columnName: String)

  def apply[S](using row: PostgresRow[S]): PostgresRow[S] = row

  /** Define row metadata explicitly. The column order must match the supplied codec. */
  def apply[S](columns: (String, String)*)(value: Codec[S]): PostgresRow[S] =
    fromFields(columns.toList.map((scalaName, columnName) => Field(scalaName, columnName)), value)

  inline given derived[S <: Product](using mirror: Mirror.ProductOf[S]): PostgresRow[S] =
    ${ PostgresRowMacros.derived[S]('mirror) }

  /** Derive a row whose nested products occupy separate columns. A supplied [[PostgresField]] keeps its value in one
    * column. An optional product is absent when all of its columns are null.
    *
    * @param columnName
    *   SQL column name for each Scala field path, such as `address.city`
    * @param overrides
    *   codecs for individual scalar fields whose database type differs from their default
    * @return
    *   ordered column metadata and a codec for the complete product
    */
  inline def flattened[S <: Product](
    columnName: String => String,
    overrides: ColumnCodec[S]*,
  )(using mirror: Mirror.ProductOf[S]): PostgresRow[S] =
    ${ PostgresFlattenedRowMacros.derived[S]('columnName, 'overrides, 'mirror) }

  /** A codec override for one scalar field of a flattened row. Use [[columnCodec]] to select the field safely. */
  final class ColumnCodec[S] private[postgres] (val path: String, val codec: Codec[Any])

  /** Select a scalar field whose PostgreSQL codec differs from its default. For example,
    * `columnCodec[Person](_.name)(varchar)` stores that field as `varchar`.
    *
    * @return
    *   a typed selector for an override passed to [[flattened]]
    */
  def columnCodec[S]: ColumnCodecBuilder[S] = new ColumnCodecBuilder[S]

  /** Selects a declared scalar field and supplies its single-column codec. */
  final class ColumnCodecBuilder[S]:

    /** Override one field's codec without changing its Scala type. Nested optional fields can use `flatMap`, provided
      * the selector's result retains the leaf field's declared type.
      *
      * @param field
      *   direct or nested scalar field; computed values are rejected
      * @param value
      *   codec that encodes and decodes the selected field in one PostgreSQL column
      * @return
      *   the override for [[flattened]]; unknown paths and duplicate overrides fail when the row is built
      */
    inline def apply[A](inline field: S => A)(value: Codec[A]): ColumnCodec[S] =
      ${ PostgresFlattenedRowMacros.columnCodec[S, A]('field, 'value) }

  /** Low-level constructor used by the typed codec selector. Prefer [[columnCodec]] for compile-time field checks.
    *
    * @param path
    *   Scala field path in the flattened row, such as `address.city`
    * @param value
    *   single-column codec for that field's declared type
    * @return
    *   the override for [[flattened]]
    */
  def columnCodecAt[S, A](path: String, value: Codec[A]): ColumnCodec[S] =
    require(value.types.size == 1, "A flattened PostgreSQL column codec must represent exactly one column")
    new ColumnCodec(path, value.asInstanceOf[Codec[Any]])

  /** Low-level constructor used by derivation and available for custom mapping tools. */
  def fromFields[S](valueFields: List[Field], valueCodec: Codec[S]): PostgresRow[S] =
    require(valueFields.nonEmpty, "A derived PostgreSQL row must contain at least one field")
    require(
      valueFields.size == valueCodec.types.size,
      s"PostgreSQL row has ${valueFields.size} columns but its codec represents ${valueCodec.types.size}",
    )
    require(
      valueFields.map(_.scalaName).distinct.size == valueFields.size,
      "PostgreSQL row Scala field names must be unique",
    )
    require(
      valueFields.map(_.columnName).distinct.size == valueFields.size,
      s"PostgreSQL row column names must be unique: ${valueFields.map(_.columnName).mkString(", ")}",
    )

    valueFields.foreach(field => SqlIdentifier.column(field.columnName))

    new PostgresRow[S]:
      override val fields: List[Field] = valueFields
      override val codec: Codec[S] = valueCodec

  /** Low-level product-codec constructor used by Scala 3 derivation. */
  def productCodec[A](
    codecs: List[Codec[Any]],
    construct: Array[Any] => A,
    deconstruct: A => Product,
  ): Codec[A] =
    require(codecs.nonEmpty, "A derived PostgreSQL product codec must contain at least one field")
    codecs.foreach(codec =>
      require(
        codec.types.size == 1,
        s"A derived PostgreSQL field codec must represent one column, but [${codec.types.mkString(", ")}] represents ${codec.types.size}",
      ),
    )

    new Codec[A]:
      override val types: List[Type] = codecs.flatMap(_.types)

      override val sql: State[Int, String] =
        codecs.traverse(_.sql).map(_.mkString(", "))

      override def encode(value: A): List[Option[Encoded]] =
        val values = deconstruct(value).productIterator.toList
        require(
          values.size == codecs.size,
          s"Derived PostgreSQL product has ${values.size} values but ${codecs.size} codecs",
        )
        codecs.zip(values).flatMap((codec, field) => codec.encode(field))

      override def decode(offset: Int, values: List[Option[String]]): Either[Decoder.Error, A] =
        if values.size != codecs.size then
          Left(
            Decoder.Error(
              offset,
              codecs.size,
              s"Expected ${codecs.size} PostgreSQL columns but received ${values.size}",
            ),
          )
        else
          codecs
            .zip(values)
            .zipWithIndex
            .traverse { case ((codec, value), index) => codec.decode(offset + index, value :: Nil) }
            .map(decoded => construct(decoded.toArray))

  private[postgres] def snakeCase(value: String): String =
    value
      .replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
      .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
      .toLowerCase(java.util.Locale.ROOT)
