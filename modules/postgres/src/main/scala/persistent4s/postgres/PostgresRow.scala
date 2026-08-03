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

/** Skunk row metadata for a flat projection state product.
  *
  * [[PostgresTable.derived]] automatically uses constructor-field order, converts Scala field names to `snake_case`,
  * and summons a [[PostgresField]] for every field. `derives PostgresRow` is also available when an instance is useful
  * independently. Use [[PostgresRow.apply]] when a row needs an entirely custom codec or stable column names that
  * differ from the convention.
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
