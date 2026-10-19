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

import cats.data.State
import cats.syntax.all.*

import skunk.data.{Encoded, Type}
import skunk.{Codec, Decoder}

/** Constructors used by flattened row derivation. */
object PostgresFlattenedRow:

  /** Validate override paths before constructing the row codec.
    *
    * @param values
    *   requested scalar codec overrides
    * @param paths
    *   scalar field paths present in the derived row
    * @return
    *   codecs indexed by path; duplicate and unknown paths are rejected
    */
  def overrides[S](values: Seq[PostgresRow.ColumnCodec[S]], paths: List[String]): Map[String, Codec[Any]] =
    require(values.map(_.path).distinct.size == values.size, "A PostgreSQL column codec cannot be overridden twice")
    values.foreach(value => require(paths.contains(value.path), s"Unknown flattened PostgreSQL field [${value.path}]"))
    values.iterator.map(value => value.path -> value.codec).toMap

  /** Combine nested product codecs while preserving their column offsets.
    *
    * @param codecs
    *   codecs in constructor-field order; each can span several columns
    * @param construct
    *   builds the product from decoded constructor fields
    * @param deconstruct
    *   exposes constructor fields in the same order
    * @return
    *   the combined codec, with decoding errors referring to their database column
    */
  def productCodec[A](
    codecs: List[Codec[Any]],
    construct: Array[Any] => A,
    deconstruct: A => Product,
  ): Codec[A] =
    require(codecs.nonEmpty, "A flattened PostgreSQL product must contain at least one field")
    new Codec[A]:
      override val types: List[Type] = codecs.flatMap(_.types)

      override val sql: State[Int, String] = codecs.traverse(_.sql).map(_.mkString(", "))

      override def encode(value: A): List[Option[Encoded]] =
        val fields = deconstruct(value).productIterator.toList
        require(fields.size == codecs.size, "Flattened PostgreSQL product has an unexpected number of fields")
        codecs.zip(fields).flatMap((codec, field) => codec.encode(field))

      override def decode(offset: Int, values: List[Option[String]]): Either[Decoder.Error, A] =
        if values.size != types.size then
          Left(
            Decoder.Error(offset, types.size, s"Expected ${types.size} PostgreSQL columns but received ${values.size}"),
          )
        else
          codecs
            .foldLeft[Either[Decoder.Error, (Int, List[Any])]](Right((0, Nil))) { (decoded, codec) =>
              decoded.flatMap { (index, fields) =>
                codec
                  .decode(offset + index, values.slice(index, index + codec.types.size))
                  .map(field => (index + codec.types.size, field :: fields))
              }
            }
            .map((_, fields) => construct(fields.reverse.toArray))
