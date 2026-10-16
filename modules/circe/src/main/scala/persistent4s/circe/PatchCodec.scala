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

package persistent4s.circe

import scala.compiletime.{constValue, erasedValue, summonInline}
import scala.deriving.Mirror

import cats.data.Validated
import cats.syntax.all.*
import io.circe.{Codec, Decoder, DecodingFailure, Encoder, FailedCursor, HCursor, JsonObject}

/** JSON for updates with `Option[Option[A]]` fields: `None` leaves a field out, `Some(None)` writes `null`, and
  * `Some(Some(value))` writes the value. Other fields use their existing Circe codecs.
  *
  * Add `derives PatchCodec` to a case class to provide both its encoder and decoder. Nested case classes can derive
  * their own `PatchCodec` when they also contain update fields.
  */
trait PatchCodec[A] extends Codec.AsObject[A]

object PatchCodec:

  /** Derives JSON encoding and decoding for an update case class.
    *
    * @param mirror
    *   the case class's fields
    * @return
    *   a codec that distinguishes missing fields, explicit nulls, and new values
    */
  inline def derived[A](using mirror: Mirror.ProductOf[A]): PatchCodec[A] =
    instance(mirror, fields[mirror.MirroredElemTypes, mirror.MirroredElemLabels])

  private final case class Field(
    name: String,
    encoder: Encoder[Any],
    decoder: Decoder[Any],
    omitNone: Boolean,
  )

  private def instance[A](mirror: Mirror.ProductOf[A], fields: List[Field]): PatchCodec[A] =
    new PatchCodec[A]:
      def encodeObject(value: A): JsonObject =
        val values = value.asInstanceOf[Product].productIterator
        JsonObject.fromIterable(fields.iterator.zip(values).collect {
          case (field, entry) if !field.omitNone || entry != None =>
            field.name -> field.encoder(entry)
        }.toList)

      def apply(cursor: HCursor): Decoder.Result[A] =
        if !cursor.value.isObject then Left(DecodingFailure("Expected an object", cursor.history))
        else
          fields
            .traverse(field => field.decoder.tryDecode(cursor.downField(field.name)))
            .map(values => mirror.fromProduct(Tuple.fromArray(values.toArray)))

      override def decodeAccumulating(cursor: HCursor): Decoder.AccumulatingResult[A] =
        if !cursor.value.isObject then Validated.invalidNel(DecodingFailure("Expected an object", cursor.history))
        else
          fields
            .traverse(field => field.decoder.tryDecodeAccumulating(cursor.downField(field.name)))
            .map(values => mirror.fromProduct(Tuple.fromArray(values.toArray)))

  private def nestedOptionDecoder[A](inner: Decoder[Option[A]]): Decoder[Option[Option[A]]] =
    Decoder.withReattempt {
      case cursor: HCursor => inner(cursor).map(Some(_))
      case cursor: FailedCursor if !cursor.incorrectFocus => Right(None)
      case cursor => Left(DecodingFailure("Expected an object field", cursor.history))
    }

  private inline def fields[Types <: Tuple, Labels <: Tuple]: List[Field] =
    inline erasedValue[(Types, Labels)] match
      case _: (EmptyTuple, EmptyTuple) => Nil
      case _: ((head *: tail), (label *: labels)) =>
        val name = constValue[label].asInstanceOf[String]
        val encoder = summonInline[Encoder[head]].asInstanceOf[Encoder[Any]]
        val field = inline erasedValue[head] match
          case _: Option[Option[a]] =>
            Field(
              name,
              encoder,
              nestedOptionDecoder(summonInline[Decoder[Option[a]]]).asInstanceOf[Decoder[Any]],
              omitNone = true,
            )
          case _ =>
            Field(name, encoder, summonInline[Decoder[head]].asInstanceOf[Decoder[Any]], omitNone = false)
        field :: fields[tail, labels]
