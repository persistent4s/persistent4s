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

import java.time.{Duration, LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import java.util.UUID

import skunk.Codec
import skunk.codec.all.*

/** The default Skunk mapping for one PostgreSQL column.
  *
  * Derived projection rows summon one instance for every case-class field. Applications can define an instance for a
  * domain-specific scalar with [[PostgresField.apply]]. A field codec must represent exactly one PostgreSQL column;
  * nested products are deliberately not flattened by the default derivation.
  */
trait PostgresField[A]:

  def codec: Codec[A]

object PostgresField:

  def apply[A](value: Codec[A]): PostgresField[A] =
    require(
      value.types.size == 1,
      s"A PostgresField codec must represent exactly one column, but [${value.types.mkString(", ")}] represents ${value.types.size}",
    )
    new PostgresField[A]:
      override val codec: Codec[A] = value

  given uuidField: PostgresField[UUID] = PostgresField(uuid)

  given stringField: PostgresField[String] = PostgresField(text)

  given shortField: PostgresField[Short] = PostgresField(int2)

  given intField: PostgresField[Int] = PostgresField(int4)

  given longField: PostgresField[Long] = PostgresField(int8)

  given bigDecimalField: PostgresField[BigDecimal] = PostgresField(numeric)

  given floatField: PostgresField[Float] = PostgresField(float4)

  given doubleField: PostgresField[Double] = PostgresField(float8)

  given booleanField: PostgresField[Boolean] = PostgresField(bool)

  given bytesField: PostgresField[Array[Byte]] = PostgresField(bytea)

  given localDateField: PostgresField[LocalDate] = PostgresField(date)

  given localTimeField: PostgresField[LocalTime] = PostgresField(time)

  given offsetTimeField: PostgresField[OffsetTime] = PostgresField(timetz)

  given localDateTimeField: PostgresField[LocalDateTime] = PostgresField(timestamp)

  given offsetDateTimeField: PostgresField[OffsetDateTime] = PostgresField(timestamptz)

  given durationField: PostgresField[Duration] = PostgresField(interval)

  given optionField[A](using field: PostgresField[A]): PostgresField[Option[A]] =
    PostgresField(field.codec.opt)
