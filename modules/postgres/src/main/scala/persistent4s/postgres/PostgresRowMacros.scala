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
import scala.quoted.*

import skunk.Codec

private object PostgresRowMacros:

  def derived[S <: Product: Type](mirror: Expr[Mirror.ProductOf[S]])(using Quotes): Expr[PostgresRow[S]] =
    import quotes.reflect.*

    val stateType = TypeRepr.of[S]
    val fields = stateType.typeSymbol.caseFields

    if fields.isEmpty then
      report.errorAndAbort(
        s"PostgresRow can only be derived for a non-empty case class; [${stateType.show}] has no fields",
      )

    val metadata = fields.map { field =>
      val scalaName = field.name
      val columnName = PostgresRow.snakeCase(scalaName)
      val fieldType = stateType.memberType(field).widen

      val codec = fieldType.asType match
        case '[field] =>
          Expr.summon[PostgresField[field]] match
            case Some(instance) => '{ $instance.codec.asInstanceOf[Codec[Any]] }
            case None           =>
              report.errorAndAbort(
                s"Cannot derive PostgresRow[${stateType.show}]: field [$scalaName] has unsupported type [${fieldType.show}]. " +
                  "Define a PostgresField for that scalar type or provide an explicit PostgresRow.",
              )

      ('{ PostgresRow.Field(${ Expr(scalaName) }, ${ Expr(columnName) }) }, codec)
    }

    val fieldExpressions = Expr.ofList(metadata.map(_._1))
    val codecExpressions = Expr.ofList(metadata.map(_._2))

    '{
      val codec = PostgresRow.productCodec[S](
        $codecExpressions,
        values => $mirror.fromProduct(Tuple.fromArray(values)),
        value => value,
      )
      PostgresRow.fromFields($fieldExpressions, codec)
    }
