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

import scala.quoted.*

import skunk.Codec

private object PostgresTableMacros:

  def key[S <: Product: Type, K: Type](
    builder: Expr[PostgresTable.Builder[S]],
    selector: Expr[S => K],
  )(using Quotes): Expr[PostgresTable[K, S]] =
    import quotes.reflect.*

    val selected = selectedFields(selector.asTerm, "A PostgreSQL key")
    val names = selected.map(_._1)

    if names.distinct.size != names.size then
      report.errorAndAbort(
        s"A PostgreSQL key cannot select the same state field more than once: ${names.mkString(", ")}",
      )

    val fieldCodecs = selected.map { case (name, term) =>
      val fieldType = term.tpe.widen.dealias
      if fieldType <:< TypeRepr.of[Option[Any]] then
        report.errorAndAbort(s"A PostgreSQL key field cannot be optional: [$name] has type [${fieldType.show}]")

      fieldType.asType match
        case '[field] =>
          Expr.summon[PostgresField[field]] match
            case Some(instance) => '{ $instance.codec.asInstanceOf[Codec[Any]] }
            case None           =>
              report.errorAndAbort(
                s"Cannot derive PostgreSQL key: field [$name] has unsupported type [${term.tpe.widen.show}]. " +
                  "Define a PostgresField for that scalar type.",
              )
    }

    val codec: Expr[Codec[K]] =
      if fieldCodecs.size == 1 then
        Expr.summon[PostgresField[K]] match
          case Some(instance) => '{ $instance.codec }
          case None           =>
            report.errorAndAbort(s"Cannot derive PostgreSQL key codec for [${TypeRepr.of[K].show}]")
      else
        if !(TypeRepr.of[K] <:< TypeRepr.of[Tuple]) then
          report.errorAndAbort("A composite PostgreSQL key selector must return a tuple of direct state fields")

        val codecs = Expr.ofList(fieldCodecs)
        '{
          PostgresRow.productCodec[K](
            $codecs,
            values => Tuple.fromArray(values).asInstanceOf[K],
            value => value.asInstanceOf[Product],
          )
        }

    '{ $builder.keyFromFields($selector, ${ Expr.ofList(names.map(Expr(_))) }, $codec) }

  def renameColumn[K: Type, S <: Product: Type, A: Type](
    table: Expr[PostgresTable[K, S]],
    selector: Expr[S => A],
    columnName: Expr[String],
  )(using Quotes): Expr[PostgresTable[K, S]] =
    import quotes.reflect.*

    val fields = selectedFields(selector.asTerm, "A PostgreSQL column override")
    if fields.size != 1 then report.errorAndAbort("A PostgreSQL column override must select one state field")
    '{ $table.renameColumn(${ Expr(fields.head._1) }, $columnName) }

  private def selectedFields(using
    quotes: Quotes,
  )(
    selector: quotes.reflect.Term,
    description: String,
  ): List[(String, quotes.reflect.Term)] =
    import quotes.reflect.*

    def strip(term: Term): Term =
      term match
        case Inlined(_, _, value) => strip(value)
        case Typed(value, _)      => strip(value)
        case Block(Nil, value)    => strip(value)
        case value                => value

    strip(selector) match
      case Lambda(List(parameter), body) =>
        def directField(term: Term): Option[(String, Term)] =
          strip(term) match
            case selected @ Select(receiver, name) if strip(receiver).symbol == parameter.symbol =>
              Some(name -> selected)
            case _ => None

        def loop(term: Term): List[(String, Term)] =
          directField(term) match
            case Some(field) => field :: Nil
            case None        =>
              strip(term) match
                case Apply(function, arguments) if function.symbol.fullName.startsWith("scala.Tuple") =>
                  arguments.map { argument =>
                    directField(argument).getOrElse {
                      report.errorAndAbort(
                        s"$description tuple elements must each select one state field directly; " +
                          s"unsupported expression: ${strip(argument).show}",
                      )
                    }
                  }
                case other =>
                  report.errorAndAbort(
                    s"$description must select a state field directly or return a tuple of direct state fields; " +
                      s"unsupported expression: ${other.show}",
                  )

        loop(body)

      case other =>
        report.errorAndAbort(s"$description must be an inline lambda, but found: ${other.show}")
