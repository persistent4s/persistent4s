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

private object PostgresFlattenedRowMacros:

  def derived[S <: Product: Type](
    columnName: Expr[String => String],
    overrides: Expr[Seq[PostgresRow.ColumnCodec[S]]],
    mirror: Expr[Mirror.ProductOf[S]],
  )(using Quotes): Expr[PostgresRow[S]] =
    import quotes.reflect.*

    final case class Node(paths: List[String], codec: Expr[Map[String, Codec[Any]]] => Expr[Codec[Any]])

    def product(tpe: TypeRepr, path: String, ancestors: List[TypeRepr]): Node =
      if ancestors.exists(_ =:= tpe) then
        report.errorAndAbort(s"Cannot flatten recursive PostgreSQL field [$path] of type [${tpe.show}]")
      val fields = tpe.typeSymbol.caseFields
      if fields.isEmpty then
        report.errorAndAbort(
          s"Cannot flatten PostgreSQL field [$path] of type [${tpe.show}]: expected a non-empty product",
        )
      val children = fields.map { field =>
        val childPath = if path.isEmpty then field.name else s"$path.${field.name}"
        expand(tpe.memberType(field).widen.dealias, childPath, tpe :: ancestors)
      }
      tpe.asType match
        case '[value] =>
          val productMirror =
            if tpe =:= TypeRepr.of[S] then mirror.asExprOf[Mirror.ProductOf[value]]
            else
              Expr.summon[Mirror.ProductOf[value]].getOrElse {
                report.errorAndAbort(s"Cannot flatten PostgreSQL field [$path]: no product mirror for [${tpe.show}]")
              }
          Node(
            children.flatMap(_.paths),
            selected =>
              val codecs = Expr.ofList(children.map(_.codec(selected)))
              '{
                PostgresFlattenedRow
                  .productCodec[value](
                    $codecs,
                    values => $productMirror.fromProduct(Tuple.fromArray(values)),
                    value => value.asInstanceOf[Product],
                  )
                  .asInstanceOf[Codec[Any]]
              },
          )

    def expand(tpe: TypeRepr, path: String, ancestors: List[TypeRepr]): Node =
      tpe.asType match
        case '[value] =>
          Expr.summon[PostgresField[value]] match
            case Some(field) =>
              Node(
                path :: Nil,
                selected => '{ $selected.getOrElse(${ Expr(path) }, $field.codec.asInstanceOf[Codec[Any]]) },
              )
            case None if tpe <:< TypeRepr.of[Option[Any]] =>
              val inner = expand(tpe.typeArgs.head.dealias, path, ancestors)
              Node(inner.paths, selected => '{ ${ inner.codec(selected) }.opt.asInstanceOf[Codec[Any]] })
            case None if tpe <:< TypeRepr.of[Product] => product(tpe, path, ancestors)
            case None                                 =>
              report.errorAndAbort(
                s"Cannot flatten PostgreSQL field [$path] of unsupported type [${tpe.show}]. Define a PostgresField for this scalar type.",
              )

    val root = product(TypeRepr.of[S], "", Nil)
    val paths = Expr.ofList(root.paths.map(Expr(_)))
    '{
      val selected = PostgresFlattenedRow.overrides($overrides, $paths)
      val fields = $paths.map(path => PostgresRow.Field(path, $columnName(path)))
      PostgresRow.fromFields(fields, ${ root.codec('{ selected }) }.asInstanceOf[Codec[S]])
    }

  def columnCodec[S: Type, A: Type](
    selector: Expr[S => A],
    codec: Expr[Codec[A]],
  )(using Quotes): Expr[PostgresRow.ColumnCodec[S]] =
    import quotes.reflect.*

    def strip(term: Term): Term = term match
      case Inlined(_, _, value) => strip(value)
      case Typed(value, _)      => strip(value)
      case Block(Nil, value)    => strip(value)
      case value                => value

    def path(term: Term, parameter: Symbol): (List[String], TypeRepr) = strip(term) match
      case value if value.symbol == parameter                                                                        => (Nil, value.tpe.widen)
      case value @ Select(receiver, name) if receiver.tpe.widen.dealias.typeSymbol.caseFields.exists(_.name == name) =>
        val (prefix, _) = path(receiver, parameter)
        (prefix :+ name, value.tpe.widen.dealias)
      case Apply(TypeApply(Select(receiver, method), _), List(function))
          if Set("map", "flatMap")(method) && receiver.tpe.widen <:< TypeRepr.of[Option[Any]] =>
        strip(function) match
          case Lambda(List(nested), body) =>
            val (prefix, _) = path(receiver, parameter)
            val (suffix, leafType) = path(body, nested.symbol)
            (prefix ++ suffix, leafType)
          case _ => report.errorAndAbort("A PostgreSQL column codec must select a scalar field directly")
      case other =>
        report.errorAndAbort(s"A PostgreSQL column codec must select a scalar field directly: ${other.show}")

    strip(selector.asTerm) match
      case Lambda(List(parameter), body) =>
        val (parts, leafType) = path(body, parameter.symbol)
        if parts.isEmpty || !(leafType =:= TypeRepr.of[A].dealias) then
          report.errorAndAbort("A PostgreSQL column codec must match the selected field's declared type; optional traversal must not wrap it")
        '{ PostgresRow.columnCodecAt[S, A](${ Expr(parts.mkString(".")) }, $codec) }
      case _ => report.errorAndAbort("A PostgreSQL column codec selector must be an inline lambda")
