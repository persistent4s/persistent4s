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

private object DerivedPostgresRepositoryMacros:

  def filterBy[F[_]: Type, K: Type, S <: Product: Type, A: Type](
    repository: Expr[DerivedPostgresRepository[F, K, S]],
    selector: Expr[S => A],
    postgresField: Expr[PostgresField[A]],
  )(using Quotes): Expr[PostgresFieldFilter[F, K, S, A]] =
    val fieldName = selectedField(selector, "A PostgreSQL filter")
    val optional = isOptional[A]

    '{ $repository.filterField(${ Expr(fieldName) }, $postgresField, ${ Expr(optional) }) }

  def and[F[_]: Type, K: Type, S <: Product: Type, A: Type](
    query: Expr[PostgresFilterQuery[F, K, S]],
    selector: Expr[S => A],
    postgresField: Expr[PostgresField[A]],
  )(using Quotes): Expr[PostgresFieldFilter[F, K, S, A]] =
    val fieldName = selectedField(selector, "A PostgreSQL AND filter")
    val optional = isOptional[A]

    '{ $query.filterField(${ Expr(fieldName) }, $postgresField, ${ Expr(optional) }) }

  private def isOptional[A: Type](using quotes: Quotes): Boolean =
    import quotes.reflect.*
    TypeRepr.of[A].dealias <:< TypeRepr.of[Option[Any]]

  private def selectedField[S: Type, A: Type](selector: Expr[S => A], description: String)(using Quotes): String =
    import quotes.reflect.*

    def strip(term: Term): Term =
      term match
        case Inlined(_, _, value) => strip(value)
        case Typed(value, _)      => strip(value)
        case Block(Nil, value)    => strip(value)
        case value                => value

    strip(selector.asTerm) match
      case Lambda(List(parameter), body) =>
        strip(body) match
          case Select(receiver, name) if strip(receiver).symbol == parameter.symbol => name
          case other                                                                =>
            report.errorAndAbort(
              s"$description must select exactly one state field directly; unsupported expression: ${other.show}",
            )
      case other =>
        report.errorAndAbort(s"$description must be an inline lambda, but found: ${other.show}")
