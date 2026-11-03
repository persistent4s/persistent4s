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

import cats.effect.Sync
import cats.syntax.all.*

/** A custom SQL query with the same pagination checks as [[PostgresFilterQuery]].
  *
  * The callback receives a validated, capped limit and a non-negative offset. Bind both values to the SQL query so
  * PostgreSQL pages the rows before loading them. Keep connection acquisition inside the callback.
  *
  * @tparam A
  *   the result of the custom query, such as a list of rows or a count and page
  */
final class PostgresQuery[F[_]: Sync, A] private (
  execute: (Int, Long) => F[A],
  limit: Int,
  offset: Long,
  maxLimit: Int,
):

  /** Limit the query to at most 200 rows. [[run]] rejects a negative count.
    *
    * @param count
    *   requested row count; zero requests an empty page
    * @return
    *   the query with this limit
    */
  def withLimit(count: Int): PostgresQuery[F, A] = withLimit(count, maxLimit = 200)

  /** Cap the requested row count at the supplied maximum. [[run]] rejects negative counts and maxima.
    *
    * @param count
    *   requested row count
    * @param maxLimit
    *   maximum permitted row count
    * @return
    *   the query with the smaller of the requested count and maximum
    */
  def withLimit(count: Int, maxLimit: Int): PostgresQuery[F, A] =
    new PostgresQuery(execute, PostgresPagination.cap(count, maxLimit), offset, maxLimit)

  /** Skip rows in PostgreSQL. [[run]] rejects a negative count.
    *
    * @param count
    *   number of rows to skip
    * @return
    *   the query with this offset
    */
  def withOffset(count: Long): PostgresQuery[F, A] =
    new PostgresQuery(execute, limit, count, maxLimit)

  /** Check pagination without executing the query, for example before returning only a total.
    *
    * @return
    *   `Unit` when valid; otherwise [[PaginationError]], or an `IllegalArgumentException` for a negative maximum
    */
  def validate: F[Unit] =
    Sync[F].delay(PostgresPagination.validate(Some(limit), Some(offset), maxLimit))

  /** Validate pagination and then execute the custom query. Zero limits still invoke the callback so it can return a
    * total alongside the empty page. Query failures and cancellation are passed through unchanged.
    *
    * @return
    *   the custom query result
    */
  def run: F[A] = validate *> Sync[F].defer(execute(limit, offset))

object PostgresQuery:

  /** Add safe pagination to a custom SQL query. Defaults to a limit of 200 and an offset of zero.
    *
    * @param execute
    *   query that binds the supplied limit and offset in SQL and loads its result
    * @return
    *   an immutable query with shared pagination checks
    */
  def paginated[F[_]: Sync, A](execute: (Int, Long) => F[A]): PostgresQuery[F, A] =
    new PostgresQuery(execute, limit = 200, offset = 0L, maxLimit = 200)
