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

import cats.effect.{MonadCancelThrow, Resource}

import skunk.Session

/** A session pool together with the [[PostgresSchema]] holding the persistent4s tables of its database.
  *
  * Everything that reads or writes those tables takes this instead of a bare pool, so it always addresses the schema
  * the module created them in. [[PostgresModule.sessions]] provides it; build one directly only for a pool the module
  * does not own, e.g. in tests.
  *
  * @param pool
  *   the session pool, also usable for application queries
  * @param schema
  *   the schema of the persistent4s tables
  */
final class PostgresSessions[F[_]](
  val pool: Resource[F, Session[F]],
  val schema: PostgresSchema,
):

  /** Run `f` with a pooled session. */
  def use[A](f: Session[F] => F[A])(using MonadCancelThrow[F]): F[A] = pool.use(f)
