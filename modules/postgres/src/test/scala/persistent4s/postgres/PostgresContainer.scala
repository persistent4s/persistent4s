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

import scala.concurrent.duration.*

import cats.effect.{IO, Resource}
import org.testcontainers.containers.PostgreSQLContainer
import skunk.*

/** One PostgreSQL container, started and *proved* reachable.
  *
  * `PostgreSQLContainer.start()` returns as soon as "database system is ready to accept connections" has appeared twice
  * in the container's log — it overrides `JdbcDatabaseContainer`'s connection probe away, so testcontainers never opens
  * a socket. Observing a log line is not the same as the mapped port accepting, and the gap between them is where a
  * suite's very first connection gets `Connection refused`. Eleven suites start eleven containers, so the run rolls
  * that dice eleven times.
  *
  * The check testcontainers skips therefore happens here, once, before any fixture is built.
  */
object PostgresContainer:

  private type Container = PostgreSQLContainer[Nothing]

  /** @param maxConnections
    *   the pool size the suite's fixture will be built with. A suite that drives contention deliberately picks a small
    *   one, so it is a parameter rather than a constant.
    */
  def resource(maxConnections: Int = 16): Resource[IO, PostgresConfig] =
    Resource
      .make(
        IO.blocking {
          val container = new PostgreSQLContainer[Nothing]("postgres:16-alpine")
          container.withStartupTimeout(java.time.Duration.ofMinutes(2))
          container.start()
          container
        },
      )(container => IO.blocking(container.stop()).handleErrorWith(_ => IO.unit))
      .map(config(_, maxConnections))
      .evalTap(awaitReachable)

  private def config(container: Container, maxConnections: Int): PostgresConfig =
    PostgresConfig(
      host = container.getHost, port = container.getMappedPort(5432), user = container.getUsername,
      password = container.getPassword, database = container.getDatabaseName, maxConnections = maxConnections,
    )

  /** Open one real connection, retrying a refusal. Forty attempts at 250ms is ten seconds — far longer than the window
    * has ever been — and it costs one connection when the container is already up, which is the usual case.
    */
  private def awaitReachable(config: PostgresConfig): IO[Unit] =
    val open =
      Session
        .Builder[IO]
        .withHost(config.host)
        .withPort(config.port)
        .withUserAndPassword(config.user, config.password)
        .withDatabase(config.database)
        .pooled(1)
        .use(_.use_)

    def attempt(remaining: Int): IO[Unit] =
      open.handleErrorWith {
        case error if remaining <= 0 => IO.raiseError(error)
        case _                       => IO.sleep(250.millis) *> attempt(remaining - 1)
      }

    attempt(40)
