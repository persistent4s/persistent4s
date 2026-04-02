/*
 * Copyright 2026 persistent4s
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

import pureconfig.ConfigReader

/** Configuration for connecting to a PostgreSQL database for the event store.
  *
  * @param host
  *   the database host (e.g., "localhost")
  * @param port
  *   the database port (default: 5432)
  * @param user
  *   the database user
  * @param password
  *   the database password
  * @param database
  *   the database name
  * @param maxConnections
  *   the maximum number of connections in the pool (default: 10)
  */
final case class PostgresConfig(
    host: String,
    port: Int = 5432,
    user: String,
    password: String,
    database: String,
    maxConnections: Int = 10
) derives ConfigReader
