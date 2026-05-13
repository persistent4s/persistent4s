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

import pureconfig.ConfigReader

/** SSL/TLS mode for the PostgreSQL connection.
  *
  *   - [[Disabled]] — no encryption. The default; suitable for local development.
  *   - [[System]] — encrypted, validate the server certificate chain against the JVM's default trust store. Honors
  *     `-Djavax.net.ssl.trustStore` and friends, so production setups that ship a custom trust store via JVM system
  *     properties work without further configuration. This is the production default.
  *   - [[TrustAll]] — encrypted, but does '''not''' validate the server certificate (accepts any cert, including
  *     self-signed). Vulnerable to MITM. Only use for debugging or against self-signed dev servers; never in
  *     production.
  */
enum PostgresSslMode derives ConfigReader:
  case Disabled, System, TrustAll

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
  *   the maximum number of connections in the pool (default: 32)
  * @param ssl
  *   SSL/TLS mode (default: [[PostgresSslMode.Disabled]]). For production, set to [[PostgresSslMode.System]]. See
  *   [[PostgresSslMode]] for the per-mode trade-offs.
  */
final case class PostgresConfig(
  host: String,
  port: Int = 5432,
  user: String,
  password: String,
  database: String,
  maxConnections: Int = 32,
  ssl: PostgresSslMode = PostgresSslMode.Disabled,
) derives ConfigReader
