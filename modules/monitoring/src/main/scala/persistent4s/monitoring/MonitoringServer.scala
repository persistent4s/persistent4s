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

package persistent4s.monitoring

import cats.effect.{Async, Resource}
import com.comcast.ip4s.*
import fs2.io.net.Network
import org.http4s.ember.server.EmberServerBuilder
import persistent4s.{EventStoreNotification, ProjectionCheckpoint}

import scala.concurrent.duration.*

object MonitoringServer:

  /** Start a developer-facing HTTP monitoring server for projection checkpoints.
    *
    * Serves a single HTML page at `GET /` showing the state of all projections. Provides
    * `POST /checkpoints/{name}/pause`, `POST /checkpoints/{name}/resume`, and `POST /checkpoints/{name}/index`
    * endpoints that send Postgres NOTIFY control messages.
    *
    * @note
    *   This server has no authentication. It binds to `0.0.0.0` by default — do not expose it outside a local
    *   development environment without adding a reverse proxy or firewall rule.
    *
    * @param checkpoint
    *   the Postgres checkpoint store used to load all projection states
    * @param sendNotification
    *   function to send a control notification (typically `components.eventStore.notify`)
    * @param port
    *   the port to bind on (default: 9595)
    */
  def make[F[_]: Async: Network](
    checkpoint: ProjectionCheckpoint[F],
    sendNotification: EventStoreNotification => F[Unit],
    port: Port = port"9595",
  ): Resource[F, Unit] =
    val routes = new CheckpointRoutes[F](checkpoint.loadAll(), sendNotification).routes
    EmberServerBuilder
      .default[F]
      .withHost(host"0.0.0.0")
      .withPort(port)
      .withShutdownTimeout(2.seconds)
      .withHttpApp(routes.orNotFound)
      .build
      .map(_ => ())
