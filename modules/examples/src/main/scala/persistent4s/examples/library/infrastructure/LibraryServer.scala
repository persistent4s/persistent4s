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

package persistent4s.examples.library.infrastructure

import cats.effect.{ExitCode, IO, IOApp}
import com.comcast.ip4s.*
import org.http4s.ember.server.EmberServerBuilder

import scala.concurrent.duration.*

object LibraryServer extends IOApp:

  private val DefaultHost: Host = host"0.0.0.0"

  private val DefaultPort: Port = port"8182"

  private val DefaultMonitoringPort: Port = port"9595"

  private def httpConfig(args: List[String]): (Host, Port, Port) =
    val host = args.lift(0).orElse(sys.env.get("LIBRARY_HTTP_HOST")).flatMap(Host.fromString).getOrElse(DefaultHost)
    val port = args.lift(1).orElse(sys.env.get("LIBRARY_HTTP_PORT")).flatMap(Port.fromString).getOrElse(DefaultPort)
    val monitoringPort = args
      .lift(2)
      .orElse(sys.env.get("LIBRARY_MONITORING_PORT"))
      .flatMap(Port.fromString)
      .getOrElse(DefaultMonitoringPort)
    (host, port, monitoringPort)

  def run(args: List[String]): IO[ExitCode] =
    val (host, port, monitoringPort) = httpConfig(args)
    LibraryModule
      .make(monitoringPort = monitoringPort)
      .use { module =>
        LibraryRoutes.make(module).use { routes =>
          EmberServerBuilder
            .default[IO]
            .withHost(host)
            .withPort(port)
            .withShutdownTimeout(2.seconds)
            .withHttpApp(routes.orNotFound)
            .build
            .useForever
        }
      }
      .as(ExitCode.Success)
