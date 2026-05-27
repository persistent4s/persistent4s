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

import cats.effect.{IO, Resource}
import com.comcast.ip4s.*
import io.circe.{Decoder, Encoder}
import io.circe.syntax.*
import org.http4s.*
import org.http4s.client.Client
import org.http4s.ember.client.EmberClientBuilder
import org.http4s.implicits.*
import org.testcontainers.containers.PostgreSQLContainer
import persistent4s.Event
import persistent4s.circe.CirceEventCodec
import persistent4s.postgres.{PostgresConfig, PostgresModule}
import weaver.IOSuite
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.log4cats.Logger

given Tracer[IO] = org.typelevel.otel4s.trace.Tracer.Implicits.noop

given Meter[IO] = org.typelevel.otel4s.metrics.Meter.Implicits.noop

given Logger[IO] = org.typelevel.log4cats.noop.NoOpLogger[IO]

object MonitoringServerSuite extends IOSuite:

  override def maxParallelism: Int = 1

  type Res = Client[IO]

  final case class TestEvent(value: String) extends Event derives Encoder.AsObject, Decoder

  private val eventCodec = CirceEventCodec.make[TestEvent](
    encodeEvent = _.asJson,
    decodeEvent = (_, json) => json.as[TestEvent].left.map(identity),
  )

  private type Container = PostgreSQLContainer[Nothing]

  private def postgresConfig(c: Container): PostgresConfig =
    PostgresConfig(
      host = c.getHost, port = c.getMappedPort(5432), user = c.getUsername, password = c.getPassword,
      database = c.getDatabaseName, maxConnections = 4,
    )

  private def postgresContainerResource: Resource[IO, Container] =
    Resource.make(
      IO.blocking {
        val c = new PostgreSQLContainer[Nothing]("postgres:16-alpine")
        c.start()
        c
      },
    )(c => IO.blocking(c.stop()).handleErrorWith(_ => IO.unit))

  override def sharedResource: Resource[IO, Client[IO]] =
    for
      container  <- postgresContainerResource
      components <- PostgresModule.makeWithConfig[IO, TestEvent](postgresConfig(container), eventCodec)
      _          <- MonitoringServer.make[IO](
             components.checkpoint,
             components.eventStore.notify,
             port"9092",
           )
      client <- EmberClientBuilder.default[IO].build
    yield client

  test("GET / returns 200") { client =>
    client.get(uri"http://localhost:9092/")(resp => IO.pure(expect(resp.status.code == 200)))
  }

  test("GET / body contains Projection Checkpoints heading") { client =>
    client.get(uri"http://localhost:9092/") { resp =>
      resp.as[String].map(body => expect(body.contains("Projection Checkpoints")))
    }
  }

  test("POST /checkpoints/{name}/pause redirects to /") { client =>
    val req = Request[IO](Method.POST, uri"http://localhost:9092/checkpoints/test-proj/pause")
    client.run(req).use(resp => IO.pure(expect(resp.status == Status.SeeOther)))
  }
