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

package persistent4s.examples.courses.catalog.infrastructure

import cats.effect.*
import com.comcast.ip4s.*
import fs2.io.net.Network
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import pureconfig.ConfigSource
import skunk.*

given Tracer[IO] = Tracer.Implicits.noop

given Meter[IO] = Meter.Implicits.noop

given Logger[IO] = Slf4jLogger.getLogger[IO]

import persistent4s.*
import persistent4s.circe.CirceEventCodec
import persistent4s.examples.courses.catalog.domain.*
import persistent4s.examples.courses.catalog.domain.course.{CourseProjection, CourseRepository}
import persistent4s.kafka.{KafkaModule, KafkaProducerConfig}
import persistent4s.monitoring.MonitoringServer
import persistent4s.postgres.{PostgresConfig, PostgresEventStore, PostgresModule}

final class CatalogModule private (
  val store: PostgresEventStore[IO, CatalogEvent],
  val courseRepository: CourseRepository[IO],
)

object CatalogModule:

  val eventCodec: EventCodec[CatalogEvent] = CirceEventCodec.derived[CatalogEvent]

  private val pgConfigPath = "persistent4s.catalog.postgres"

  private val kafkaConfigPath = "persistent4s.courses.kafka"

  val catalogTopic = "catalog.events"

  def make: Resource[IO, CatalogModule] =
    for
      components <- PostgresModule.make[IO, CatalogEvent](eventCodec, pgConfigPath, enableOutbox = true)
      store       = components.eventStore
      checkpoint  = components.checkpoint
      outbox     <- Resource.eval(
                  IO.fromOption(components.outbox)(
                    new IllegalStateException("Outbox missing despite enableOutbox = true"),
                  ),
                )
      _         <- MonitoringServer.make[IO](checkpoint, store.notify, port = port"9091")
      pgConfig  <- Resource.eval(loadPgConfig)
      bootstrap <- Resource.eval(loadKafkaBootstrap)
      viewPool  <- Session
                    .Builder[IO]
                    .withHost(pgConfig.host)
                    .withPort(pgConfig.port)
                    .withUserAndPassword(pgConfig.user, pgConfig.password)
                    .withDatabase(pgConfig.database)
                    .pooled(pgConfig.maxConnections)
      courseRepo  = CourseRepository.make[IO](viewPool)
      courseProj <- Resource.eval(CourseProjection.make[IO](courseRepo))
      projector   = DefaultProjector[IO, CatalogEvent](store, checkpoint)
      _          <- projector.run(courseProj).compile.drain.background
      producerCfg = KafkaProducerConfig[CatalogEvent](
                      bootstrapServers = bootstrap,
                      recordKey = _.metadata.tags.find(_.category == "course").map(_.id).getOrElse("unknown"),
                    )
      relay <- KafkaModule.relay[IO, CatalogEvent](outbox, producerCfg, eventCodec, topic = catalogTopic)
      _     <- relay.run.background
    yield new CatalogModule(store, courseRepo)

  private def loadPgConfig: IO[PostgresConfig] =
    IO.delay(ConfigSource.default.at(pgConfigPath).load[PostgresConfig]).flatMap {
      case Right(c) => IO.pure(c)
      case Left(e)  => IO.raiseError(new RuntimeException(e.prettyPrint()))
    }

  private def loadKafkaBootstrap: IO[String] =
    IO.delay(ConfigSource.default.at(s"$kafkaConfigPath.bootstrap-servers").load[String]).flatMap {
      case Right(s) => IO.pure(s)
      case Left(e)  => IO.raiseError(new RuntimeException(e.prettyPrint()))
    }
