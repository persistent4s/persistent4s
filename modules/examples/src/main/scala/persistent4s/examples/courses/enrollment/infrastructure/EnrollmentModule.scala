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

package persistent4s.examples.courses.enrollment.infrastructure

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
import persistent4s.examples.courses.enrollment.domain.*
import persistent4s.examples.courses.enrollment.domain.courseview.{CatalogEventConsumer, CourseViewRepository}
import persistent4s.examples.courses.enrollment.domain.enrollment.{
  EnrollStudentHandler,
  EnrollmentProjection,
  EnrollmentRepository,
}
import persistent4s.examples.courses.enrollment.domain.student.{StudentProjection, StudentRepository}
import persistent4s.kafka.{KafkaConsumerConfig, KafkaModule, KafkaProducerConfig}
import persistent4s.monitoring.MonitoringServer
import persistent4s.postgres.{PostgresConfig, PostgresEventStore, PostgresModule}

final class EnrollmentModule private (
  val store: PostgresEventStore[IO, SchoolEvent],
  val studentRepository: StudentRepository[IO],
  val enrollmentRepository: EnrollmentRepository[IO],
  val courseRepository: CourseViewRepository[IO],
)

object EnrollmentModule:

  val eventCodec: EventCodec[SchoolEvent] = CirceEventCodec.derived[SchoolEvent]

  private val pgConfigPath = "persistent4s.enrollment.postgres"

  private val kafkaConfigPath = "persistent4s.courses.kafka"

  val enrollmentTopic = "enrollment.events"

  val catalogTopic = "catalog.events"

  val catalogGroupId = "enrollment-service.catalog-consumer"

  def make: Resource[IO, EnrollmentModule] =
    for
      components <- PostgresModule.make[IO, SchoolEvent](eventCodec, pgConfigPath, enableOutbox = true)
      store       = components.eventStore
      checkpoint  = components.checkpoint
      outbox     <- Resource.eval(
                  IO.fromOption(components.outbox)(
                    new IllegalStateException("Outbox missing despite enableOutbox = true"),
                  ),
                )
      _         <- MonitoringServer.make[IO](checkpoint, store.notify, port = port"9092")
      pgConfig  <- Resource.eval(loadPgConfig)
      bootstrap <- Resource.eval(loadKafkaBootstrap)
      viewPool  <- Session
                    .Builder[IO]
                    .withHost(pgConfig.host)
                    .withPort(pgConfig.port)
                    .withUserAndPassword(pgConfig.user, pgConfig.password)
                    .withDatabase(pgConfig.database)
                    .pooled(pgConfig.maxConnections)
      studentRepo     = StudentRepository.make[IO](viewPool)
      enrollmentRepo  = EnrollmentRepository.make[IO](viewPool)
      courseViewRepo  = CourseViewRepository.make[IO](viewPool)
      studentProj    <- Resource.eval(StudentProjection.make[IO](studentRepo))
      enrollmentProj <- Resource.eval(EnrollmentProjection.make[IO](enrollmentRepo))
      projector       = DefaultProjector[IO, SchoolEvent](store, checkpoint)
      _              <- projector.run(studentProj).compile.drain.background
      _              <- projector.run(enrollmentProj).compile.drain.background
      consumerCfg     = KafkaConsumerConfig(
                      bootstrapServers = bootstrap,
                      groupId = catalogGroupId,
                    )
    /*       catalogStream <- CatalogEventConsumer.stream[IO](consumerCfg, catalogCodec, courseViewRepo, catalogTopic)
      _             <- catalogStream.compile.drain.background
      producerCfg    = KafkaProducerConfig[SchoolEvent](
                      bootstrapServers = bootstrap,
                      recordKey = _.metadata.tags.find(_.category == "student").map(_.id).getOrElse("unknown"),
                    )
      relay        <- KafkaModule.relay[IO, SchoolEvent](outbox, producerCfg, eventCodec, topic = enrollmentTopic)
      _            <- relay.run.background */
    yield new EnrollmentModule(store, studentRepo, enrollmentRepo, courseViewRepo)

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
