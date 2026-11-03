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

import cats.effect.{IO, Resource}
import cats.syntax.all.*
import io.circe.{Decoder, Encoder}
import io.circe.syntax.*
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.trace.{SpanKind, StatusCode}
import org.testcontainers.containers.PostgreSQLContainer
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.noop.NoOpLogger
import org.typelevel.otel4s.metrics.{Meter, MeterProvider}
import org.typelevel.otel4s.oteljava.testkit.OtelJavaTestkit
import org.typelevel.otel4s.trace.{Tracer, TracerProvider}
import persistent4s.Event
import persistent4s.circe.CirceEventCodec
import skunk.codec.all.*
import skunk.exception.PostgresErrorException
import skunk.implicits.*
import weaver.IOSuite

import scala.jdk.CollectionConverters.*

object PostgresTelemetrySuite extends IOSuite:

  override type Res = Fixture

  override def maxParallelism: Int = 1

  final case class TestEvent(value: String) extends Event derives Encoder, Decoder

  final case class Fixture(
    postgres: PostgresDomain[IO, TestEvent],
    testkit: OtelJavaTestkit[IO],
    tracer: Tracer[IO],
  )

  private given Logger[IO] = NoOpLogger[IO]

  override def sharedResource: Resource[IO, Fixture] =
    for
      container <- Resource.make(IO.blocking {
                     val container = new PostgreSQLContainer[Nothing]("postgres:16-alpine")
                     container.start()
                     container
                   })(container => IO.blocking(container.stop()))
      testkit  <- OtelJavaTestkit.inMemory[IO]()
      tracer   <- Resource.eval(testkit.tracerProvider.get("postgres-test"))
      meter    <- Resource.eval(testkit.meterProvider.get("postgres-test"))
      postgres <- {
        given TracerProvider[IO] = testkit.tracerProvider
        given MeterProvider[IO] = testkit.meterProvider
        given Tracer[IO] = tracer
        given Meter[IO] = meter
        PostgresModule
          .makeWithConfig[IO](
            PostgresConfig(
              host = container.getHost, port = container.getMappedPort(5432), user = container.getUsername,
              password = container.getPassword, database = container.getDatabaseName, maxConnections = 2,
            ),
          )
          .flatMap(_.domain(CirceEventCodec.make[TestEvent](_.asJson, (_, json) => json.as[TestEvent])))
      }
    yield Fixture(postgres, testkit, tracer)

  test("connection maintenance emits database spans without pool or protocol spans") { fixture =>
    for
      before <- fixture.testkit.finishedSpans
      _      <- fixture.postgres.module.sessions.use(_ => IO.unit)
      _      <- fixture.postgres.module.sessions.use(_ => IO.unit)
      after  <- fixture.testkit.finishedSpans
    yield expect(after.drop(before.size).forall(_.getKind == SpanKind.CLIENT)) and
      expect(
        after.forall(span =>
          !Set("pool.allocate", "pool.free", "recycle", "dispose", "parse+describe", "bind+execute", "close-portal")
            .contains(span.getName),
        ),
      )
  }

  test("SQL query spans keep the request parent and omit bound values and literal contents") { fixture =>
    val privateValue = "private-bound-patient-value"
    for
      values <- fixture.tracer.span("patient.find").surround {
                  fixture.postgres.module.sessions.use { session =>
                    for
                      _       <- session.execute(sql"CREATE TABLE trace_records (value TEXT NOT NULL)".command)
                      _       <- session.execute(sql"INSERT INTO trace_records (value) VALUES ($text)".command)(privateValue)
                      bound   <- session.unique(sql"SELECT value FROM trace_records".query(text))
                      literal <- session.unique(sql"SELECT 'private-literal-patient-value'::text".query(text))
                    yield (bound, literal)
                  }
                }
      spans     <- fixture.testkit.finishedSpans
      root      <- IO.fromOption(spans.find(_.getName == "patient.find"))(new AssertionError("Missing request span"))
      children   = spans.filter(span => span.getTraceId == root.getTraceId && span.getSpanId != root.getSpanId)
      attributes = children.flatMap(_.getAttributes.asMap.asScala.values).mkString
      metrics   <- fixture.testkit.collectMetrics
    yield expect(values == (privateValue, "private-literal-patient-value")) and
      expect(children.nonEmpty) and
      expect(children.forall(span => span.getKind == SpanKind.CLIENT && span.getParentSpanId == root.getSpanId)) and
      expect(children.exists(span => span.getName.startsWith("INSERT") && span.getName.contains("trace_records"))) and
      expect(children.exists(span => span.getName.startsWith("SELECT") && span.getName.contains("trace_records"))) and
      expect(children.forall(_.getAttributes.get(AttributeKey.stringKey("db.system.name")) == "postgresql")) and
      expect(children.exists(_.getAttributes.asMap.containsKey(AttributeKey.stringKey("db.query.text")))) and
      expect(!attributes.contains(privateValue) && !attributes.contains("private-literal-patient-value")) and
      expect(
        children.forall(span =>
          !span.getAttributes.asMap.asScala.keys.exists(_.getKey.startsWith("db.query.parameter.")),
        ),
      ) and
      expect(
        metrics.exists(metric =>
          metric.getName == "db.client.operation.duration" && metric.getUnit == "s" &&
            metric.getHistogramData.getPoints.asScala.exists(_.getCount > 0L),
        ),
      )
  }

  test("failed SQL queries retain error status and SQLSTATE without parameter attributes") { fixture =>
    val privateValue = "private-error-patient-value"
    for
      result <- fixture.tracer.span("patient.failed-query").surround {
                  fixture.postgres.module.sessions.use { session =>
                    session.unique(sql"SELECT $text::text, 1 / $int4".query(text *: int4))(
                      privateValue *: 0 *: EmptyTuple,
                    )
                  }.attempt
                }
      spans  <- fixture.testkit.finishedSpans
      root   <- IO.fromOption(spans.find(_.getName == "patient.failed-query"))(new AssertionError("Missing request span"))
      queries = spans.filter(span => span.getTraceId == root.getTraceId && span.getKind == SpanKind.CLIENT)
    yield expect(result.left.toOption.collect { case error: PostgresErrorException =>
      error.code
    }.contains("22012")) and
      expect(
        queries.exists(span =>
          span.getStatus.getStatusCode == StatusCode.ERROR && span.getParentSpanId == root.getSpanId,
        ),
      ) and
      expect(queries.exists(_.getAttributes.get(AttributeKey.stringKey("db.response.status_code")) == "22012")) and
      expect(
        queries.forall(span =>
          !span.getAttributes.asMap.asScala.keys.exists(_.getKey.startsWith("db.query.parameter.")),
        ),
      )
  }
