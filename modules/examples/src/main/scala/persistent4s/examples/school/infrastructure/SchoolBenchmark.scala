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

package persistent4s.examples.school.infrastructure

import java.util.UUID

import cats.effect.{IO, IOApp}
import fs2.io.net.Network
import natchez.Trace.Implicits.noop
import skunk.*
import skunk.implicits.*

import persistent4s.EventStore
import persistent4s.examples.school.domain.SchoolEvent
import persistent4s.examples.school.domain.student.*
import persistent4s.examples.school.infrastructure.SchoolEventCodec.codec
import persistent4s.postgres.PostgresEventStore

object SchoolBenchmark extends IOApp.Simple:

  private val totalEvents  = 10_000
  private val parallelism  = 50

  private val config = PostgresEventStore.Config(
    host = "localhost", port = 5450, database = "persistent4s", user = "persistent4s", password = "persistent4s",
    maxConnections = 50,
  )

  def run: IO[Unit] =
    PostgresEventStore.make[IO, SchoolEvent](config, codec).use { store =>
      given EventStore[IO, SchoolEvent] = store

      for
        _ <- IO.println(s"Benchmark: creating $totalEvents students with parallelism=$parallelism")
        _ <- IO.println("Truncating events table...")
        _ <- truncateEvents
        _ <- IO.println("Starting benchmark...")

        start <- IO.monotonic
        _     <- createStudents
        end   <- IO.monotonic

        duration  = (end - start).toMillis
        perSecond = if duration > 0 then (totalEvents.toDouble / duration * 1000).toLong else 0L

        _ <- IO.println(s"Created $totalEvents events in ${duration}ms")
        _ <- IO.println(s"Throughput: $perSecond events/second")
      yield ()
    }

  private def createStudents(using EventStore[IO, SchoolEvent]): IO[Unit] =
    fs2.Stream
      .range(0, totalEvents)
      .covary[IO]
      .parEvalMap(parallelism) { _ =>
        val studentId = UUID.randomUUID().toString
        CreateStudentHandler.run[IO](CreateStudent(studentId, s"Student $studentId", s"$studentId@test.com"))
      }
      .compile
      .drain

  private def truncateEvents: IO[Unit] =
    Session
      .single[IO](
        host = config.host, port = config.port, database = config.database, user = config.user,
        password = Some(config.password),
      )
      .use(_.execute(sql"TRUNCATE events RESTART IDENTITY".command).void)
