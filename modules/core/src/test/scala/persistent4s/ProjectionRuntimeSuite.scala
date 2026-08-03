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

package persistent4s

import java.util.UUID

import scala.concurrent.duration.*

import cats.effect.{Deferred, IO}
import cats.syntax.all.*

import fs2.Stream

import weaver.SimpleIOSuite

object ProjectionRuntimeSuite extends SimpleIOSuite:

  sealed trait TestEvent extends Event

  private def projection[K, S](projectionName: String): Projection[IO, TestEvent, K, S] =
    new Projection[IO, TestEvent, K, S]:
      protected val repository: Repository[IO, K, S] = new Repository[IO, K, S]:
        def findMany(keys: List[K]): IO[Map[K, Option[S]]] = IO.pure(Map.empty)
        def persist(upserts: Map[K, S], deletes: List[K]): IO[Unit] = IO.unit

      def name: String = projectionName
      def filter: Set[EventTypeName] = Set.empty
      def resolveKeys(event: EventEnvelope[TestEvent]): List[K] = Nil
      def handle(state: Option[S], event: EventEnvelope[TestEvent]): IO[Option[S]] = IO.pure(state)

  private def runtime(streamFor: String => Stream[IO, Unit]): ProjectionRuntime[IO, TestEvent] =
    ProjectionRuntime(
      new Projector[IO, TestEvent]:
        def run[K, S](projection: Projection[IO, TestEvent, K, S]): Stream[IO, Unit] =
          streamFor(projection.name),
    )

  test("duplicate projection names fail before startup across heterogeneous projections") {
    runtime(_ => Stream.never[IO])
      .startAll { registered =>
        registered.run(projection[String, Int]("duplicate"))
        registered.run(projection[UUID, String]("duplicate"))
      }
      .use(_ => IO.unit)
      .attempt
      .map {
        case Left(error: DuplicateProjectionNames) => expect(error.names == List("duplicate"))
        case other                                 => failure(s"Expected DuplicateProjectionNames, got $other")
      }
  }

  test("normal projection completion is observable as unexpected termination") {
    runtime(_ => Stream.empty)
      .startAll(_.run(projection[String, Int]("completed")))
      .use(_.await.attempt)
      .map {
        case Left(error: ProjectionTerminatedUnexpectedly) => expect(error.projectionName == "completed")
        case other                                         => failure(s"Expected named termination, got $other")
      }
  }

  test("projection failures remain observable with their projection name and cause") {
    val expected = new RuntimeException("boom")

    runtime(_ => Stream.raiseError[IO](expected))
      .startAll(_.run(projection[String, Int]("failed")))
      .use(_.await.attempt)
      .map {
        case Left(error: ProjectionExecutionFailed) =>
          expect(error.projectionName == "failed") and expect(error.getCause eq expected)
        case other => failure(s"Expected ProjectionExecutionFailed, got $other")
      }
  }

  test("releasing the runtime cancels every heterogeneous projection") {
    for
      firstStarted  <- Deferred[IO, Unit]
      secondStarted <- Deferred[IO, Unit]
      firstStopped  <- Deferred[IO, Unit]
      secondStopped <- Deferred[IO, Unit]

      streamFor = (name: String) =>
                    val (started, stopped) =
                      if name == "first" then (firstStarted, firstStopped)
                      else (secondStarted, secondStopped)
                    (Stream.eval(started.complete(()).void) ++ Stream.never[IO])
                      .onFinalize(stopped.complete(()).void)

      _ <- runtime(streamFor)
             .startAll { registered =>
               registered.run(projection[String, Int]("first"))
               registered.run(projection[UUID, String]("second"))
             }
             .use(_ => (firstStarted.get, secondStarted.get).parTupled.void)

      _ <- (firstStopped.get, secondStopped.get).parTupled.timeout(2.seconds)
    yield success
  }
