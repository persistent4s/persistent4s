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

import cats.effect.{IO, Ref}

import fs2.Stream
import fs2.concurrent.Topic

import weaver.SimpleIOSuite

object LeaderElectedProjectorSuite extends SimpleIOSuite:

  final case class TestEvent(id: String) extends Event

  /** Only the projection's name and identity matter to the decorator. */
  private def projection(projectionName: String): Projection[IO, TestEvent, String, Int] =
    new Projection[IO, TestEvent, String, Int]:

      protected val repository: Repository[IO, String, Int] = new Repository[IO, String, Int]:
        def findMany(keys: List[String]): IO[Map[String, Option[Int]]] = IO.pure(Map.empty)
        def persist(upserts: Map[String, Int], deletes: List[String]): IO[Unit] = IO.unit

      def name: String = projectionName

      def filter: Set[EventTypeName] = Set.empty

      def resolveKeys(event: EventEnvelope[TestEvent]): List[String] = Nil

      def handle(state: Option[Int], event: EventEnvelope[TestEvent]): IO[Option[Int]] = IO.pure(state)

  final private case class Call(projectionName: String, topicPassed: Boolean)

  /** A projector that records each `run` instead of touching a store. */
  private def recordingProjector(calls: Ref[IO, List[Call]]): Projector[IO, TestEvent] =
    new Projector[IO, TestEvent]:
      def run[K, S](
        projection: Projection[IO, TestEvent, K, S],
        topic: Option[Topic[IO, (UUID, Either[Throwable, Map[K, Option[S]]])]] = None,
      ): Stream[IO, Unit] =
        Stream.eval(calls.update(_ :+ Call(projection.name, topic.isDefined)))

  private def grantingLeadership(acquired: Ref[IO, List[String]]): LeaderElection[IO] =
    new LeaderElection[IO]:
      def runAsLeader(name: String)(task: IO[Unit]): IO[Unit] =
        acquired.update(_ :+ name) *> task

  /** Models another instance holding the lease: `runAsLeader` stands by and never returns. */
  private val neverLeader: LeaderElection[IO] =
    new LeaderElection[IO]:
      def runAsLeader(name: String)(task: IO[Unit]): IO[Unit] = IO.never

  test("runs the underlying projector under leadership taken for the projection's own name") {
    for
      calls    <- Ref.of[IO, List[Call]](Nil)
      acquired <- Ref.of[IO, List[String]](Nil)
      _        <- LeaderElectedProjector(recordingProjector(calls), grantingLeadership(acquired))
             .run(projection("books"))
             .compile
             .drain
      ran   <- calls.get
      names <- acquired.get
    yield expect.all(
      ran == List(Call("books", topicPassed = false)),
      // leadership is keyed per projection, not once for the whole group
      names == List("books"),
    )
  }

  test("passes the topic through to the underlying projector") {
    // Without this, a leader-elected projection could never drive a SyncCommandHandler.
    for
      calls    <- Ref.of[IO, List[Call]](Nil)
      acquired <- Ref.of[IO, List[String]](Nil)
      topic    <- Topic[IO, (UUID, Either[Throwable, Map[String, Option[Int]]])]
      _        <- LeaderElectedProjector(recordingProjector(calls), grantingLeadership(acquired))
             .run(projection("books"), Some(topic))
             .compile
             .drain
      ran <- calls.get
    yield expect(ran == List(Call("books", topicPassed = true)))
  }

  test("does not touch the projection while another instance holds leadership") {
    for
      calls   <- Ref.of[IO, List[Call]](Nil)
      outcome <- LeaderElectedProjector(recordingProjector(calls), neverLeader)
                   .run(projection("books"))
                   .compile
                   .drain
                   .timeout(200.millis)
                   .attempt
      ran <- calls.get
    yield expect.all(
      // standing by neither completes the stream nor fails it, so grouping it with startAll stays quiet
      outcome.isLeft,
      ran.isEmpty,
    )
  }
