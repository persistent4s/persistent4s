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

package persistent4s.testkit

import cats.effect.Concurrent
import cats.effect.Deferred
import cats.effect.IO
import cats.effect.Ref
import cats.syntax.all.*
import fs2.Stream

import persistent4s.{CommandHandler, EventEnvelope, EventStore, IndexConflictException, Tag}
import weaver.SimpleIOSuite

object CommandHandlerRunSuite extends SimpleIOSuite:

  private def studentTag(studentId: String): Tag =
    Tag("student", studentId)

  final case class CreateStudent(studentId: String)

  final case class DeleteStudent(studentId: String)

  final case class StudentState(exists: Boolean)

  sealed trait TestEvent

  object TestEvent:

    final case class StudentCreated(studentId: String) extends TestEvent

    final case class StudentDeleted(studentId: String) extends TestEvent

  private def evolve(state: StudentState, event: TestEvent): StudentState =
    event match
      case _: TestEvent.StudentCreated => state.copy(exists = true)
      case _: TestEvent.StudentDeleted => state.copy(exists = false)

  private def isConflict(result: Either[Throwable, Unit]): Boolean =
    result match
      case Left(_: IndexConflictException) => true
      case _                               => false

  private def barrieredStore(
    underlying: InMemoryEventStore[IO, TestEvent],
    arrivals: Ref[IO, Int],
    gate: Deferred[IO, Unit],
  ): EventStore[IO, TestEvent] =
    new EventStore[IO, TestEvent]:

      def append(expectedIndex: Long, events: List[(Set[Tag], String, TestEvent)]*): IO[Unit] =
        underlying.append(expectedIndex, events*)

      def read(eventTypes: List[String], tags: Set[Tag]*): Stream[IO, EventEnvelope[TestEvent]] =
        underlying.read(eventTypes, tags*).onFinalize {
          arrivals.updateAndGet(_ + 1).flatMap { count =>
            if count == 2 then gate.complete(()).attempt.void else IO.unit
          } >> gate.get
        }

  object CreateStudentHandler extends CommandHandler[CreateStudent, StudentState, TestEvent]:

    def tags(command: CreateStudent): Set[Tag] =
      Set(studentTag(command.studentId))

    def initial: StudentState =
      StudentState(exists = false)

    def evolve(state: StudentState, event: TestEvent): StudentState =
      CommandHandlerRunSuite.evolve(state, event)

    def validate[F[_]: Concurrent](state: StudentState, command: CreateStudent): F[Unit] =
      Concurrent[F].raiseError(new RuntimeException("Student already exists")).whenA(state.exists)

    def decide(state: StudentState, command: CreateStudent): List[(Set[Tag], TestEvent)] =
      List((Set(studentTag(command.studentId)), TestEvent.StudentCreated(command.studentId)))

  object DeleteStudentHandler extends CommandHandler[DeleteStudent, StudentState, TestEvent]:

    def tags(command: DeleteStudent): Set[Tag] =
      Set(studentTag(command.studentId))

    def initial: StudentState =
      StudentState(exists = false)

    def evolve(state: StudentState, event: TestEvent): StudentState =
      CommandHandlerRunSuite.evolve(state, event)

    def validate[F[_]: Concurrent](state: StudentState, command: DeleteStudent): F[Unit] =
      Concurrent[F].raiseError(new RuntimeException("Student does not exist")).unlessA(state.exists)

    def decide(state: StudentState, command: DeleteStudent): List[(Set[Tag], TestEvent)] =
      List((Set(studentTag(command.studentId)), TestEvent.StudentDeleted(command.studentId)))

  test("run appends the decided event for a valid command") {
    for
      store <- InMemoryEventStore.make[IO, TestEvent]
      _     <- {
        given persistent4s.EventStore[IO, TestEvent] = store
        CreateStudentHandler.run[IO](CreateStudent("1"))
      }
      events <- store.getEvents
    yield expect.all(
      events.length == 1,
      events.head.metadata.tags == Set(studentTag("1")),
      events.head.metadata.eventType == "StudentCreated",
      events.head.payload == TestEvent.StudentCreated("1"),
    )
  }

  test("run rebuilds state from previous events before deciding") {
    for
      store <- InMemoryEventStore.make[IO, TestEvent]
      _     <- store.append(
             0L,
             List((Set(studentTag("1")), "StudentCreated", TestEvent.StudentCreated("1"))),
           )
      _ <- {
        given persistent4s.EventStore[IO, TestEvent] = store
        DeleteStudentHandler.run[IO](DeleteStudent("1"))
      }
      events <- store.getEvents
    yield expect.all(
      events.length == 2,
      events.last.metadata.tags == Set(studentTag("1")),
      events.last.metadata.eventType == "StudentDeleted",
      events.last.payload == TestEvent.StudentDeleted("1"),
    )
  }

  test("run does not append events when validation fails") {
    for
      store  <- InMemoryEventStore.make[IO, TestEvent]
      result <- {
                  given persistent4s.EventStore[IO, TestEvent] = store
                  DeleteStudentHandler.run[IO](DeleteStudent("1"))
                }.attempt
      events <- store.getEvents
    yield expect.all(
      result.isLeft,
      events.isEmpty,
    )
  }

  test("concurrent runs on the same tag cause one optimistic concurrency conflict") {
    for
      store            <- InMemoryEventStore.make[IO, TestEvent]
      arrivals         <- Ref.of[IO, Int](0)
      gate             <- Deferred[IO, Unit]
      synchronizedStore = barrieredStore(store, arrivals, gate)
      first             = {
        given EventStore[IO, TestEvent] = synchronizedStore
        CreateStudentHandler.run[IO](CreateStudent("1")).attempt
      }
      second = {
        given EventStore[IO, TestEvent] = synchronizedStore
        CreateStudentHandler.run[IO](CreateStudent("1")).attempt
      }
      results      <- (first, second).parTupled
      events       <- store.getEvents
      successCount  = List(results._1, results._2).count(_.isRight)
      conflictCount = List(results._1, results._2).count(isConflict)
    yield expect.all(
      successCount == 1,
      conflictCount == 1,
      events.length == 1,
      events.head.metadata.tags == Set(studentTag("1")),
      events.head.metadata.eventType == "StudentCreated",
      events.head.payload == TestEvent.StudentCreated("1"),
    )
  }
