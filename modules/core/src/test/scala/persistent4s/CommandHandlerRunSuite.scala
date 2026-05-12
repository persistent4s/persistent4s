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

import cats.Monad
import cats.effect.{Async, Deferred, IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import weaver.SimpleIOSuite
import persistent4s.CommandHandlerRunSuite.TestEvent.StudentCreated
import persistent4s.CommandHandlerRunSuite.TestEvent.StudentDeleted
import persistent4s.CommandHandlerRunSuite.CounterEvent.Incremented

object CommandHandlerRunSuite extends SimpleIOSuite:

  // ---------------------------------------------------------------------------
  // Minimal in-memory EventStore for testing — no testkit dependency needed
  // ---------------------------------------------------------------------------

  final class InMemoryEventStore[F[_]: Monad: Async, A <: Event] private (
    store: Ref[F, Vector[EventEnvelope[A]]],
  ) extends EventStore[F, A]:

    def getEvents: F[Vector[EventEnvelope[A]]] = store.get

    override def append(
      filter: EventFilter,
      expectedIndex: Long,
      events: List[(Set[Tag], EventTypeName, A)]*,
    ): F[Unit] =
      store.modify { currentEvents =>
        val incomingTags = events.flatten.map(_._1).flatten.toSet
        val relevantEvents = currentEvents.filter(env => env.metadata.tags.exists(incomingTags.contains))
        val actualIndex = relevantEvents.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
        if actualIndex != expectedIndex then
          (currentEvents, Left(new IndexConflictException(expectedIndex, actualIndex)))
        else
          val lastGlobalPosition = currentEvents.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
          val newEvents = events.flatten.zipWithIndex.map { case ((tags, eventType, event), i) =>
            EventEnvelope(
              EventMetadata(
                globalPosition = lastGlobalPosition + i.toLong + 1L,
                tags = tags,
                eventType = eventType,
                timestamp = java.time.Instant.now(),
              ),
              event,
            )
          }
          (currentEvents ++ newEvents, Right(()))
      }.flatMap {
        case Left(error) => Async[F].raiseError(error)
        case Right(_)    => Async[F].unit
      }

    override def readFrom(
      fromPosition: Long,
      eventFilter: EventFilter,
      maxEvents: Option[Int],
    ): Stream[F, EventEnvelope[A]] =
      Stream
        .eval(store.get)
        .flatMap(Stream.emits)
        .filter { env =>
          val matchesTags = env.metadata.tags.exists(eventFilter.tags.contains)
          val matchesTypes = eventFilter.eventTypes.isEmpty || eventFilter.eventTypes.contains(env.metadata.eventType)
          matchesTags && matchesTypes
        }

  object InMemoryEventStore:

    def make[F[_]: Async, A <: Event]: F[InMemoryEventStore[F, A]] =
      Ref.of[F, Vector[EventEnvelope[A]]](Vector.empty).map(new InMemoryEventStore(_))

  // ---------------------------------------------------------------------------
  // Test domain
  // ---------------------------------------------------------------------------

  private def studentTag(studentId: String): Tag = Tag("student", studentId)

  final case class CreateStudent(studentId: String)

  final case class DeleteStudent(studentId: String)

  final case class StudentState(exists: Boolean)

  sealed trait TestEvent extends Event

  object TestEvent:

    final case class StudentCreated(studentId: String) extends TestEvent

    final case class StudentDeleted(studentId: String) extends TestEvent

  private def evolve(state: StudentState, event: TestEvent): StudentState =
    event match
      case _: TestEvent.StudentCreated => state.copy(exists = true)
      case _: TestEvent.StudentDeleted => state.copy(exists = false)

  // Counter domain — always-valid command, good for testing that retry can succeed
  final case class IncrementCounter(counterId: String)

  final case class CounterState(value: Int)

  sealed trait CounterEvent extends Event

  object CounterEvent:

    final case class Incremented(counterId: String) extends CounterEvent

  object IncrementCounterHandler extends CommandHandler[IncrementCounter, CounterState, CounterEvent]:

    def tags(command: IncrementCounter): Set[Tag] = Set(Tag("counter", command.counterId))

    def initial: CounterState = CounterState(0)

    def evolve(command: IncrementCounter, state: CounterState, event: CounterEvent): CounterState =
      CounterState(state.value + 1)

    def validate(state: CounterState, command: IncrementCounter): Either[Throwable, Unit] =
      Right(())

    def decide(state: CounterState, command: IncrementCounter): List[(Set[Tag], CounterEvent)] =
      List((Set(Tag("counter", command.counterId)), CounterEvent.Incremented(command.counterId)))

  private def isConflict(result: Either[Throwable, Unit]): Boolean =
    result match
      case Left(_: IndexConflictException) => true
      case _                               => false

  private def barrieredStore[A <: Event](
    underlying: InMemoryEventStore[IO, A],
    arrivals: Ref[IO, Int],
    gate: Deferred[IO, Unit],
  ): EventStore[IO, A] =
    new EventStore[IO, A]:
      def append(filter: EventFilter, expectedIndex: Long, events: List[(Set[Tag], EventTypeName, A)]*): IO[Unit] =
        underlying.append(filter, expectedIndex, events*)
      def readFrom(
        fromPosition: Long,
        eventFilter: EventFilter,
        maxEvents: Option[Int],
      ): Stream[IO, EventEnvelope[A]] =
        underlying.readFrom(fromPosition, eventFilter, maxEvents).onFinalize {
          arrivals.updateAndGet(_ + 1).flatMap { count =>
            if count == 2 then gate.complete(()).attempt.void else IO.unit
          } >> gate.get
        }

  // ---------------------------------------------------------------------------
  // Command handlers
  // ---------------------------------------------------------------------------

  object CreateStudentHandler extends CommandHandler[CreateStudent, StudentState, TestEvent]:

    def tags(command: CreateStudent): Set[Tag] = Set(studentTag(command.studentId))

    def initial: StudentState = StudentState(exists = false)

    def evolve(command: CreateStudent, state: StudentState, event: TestEvent): StudentState =
      CommandHandlerRunSuite.evolve(state, event)

    def validate(state: StudentState, command: CreateStudent): Either[Throwable, Unit] =
      if (state.exists) Left(new RuntimeException("Student already exists")) else Right(())

    def decide(state: StudentState, command: CreateStudent): List[(Set[Tag], TestEvent)] =
      List((Set(studentTag(command.studentId)), TestEvent.StudentCreated(command.studentId)))

  object DeleteStudentHandler extends CommandHandler[DeleteStudent, StudentState, TestEvent]:

    def tags(command: DeleteStudent): Set[Tag] = Set(studentTag(command.studentId))

    def initial: StudentState = StudentState(exists = false)

    def evolve(command: DeleteStudent, state: StudentState, event: TestEvent): StudentState =
      CommandHandlerRunSuite.evolve(state, event)

    def validate(state: StudentState, command: DeleteStudent): Either[Throwable, Unit] =
      if (!state.exists) Left(new RuntimeException("Student does not exist")) else Right(())

    def decide(state: StudentState, command: DeleteStudent): List[(Set[Tag], TestEvent)] =
      List((Set(studentTag(command.studentId)), TestEvent.StudentDeleted(command.studentId)))

  object CreateStudentHandlerNoRetry extends CommandHandler[CreateStudent, StudentState, TestEvent]:

    override def maxRetries: Int = 0

    def tags(command: CreateStudent): Set[Tag] = Set(studentTag(command.studentId))

    def initial: StudentState = StudentState(exists = false)

    def evolve(command: CreateStudent, state: StudentState, event: TestEvent): StudentState =
      CommandHandlerRunSuite.evolve(state, event)

    def validate(state: StudentState, command: CreateStudent): Either[Throwable, Unit] =
      if (state.exists) Left(new RuntimeException("Student already exists")) else Right(())

    def decide(state: StudentState, command: CreateStudent): List[(Set[Tag], TestEvent)] =
      List((Set(studentTag(command.studentId)), TestEvent.StudentCreated(command.studentId)))

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  test("run appends the decided event for a valid command") {
    for
      store <- InMemoryEventStore.make[IO, TestEvent]
      _     <- {
        given EventStore[IO, TestEvent] = store
        CreateStudentHandler.run[IO](CreateStudent("1"))
      }
      events <- store.getEvents
    yield expect.all(
      events.length == 1,
      events.head.metadata.tags == Set(studentTag("1")),
      events.head.metadata.eventType == EventTypeName.of[StudentCreated],
      events.head.payload == TestEvent.StudentCreated("1"),
    )
  }

  test("run rebuilds state from previous events before deciding") {
    for
      store <- InMemoryEventStore.make[IO, TestEvent]
      _     <- store.append(
             EventFilter(),
             0L,
             List((Set(studentTag("1")), EventTypeName.of[StudentCreated], TestEvent.StudentCreated("1"))),
           )
      _ <- {
        given EventStore[IO, TestEvent] = store
        DeleteStudentHandler.run[IO](DeleteStudent("1"))
      }
      events <- store.getEvents
    yield expect.all(
      events.length == 2,
      events.last.metadata.tags == Set(studentTag("1")),
      events.last.metadata.eventType == EventTypeName.of[StudentDeleted],
      events.last.payload == TestEvent.StudentDeleted("1"),
    )
  }

  test("run does not append events when validation fails") {
    for
      store  <- InMemoryEventStore.make[IO, TestEvent]
      result <- {
                  given EventStore[IO, TestEvent] = store
                  DeleteStudentHandler.run[IO](DeleteStudent("1"))
                }.attempt
      events <- store.getEvents
    yield expect.all(
      result.isLeft,
      events.isEmpty,
    )
  }

  test("concurrent duplicate commands: retry hits validation after reading updated state") {
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
      results     <- (first, second).parTupled
      events      <- store.getEvents
      successCount = List(results._1, results._2).count(_.isRight)
      failureCount = List(results._1, results._2).count(_.isLeft)
    yield expect.all(
      successCount == 1,
      failureCount == 1,
      events.length == 1,
      events.head.metadata.tags == Set(studentTag("1")),
      events.head.metadata.eventType == EventTypeName.of[StudentCreated],
      events.head.payload == TestEvent.StudentCreated("1"),
    )
  }

  test("concurrent commands on the same tag: retry succeeds when validation is unconditional") {
    for
      store            <- InMemoryEventStore.make[IO, CounterEvent]
      arrivals         <- Ref.of[IO, Int](0)
      gate             <- Deferred[IO, Unit]
      synchronizedStore = barrieredStore(store, arrivals, gate)
      first             = {
        given EventStore[IO, CounterEvent] = synchronizedStore
        IncrementCounterHandler.run[IO](IncrementCounter("1")).attempt
      }
      second = {
        given EventStore[IO, CounterEvent] = synchronizedStore
        IncrementCounterHandler.run[IO](IncrementCounter("1")).attempt
      }
      results     <- (first, second).parTupled
      events      <- store.getEvents
      successCount = List(results._1, results._2).count(_.isRight)
    yield expect.all(
      successCount == 2,
      events.length == 2,
      events.forall(_.metadata.eventType == EventTypeName.of[Incremented]),
    )
  }

  test("retry exhaustion propagates IndexConflictException when maxRetries is 0") {
    for
      store            <- InMemoryEventStore.make[IO, TestEvent]
      arrivals         <- Ref.of[IO, Int](0)
      gate             <- Deferred[IO, Unit]
      synchronizedStore = barrieredStore(store, arrivals, gate)
      first             = {
        given EventStore[IO, TestEvent] = synchronizedStore
        CreateStudentHandlerNoRetry.run[IO](CreateStudent("1")).attempt
      }
      second = {
        given EventStore[IO, TestEvent] = synchronizedStore
        CreateStudentHandlerNoRetry.run[IO](CreateStudent("1")).attempt
      }
      results      <- (first, second).parTupled
      events       <- store.getEvents
      successCount  = List(results._1, results._2).count(_.isRight)
      conflictCount = List(results._1, results._2).count(isConflict)
    yield expect.all(
      successCount == 1,
      conflictCount == 1,
      events.length == 1,
    )
  }

  test("run only folds events matching eventTypes override into state") {
    val handlerFilteredToDeletedOnly = new CommandHandler[DeleteStudent, StudentState, TestEvent]:
      override def eventTypes: Option[Set[EventTypeName]] = Some(Set(EventTypeName.of[StudentDeleted]))
      def tags(command: DeleteStudent): Set[Tag] = Set(studentTag(command.studentId))
      def initial: StudentState = StudentState(exists = false)
      def evolve(command: DeleteStudent, state: StudentState, event: TestEvent): StudentState =
        CommandHandlerRunSuite.evolve(state, event)
      def validate(state: StudentState, command: DeleteStudent): Either[Throwable, Unit] =
        if (!state.exists) Left(new RuntimeException("Student does not exist")) else Right(())
      def decide(state: StudentState, command: DeleteStudent): List[(Set[Tag], TestEvent)] =
        List((Set(studentTag(command.studentId)), TestEvent.StudentDeleted(command.studentId)))

    for
      store <- InMemoryEventStore.make[IO, TestEvent]
      _     <- store.append(
             EventFilter(),
             0L,
             List((Set(studentTag("1")), EventTypeName.of[StudentCreated], TestEvent.StudentCreated("1"))),
           )
      result <- {
                  given EventStore[IO, TestEvent] = store
                  handlerFilteredToDeletedOnly.run[IO](DeleteStudent("1"))
                }.attempt
      events <- store.getEvents
    yield expect.all(
      result.isLeft,
      events.length == 1, // only the pre-seeded StudentCreated, nothing appended
    )
  }

  test("run appends all events returned by decide") {
    val multiEventHandler = new CommandHandler[CreateStudent, StudentState, TestEvent]:
      def tags(command: CreateStudent): Set[Tag] = Set(studentTag(command.studentId))
      def initial: StudentState = StudentState(exists = false)
      def evolve(command: CreateStudent, state: StudentState, event: TestEvent): StudentState =
        CommandHandlerRunSuite.evolve(state, event)
      def validate(state: StudentState, command: CreateStudent): Either[Throwable, Unit] =
        if (state.exists) Left(new RuntimeException("Student already exists")) else Right(())
      def decide(state: StudentState, command: CreateStudent): List[(Set[Tag], TestEvent)] =
        List(
          (Set(studentTag(command.studentId)), TestEvent.StudentCreated(command.studentId)),
          (Set(studentTag(command.studentId)), TestEvent.StudentDeleted(command.studentId)),
        )

    for
      store <- InMemoryEventStore.make[IO, TestEvent]
      _     <- {
        given EventStore[IO, TestEvent] = store
        multiEventHandler.run[IO](CreateStudent("1"))
      }
      events <- store.getEvents
    yield expect.all(
      events.length == 2,
      events(0).metadata.eventType == EventTypeName.of[StudentCreated],
      events(0).payload == TestEvent.StudentCreated("1"),
      events(1).metadata.eventType == EventTypeName.of[StudentDeleted],
      events(1).payload == TestEvent.StudentDeleted("1"),
    )
  }

  test("stress: concurrent retry succeeds consistently under repeated execution") {
    (1 to 100).toList.traverse_ { _ =>
      for
        store            <- InMemoryEventStore.make[IO, CounterEvent]
        arrivals         <- Ref.of[IO, Int](0)
        gate             <- Deferred[IO, Unit]
        synchronizedStore = barrieredStore(store, arrivals, gate)
        first             = {
          given EventStore[IO, CounterEvent] = synchronizedStore
          IncrementCounterHandler.run[IO](IncrementCounter("1")).attempt
        }
        second = {
          given EventStore[IO, CounterEvent] = synchronizedStore
          IncrementCounterHandler.run[IO](IncrementCounter("1")).attempt
        }
        results <- (first, second).parTupled
        events  <- store.getEvents
      yield assert(results._1.isRight && results._2.isRight && events.length == 2)
    }.map(_ => success)
  }
