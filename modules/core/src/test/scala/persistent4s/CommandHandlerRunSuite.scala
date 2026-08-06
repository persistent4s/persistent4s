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

import cats.effect.{Async, Deferred, IO, Ref}
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.otel4s.metrics.Counter
import org.typelevel.otel4s.trace.Tracer
import weaver.SimpleIOSuite

import java.time.Instant
import java.util.UUID
import persistent4s.CommandHandlerRunSuite.TestEvent.StudentCreated
import persistent4s.CommandHandlerRunSuite.TestEvent.StudentDeleted
import persistent4s.CommandHandlerRunSuite.CounterEvent.Incremented

object CommandHandlerRunSuite extends SimpleIOSuite:

  given Tracer[IO] = Tracer.Implicits.noop

  given CommandHandlerMetrics[IO] = CommandHandlerMetrics(Counter.noop[IO, Long])

  // ---------------------------------------------------------------------------
  // Minimal in-memory EventStore for testing — no testkit dependency needed
  // ---------------------------------------------------------------------------

  final class InMemoryEventStore[F[_]: Async, A <: Event] private (
    store: Ref[F, Vector[EventEnvelope[A]]],
    outbox: Ref[F, Vector[OutgoingMessage]],
  ) extends EventStore[F, A]
      with TransactionalMessages[F, A]:

    def getEvents: F[Vector[EventEnvelope[A]]] = store.get

    def getMessages: F[Vector[OutgoingMessage]] = outbox.get

    override def append(
      filter: EventFilter,
      expectedIndex: Long,
      events: List[PendingEvent[A]]*,
    ): F[List[EventEnvelope[A]]] =
      store.modify { currentEvents =>
        val incomingTags = events.flatten.flatMap(_.tags).toSet
        val relevantEvents = currentEvents.filter(env => env.metadata.tags.exists(incomingTags.contains))
        val actualIndex = relevantEvents.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
        if actualIndex != expectedIndex then
          (currentEvents, Left(new IndexConflictException(expectedIndex, actualIndex)))
        else
          val lastGlobalPosition = currentEvents.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
          val newEvents = events.flatten.zipWithIndex.map { case (pending, i) =>
            EventEnvelope(
              EventMetadata(
                globalPosition = lastGlobalPosition + i.toLong + 1L,
                id = pending.id.getOrElse(UUID.randomUUID()),
                tags = pending.tags,
                eventType = pending.eventType,
                isExternal = pending.isExternal,
                timestamp = java.time.Instant.now(),
                headers = pending.headers,
              ),
              pending.payload,
            )
          }
          (currentEvents ++ newEvents, Right(newEvents.toList))
      }.flatMap {
        case Left(error)   => Async[F].raiseError(error)
        case Right(result) => Async[F].pure(result)
      }

    override def appendUnchecked(events: List[PendingEvent[A]]*): F[List[EventEnvelope[A]]] =
      Async[F].pure(List.empty) // not needed for these tests

    /** Not atomic with the append — two `Ref`s cannot be — but it does preserve the one rule the real store's
      * transaction gives and the tests rely on: an append that loses a conflict leaves no message behind.
      */
    override def appendWithMessages(
      eventFilter: EventFilter,
      expectedIndex: Long,
      messages: List[OutgoingMessage],
      events: List[PendingEvent[A]]*,
    ): F[List[EventEnvelope[A]]] =
      if events.flatten.isEmpty then
        // Per TransactionalMessages: nothing is being written, so `expectedIndex` is ignored rather than checked and
        // the messages are enqueued on their own. Checking it would also be meaningless here — this fake derives the
        // actual index from the tags of the events being appended, and there are none.
        outbox.update(_ ++ messages).as(List.empty)
      else append(eventFilter, expectedIndex, events*).flatTap(_ => outbox.update(_ ++ messages))

    override def appendUncheckedWithMessages(
      messages: List[OutgoingMessage],
      events: List[PendingEvent[A]]*,
    ): F[List[EventEnvelope[A]]] =
      // `appendUnchecked` above is a stub, so this only carries the messages; nothing here calls it.
      outbox.update(_ ++ messages) *> appendUnchecked(events*)

    def currentRevision(eventFilter: EventFilter): F[Long] =
      readFrom(0L, eventFilter, None).compile.toList
        .map(_.lastOption.map(_.metadata.globalPosition).getOrElse(0L))

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
      (
        Ref.of[F, Vector[EventEnvelope[A]]](Vector.empty),
        Ref.of[F, Vector[OutgoingMessage]](Vector.empty),
      ).mapN(new InMemoryEventStore(_, _))

  /** What [[CommandHandler.runWithMessages]] asks for: a store that can append and enqueue in one go. */
  private type MessagingStore[A <: Event] = EventStore[IO, A] & TransactionalMessages[IO, A]

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

  private def isConflict(result: Either[Throwable, Any]): Boolean =
    result match
      case Left(_: IndexConflictException) => true
      case _                               => false

  private def barrieredStore[A <: Event](
    underlying: InMemoryEventStore[IO, A],
    arrivals: Ref[IO, Int],
    gate: Deferred[IO, Unit],
  ): MessagingStore[A] =
    new EventStore[IO, A] with TransactionalMessages[IO, A]:
      def append(
        filter: EventFilter,
        expectedIndex: Long,
        events: List[PendingEvent[A]]*,
      ): IO[List[EventEnvelope[A]]] =
        underlying.append(filter, expectedIndex, events*)
      def appendUnchecked(events: List[PendingEvent[A]]*): IO[List[EventEnvelope[A]]] =
        underlying.appendUnchecked(events*)
      def appendWithMessages(
        eventFilter: EventFilter,
        expectedIndex: Long,
        messages: List[OutgoingMessage],
        events: List[PendingEvent[A]]*,
      ): IO[List[EventEnvelope[A]]] =
        underlying.appendWithMessages(eventFilter, expectedIndex, messages, events*)
      def appendUncheckedWithMessages(
        messages: List[OutgoingMessage],
        events: List[PendingEvent[A]]*,
      ): IO[List[EventEnvelope[A]]] =
        underlying.appendUncheckedWithMessages(messages, events*)
      def currentRevision(eventFilter: EventFilter): IO[Long] =
        readFrom(0L, eventFilter, None).compile.toList
          .map(_.lastOption.map(_.metadata.globalPosition).getOrElse(0L))

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

  private val ReplyTopic = "replies"

  /** The shape a service answering another one needs: the reply is enqueued in the transaction that appends the event,
    * and it is enqueued *especially* on the path that decides to write no event at all. A case class rather than an
    * object because the address to answer arrives with the request, so it has to be closed over.
    */
  final case class AnsweringCreateStudentHandler(replyKey: String)
      extends CommandHandler[CreateStudent, StudentState, TestEvent]:

    def tags(command: CreateStudent): Set[Tag] = Set(studentTag(command.studentId))

    def initial: StudentState = StudentState(exists = false)

    def evolve(command: CreateStudent, state: StudentState, event: TestEvent): StudentState =
      CommandHandlerRunSuite.evolve(state, event)

    def validate(state: StudentState, command: CreateStudent): Either[Throwable, Unit] =
      if state.exists then Left(new RuntimeException("Student already exists")) else Right(())

    def decide(state: StudentState, command: CreateStudent): List[(Set[Tag], TestEvent)] =
      List((Set(studentTag(command.studentId)), TestEvent.StudentCreated(command.studentId)))

    override def messages(
      state: StudentState,
      command: CreateStudent,
      outcome: Either[Throwable, List[TestEvent]],
    ): Either[Throwable, List[OutgoingMessage]] =
      Right(
        List(
          OutgoingMessage(
            topic = ReplyTopic,
            key = Some(replyKey),
            payload = outcome.fold(error => s"rejected:${error.getMessage}", events => s"accepted:${events.size}"),
            headers = Map("studentId" -> command.studentId),
          ),
        ),
      )

  // ----- the same job, as a SagaCommandHandler -----

  private val SagaInstance = "3f2a1c00-0000-0000-0000-000000000001"

  /** A request as [[SagaRunner]] would have stamped it, addressed unless `addressed` says otherwise. */
  private def sagaRequest(addressed: Boolean = true): RequestContext =
    val correlation =
      if addressed then
        Map(
          SagaHeaders.Name           -> "enrol",
          SagaHeaders.Id             -> SagaInstance,
          SagaHeaders.ReplyTo        -> ReplyTopic,
          SagaHeaders.IdempotencyKey -> s"$SagaInstance:0:0:create",
        )
      else Map.empty
    RequestContext(IncomingMessage("students.commands", Some("s-1"), "1", correlation), Instant.EPOCH)

  private given MessageEncoder[String] with

    def encode(message: String): Either[Throwable, String] = Right(message)

  /** The encoder that cannot, for the one path where a reply's serialization fails. */
  private val unencodable: MessageEncoder[String] =
    new MessageEncoder[String]:
      def encode(message: String): Either[Throwable, String] = Left(new RuntimeException("boom"))

  /** [[AnsweringCreateStudentHandler]]'s job expressed the other way: it *names* the reply and lets the library encode
    * and address it, instead of assembling an [[OutgoingMessage]] and a payload by hand.
    *
    * Answers nothing for the student called "quiet", which is the only way to reach the `None` branch — a partner that
    * is genuinely fire-and-forget for some commands and answers others.
    */
  final case class SagaAnsweringHandler(request: RequestContext)(using MessageEncoder[String])
      extends SagaCommandHandler[CreateStudent, StudentState, TestEvent]:

    def tags(command: CreateStudent): Set[Tag] = Set(studentTag(command.studentId))

    def initial: StudentState = StudentState(exists = false)

    def evolve(command: CreateStudent, state: StudentState, event: TestEvent): StudentState =
      CommandHandlerRunSuite.evolve(state, event)

    def validate(state: StudentState, command: CreateStudent): Either[Throwable, Unit] =
      if state.exists then Left(new RuntimeException("Student already exists")) else Right(())

    def decide(state: StudentState, command: CreateStudent): List[(Set[Tag], TestEvent)] =
      List((Set(studentTag(command.studentId)), TestEvent.StudentCreated(command.studentId)))

    override def reply(
      state: StudentState,
      command: CreateStudent,
      outcome: Either[Throwable, List[TestEvent]],
    ): Option[PendingReply] =
      Option.unless(command.studentId == "quiet")(
        PendingReply(outcome.fold(error => s"rejected:${error.getMessage}", events => s"accepted:${events.size}")),
      )

  private def seedStudentCreated(store: InMemoryEventStore[IO, TestEvent], studentId: String): IO[Unit] =
    store
      .append(
        EventFilter(),
        0L,
        List(
          PendingEvent(
            TestEvent.StudentCreated(studentId),
            Set(studentTag(studentId)),
            EventTypeName.of[StudentCreated],
            isExternal = false,
          ),
        ),
      )
      .void

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
             List(
               PendingEvent(
                 TestEvent.StudentCreated("1"),
                 Set(studentTag("1")),
                 EventTypeName.of[StudentCreated],
                 isExternal = false,
               ),
             ),
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
             List(
               PendingEvent(
                 TestEvent.StudentCreated("1"),
                 Set(studentTag("1")),
                 EventTypeName.of[StudentCreated],
                 isExternal = false,
               ),
             ),
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

  test("run attaches the handler's headers(command) to every produced event") {
    val handlerWithHeaders = new CommandHandler[CreateStudent, StudentState, TestEvent]:
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
      override def headers(command: CreateStudent): Map[String, String] =
        Map("correlationId" -> command.studentId, "source" -> "test")

    for
      store <- InMemoryEventStore.make[IO, TestEvent]
      _     <- {
        given EventStore[IO, TestEvent] = store
        handlerWithHeaders.run[IO](CreateStudent("1"))
      }
      events <- store.getEvents
    yield expect.all(
      events.length == 2,
      events.forall(_.metadata.headers == Map("correlationId" -> "1", "source" -> "test")),
    )
  }

  test("runWithMessages commits the decided events and the handler's messages together") {
    val handler = AnsweringCreateStudentHandler("saga-1")
    for
      store  <- InMemoryEventStore.make[IO, TestEvent]
      result <- {
        given MessagingStore[TestEvent] = store
        handler.runWithMessages[IO](CreateStudent("1"))
      }
      events   <- store.getEvents
      messages <- store.getMessages
    yield expect.all(
      // `runWithMessages` hands back envelopes, like `run` — the payloads are what this test is about.
      result.map(_.map(_.payload)) == Right(List(TestEvent.StudentCreated("1"))),
      events.length == 1,
      events.head.payload == TestEvent.StudentCreated("1"),
      messages.length == 1,
      messages.head.topic == ReplyTopic,
      messages.head.key == Some("saga-1"),
      // `messages` saw what decide produced, not just that it succeeded.
      messages.head.payload == "accepted:1",
      messages.head.headers == Map("studentId" -> "1"),
    )
  }

  test("runWithMessages answers a rejected command as a Left instead of raising, and writes no event") {
    val handler = AnsweringCreateStudentHandler("saga-1")
    for
      store  <- InMemoryEventStore.make[IO, TestEvent]
      _      <- seedStudentCreated(store, "1")
      result <- {
        given MessagingStore[TestEvent] = store
        handler.runWithMessages[IO](CreateStudent("1"))
      }
      events   <- store.getEvents
      messages <- store.getMessages
    yield expect.all(
      result.isLeft,
      events.length == 1, // the seeded event only
      messages.length == 1,
      messages.head.payload == "rejected:Student already exists",
    )
  }

  test("runWithMessages enqueues nothing for a handler that does not override messages") {
    for
      store    <- InMemoryEventStore.make[IO, TestEvent]
      accepted <- {
        given MessagingStore[TestEvent] = store
        CreateStudentHandler.runWithMessages[IO](CreateStudent("1"))
      }
      // Rejected this time, and with no message to carry there is nothing at all for the store to do.
      rejected <- {
        given MessagingStore[TestEvent] = store
        CreateStudentHandler.runWithMessages[IO](CreateStudent("1"))
      }
      events   <- store.getEvents
      messages <- store.getMessages
    yield expect.all(
      // `runWithMessages` hands back envelopes, like `run` — the payloads are what this test is about.
      accepted.map(_.map(_.payload)) == Right(List(TestEvent.StudentCreated("1"))),
      rejected.isLeft,
      events.length == 1,
      messages.isEmpty,
    )
  }

  test("a saga handler's reply is encoded, addressed and committed alongside its events") {
    val handler = SagaAnsweringHandler(sagaRequest())
    for
      store  <- InMemoryEventStore.make[IO, TestEvent]
      result <- {
        given MessagingStore[TestEvent] = store
        handler.runWithMessages[IO](CreateStudent("1"))
      }
      events   <- store.getEvents
      messages <- store.getMessages
    yield expect.all(
      // `runWithMessages` hands back envelopes, like `run` — the payloads are what this test is about.
      result.map(_.map(_.payload)) == Right(List(TestEvent.StudentCreated("1"))),
      events.length == 1,
      messages.length == 1,
      // Everything below came from the request rather than from the handler, which named a payload and nothing else.
      messages.head.topic == ReplyTopic,
      messages.head.payload == "accepted:1",
      messages.head.key == Some(SagaInstance),
      messages.head.headers.get(SagaHeaders.Id) == Some(SagaInstance),
      messages.head.headers.get(SagaHeaders.InReplyTo) == Some(s"$SagaInstance:0:0:create"),
    )
  }

  test("a saga handler answers a rejection too, in the transaction that writes no event") {
    // The path a partner exists for: "no, and here is why" has to reach the caller as reliably as "yes", or the asking
    // saga cannot tell a refusal from a partner that has died and must wait out its whole deadline to find out.
    val handler = SagaAnsweringHandler(sagaRequest())
    for
      store  <- InMemoryEventStore.make[IO, TestEvent]
      _      <- seedStudentCreated(store, "1")
      result <- {
        given MessagingStore[TestEvent] = store
        handler.runWithMessages[IO](CreateStudent("1"))
      }
      events   <- store.getEvents
      messages <- store.getMessages
    yield expect.all(
      result.isLeft,
      events.length == 1, // the seeded event only
      messages.map(_.payload) == List("rejected:Student already exists"),
    )
  }

  test("a saga handler that answers nothing enqueues nothing, and still commits its events") {
    val handler = SagaAnsweringHandler(sagaRequest())
    for
      store  <- InMemoryEventStore.make[IO, TestEvent]
      result <- {
        given MessagingStore[TestEvent] = store
        handler.runWithMessages[IO](CreateStudent("quiet"))
      }
      events   <- store.getEvents
      messages <- store.getMessages
    yield expect.all(result.isRight, events.length == 1, messages.isEmpty)
  }

  test("a saga handler asked to answer a request that nominates nowhere sends nothing, and still commits") {
    // A command from something that is not a saga. There is nobody to answer, which is not an error — the events are
    // the point, and the reply was only ever a courtesy to whoever asked.
    val handler = SagaAnsweringHandler(sagaRequest(addressed = false))
    for
      store  <- InMemoryEventStore.make[IO, TestEvent]
      result <- {
        given MessagingStore[TestEvent] = store
        handler.runWithMessages[IO](CreateStudent("1"))
      }
      events   <- store.getEvents
      messages <- store.getMessages
    yield expect.all(result.isRight, events.length == 1, messages.isEmpty)
  }

  test("a reply that cannot be encoded aborts the command: no events, no messages") {
    // The whole reason `messages` returns an Either. Committing the reservation and then failing to say so would
    // strand the asking saga until its deadline while the resource stayed held; raising leaves the request unacked, so
    // it comes back — loudly, and repeatedly, which is the correct amount of noise for a programming error.
    val handler = SagaAnsweringHandler(sagaRequest())(using unencodable)
    for
      store   <- InMemoryEventStore.make[IO, TestEvent]
      outcome <- {
        given MessagingStore[TestEvent] = store
        handler.runWithMessages[IO](CreateStudent("1")).attempt
      }
      events   <- store.getEvents
      messages <- store.getMessages
    yield expect.all(
      outcome.isLeft,
      outcome.left.exists(_.getMessage == "boom"),
      events.isEmpty,
      messages.isEmpty,
    )
  }

  /** Throws where a handler is least expected to: computing the scope to read, before any effect has been built. */
  final case class ThrowingTagsHandler(request: RequestContext)
      extends SagaCommandHandler[CreateStudent, StudentState, TestEvent]:

    def tags(command: CreateStudent): Set[Tag] = throw new RuntimeException("tags exploded")

    def initial: StudentState = StudentState(exists = false)

    def evolve(command: CreateStudent, state: StudentState, event: TestEvent): StudentState = state

    def validate(state: StudentState, command: CreateStudent): Either[Throwable, Unit] = Right(())

    def decide(state: StudentState, command: CreateStudent): List[(Set[Tag], TestEvent)] = Nil

    override def reply(
      state: StudentState,
      command: CreateStudent,
      outcome: Either[Throwable, List[TestEvent]],
    ): Option[PendingReply] = None

  test("a handler that throws instead of returning fails the command rather than the caller") {
    // `runWithMessages` reads the command's scope before it has built anything, so a `tags` that throws would escape
    // synchronously and take down whatever was driving it — under `SagaParticipant`, the whole subscription — instead
    // of failing this one message and letting the broker redeliver it. Everything a handler contributes is therefore
    // evaluated inside the effect.
    val handler = ThrowingTagsHandler(sagaRequest())
    for
      store   <- InMemoryEventStore.make[IO, TestEvent]
      outcome <- {
        given MessagingStore[TestEvent] = store
        handler.runWithMessages[IO](CreateStudent("1")).attempt
      }
      events <- store.getEvents
    yield expect.all(
      outcome.isLeft,
      outcome.left.exists(_.getMessage == "tags exploded"),
      events.isEmpty,
    )
  }

  test("runWithMessages: the loser of a conflict re-reads, changes its answer, and leaves no stale message") {
    val handler = AnsweringCreateStudentHandler("saga-1")
    for
      store            <- InMemoryEventStore.make[IO, TestEvent]
      arrivals         <- Ref.of[IO, Int](0)
      gate             <- Deferred[IO, Unit]
      synchronizedStore = barrieredStore(store, arrivals, gate)
      first             = {
        given MessagingStore[TestEvent] = synchronizedStore
        handler.runWithMessages[IO](CreateStudent("1"))
      }
      second = {
        given MessagingStore[TestEvent] = synchronizedStore
        handler.runWithMessages[IO](CreateStudent("1"))
      }
      results  <- (first, second).parTupled
      events   <- store.getEvents
      messages <- store.getMessages
      reads    <- arrivals.get
      outcomes  = List(results._1, results._2)
    yield expect.all(
      outcomes.count(_.isRight) == 1,
      outcomes.count(_.isLeft) == 1,
      events.length == 1,
      // Three reads, not two: the gate holds both commands until each has read, so the conflict is unavoidable and the
      // loser had to read again. Without this the assertions below would also hold for two sequential runs.
      reads == 3,
      // Both callers are answered, and never twice with "accepted" — the retry read the winner's event and changed its
      // mind. The attempt that lost the conflict contributed no message of its own.
      messages.length == 2,
      messages.count(_.payload == "accepted:1") == 1,
      messages.count(_.payload == "rejected:Student already exists") == 1,
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
