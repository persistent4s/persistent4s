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

import java.time.Instant
import java.util.UUID

import scala.util.Try

import cats.effect.{Concurrent, IO}

import fs2.Stream

import org.typelevel.otel4s.metrics.Counter
import org.typelevel.otel4s.trace.Tracer

import weaver.SimpleIOSuite

object EventSourcedCommandHandlerSuite extends SimpleIOSuite:

  final case class TestCommand(id: String, blocked: Boolean = false)

  final case class TestState(exists: Boolean, value: Int)

  enum TestRejection:

    case Missing, Blocked

  sealed trait TestEvent extends Event

  final case class Created(id: String, value: Int) extends TestEvent

  final case class Incremented(id: String, amount: Int) extends TestEvent

  final case class Observed(id: String) extends TestEvent

  final case class Ignored(id: String) extends TestEvent

  final case class LegacyObserved(id: String) extends TestEvent

  final case class CrossScoped(id: String, ownerId: String) extends TestEvent

  final case class MultiScoped(ids: List[String]) extends TestEvent

  private object Expected:

    final case class Same(id: String) extends TestEvent

  private object Actual:

    final case class Same(id: String) extends TestEvent

  private val EntityScope = Scope[String]("test.entity")

  private val OwnerScope = Scope[String]("test.owner")

  private given createdSchema: EventSchema[Created] =
    EventSchema[Created]("test.created").scopedBy(EntityScope)(_.id)

  private given incrementedSchema: EventSchema[Incremented] =
    EventSchema[Incremented]("test.incremented").scopedBy(EntityScope)(_.id)

  private given observedSchema: EventSchema[Observed] =
    EventSchema[Observed]("test.observed").scopedBy(EntityScope)(_.id)

  private given ignoredSchema: EventSchema[Ignored] =
    EventSchema[Ignored]("test.ignored").scopedBy(EntityScope)(_.id)

  private given legacyObservedSchema: EventSchema[LegacyObserved] =
    EventSchema[LegacyObserved]("test.legacy-observed")

  private given crossScopedSchema: EventSchema[CrossScoped] =
    EventSchema[CrossScoped]("test.cross-scoped")
      .scopedBy(EntityScope)(_.id)
      .scopedBy(OwnerScope)(_.ownerId)

  private given multiScopedSchema: EventSchema[MultiScoped] =
    EventSchema[MultiScoped]("test.multi-scoped").scopedByMany(EntityScope)(_.ids)

  private given expectedSameSchema: EventSchema[Expected.Same] =
    EventSchema[Expected.Same]("test.expected-same")

  private given testStateSnapshotCodec: SnapshotCodec[TestState] with

    def encode(state: TestState): String =
      s"${state.exists}:${state.value}"

    def decode(payload: String): Either[Throwable, TestState] =
      payload.split(":", 2) match
        case Array(exists, value) =>
          Try(TestState(exists.toBoolean, value.toInt)).toEither
        case _ =>
          Left(new IllegalArgumentException(s"Invalid test snapshot: $payload"))

  private val missing = TestRejection.Missing

  private val blocked = TestRejection.Blocked

  private val snapshotId = SnapshotId("test-command")

  /** otel4s seals [[Counter]] and ships no public recording implementation, so what a span or a counter *emitted*
    * cannot be asserted here — only that carrying them changes no outcome. The instruments are real values on the same
    * code path a production runtime takes; asserting emission needs the otel4s testkit.
    */
  private val telemetry: CommandTelemetry[IO] =
    CommandTelemetry(Tracer.Implicits.noop, CommandHandlerMetrics(Counter.noop[IO, Long]))

  private val ReplyTopic = "test.replies"

  private val SagaInstance = "3f2a1c00-0000-0000-0000-0000000000001"

  /** A request as [[SagaRunner]] would have stamped it, addressed unless 'addressed' says otherwise. */
  private def sagaRequest(addressed: Boolean = true): RequestContext =
    val correlation =
      if addressed then
        Map(
          SagaHeaders.Name           -> "reserve",
          SagaHeaders.Id             -> SagaInstance,
          SagaHeaders.ReplyTo        -> ReplyTopic,
          SagaHeaders.IdempotencyKey -> s"$SagaInstance:0:0:reserve",
        )
      else Map.empty
    RequestContext(IncomingMessage("test.commands", Some("a"), "a", correlation), Instant.EPOCH)

  private given MessageEncoder[String] with

    def encode(message: String): Either[Throwable, String] = Right(message)

  /** The encoder that cannot, for the one path where a reply's serilization fails. */
  private val unencodableReply: MessageEncoder[String] =
    new MessageEncoder[String]:
      def encode(message: String): Either[Throwable, String] = Left(new RuntimeException("cannot encode the reply"))

  /** Answers nothing for the command called "quiet", which is the only way to reach the `None` branch: a partner that
    * is genuinely fire-and-forget for some commands and answers others.
    */
  final private case class Answering(request: RequestContext)(using MessageEncoder[String])
      extends EventSourcedSagaCommandHandler[TestCommand, TestState, TestEvent, TestRejection]:

    protected val behavior = handler(TestState(exists = false, value = 0)):
      scope(EntityScope)(_.id)

      on[Created].matching(_.id, _.id).evolve((state, event) => state.copy(exists = true, value = event.value))

      on[Incremented].within(EntityScope).evolve((state, event) => state.copy(value = state.value + event.amount))

      reject:
        case (state, _) if !state.exists => missing

      reply((_, command, outcome) =>
        if command.id == "quiet" then None
        else Some(PendingReply(outcome.fold(rejection => s"rejected:$rejection", events => s"accepted:${events.size}"))),
      )

      emit((state, command) => Incremented(command.id, state.value + 1))

  private val subject = new EventSourcedCommandHandler[TestCommand, TestState, TestEvent, TestRejection]:

    protected val behavior = handler(TestState(exists = false, value = 0)):
      scope(EntityScope)(_.id)

      on[Created]
        .matching(_.id, _.id)
        .evolve((state, event) => state.copy(exists = true, value = event.value))

      on[Incremented]
        .within(EntityScope)
        .evolve((state, event) => state.copy(value = state.value + event.amount))

      on[Observed].within(EntityScope).ignore

      reject:
        case (state, _) if !state.exists     => missing
        case (_, command) if command.blocked => blocked

      snapshot(snapshotId, every = 2)

      emit((state, command) => Incremented(command.id, state.value + 1))

  private def replyMessage(command: TestCommand, body: String): OutgoingMessage =
    OutgoingMessage(topic = "replies", key = Some(command.id), payload = body)

  /** Answers whoever asked, which is what `runWithMessages` exists for: the reply commits with the events, and on
    * rejection it is all that commits.
    */
  private val answering = new EventSourcedCommandHandler[TestCommand, TestState, TestEvent, TestRejection]:

    protected val behavior = handler(TestState(exists = false, value = 0)):
      scope(EntityScope)(_.id)

      on[Created].matching(_.id, _.id).evolve((state, event) => state.copy(exists = true, value = event.value))

      on[Incremented].within(EntityScope).evolve((state, event) => state.copy(value = state.value + event.amount))

      reject:
        case (state, _) if !state.exists => missing

      messages((_, command, outcome) =>
        Right(List(replyMessage(command, outcome.fold(_.toString, _.map(_.toString).mkString(","))))),
      )

      emit((state, command) => Incremented(command.id, state.value + 1))

  private def envelope[E <: TestEvent: EventSchema](
    position: Long,
    payload: E,
    tags: Set[Tag],
  ): EventEnvelope[TestEvent] =
    EventEnvelope(
      EventMetadata(
        position,
        UUID.randomUUID(),
        tags,
        summon[EventSchema[E]].eventType,
        isExternal = false,
        Instant.now(),
        Map.empty,
        summon[EventSchema[E]].version,
      ),
      payload,
    )

  private def envelopeAs(
    position: Long,
    payload: TestEvent,
    tags: Set[Tag],
    eventType: EventTypeName,
  ): EventEnvelope[TestEvent] =
    EventEnvelope(
      EventMetadata(position, UUID.randomUUID(), tags, eventType, isExternal = false, Instant.now(), Map.empty),
      payload,
    )

  /** @param appendConflicts
    *   how many appends lose an optimistic-concurrency conflict before one is allowed through, so the retry loop is
    *   observable from outside via [[readCount]] and [[appendAttempts]]
    */
  private class RecordingStore(
    history: List[EventEnvelope[TestEvent]],
    readFailure: Option[Throwable] = None,
    schema: TestEvent => Option[EventStorageSchema] = _ => None,
    appendConflicts: Int = 0,
  ) extends EventStore[IO, TestEvent]:

    var readFromPosition: Option[Long] = None

    var readFilter: Option[EventFilter] = None

    var readCount: Int = 0

    var appendAttempts: Int = 0

    var appended: Option[(EventFilter, Long, List[PendingEvent[TestEvent]])] = None

    private var conflictsLeft: Int = appendConflicts

    private def matches(event: EventEnvelope[TestEvent], filter: EventFilter): Boolean =
      val eventTypeMatches = filter.eventTypes.isEmpty || filter.eventTypes.contains(event.metadata.eventType)
      val tagMatches = filter.tags.isEmpty || event.metadata.tags.exists(filter.tags.contains)
      eventTypeMatches && tagMatches

    def currentRevision(eventFilter: EventFilter): IO[Long] =
      IO.pure(history.filter(matches(_, eventFilter)).lastOption.fold(0L)(_.metadata.globalPosition))

    override def storageSchema(event: TestEvent): Option[EventStorageSchema] =
      schema(event)

    def readFrom(
      fromPosition: Long,
      eventFilter: EventFilter,
      maxEvents: Option[Int],
    ): Stream[IO, EventEnvelope[TestEvent]] =
      readCount += 1
      readFromPosition = Some(fromPosition)
      readFilter = Some(eventFilter)
      readFailure.fold(Stream.emits(history).covary[IO])(Stream.raiseError[IO])

    def append(
      eventFilter: EventFilter,
      expectedIndex: Long,
      events: List[PendingEvent[TestEvent]]*,
    ): IO[List[EventEnvelope[TestEvent]]] =
      appendAttempts += 1
      if conflictsLeft > 0 then
        conflictsLeft -= 1
        IO.raiseError(IndexConflictException(expectedIndex, expectedIndex + 1))
      else
        val flattened = events.toList.flatten
        appended = Some((eventFilter, expectedIndex, flattened))
        IO.pure(flattened.map(envelopeFor))

    def appendUnchecked(
      events: List[PendingEvent[TestEvent]]*,
    ): IO[List[EventEnvelope[TestEvent]]] =
      IO.pure(events.toList.flatten.map(envelopeFor))

    private def envelopeFor(pending: PendingEvent[TestEvent]): EventEnvelope[TestEvent] =
      EventEnvelope(
        EventMetadata(
          history.lastOption.fold(0L)(_.metadata.globalPosition) + 1L,
          pending.id.getOrElse(UUID.randomUUID()),
          pending.tags,
          pending.eventType,
          pending.isExternal,
          Instant.now(),
          pending.headers,
        ),
        pending.payload,
      )

  final private class RecordingTransactionalStore(
    history: List[EventEnvelope[TestEvent]] = Nil,
    appendConflicts: Int = 0,
  ) extends RecordingStore(history, appendConflicts = appendConflicts)
      with TransactionalMessages[IO, TestEvent]:

    var enqueued: List[List[OutgoingMessage]] = Nil

    private def record(messages: List[OutgoingMessage]): IO[Unit] =
      IO { enqueued = enqueued :+ messages }

    def appendWithMessages(
      eventFilter: EventFilter,
      expectedIndex: Long,
      messages: List[OutgoingMessage],
      events: List[PendingEvent[TestEvent]]*,
    ): IO[List[EventEnvelope[TestEvent]]] =
      val flattened = events.toList.flatten
      if flattened.isEmpty then record(messages).as(List.empty)
      else append(eventFilter, expectedIndex, flattened).flatTap(_ => record(messages))

    def appendUncheckedWithMessages(
      messages: List[OutgoingMessage],
      events: List[PendingEvent[TestEvent]]*,
    ): IO[List[EventEnvelope[TestEvent]]] =
      record(messages) *> appendUnchecked(events*)

  final private class RecordingSnapshotStore(initial: Option[StoredCommandSnapshot]) extends CommandSnapshotStore[IO]:

    var current: Option[StoredCommandSnapshot] = initial

    var loaded: Option[(SnapshotId, String, Int)] = None

    var saved: Option[(SnapshotId, String, Int, StoredCommandSnapshot)] = None

    var deleted: Option[(SnapshotId, String, Int)] = None

    def load(snapshotId: SnapshotId, key: String, version: Int): IO[Option[StoredCommandSnapshot]] =
      loaded = Some((snapshotId, key, version))
      IO.pure(current)

    def save(snapshotId: SnapshotId, key: String, version: Int, snapshot: StoredCommandSnapshot): IO[Unit] =
      saved = Some((snapshotId, key, version, snapshot))
      current = Some(snapshot)
      IO.unit

    def delete(snapshotId: SnapshotId, key: String, version: Int): IO[Unit] =
      deleted = Some((snapshotId, key, version))
      current = None
      IO.unit

  final private class FailingSnapshotStore(error: Throwable) extends CommandSnapshotStore[IO]:

    def load(snapshotId: SnapshotId, key: String, version: Int): IO[Option[StoredCommandSnapshot]] =
      IO.raiseError(error)

    def save(snapshotId: SnapshotId, key: String, version: Int, snapshot: StoredCommandSnapshot): IO[Unit] =
      IO.raiseError(error)

    def delete(snapshotId: SnapshotId, key: String, version: Int): IO[Unit] =
      IO.raiseError(error)

  pureTest("event types are derived from typed state transitions and stable schemas") {
    expect(
      subject.eventTypes == Set(
        createdSchema.eventType,
        incrementedSchema.eventType,
        observedSchema.eventType,
      ),
    ) and expect(!subject.eventTypes.contains(ignoredSchema.eventType))
  }

  pureTest("matching and within transitions evolve only the command's scoped state") {
    val command = TestCommand("target")
    val initial = subject.initial
    val otherCreated = subject.evolve(command, initial, Created("other", 10))
    val created = subject.evolve(command, otherCreated, Created("target", 10))
    val otherIncrement = subject.evolve(command, created, Incremented("other", 2))
    val updated = subject.evolve(command, otherIncrement, Incremented("target", 2))
    val observed = subject.evolve(command, updated, Observed("target"))
    val ignored = subject.evolve(command, observed, Ignored("target"))

    expect(subject.scopes(command) == Set(EntityScope("target"))) and
      expect(subject.tags(command) == Set(EntityScope("target").toTag)) and
      expect(otherCreated == initial) and
      expect(created == TestState(exists = true, value = 10)) and
      expect(otherIncrement == created) and
      expect(updated == TestState(exists = true, value = 12)) and
      expect(observed == updated) and
      expect(ignored == updated)
  }

  pureTest("a single shared typed scope is inferred for a bare on handler") {
    val inferredHandler = new EventSourcedCommandHandler[TestCommand, Int, TestEvent, Unit]:
      protected val behavior = handler(0):
        scope(EntityScope)(_.id)
        on[Created].evolve((state, event) => state + event.value)
        emit(command => Created(command.id, 1))

    val command = TestCommand("target")
    expect(inferredHandler.evolve(command, 0, Created("target", 2)) == 2) and
      expect(inferredHandler.evolve(command, 0, Created("other", 2)) == 0)
  }

  pureTest("a typed event with no shared command scope requires allEvents") {
    val missingIntent = Try {
      new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
        protected val behavior = handler(()):
          scope(EntityScope)(_.id)
          on[LegacyObserved].ignore
          emit(command => LegacyObserved(command.id))
    }

    val explicitHandler = new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
      protected val behavior = handler(()):
        scope(EntityScope)(_.id)
        on[LegacyObserved].allEvents.ignore
        emit(command => LegacyObserved(command.id))

    expect(missingIntent.failed.toOption.exists(_.getMessage.contains("shares no typed scope"))) and
      expect(explicitHandler.eventTypes == Set(legacyObservedSchema.eventType))
  }

  pureTest("an event sharing several command scopes requires explicit matching intent") {
    val ambiguous = Try {
      new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
        protected val behavior = handler(()):
          scope(EntityScope)(_.id)
          scope(OwnerScope)(_.id)
          on[CrossScoped].ignore
          emit(command => CrossScoped(command.id, command.id))
    }

    expect(ambiguous.failed.toOption.exists(_.getMessage.contains("shares multiple command scopes"))) and
      expect(ambiguous.failed.toOption.exists(_.getMessage.contains("withinAll")))
  }

  pureTest("withinAll requires every shared scope key to match") {
    final case class CrossCommand(id: String, ownerId: String)

    val withinAllHandler = new EventSourcedCommandHandler[CrossCommand, Int, TestEvent, Unit]:
      protected val behavior = handler(0):
        scope(EntityScope)(_.id)
        scope(OwnerScope)(_.ownerId)
        on[CrossScoped].withinAll.evolve(_ + 1)
        emit(command => CrossScoped(command.id, command.ownerId))

    val command = CrossCommand("entity-a", "owner-a")
    expect(withinAllHandler.evolve(command, 0, CrossScoped("entity-a", "owner-a")) == 1) and
      expect(withinAllHandler.evolve(command, 0, CrossScoped("other", "owner-a")) == 0) and
      expect(withinAllHandler.evolve(command, 0, CrossScoped("entity-a", "other")) == 0)
  }

  pureTest("the first matching typed rejection wins") {
    expect(subject.validate(subject.initial, TestCommand("a", blocked = true)) == Left(missing)) and
      expect(
        subject.validate(TestState(exists = true, value = 1), TestCommand("a", blocked = true)) == Left(blocked),
      ) and
      expect(subject.validate(TestState(exists = true, value = 1), TestCommand("a")) == Right(()))
  }

  pureTest("emitted events inherit their declared event scopes") {
    val command = TestCommand("a")
    val tags = Set(EntityScope("a").toTag)
    expect(
      subject.decide(TestState(exists = true, value = 2), command) ==
        List(tags -> Incremented("a", 3)),
    )
  }

  pureTest("scopeMany acquires and emits several keys from the same durable scope") {
    final case class MultiCommand(ids: List[String])

    val multiHandler = new EventSourcedCommandHandler[MultiCommand, Unit, TestEvent, Unit]:
      protected val behavior = handler(()):
        scopeMany(EntityScope)(_.ids)
        on[MultiScoped].within(EntityScope).ignore
        emit(command => MultiScoped(command.ids))

    val command = MultiCommand(List("a", "b", "a"))
    val expectedScopes = Set(EntityScope("a"), EntityScope("b"))

    expect(multiHandler.scopes(command) == expectedScopes) and
      expect(multiHandler.decide((), command) == List(expectedScopes.map(_.toTag) -> MultiScoped(command.ids)))
  }

  test("run uses the scope filter, last position, stable schema, and returns accepted events") {
    val commandTags = Set(EntityScope("a").toTag)
    val store = new RecordingStore(List(envelope(7L, Created("a", 2), commandTags)))
    val runtime = CommandRuntime.eventStoreOnly(store)

    subject.run[IO](TestCommand("a"))(using summon[Concurrent[IO]], runtime).map { result =>
      val expectedFilter = EventFilter(Set.empty, commandTags)
      val expectedEvent = Incremented("a", 3)

      expect(result.map(_.map(_.payload)) == Right(List(expectedEvent))) and
        expect(store.readFromPosition.contains(0L)) and
        expect(store.readFilter.contains(expectedFilter)) and
        expect(
          store.appended.contains(
            (
              expectedFilter,
              7L,
              List(
                PendingEvent(expectedEvent, commandTags, incrementedSchema.eventType, isExternal = false),
              ),
            ),
          ),
        )
    }
  }

  test("CommandRuntime executes handlers without repeating the effect type and can discard accepted events") {
    val tags = Set(EntityScope("a").toTag)
    val acceptedRuntime = CommandRuntime.eventStoreOnly(
      new RecordingStore(List(envelope(1L, Created("a", 2), tags))),
    )
    val rejectedRuntime = CommandRuntime.eventStoreOnly(new RecordingStore(Nil))
    val mapped = new RuntimeException("mapped rejection")

    for
      accepted <- acceptedRuntime.execute(subject, TestCommand("a"))
      rejected <- rejectedRuntime.executeUnit(subject, TestCommand("a"))
      raised   <- rejectedRuntime
                  .executeOrRaise(subject, TestCommand("a"))(_ => mapped)
                  .attempt
    yield expect(accepted.map(_.map(_.payload)) == Right(List(Incremented("a", 3)))) and
      expect(rejected == Left(TestRejection.Missing)) and
      expect(raised == Left(mapped))
  }

  test("readAllEventTypes retains the legacy open-filter escape hatch") {
    val openFilterHandler = new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
      override protected def readAllEventTypes = true
      protected val behavior = handler(()):
        tags(_ => Set(Tag("entity", "a")))
        on[LegacyObserved].ignore
        emit(_ => LegacyObserved("a"))

    val store = new RecordingStore(Nil)
    openFilterHandler
      .run[IO](TestCommand("a"))(using summon[Concurrent[IO]], CommandRuntime.eventStoreOnly(store))
      .map(result =>
        expect(result.map(_.map(_.payload)) == Right(List(LegacyObserved("a")))) and expect(
          store.readFilter.exists(_.eventTypes.isEmpty),
        ),
      )
  }

  pureTest("explicitly tagged unscoped emissions can differ from legacy command tags") {
    val outputTags = Set(Tag("output", "a"))
    val taggedHandler = new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
      protected val behavior = handler(()):
        tags(_ => Set(Tag("input", "a")))
        on[LegacyObserved].ignore
        emitTagged(_ => outputTags -> LegacyObserved("a"))

    expect(taggedHandler.decide((), TestCommand("a")) == List(outputTags -> LegacyObserved("a")))
  }

  pureTest("duplicate event handlers are rejected") {
    val result = Try {
      new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
        protected val behavior = handler(()):
          tags(_ => Set(Tag("entity", "a")))
          on[LegacyObserved].ignore
          on[LegacyObserved].ignore
          emit(_ => LegacyObserved("a"))
    }

    result.failed.toOption match
      case Some(error: IllegalArgumentException) =>
        expect(error.getMessage.contains(s"Duplicate command event handlers: ${legacyObservedSchema.eventType.value}"))
      case other => failure(s"Expected duplicate handler error, got $other")
  }

  pureTest("incomplete behavior declarations are rejected") {
    val result = Try {
      new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
        protected val behavior = handler(()):
          on[LegacyObserved].ignore
          emit(_ => LegacyObserved("a"))
    }

    result.failed.toOption match
      case Some(error: IllegalArgumentException) => expect(error.getMessage.contains("at least one scope"))
      case other                                 => failure(s"Expected missing scope error, got $other")
  }

  test("metadata selecting a differently typed payload fails descriptively") {
    val mismatchHandler = new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
      protected val behavior = handler(()):
        tags(_ => Set(Tag("entity", "a")))
        on[Expected.Same].ignore
        emit(_ => Expected.Same("a"))

    val history = List(
      envelopeAs(1L, Actual.Same("a"), Set(Tag("entity", "a")), expectedSameSchema.eventType),
    )
    val store = new RecordingStore(history)

    mismatchHandler
      .run[IO](TestCommand("a"))(using summon[Concurrent[IO]], CommandRuntime.eventStoreOnly(store))
      .attempt
      .map {
        case Left(error: EventPayloadTypeMismatch) =>
          expect(error.eventType == expectedSameSchema.eventType) and expect(error.actualClass.contains("Actual"))
        case other => failure(s"Expected EventPayloadTypeMismatch, got $other")
      }
  }

  test("synchronous callback failures are captured by run") {
    val expected = new RuntimeException("tag failure")
    val failingHandler = new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
      protected val behavior = handler(()):
        tags(_ => throw expected)
        on[LegacyObserved].ignore
        emit(_ => LegacyObserved("a"))

    val store = new RecordingStore(Nil)
    val runtime = CommandRuntime.eventStoreOnly(store)

    IO(Try(failingHandler.run[IO](TestCommand("a"))(using summon[Concurrent[IO]], runtime))).flatMap {
      case scala.util.Success(effect) => effect.attempt.map(result => expect(result == Left(expected)))
      case scala.util.Failure(error)  => IO.pure(failure(s"run construction threw $error"))
    }
  }

  test("typed rejection is returned while infrastructure failure fails the effect") {
    val infrastructureFailure = new RuntimeException("store unavailable")
    val rejectingStore = new RecordingStore(Nil)
    val failingStore = new RecordingStore(Nil, readFailure = Some(infrastructureFailure))

    for
      rejected <- subject.run[IO](TestCommand("a"))(using
                    summon[Concurrent[IO]],
                    CommandRuntime.eventStoreOnly(rejectingStore),
                  )
      failed <- subject
                  .run[IO](TestCommand("a"))(using
                    summon[Concurrent[IO]],
                    CommandRuntime.eventStoreOnly(failingStore),
                  )
                  .attempt
    yield expect(rejected == Left(TestRejection.Missing)) and expect(failed == Left(infrastructureFailure))
  }

  test("handler and storage event schema disagreement fails before append") {
    val tags = Set(EntityScope("a").toTag)
    val storageSchema = EventStorageSchema(EventTypeName.fromString("storage.incremented"), version = 2)
    val store = new RecordingStore(
      List(envelope(1L, Created("a", 1), tags)),
      schema = _ => Some(storageSchema),
    )

    subject
      .run[IO](TestCommand("a"))(using summon[Concurrent[IO]], CommandRuntime.eventStoreOnly(store))
      .attempt
      .map {
        case Left(error: EventSchemaMismatch) =>
          expect(error.declared == EventStorageSchema(incrementedSchema.eventType, incrementedSchema.version)) and
            expect(error.storage == storageSchema) and
            expect(store.appended.isEmpty)
        case other => failure(s"Expected EventSchemaMismatch, got $other")
      }
  }

  test("an IndexConflictException outside append is not misclassified as retryable concurrency") {
    val callbackConflict = IndexConflictException(1L, 2L)
    val conflictingHandler = new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
      protected val behavior = handler(()):
        scope(EntityScope)(_.id)
        on[Created]
          .within(EntityScope)
          .evolve((_: Unit, _: Created) => throw callbackConflict)
        emit(command => Observed(command.id))

    val tags = Set(EntityScope("a").toTag)
    val store = new RecordingStore(List(envelope(1L, Created("a", 1), tags)))

    conflictingHandler
      .run[IO](TestCommand("a"))(using summon[Concurrent[IO]], CommandRuntime.eventStoreOnly(store))
      .attempt
      .map(result => expect(result == Left(callbackConflict)) and expect(store.readCount == 1))
  }

  test("snapshot resumes from its offset and saves caught-up state with a stable fingerprint") {
    val tags = Set(EntityScope("a").toTag)
    val fingerprint =
      "events=14:test.created@1|18:test.incremented@1|15:test.observed@1;scopes=13:test.entity:a"
    val snapshotKey = "13:test.entity:a"
    val snapshots = new RecordingSnapshotStore(
      Some(StoredCommandSnapshot(5L, 1L, fingerprint, "true:10")),
    )
    val store = new RecordingStore(List(envelope(7L, Incremented("a", 2), tags)))
    val runtime = CommandRuntime(store, Some(snapshots))

    subject.run[IO](TestCommand("a"))(using summon[Concurrent[IO]], runtime).map { result =>
      val expectedEvent = Incremented("a", 13)
      val expectedSnapshot = StoredCommandSnapshot(7L, 2L, fingerprint, "true:12")

      expect(result.map(_.map(_.payload)) == Right(List(expectedEvent))) and
        expect(store.readFromPosition.contains(5L)) and
        expect(store.appended.exists(_._2 == 7L)) and
        expect(snapshots.loaded.contains((snapshotId, snapshotKey, 1))) and
        expect(snapshots.saved.contains((snapshotId, snapshotKey, 1, expectedSnapshot)))
    }
  }

  test("best-effort snapshot failures fall back to replay without hiding command outcomes") {
    val snapshotFailure = new RuntimeException("snapshot unavailable")
    val snapshots = new FailingSnapshotStore(snapshotFailure)
    val tags = Set(EntityScope("a").toTag)
    val acceptedStore = new RecordingStore(
      List(
        envelope(1L, Created("a", 2), tags),
        envelope(2L, Incremented("a", 1), tags),
      ),
    )
    val rejectedStore = new RecordingStore(Nil)

    for
      accepted <- subject.run[IO](TestCommand("a"))(using
                    summon[Concurrent[IO]],
                    CommandRuntime(acceptedStore, Some(snapshots)),
                  )
      rejected <- subject.run[IO](TestCommand("a"))(using
                    summon[Concurrent[IO]],
                    CommandRuntime(rejectedStore, Some(snapshots)),
                  )
    yield expect(accepted.map(_.map(_.payload)) == Right(List(Incremented("a", 4)))) and
      expect(rejected == Left(TestRejection.Missing))
  }

  test("snapshot with another filter fingerprint is deleted and ignored") {
    val tags = Set(EntityScope("a").toTag)
    val snapshots = new RecordingSnapshotStore(
      Some(StoredCommandSnapshot(99L, 10L, "different-filter", "true:99")),
    )
    val store = new RecordingStore(List(envelope(2L, Created("a", 4), tags)))
    val runtime = CommandRuntime(store, Some(snapshots))

    subject.run[IO](TestCommand("a"))(using summon[Concurrent[IO]], runtime).map { result =>
      expect(result.map(_.map(_.payload)) == Right(List(Incremented("a", 5)))) and
        expect(store.readFromPosition.contains(0L)) and
        expect(snapshots.saved.isEmpty) and
        expect(snapshots.deleted.nonEmpty)
    }
  }

  test("snapshot ahead of the authoritative scope revision is deleted and replayed cold") {
    val tags = Set(EntityScope("a").toTag)
    val fingerprint =
      "events=14:test.created@1|18:test.incremented@1|15:test.observed@1;scopes=13:test.entity:a"
    val snapshots = new RecordingSnapshotStore(
      Some(StoredCommandSnapshot(99L, 10L, fingerprint, "true:99")),
    )
    val store = new RecordingStore(List(envelope(2L, Created("a", 4), tags)))
    val runtime = CommandRuntime(store, Some(snapshots))

    subject.run[IO](TestCommand("a"))(using summon[Concurrent[IO]], runtime).map { result =>
      expect(result.map(_.map(_.payload)) == Right(List(Incremented("a", 5)))) and
        expect(store.readFromPosition.contains(0L)) and
        expect(snapshots.deleted.nonEmpty) and
        expect(store.appended.exists(_._2 == 2L))
    }
  }

  test("emitting an event outside the acquired typed scopes fails") {
    val scopeViolationHandler = new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
      protected val behavior = handler(()):
        scope(EntityScope)(_.id)
        on[CrossScoped].within(EntityScope).ignore
        emit(command => CrossScoped(command.id, "owner-a"))

    val store = new RecordingStore(Nil)

    scopeViolationHandler
      .run[IO](TestCommand("a"))(using
        summon[Concurrent[IO]],
        CommandRuntime.eventStoreOnly(store),
      )
      .attempt
      .map {
        case Left(error: CommandScopeViolation) =>
          expect(error.eventType == crossScopedSchema.eventType) and
            expect(error.missingScopes == Set(OwnerScope("owner-a"))) and
            expect(store.appended.isEmpty)
        case other => failure(s"Expected CommandScopeViolation, got $other")
      }
  }

  /** The span wraps the whole command, so it now sits between the caller and the outcome. A `surround` that turned a
    * typed rejection into a failure — or swallowed a storage failure into a `Left` — would break the one contract that
    * separates this handler from [[CommandHandler]], and would do it silently.
    */
  test("a traced command keeps rejections in Left and infrastructure failures in the effect") {
    val infrastructureFailure = new RuntimeException("store unavailable")
    val tags = Set(EntityScope("a").toTag)

    for
      accepted <- subject.run[IO](TestCommand("a"))(using
                    summon[Concurrent[IO]],
                    CommandRuntime(new RecordingStore(List(envelope(1L, Created("a", 2), tags))), None, Some(telemetry)),
                  )
      rejected <- subject.run[IO](TestCommand("a"))(using
                    summon[Concurrent[IO]],
                    CommandRuntime(new RecordingStore(Nil), None, Some(telemetry)),
                  )
      failed <- subject
                  .run[IO](TestCommand("a"))(using
                    summon[Concurrent[IO]],
                    CommandRuntime(
                      new RecordingStore(Nil, readFailure = Some(infrastructureFailure)),
                      None,
                      Some(telemetry),
                    ),
                  )
                  .attempt
    yield expect(accepted.map(_.map(_.payload)) == Right(List(Incremented("a", 3)))) and
      expect(rejected == Left(TestRejection.Missing)) and
      expect(failed == Left(infrastructureFailure))
  }

  test("retry on an append conflict is unaffected by whether telemetry is carried") {
    val tags = Set(EntityScope("a").toTag)

    def storeWith(conflicts: Int) =
      new RecordingStore(List(envelope(1L, Created("a", 2), tags)), appendConflicts = conflicts)

    val tracedStore = storeWith(1)
    val plainStore = storeWith(1)
    val exhaustingStore = storeWith(4)

    for
      traced <- subject.run[IO](TestCommand("a"))(using
                  summon[Concurrent[IO]],
                  CommandRuntime(tracedStore, None, Some(telemetry)),
                )
      plain <- subject.run[IO](TestCommand("a"))(using
                 summon[Concurrent[IO]],
                 CommandRuntime.eventStoreOnly(plainStore),
               )
      // maxRetries is 3, so a fourth conflict exhausts them and the original conflict must surface unwrapped rather
      // than as the internal RetryableCommandAppendConflict.
      exhausted <- subject
                     .run[IO](TestCommand("a"))(using
                       summon[Concurrent[IO]],
                       CommandRuntime(exhaustingStore, None, Some(telemetry)),
                     )
                     .attempt
    yield expect(traced.map(_.map(_.payload)) == Right(List(Incremented("a", 3)))) and
      // Envelopes carry a fresh id and timestamp, so the decided payloads are what has to agree.
      expect(plain.map(_.map(_.payload)) == traced.map(_.map(_.payload))) and
      // Each retry re-reads and re-appends, and the counts agree across both runtimes.
      expect(tracedStore.appendAttempts == 2) and
      expect(tracedStore.readCount == 2) and
      expect(plainStore.appendAttempts == tracedStore.appendAttempts) and
      expect(plainStore.readCount == tracedStore.readCount) and
      expect(exhaustingStore.appendAttempts == 4) and
      expect(exhausted.left.map(_.getClass) == Left(classOf[IndexConflictException]))
  }

  pureTest("the lightweight runtime constructors leave telemetry and snapshots unset") {
    val store = new RecordingStore(Nil)
    given EventStore[IO, TestEvent] = store
    val derived = summon[CommandRuntime[IO, TestEvent]]

    expect(CommandRuntime.eventStoreOnly(store).telemetry.isEmpty) and
      expect(CommandRuntime.eventStoreOnly(store).snapshots.isEmpty) and
      expect(derived.telemetry.isEmpty) and
      expect(derived.snapshots.isEmpty)
  }

  test("declared headers are attached to every event the command emits") {
    val declared = Map("correlationId" -> "abc-123", "causationId" -> "def-456")
    val multiEmitHandler = new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
      protected val behavior = handler(()):
        tags(command => Set(Tag("entity", command.id)))
        headers(command => declared + ("command.id" -> command.id))
        on[LegacyObserved].ignore
        emitMany(command => List(LegacyObserved(command.id), LegacyObserved(s"${command.id}-2")))

    val store = new RecordingStore(Nil)
    val expected = declared + ("command.id" -> "a")

    multiEmitHandler
      .run[IO](TestCommand("a"))(using summon[Concurrent[IO]], CommandRuntime.eventStoreOnly(store))
      .map { result =>
        expect(result.map(_.map(_.payload)) == Right(List(LegacyObserved("a"), LegacyObserved("a-2")))) and
          expect(store.appended.exists(_._3.sizeIs == 2)) and
          expect(store.appended.exists(_._3.forall(_.headers == expected))) and
          expect(multiEmitHandler.headers(TestCommand("a")) == expected)
      }
  }

  test("a throwing headers resolver fails the command instead of the fiber") {
    val expected = new RuntimeException("headers failure")
    val failingHeaders = new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
      protected val behavior = handler(()):
        tags(_ => Set(Tag("entity", "a")))
        headers(_ => throw expected)
        on[LegacyObserved].ignore
        emit(_ => LegacyObserved("a"))

    val store = new RecordingStore(Nil)

    failingHeaders
      .run[IO](TestCommand("a"))(using summon[Concurrent[IO]], CommandRuntime.eventStoreOnly(store))
      .attempt
      .map(result => expect(result == Left(expected)) and expect(store.appended.isEmpty))
  }

  pureTest("headers declared more than once are rejected") {
    val result = Try {
      new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
        protected val behavior = handler(()):
          tags(_ => Set(Tag("entity", "a")))
          headers(_ => Map("a" -> "1"))
          headers(_ => Map("b" -> "2"))
          on[LegacyObserved].ignore
          emit(_ => LegacyObserved("a"))
    }

    result.failed.toOption match
      case Some(error: IllegalArgumentException) => expect(error.getMessage.contains("headers at most once"))
      case other                                 => failure(s"Expected duplicate headers error, got $other")
  }

  test("the headers resolver runs once per attempt: not once per event, and again after a conflict") {
    var calls = 0
    val countingHandler = new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
      protected val behavior = handler(()):
        tags(_ => Set(Tag("entity", "a")))
        headers { _ =>
          calls += 1
          Map("attempt" -> calls.toString)
        }
        on[LegacyObserved].ignore
        emitMany(_ => List(LegacyObserved("a"), LegacyObserved("b"), LegacyObserved("c")))

    val store = new RecordingStore(Nil, appendConflicts = 1)

    countingHandler
      .run[IO](TestCommand("a"))(using summon[Concurrent[IO]], CommandRuntime.eventStoreOnly(store))
      .map { result =>
        expect(result.map(_.map(_.payload).size) == Right(3)) and
          // Two attempts over three events each: once per attempt, not once per event.
          expect(calls == 2) and
          // The committed attempt is the second, and all three of its events share that one map.
          expect(store.appended.exists(_._3.forall(_.headers == Map("attempt" -> "2"))))
      }
  }

  test("runWithMessages commits the decided events and the handler's answer together") {
    val tags = Set(EntityScope("a").toTag)
    val store = new RecordingTransactionalStore(List(envelope(1L, Created("a", 2), tags)))
    val runtime = TransactionalCommandRuntime(store, None, Some(telemetry))

    answering.runWithMessages[IO](TestCommand("a"))(using summon[Concurrent[IO]], runtime).map { result =>
      expect(result.map(_.map(_.payload)) == Right(List(Incremented("a", 3)))) and
        expect(store.appended.exists(_._3.map(_.payload) == List(Incremented("a", 3)))) and
        expect(store.enqueued.flatten.map(_.payload) == List("Incremented(a,3)"))
    }
  }

  /** The reason the rejection is a `Left` rather than a raised error: it wrote no events, but it did answer, and the
    * caller has to be able to tell that from a partner that died.
    */
  test("a rejected command still answers, and writes no event") {
    val store = new RecordingTransactionalStore(Nil)

    answering
      .runWithMessages[IO](TestCommand("a"))(using summon[Concurrent[IO]], TransactionalCommandRuntime(store))
      .map { result =>
        expect(result == Left(TestRejection.Missing)) and
          expect(store.appended.isEmpty) and
          expect(store.enqueued.flatten.map(_.payload) == List("Missing"))
      }
  }

  test("a handler declaring no messages enqueues nothing, and the same runtime serves it through plain") {
    val tags = Set(EntityScope("a").toTag)
    val viaMessages = new RecordingTransactionalStore(List(envelope(1L, Created("a", 2), tags)))
    val viaPlain = new RecordingTransactionalStore(List(envelope(1L, Created("a", 2), tags)))

    for
      answered <- subject.runWithMessages[IO](TestCommand("a"))(using
                    summon[Concurrent[IO]],
                    TransactionalCommandRuntime(viaMessages),
                  )
      // A service with both kinds of handler builds one runtime and reaches for `plain` for the ones that answer nobody.
      plain <-
        subject.run[IO](TestCommand("a"))(using summon[Concurrent[IO]], TransactionalCommandRuntime(viaPlain).plain)
    yield expect(answered.map(_.map(_.payload)) == Right(List(Incremented("a", 3)))) and
      expect(viaPlain.enqueued.isEmpty)
  }

  test("a handler that cannot encode its answer writes nothing at all") {
    val encodingFailure = new RuntimeException("cannot encode the reply")
    val tags = Set(EntityScope("a").toTag)
    val unencodable = new EventSourcedCommandHandler[TestCommand, TestState, TestEvent, TestRejection]:
      protected val behavior = handler(TestState(exists = false, value = 0)):
        scope(EntityScope)(_.id)
        on[Created].matching(_.id, _.id).evolve((state, event) => state.copy(exists = true, value = event.value))
        on[Incremented].within(EntityScope).ignore
        messages((_, _, _) => Left(encodingFailure))
        emit((state, command) => Incremented(command.id, state.value + 1))

    val store = new RecordingTransactionalStore(List(envelope(1L, Created("a", 2), tags)))

    unencodable
      .runWithMessages[IO](TestCommand("a"))(using summon[Concurrent[IO]], TransactionalCommandRuntime(store))
      .attempt
      .map { result =>
        expect(result == Left(encodingFailure)) and
          expect(store.appended.isEmpty) and
          expect(store.enqueued.isEmpty)
      }
  }

  test("the loser of a conflict recomputes its answer and leaves no stale message behind") {
    var answers = 0
    val tags = Set(EntityScope("a").toTag)
    val retrying = new EventSourcedCommandHandler[TestCommand, TestState, TestEvent, TestRejection]:
      protected val behavior = handler(TestState(exists = false, value = 0)):
        scope(EntityScope)(_.id)
        on[Created].matching(_.id, _.id).evolve((state, event) => state.copy(exists = true, value = event.value))
        on[Incremented].within(EntityScope).evolve((state, event) => state.copy(value = state.value + event.amount))
        messages { (_, command, _) =>
          answers += 1
          Right(List(replyMessage(command, s"answer-$answers")))
        }
        emit((state, command) => Incremented(command.id, state.value + 1))

    val store = new RecordingTransactionalStore(List(envelope(1L, Created("a", 2), tags)), appendConflicts = 1)

    retrying
      .runWithMessages[IO](TestCommand("a"))(using summon[Concurrent[IO]], TransactionalCommandRuntime(store))
      .map { result =>
        expect(result.map(_.map(_.payload)) == Right(List(Incremented("a", 3)))) and
          expect(store.appendAttempts == 2) and
          expect(answers == 2) and
          expect(store.enqueued.flatten.map(_.payload) == List("answer-2"))
      }
  }

  test("a throwing messages resolver fails the command instead of the fiber") {
    val expected = new RuntimeException("messages failure")
    val tags = Set(EntityScope("a").toTag)
    val failingMessages = new EventSourcedCommandHandler[TestCommand, TestState, TestEvent, TestRejection]:
      protected val behavior = handler(TestState(exists = false, value = 0)):
        scope(EntityScope)(_.id)
        on[Created].matching(_.id, _.id).evolve((state, event) => state.copy(exists = true, value = event.value))
        on[Incremented].within(EntityScope).ignore
        messages((_, _, _) => throw expected)
        emit((state, command) => Incremented(command.id, state.value + 1))
    val store = new RecordingTransactionalStore(List(envelope(1L, Created("a", 2), tags)))

    failingMessages
      .runWithMessages[IO](TestCommand("a"))(using summon[Concurrent[IO]], TransactionalCommandRuntime(store))
      .attempt
      .map(result => expect(result == Left(expected)) and expect(store.appended.isEmpty))
  }

  pureTest("messages declared more than once are rejected") {
    val result = Try {
      new EventSourcedCommandHandler[TestCommand, Unit, TestEvent, Unit]:
        protected val behavior = handler(()):
          tags(_ => Set(Tag("entity", "a")))
          messages((_, _, _) => Right(Nil))
          messages((_, _, _) => Right(Nil))
          on[LegacyObserved].ignore
          emit(_ => LegacyObserved("a"))
    }

    result.failed.toOption match
      case Some(error: IllegalArgumentException) => expect(error.getMessage.contains("messages at most once"))
      case other                                 => failure(s"Expected duplicate messages error, got $other")
  }

  test("a saga handler's reply is encoded, addressed and comitted alongside its events") {
    val tags = Set(EntityScope("a").toTag)
    val store = new RecordingTransactionalStore(List(envelope(1L, Created("a", 2), tags)))

    Answering(sagaRequest())
      .runWithMessages[IO](TestCommand("a"))(using summon[Concurrent[IO]], TransactionalCommandRuntime(store))
      .map { result =>
        val sent = store.enqueued.flatten
        expect(result.map(_.map(_.payload)) == Right(List(Incremented("a", 3)))) and
          expect(store.appended.exists(_._3.map(_.payload) == List(Incremented("a", 3)))) and
          expect(sent.map(_.payload) == List("accepted:1")) and
          expect(sent.map(_.topic) == List(ReplyTopic)) and
          expect(sent.map(_.key) == List(Some(SagaInstance))) and
          expect(sent.forall(_.headers.get(SagaHeaders.Id).contains(SagaInstance))) and
          expect(sent.forall(_.headers.get(SagaHeaders.InReplyTo).contains(s"$SagaInstance:0:0:reserve")))
      }
  }

  test("a saga handler answers a rejection too, in the transaction that writes no event") {
    val store = new RecordingTransactionalStore(Nil)

    Answering(sagaRequest())
      .runWithMessages[IO](TestCommand("a"))(using summon[Concurrent[IO]], TransactionalCommandRuntime(store))
      .map { result =>
        expect(result == Left(TestRejection.Missing)) and
          expect(store.appended.isEmpty) and
          expect(store.enqueued.flatten.map(_.payload) == List("rejected:Missing"))
      }
  }

  test("a saga handler that answers nothing enqueues nothing, and still commits its events") {
    val tags = Set(EntityScope("quiet").toTag)
    val store = new RecordingTransactionalStore(List(envelope(1L, Created("quiet", 2), tags)))

    Answering(sagaRequest())
      .runWithMessages[IO](TestCommand("quiet"))(using summon[Concurrent[IO]], TransactionalCommandRuntime(store))
      .map { result =>
        expect(result.map(_.map(_.payload)) == Right(List(Incremented("quiet", 3)))) and
          expect(store.appended.nonEmpty) and
          expect(store.enqueued.flatten.isEmpty)
      }
  }

  test("a saga handler asked to answer a request that nominates nowhere send nothing, and still commits") {
    val tags = Set(EntityScope("a").toTag)
    val store = new RecordingTransactionalStore(List(envelope(1L, Created("a", 2), tags)))

    Answering(sagaRequest(addressed = false))
      .runWithMessages[IO](TestCommand("a"))(using summon[Concurrent[IO]], TransactionalCommandRuntime(store))
      .map { result =>
        expect(result.map(_.map(_.payload)) == Right(List(Incremented("a", 3)))) and
          expect(store.appended.nonEmpty) and
          expect(store.enqueued.flatten.isEmpty)
      }
  }

  test("a reply that cannot be encoded aborts the command: no events, no messages") {
    val tags = Set(EntityScope("a").toTag)
    val store = new RecordingTransactionalStore(List(envelope(1L, Created("a", 2), tags)))

    Answering(sagaRequest())(using unencodableReply)
      .runWithMessages[IO](TestCommand("a"))(using summon[Concurrent[IO]], TransactionalCommandRuntime(store))
      .attempt
      .map { result =>
        expect(result.isLeft) and
          expect(store.appended.isEmpty) and
          expect(store.enqueued.isEmpty)
      }
  }

  pureTest("a behavior cannot declare both reply and messages") {
    val result = Try {
      new EventSourcedSagaCommandHandler[TestCommand, Unit, TestEvent, Unit]:
        val request: RequestContext = sagaRequest()
        protected val behavior = handler(()):
          tags(_ => Set(Tag("entity", "a")))
          reply((_, _, _) => None)
          messages((_, _, _) => Right(Nil))
          on[LegacyObserved].ignore
          emit(_ => LegacyObserved("a"))
    }

    result.failed.toOption match
      case Some(error: IllegalArgumentException) => expect(error.getMessage.contains("messages at most once"))
      case other                                 => failure(s"Expected a duplicate messages error, got $other")
  }
