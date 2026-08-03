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

import cats.effect.IO

import weaver.SimpleIOSuite

object EventSourcedProjectionSuite extends SimpleIOSuite:

  sealed trait E extends Event

  final case class Created(id: String, value: Int) extends E

  final case class Incremented(id: String) extends E

  final case class Replaced(id: String, value: Int) extends E

  final case class FannedOut(ids: List[String]) extends E

  final case class Removed(id: String) extends E

  final case class EffectFailed(id: String) extends E

  final case class MetadataRecorded(fallbackId: String) extends E

  final case class Guarded(id: String, value: Int, rejectMissing: Boolean, rejectExisting: Boolean) extends E

  final case class Ignored(id: String) extends E

  final case class StableCreated(id: String, value: Int) extends E

  final case class BookScoped(bookId: UUID, value: Int) extends E

  final case class BorrowingScoped(bookId: UUID, memberId: UUID, value: Int) extends E

  final case class ManyBorrowingsScoped(bookIds: List[UUID], memberIds: List[UUID], value: Int) extends E

  private val books = Scope[UUID]("library.book")

  private val members = Scope[UUID]("library.member")

  private given EventSchema[Created] = EventSchema.legacy[Created]

  private given EventSchema[Incremented] = EventSchema.legacy[Incremented]

  private given EventSchema[Replaced] = EventSchema.legacy[Replaced]

  private given EventSchema[FannedOut] = EventSchema.legacy[FannedOut]

  private given EventSchema[Removed] = EventSchema.legacy[Removed]

  private given EventSchema[EffectFailed] = EventSchema.legacy[EffectFailed]

  private given EventSchema[MetadataRecorded] = EventSchema.legacy[MetadataRecorded]

  private given EventSchema[Guarded] = EventSchema.legacy[Guarded]

  private given stableCreatedSchema: EventSchema[StableCreated] =
    EventSchema[StableCreated]("library.book-created").withAlias("BookCreated")

  private given EventSchema[BookScoped] =
    EventSchema[BookScoped]("library.book-scoped").scopedBy(books)(_.bookId)

  private given EventSchema[BorrowingScoped] =
    EventSchema[BorrowingScoped]("library.borrowing-scoped")
      .scopedBy(books)(_.bookId)
      .scopedBy(members)(_.memberId)

  private given EventSchema[ManyBorrowingsScoped] =
    EventSchema[ManyBorrowingsScoped]("library.many-borrowings-scoped")
      .scopedByMany(books)(_.bookIds)
      .scopedByMany(members)(_.memberIds)

  private val expectedFailure = new RuntimeException("expected handler failure")

  private val missingRejection = new RuntimeException("missing state rejected")

  private val existingRejection = new RuntimeException("existing state rejected")

  private val noopRepository: Repository[IO, String, Int] = new Repository[IO, String, Int]:
    def findMany(keys: List[String]): IO[Map[String, Option[Int]]] = IO.pure(Map.empty)
    def persist(upserts: Map[String, Int], deletes: List[String]): IO[Unit] = IO.unit

  private val projection: EventSourcedProjection[IO, E, String, Int] =
    new EventSourcedProjection[IO, E, String, Int]:
      val name = "test"
      protected val repository: Repository[IO, String, Int] = noopRepository
      protected val eventHandlers = handlers:
        on[Created](_.id).create(_.value)
        on[Incremented](_.id).update(_ + 1)
        on[Replaced](_.id).set(_.value)
        onMany[FannedOut](_.ids).update(_ + 10)
        on[Removed](_.id).delete
        on[EffectFailed](_.id).handleF((_, _) => IO.raiseError(expectedFailure))
        onEnvelope[MetadataRecorded](_.metadata.id.toString).set(_.metadata.globalPosition.toInt)
        on[Guarded](_.id)
          .reject:
            case (None, event) if event.rejectMissing     => missingRejection
            case (Some(_), event) if event.rejectExisting => existingRejection
          .set(_.value)

  private def env(
    payload: E,
    eventType: Option[EventTypeName] = None,
    position: Long = 1L,
    id: UUID = UUID.randomUUID(),
  ): EventEnvelope[E] =
    EventEnvelope(
      EventMetadata(
        position, id, Set.empty, eventType.getOrElse(EventTypeName.fromInstance(payload)), isExternal = false,
        Instant.now(),
      ),
      payload,
    )

  pureTest("filter is derived from the handlers") {
    expect(
      projection.filter == Set(
        EventTypeName.of[Created],
        EventTypeName.of[Incremented],
        EventTypeName.of[Replaced],
        EventTypeName.of[FannedOut],
        EventTypeName.of[Removed],
        EventTypeName.of[EffectFailed],
        EventTypeName.of[MetadataRecorded],
        EventTypeName.of[Guarded],
      ),
    ) and expect(!projection.filter.contains(EventTypeName.of[Ignored]))
  }

  pureTest("resolveKeys dispatches by event type and ignores unregistered events") {
    expect(projection.resolveKeys(env(Created("a", 1))) == List("a")) and
      expect(projection.resolveKeys(env(Incremented("b"))) == List("b")) and
      expect(projection.resolveKeys(env(Ignored("c"))) == Nil)
  }

  pureTest("onMany removes duplicate keys") {
    expect(projection.resolveKeys(env(FannedOut(List("a", "b", "a")))) == List("a", "b"))
  }

  test("create initializes missing state") {
    projection.handle(None, env(Created("a", 7))).map(result => expect(result == Some(7)))
  }

  test("create rejects existing state") {
    projection.handle(Some(1), env(Created("a", 7))).attempt.map {
      case Left(error: ProjectionStateAlreadyExists) => expect(error.eventType == EventTypeName.of[Created])
      case other                                     => failure(s"Expected ProjectionStateAlreadyExists, got $other")
    }
  }

  test("update changes existing state") {
    projection.handle(Some(7), env(Incremented("a"))).map(result => expect(result == Some(8)))
  }

  test("update rejects missing state instead of turning it into a deletion") {
    projection.handle(None, env(Incremented("a"))).attempt.map {
      case Left(error: ProjectionStateNotFound) => expect(error.eventType == EventTypeName.of[Incremented])
      case other                                => failure(s"Expected ProjectionStateNotFound, got $other")
    }
  }

  test("set creates or replaces state") {
    for
      created  <- projection.handle(None, env(Replaced("a", 3)))
      replaced <- projection.handle(Some(1), env(Replaced("a", 3)))
    yield expect(created == Some(3)) and expect(replaced == Some(3))
  }

  test("delete is idempotent") {
    for
      deleted  <- projection.handle(Some(1), env(Removed("a")))
      replayed <- projection.handle(None, env(Removed("a")))
    yield expect(deleted.isEmpty) and expect(replayed.isEmpty)
  }

  test("effectful handler failures are preserved") {
    projection.handle(Some(1), env(EffectFailed("a"))).attempt.map(result => expect(result == Left(expectedFailure)))
  }

  test("reject supports domain errors for missing or existing state") {
    for
      missing  <- projection.handle(None, env(Guarded("a", 2, rejectMissing = true, rejectExisting = false))).attempt
      existing <-
        projection.handle(Some(1), env(Guarded("a", 2, rejectMissing = false, rejectExisting = true))).attempt
      allowedMissing <-
        projection.handle(None, env(Guarded("a", 2, rejectMissing = false, rejectExisting = false)))
      allowedExisting <-
        projection.handle(Some(1), env(Guarded("a", 2, rejectMissing = false, rejectExisting = false)))
    yield expect(missing == Left(missingRejection)) and
      expect(existing == Left(existingRejection)) and
      expect(allowedMissing == Some(2)) and
      expect(allowedExisting == Some(2))
  }

  test("envelope handlers can resolve keys and state from metadata") {
    val id = UUID.randomUUID()
    val envelope = env(MetadataRecorded("unused"), position = 42L, id = id)

    for result <- projection.handle(None, envelope)
    yield expect(projection.resolveKeys(envelope) == List(id.toString)) and expect(result == Some(42))
  }

  test("unregistered events leave state untouched") {
    projection.handle(Some(42), env(Ignored("a"))).map(result => expect(result == Some(42)))
  }

  pureTest("duplicate event handlers are rejected") {
    val result = Try {
      new EventSourcedProjection[IO, E, String, Int]:
        val name = "duplicates"
        protected val repository: Repository[IO, String, Int] = noopRepository
        protected val eventHandlers = handlers:
          on[Created](_.id).set(_.value)
          on[Created](_.id).set(_.value)
    }

    result.failed.toOption match
      case Some(error: IllegalArgumentException) =>
        expect(error.getMessage.contains("Duplicate event handlers: Created"))
      case other => failure(s"Expected duplicate handler error, got $other")
  }

  pureTest("an empty handler registry is rejected") {
    val result = Try {
      new EventSourcedProjection[IO, E, String, Int]:
        val name = "empty"
        protected val repository: Repository[IO, String, Int] = noopRepository
        protected val eventHandlers = handlers:
          ()
    }

    result.failed.toOption match
      case Some(error: IllegalArgumentException) => expect(error.getMessage.contains("at least one event handler"))
      case other                                 => failure(s"Expected empty registry error, got $other")
  }

  pureTest("metadata and payload type mismatches fail descriptively during key resolution") {
    val result = Try(projection.resolveKeys(env(Ignored("a"), Some(EventTypeName.of[Created]))))

    result.failed.toOption match
      case Some(error: EventPayloadTypeMismatch) =>
        expect(error.eventType == EventTypeName.of[Created]) and expect(error.actualClass.contains("Ignored"))
      case other => failure(s"Expected EventPayloadTypeMismatch, got $other")
  }

  test("metadata and payload type mismatches are raised in the effect while handling") {
    projection.handle(Some(1), env(Ignored("a"), Some(EventTypeName.of[Created]))).attempt.map {
      case Left(error: EventPayloadTypeMismatch) => expect(error.eventType == EventTypeName.of[Created])
      case other                                 => failure(s"Expected EventPayloadTypeMismatch, got $other")
    }
  }

  test("stable event identifiers and historical aliases dispatch to the same handler") {
    val stableProjection = new EventSourcedProjection[IO, E, String, Int]:
      val name = "stable-event-types"
      protected val repository: Repository[IO, String, Int] = noopRepository
      protected val eventHandlers = handlers:
        on[StableCreated](_.id).set(_.value)

    val payload = StableCreated("book-1", 7)
    val primary = env(payload, Some(stableCreatedSchema.eventType))
    val alias = env(payload, Some(EventTypeName.fromString("BookCreated")))

    for
      primaryState <- stableProjection.handle(None, primary)
      aliasState   <- stableProjection.handle(None, alias)
    yield expect.all(
      stableProjection.filter == stableCreatedSchema.acceptedEventTypes,
      stableProjection.resolveKeys(primary) == List("book-1"),
      stableProjection.resolveKeys(alias) == List("book-1"),
      primaryState.contains(7),
      aliasState.contains(7),
    )
  }

  pureTest("keyedBy derives a typed projection key from the event schema scope") {
    val scopedRepository = new Repository[IO, UUID, Int]:
      def findMany(keys: List[UUID]): IO[Map[UUID, Option[Int]]] = IO.pure(Map.empty)
      def persist(upserts: Map[UUID, Int], deletes: List[UUID]): IO[Unit] = IO.unit

    val scopedProjection = new EventSourcedProjection[IO, E, UUID, Int]:
      val name = "book-scope"
      protected val repository: Repository[IO, UUID, Int] = scopedRepository
      protected val eventHandlers = handlers:
        on[BookScoped].keyedBy(books).set(_.value)

    val bookId = UUID.randomUUID()
    val envelope = env(BookScoped(bookId, 3), Some(summon[EventSchema[BookScoped]].eventType))

    expect(scopedProjection.resolveKeys(envelope) == List(bookId))
  }

  pureTest("keyedBy preserves two scope arguments as an ordered tuple key") {
    val scopedRepository = new Repository[IO, (UUID, UUID), Int]:
      def findMany(keys: List[(UUID, UUID)]): IO[Map[(UUID, UUID), Option[Int]]] = IO.pure(Map.empty)
      def persist(upserts: Map[(UUID, UUID), Int], deletes: List[(UUID, UUID)]): IO[Unit] = IO.unit

    val scopedProjection = new EventSourcedProjection[IO, E, (UUID, UUID), Int]:
      val name = "borrowing-scopes"
      protected val repository: Repository[IO, (UUID, UUID), Int] = scopedRepository
      protected val eventHandlers = handlers:
        on[BorrowingScoped].keyedBy(books, members).set(_.value)

    val bookId = UUID.randomUUID()
    val memberId = UUID.randomUUID()
    val envelope = env(BorrowingScoped(bookId, memberId, 5), Some(summon[EventSchema[BorrowingScoped]].eventType))

    expect(scopedProjection.resolveKeys(envelope) == List(bookId -> memberId))
  }

  pureTest("handlersBy applies one declared scope to every handler") {
    val scopedRepository = new Repository[IO, UUID, Int]:
      def findMany(keys: List[UUID]): IO[Map[UUID, Option[Int]]] = IO.pure(Map.empty)
      def persist(upserts: Map[UUID, Int], deletes: List[UUID]): IO[Unit] = IO.unit

    val scopedProjection = new EventSourcedProjection[IO, E, UUID, Int]:
      val name = "default-book-scope"
      protected val repository: Repository[IO, UUID, Int] = scopedRepository
      protected val eventHandlers = handlersBy(books):
        on[BookScoped].set(_.value)

    val bookId = UUID.randomUUID()
    val envelope = env(BookScoped(bookId, 3), Some(summon[EventSchema[BookScoped]].eventType))

    expect(scopedProjection.resolveKeys(envelope) == List(bookId))
  }

  pureTest("handlersBy preserves two default scopes as an ordered tuple key") {
    val scopedRepository = new Repository[IO, (UUID, UUID), Int]:
      def findMany(keys: List[(UUID, UUID)]): IO[Map[(UUID, UUID), Option[Int]]] = IO.pure(Map.empty)
      def persist(upserts: Map[(UUID, UUID), Int], deletes: List[(UUID, UUID)]): IO[Unit] = IO.unit

    val scopedProjection = new EventSourcedProjection[IO, E, (UUID, UUID), Int]:
      val name = "default-borrowing-scopes"
      protected val repository: Repository[IO, (UUID, UUID), Int] = scopedRepository
      protected val eventHandlers = handlersBy(books, members):
        on[BorrowingScoped].set(_.value)

    val bookId = UUID.randomUUID()
    val memberId = UUID.randomUUID()
    val envelope = env(BorrowingScoped(bookId, memberId, 5), Some(summon[EventSchema[BorrowingScoped]].eventType))

    expect(scopedProjection.resolveKeys(envelope) == List(bookId -> memberId))
  }

  pureTest("two-scope handlers derive the Cartesian product of every resolved scope key") {
    val scopedRepository = new Repository[IO, (UUID, UUID), Int]:
      def findMany(keys: List[(UUID, UUID)]): IO[Map[(UUID, UUID), Option[Int]]] = IO.pure(Map.empty)
      def persist(upserts: Map[(UUID, UUID), Int], deletes: List[(UUID, UUID)]): IO[Unit] = IO.unit

    val scopedProjection = new EventSourcedProjection[IO, E, (UUID, UUID), Int]:
      val name = "many-borrowing-scopes"
      protected val repository: Repository[IO, (UUID, UUID), Int] = scopedRepository
      protected val eventHandlers = handlersBy(books, members):
        on[ManyBorrowingsScoped].set(_.value)

    val book1 = UUID.randomUUID()
    val book2 = UUID.randomUUID()
    val member1 = UUID.randomUUID()
    val member2 = UUID.randomUUID()
    val envelope = env(
      ManyBorrowingsScoped(List(book1, book2, book1), List(member1, member2, member1), 5),
      Some(summon[EventSchema[ManyBorrowingsScoped]].eventType),
    )

    expect(
      scopedProjection.resolveKeys(envelope) == List(
        book1 -> member1,
        book1 -> member2,
        book2 -> member1,
        book2 -> member2,
      ),
    )
  }

  pureTest("a handler can explicitly override its projection-level default scope") {
    val scopedRepository = new Repository[IO, UUID, Int]:
      def findMany(keys: List[UUID]): IO[Map[UUID, Option[Int]]] = IO.pure(Map.empty)
      def persist(upserts: Map[UUID, Int], deletes: List[UUID]): IO[Unit] = IO.unit

    val scopedProjection = new EventSourcedProjection[IO, E, UUID, Int]:
      val name = "overridden-scope"
      protected val repository: Repository[IO, UUID, Int] = scopedRepository
      protected val eventHandlers = handlersBy(members):
        on[BookScoped].keyedBy(books).set(_.value)

    val bookId = UUID.randomUUID()
    val envelope = env(BookScoped(bookId, 5), Some(summon[EventSchema[BookScoped]].eventType))

    expect(scopedProjection.resolveKeys(envelope) == List(bookId))
  }

  pureTest("handlersBy rejects an event schema that does not declare the selected scope") {
    val scopedRepository = new Repository[IO, UUID, Int]:
      def findMany(keys: List[UUID]): IO[Map[UUID, Option[Int]]] = IO.pure(Map.empty)
      def persist(upserts: Map[UUID, Int], deletes: List[UUID]): IO[Unit] = IO.unit

    val result = Try {
      new EventSourcedProjection[IO, E, UUID, Int]:
        val name = "missing-default-scope"
        protected val repository: Repository[IO, UUID, Int] = scopedRepository
        protected val eventHandlers = handlersBy(members):
          on[BookScoped].set(_.value)
    }

    result.failed.toOption match
      case Some(error: IllegalArgumentException) =>
        expect(error.getMessage.contains("does not declare scope library.member"))
      case other => failure(s"Expected missing scope validation, got $other")
  }
