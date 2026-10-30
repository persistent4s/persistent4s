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

import scala.compiletime.testing.typeCheckErrors
import scala.util.Try

import cats.effect.IO

import weaver.SimpleIOSuite

object EventSourcedProjectionFamilySuite extends SimpleIOSuite:

  sealed trait E extends Event

  sealed trait AccountEvent extends E:

    def id: String

  final case class Opened(id: String, initial: Int) extends AccountEvent

  final case class Deposited(id: String, amount: Int) extends AccountEvent

  sealed trait ClosureEvent extends AccountEvent

  final case class Closed(id: String) extends ClosureEvent

  final case class Frozen(id: String) extends ClosureEvent

  object Frozen:

    given EventSchema[Frozen] = EventSchema[Frozen]("account.frozen")

  final case class Renamed(id: String, name: String) extends E

  sealed trait BookEvent extends E

  final case class BookAdded(bookId: UUID, copies: Int) extends BookEvent

  final case class BookLent(bookId: UUID, memberId: UUID) extends BookEvent

  sealed trait UnscopedEvent extends E

  final case class ScopedOnce(bookId: UUID) extends UnscopedEvent

  final case class NotScoped(bookId: UUID) extends UnscopedEvent

  enum CounterEvent extends E:

    case Started(id: String)

    case Bumped(id: String)

  enum SignalEvent extends E:

    case Reset

    case Ping(id: String)

  sealed trait EmptyFamily extends E

  private val books = Scope[UUID]("library.book")

  private val members = Scope[UUID]("library.member")

  private given depositedSchema: EventSchema[Deposited] =
    EventSchema[Deposited]("account.deposited").withAlias("MoneyDeposited")

  private given EventSchema[BookAdded] =
    EventSchema[BookAdded]("library.book-added").scopedBy(books)(_.bookId)

  private given EventSchema[BookLent] =
    EventSchema[BookLent]("library.book-lent").scopedBy(books)(_.bookId).scopedBy(members)(_.memberId)

  private given EventSchema[ScopedOnce] =
    EventSchema[ScopedOnce]("library.scoped-once").scopedBy(books)(_.bookId)

  private given resetSchema: EventSchema[SignalEvent.Reset.type] = EventSchema[SignalEvent.Reset.type]("signal.reset")

  private def evolve(state: Option[Int], event: AccountEvent): Option[Int] =
    event match
      case Opened(_, initial)   => Some(initial)
      case Deposited(_, amount) => state.map(_ + amount)
      case Closed(_)            => None
      case Frozen(_)            => state.map(_ => 0)

  private def noopRepository[K]: Repository[IO, K, Int] = new Repository[IO, K, Int]:
    def findMany(keys: List[K]): IO[Map[K, Option[Int]]] = IO.pure(Map.empty)
    def persist(upserts: Map[K, Int], deletes: List[K]): IO[Unit] = IO.unit

  private val projection: EventSourcedProjection[IO, E, String, Int] =
    new EventSourcedProjection[IO, E, String, Int]:
      val name = "accounts"
      protected val repository: Repository[IO, String, Int] = noopRepository
      protected val eventHandlers = handlers:
        onFamily[AccountEvent](_.id).handle(evolve)
        on[Renamed](_.id).update(_ + 100)

  private def env[P <: E](payload: P)(using schema: EventSchema[P]): EventEnvelope[E] =
    envAs(payload, schema.eventType)

  private def envAs(payload: E, eventType: EventTypeName): EventEnvelope[E] =
    EventEnvelope(
      EventMetadata(1L, UUID.randomUUID(), Set.empty, eventType, isExternal = false, Instant.now(), Map.empty),
      payload,
    )

  private def failureMessage(result: Try[?]): Option[String] =
    result.failed.toOption.collect { case error: IllegalArgumentException => error.getMessage }

  pureTest("the filter holds every concrete event of the family, including nested families and aliases") {
    expect(
      projection.filter == Set(
        EventTypeName.of[Opened],
        EventTypeName.fromString("account.deposited"),
        EventTypeName.fromString("MoneyDeposited"),
        EventTypeName.of[Closed],
        EventTypeName.fromString("account.frozen"),
        EventTypeName.of[Renamed],
      ),
    )
  }

  test("every event of the family reaches the shared function with its own key") {
    for
      opened    <- projection.handle(None, env(Opened("a", 10)))
      deposited <- projection.handle(Some(10), env(Deposited("b", 5)))
      closed    <- projection.handle(Some(15), env(Closed("c")))
      frozen    <- projection.handle(Some(15), env(Frozen("d")))
      renamed   <- projection.handle(Some(1), env(Renamed("e", "new")))
    yield expect.all(
      projection.resolveKeys(env(Opened("a", 10))) == List("a"),
      projection.resolveKeys(env(Deposited("b", 5))) == List("b"),
      projection.resolveKeys(env(Closed("c"))) == List("c"),
      projection.resolveKeys(env(Frozen("d"))) == List("d"),
      opened.contains(10),
      deposited.contains(15),
      closed.isEmpty,
      frozen.contains(0),
      renamed.contains(101),
    )
  }

  test("a custom schema and its alias dispatch to the family handler") {
    val payload = Deposited("a", 5)
    for
      primary <- projection.handle(Some(1), envAs(payload, depositedSchema.eventType))
      alias   <- projection.handle(Some(1), envAs(payload, EventTypeName.fromString("MoneyDeposited")))
    yield expect(primary.contains(6)) and expect(alias.contains(6))
  }

  test("state preconditions apply to each event with its own type") {
    val updates = new EventSourcedProjection[IO, E, String, Int]:
      val name = "account-updates"
      protected val repository: Repository[IO, String, Int] = noopRepository
      protected val eventHandlers = handlers:
        onFamily[AccountEvent](_.id).update((state, _) => state + 1)

    updates.handle(None, env(Closed("a"))).attempt.map {
      case Left(error: ProjectionStateNotFound) => expect(error.eventType == EventTypeName.of[Closed])
      case other                                => failure(s"Expected ProjectionStateNotFound, got $other")
    }
  }

  test("reject receives the event typed as the family") {
    val rejection = new RuntimeException("closed accounts cannot change")
    val guarded = new EventSourcedProjection[IO, E, String, Int]:
      val name = "guarded-accounts"
      protected val repository: Repository[IO, String, Int] = noopRepository
      protected val eventHandlers = handlers:
        onFamily[AccountEvent](_.id).reject { case (None, _: Deposited) => rejection }
          .handle(evolve)

    for
      rejected <- guarded.handle(None, env(Deposited("a", 5))).attempt
      opened   <- guarded.handle(None, env(Opened("a", 3)))
    yield expect(rejected == Left(rejection)) and expect(opened.contains(3))
  }

  pureTest("an event declared both in a family and with on is a duplicate handler") {
    val result = Try {
      new EventSourcedProjection[IO, E, String, Int]:
        val name = "duplicates"
        protected val repository: Repository[IO, String, Int] = noopRepository
        protected val eventHandlers = handlers:
          onFamily[AccountEvent](_.id).handle(evolve)
          on[Opened](_.id).set(_.initial)
    }

    expect(failureMessage(result).exists(_.contains("Duplicate event handlers: Opened")))
  }

  pureTest("handlersBy supplies the default key to every event of the family") {
    val scoped = new EventSourcedProjection[IO, E, UUID, Int]:
      val name = "books"
      protected val repository: Repository[IO, UUID, Int] = noopRepository
      protected val eventHandlers = handlersBy(books):
        onFamily[BookEvent].upsert((state, _) => state.getOrElse(0) + 1)

    val bookId = UUID.randomUUID()
    expect(scoped.resolveKeys(env(BookAdded(bookId, 2))) == List(bookId)) and
      expect(scoped.resolveKeys(env(BookLent(bookId, UUID.randomUUID()))) == List(bookId))
  }

  pureTest("handlersBy rejects a family event that does not declare the default scope") {
    val result = Try {
      new EventSourcedProjection[IO, E, UUID, Int]:
        val name = "unscoped"
        protected val repository: Repository[IO, UUID, Int] = noopRepository
        protected val eventHandlers = handlersBy(books):
          onFamily[UnscopedEvent].set(_ => 1)
    }

    expect(failureMessage(result).exists(_.contains("Event NotScoped does not declare scope library.book")))
  }

  pureTest("keyedBy overrides the key of every event of the family") {
    val scoped = new EventSourcedProjection[IO, E, UUID, Int]:
      val name = "members"
      protected val repository: Repository[IO, UUID, Int] = noopRepository
      protected val eventHandlers = handlersBy(books):
        onFamily[BookEvent].keyedBy(books).set(_ => 1)

    val bookId = UUID.randomUUID()
    expect(scoped.resolveKeys(env(BookLent(bookId, UUID.randomUUID()))) == List(bookId))
  }

  pureTest("a family without a key or a default scope is rejected") {
    val result = Try {
      new EventSourcedProjection[IO, E, String, Int]:
        val name = "missing-key"
        protected val repository: Repository[IO, String, Int] = noopRepository
        protected val eventHandlers = handlers:
          onFamily[AccountEvent].handle(evolve)
    }

    expect(failureMessage(result).exists(_.contains("must select a projection key")))
  }

  test("an enum is handled as a family of its cases") {
    import CounterEvent.*

    val counters = new EventSourcedProjection[IO, E, String, Int]:
      val name = "counters"
      protected val repository: Repository[IO, String, Int] = noopRepository
      protected val eventHandlers = handlers:
        onFamily[CounterEvent] {
          case Started(id) => id
          case Bumped(id)  => id
        }.upsert((state, _) => state.getOrElse(0) + 1)

    for bumped <- counters.handle(Some(1), env(new Bumped("a")))
    yield expect(counters.filter == Set(EventTypeName.of[Started], EventTypeName.of[Bumped])) and
      expect(counters.resolveKeys(env(new Started("a"))) == List("a")) and
      expect(bumped.contains(2))
  }

  pureTest("onFamily does not compile for a type that is not a sealed family") {
    val errors = typeCheckErrors(
      """
      new EventSourcedProjection[IO, Event, String, Int]:
        val name = "open"
        protected val repository: Repository[IO, String, Int] = noopRepository
        protected val eventHandlers = handlers:
          onFamily[Event](_ => "key").set(_ => 1)
      """,
    )

    expect(errors.exists(_.message.contains("onFamily requires a sealed trait or enum")))
  }

  pureTest("onFamily does not compile for a sealed trait without concrete events") {
    val errors = typeCheckErrors(
      """
      new EventSourcedProjection[IO, E, String, Int]:
        val name = "empty-family"
        protected val repository: Repository[IO, String, Int] = noopRepository
        protected val eventHandlers = handlers:
          onFamily[EmptyFamily](_ => "key").set(_ => 1)
      """,
    )

    expect(errors.exists(_.message.contains("onFamily requires a sealed trait or enum")))
  }

  pureTest("every on variant rejects a sealed family, including an enum, and points to onFamily") {
    val variants = List(
      typeCheckErrors(
        """
        new EventSourcedProjection[IO, E, String, Int]:
          val name = "on"
          protected val repository: Repository[IO, String, Int] = noopRepository
          protected val eventHandlers = handlers:
            on[AccountEvent](_.id).handle(evolve)
        """,
      ),
      typeCheckErrors(
        """
        new EventSourcedProjection[IO, E, UUID, Int]:
          val name = "on-default-key"
          protected val repository: Repository[IO, UUID, Int] = noopRepository
          protected val eventHandlers = handlersBy(books):
            on[BookEvent].set(_ => 1)
        """,
      ),
      typeCheckErrors(
        """
        new EventSourcedProjection[IO, E, String, Int]:
          val name = "on-many"
          protected val repository: Repository[IO, String, Int] = noopRepository
          protected val eventHandlers = handlers:
            onMany[CounterEvent](_ => Nil).set(_ => 1)
        """,
      ),
      typeCheckErrors(
        """
        new EventSourcedProjection[IO, E, String, Int]:
          val name = "on-envelope"
          protected val repository: Repository[IO, String, Int] = noopRepository
          protected val eventHandlers = handlers:
            onEnvelope[SignalEvent](_ => "key").set(_ => 1)
        """,
      ),
      typeCheckErrors(
        """
        new EventSourcedProjection[IO, E, String, Int]:
          val name = "on-many-envelope"
          protected val repository: Repository[IO, String, Int] = noopRepository
          protected val eventHandlers = handlers:
            onManyEnvelope[E](_ => Nil).set(_ => 1)
        """,
      ),
    )

    val named = variants.head.map(_.message)

    forEach(variants)(errors => expect(errors.exists(_.message.contains("is a sealed family of events")))) and
      expect(named.exists(_.contains("AccountEvent is a sealed family of events"))) and
      expect(named.exists(_.contains("Use onFamily[AccountEvent]")))
  }

  pureTest("on still compiles for an abstract event type in a generic helper") {
    val errors = typeCheckErrors(
      """
      new EventSourcedProjection[IO, E, String, Int]:
        val name = "generic"
        protected val repository: Repository[IO, String, Int] = noopRepository

        private def counted[X <: E: scala.reflect.ClassTag: EventSchema](key: X => String)(using
          EventHandlerCollector[IO, E, String, Int],
        ): Unit =
          on[X](key).upsert((state, _) => state.getOrElse(0) + 1)

        protected val eventHandlers = handlers:
          counted[Renamed](_.id)
      """,
    )

    expect(clue(errors.map(_.message)).isEmpty)
  }

  test("on still accepts a parameterless enum case with its own schema") {
    val signals = new EventSourcedProjection[IO, E, String, Int]:
      val name = "signals"
      protected val repository: Repository[IO, String, Int] = noopRepository
      protected val eventHandlers = handlers:
        on[SignalEvent.Reset.type](_ => "signal").set(_ => 0)

    for reset <- signals.handle(Some(5), envAs(SignalEvent.Reset, resetSchema.eventType))
    yield expect(signals.filter == Set(EventTypeName.fromString("signal.reset"))) and expect(reset.contains(0))
  }
