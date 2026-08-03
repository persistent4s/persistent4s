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

import scala.annotation.implicitNotFound
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import cats.ApplicativeThrow
import cats.syntax.all.*

/** Raised by a strict `create` handler when the event expects a missing state but one already exists. */
final case class ProjectionStateAlreadyExists(eventType: EventTypeName)
    extends IllegalStateException(s"Projection state already exists while handling ${eventType.value}")

/** Raised by an `update` handler when the event expects an existing state but none exists. */
final case class ProjectionStateNotFound(eventType: EventTypeName)
    extends IllegalStateException(s"Projection state does not exist while handling ${eventType.value}")

/** Raised when an envelope's stored event type points to a handler for a different payload class. */
final case class EventPayloadTypeMismatch(
  eventType: EventTypeName,
  expectedClass: String,
  actualClass: String,
) extends IllegalArgumentException(
      s"Event ${eventType.value} expects payload $expectedClass but received $actualClass",
    )

/** A handler registered by an [[EventSourcedProjection]]. Use the projection's handler DSL to create one. */
final class EventHandler[F[_], A <: Event, K, S] private (
  private[persistent4s] val acceptedEventTypes: Set[EventTypeName],
  private val resolve: EventEnvelope[A] => List[K],
  private val run: (Option[S], EventEnvelope[A]) => F[Option[S]],
):

  private[persistent4s] def resolveKeys(event: EventEnvelope[A]): List[K] = resolve(event)

  private[persistent4s] def handle(state: Option[S], event: EventEnvelope[A]): F[Option[S]] = run(state, event)

object EventHandler:

  private[persistent4s] def make[F[_]: ApplicativeThrow, A <: Event, E <: A, K, S](
    keys: EventEnvelope[E] => IterableOnce[K],
    f: (Option[S], EventEnvelope[E]) => F[Option[S]],
  )(using classTag: ClassTag[E], schema: EventSchema[E]): EventHandler[F, A, K, S] =

    def narrow(event: EventEnvelope[A]): Either[EventPayloadTypeMismatch, EventEnvelope[E]] =
      classTag.unapply(event.payload) match
        case Some(payload) => Right(EventEnvelope(event.metadata, payload))
        case None          =>
          Left(
            EventPayloadTypeMismatch(
              event.metadata.eventType,
              classTag.runtimeClass.getName,
              event.payload.getClass.getName,
            ),
          )

    new EventHandler[F, A, K, S](
      schema.acceptedEventTypes,
      event => narrow(event).fold(throw _, e => keys(e).iterator.distinct.toList),
      (state, event) => narrow(event).fold(_.raiseError[F, Option[S]], e => f(state, e)),
    )

/** The registration context supplied inside an [[EventSourcedProjection.handlers]] block. */
@implicitNotFound("Event handlers must be declared inside `handlers: ...`")
final class EventHandlerCollector[F[_], A <: Event, K, S] private[persistent4s] (
  private[persistent4s] val defaultKey: Option[ProjectionDefaultKey[K]],
):

  private val buffer = ListBuffer.empty[EventHandler[F, A, K, S]]

  private[persistent4s] def add(handler: EventHandler[F, A, K, S]): Unit = buffer += handler

  private[persistent4s] def result: List[EventHandler[F, A, K, S]] = buffer.toList

/** A projection-level key resolver installed by [[EventSourcedProjection.handlersBy]]. */
sealed abstract private[persistent4s] class ProjectionDefaultKey[K]:

  private[persistent4s] def validate[E](schema: EventSchema[E]): Unit

  private[persistent4s] def resolve[E](schema: EventSchema[E], event: E): List[K]

private[persistent4s] object ProjectionDefaultKey:

  def single[K](scope: Scope[K]): ProjectionDefaultKey[K] =
    new ProjectionDefaultKey[K]:
      def validate[E](schema: EventSchema[E]): Unit = requireScope(schema, scope)

      def resolve[E](schema: EventSchema[E], event: E): List[K] =
        schema
          .resolveScopeIds(scope, event)
          .map(id => scope.decode(id).fold(throw _, identity))

  def tuple[K, K1, K2](first: Scope[K1], second: Scope[K2])(using
    tupleKey: (K1, K2) =:= K,
  ): ProjectionDefaultKey[K] =
    new ProjectionDefaultKey[K]:
      def validate[E](schema: EventSchema[E]): Unit =
        requireScope(schema, first)
        requireScope(schema, second)

      def resolve[E](schema: EventSchema[E], event: E): List[K] =
        val firstKeys =
          schema.resolveScopeIds(first, event).map(id => first.decode(id).fold(throw _, identity))
        val secondKeys =
          schema.resolveScopeIds(second, event).map(id => second.decode(id).fold(throw _, identity))
        for
          firstKey  <- firstKeys
          secondKey <- secondKeys
        yield tupleKey((firstKey, secondKey))

  private def requireScope[E, K](schema: EventSchema[E], scope: Scope[K]): Unit =
    require(
      schema.scopeNames.contains(scope.name),
      s"Event ${schema.eventType.value} does not declare scope ${scope.name}",
    )

/** The staged definition returned by [[EventSourcedProjection.on]] and [[EventSourcedProjection.onMany]]. */
final class EventHandlerDefinition[F[_], A <: Event, E <: A, K, S, P] private[persistent4s] (
  keys: Option[EventEnvelope[E] => IterableOnce[K]],
  input: EventEnvelope[E] => P,
  collector: EventHandlerCollector[F, A, K, S],
  rejection: PartialFunction[(Option[S], P), Throwable] = PartialFunction.empty,
  keyValidation: () => Unit = () => (),
)(using F: ApplicativeThrow[F], classTag: ClassTag[E], schema: EventSchema[E]):

  private type HandlerInput = (Option[S], P, EventEnvelope[E])

  /** Override a projection-level default and key this handler by one declared event scope. */
  def keyedBy(scope: Scope[K]): EventHandlerDefinition[F, A, E, K, S, P] =
    val key = ProjectionDefaultKey.single(scope)
    new EventHandlerDefinition(
      Some(event => key.resolve(schema, event.payload)),
      input,
      collector,
      rejection,
      () => key.validate(schema),
    )

  /** Override a projection-level default and key this handler by two declared event scopes. Multi-valued scopes produce
    * the ordered Cartesian product of their resolved keys.
    */
  def keyedBy[K1, K2](first: Scope[K1], second: Scope[K2])(using
    tupleKey: (K1, K2) =:= K,
  ): EventHandlerDefinition[F, A, E, K, S, P] =
    val key = ProjectionDefaultKey.tuple[K, K1, K2](first, second)
    new EventHandlerDefinition(
      Some(event => key.resolve(schema, event.payload)),
      input,
      collector,
      rejection,
      () => key.validate(schema),
    )

  private def register(f: HandlerInput => F[Option[S]]): Unit =
    keyValidation()
    val resolveKeys = keys.getOrElse {
      throw new IllegalArgumentException(
        s"Event ${schema.eventType.value} must select a projection key with `keyedBy(...)` or be declared inside `handlersBy(...)`",
      )
    }
    collector.add(
      EventHandler.make[F, A, E, K, S](
        resolveKeys,
        (state, event) =>
          try
            val selected = input(event)
            rejection.lift((state, selected)) match
              case Some(error) => F.raiseError(error)
              case None        => f((state, selected, event))
          catch case NonFatal(error) => F.raiseError(error),
      ),
    )

  /** Reject matching state/event combinations with a domain-specific error before applying the state operation. */
  def reject(error: PartialFunction[(Option[S], P), Throwable]): EventHandlerDefinition[F, A, E, K, S, P] =
    new EventHandlerDefinition(keys, input, collector, rejection.orElse(error), keyValidation)

  /** Create state only when none exists. Use [[set]] when replaying the event should replace existing state. */
  def create(f: P => S): Unit =
    register:
      case (None, selected, _) => F.pure(f(selected).some)
      case (Some(_), _, event) => F.raiseError(ProjectionStateAlreadyExists(event.metadata.eventType))

  /** Effectful version of [[create]]. */
  def createF(f: P => F[S]): Unit =
    register:
      case (None, selected, _) => f(selected).map(_.some)
      case (Some(_), _, event) => F.raiseError(ProjectionStateAlreadyExists(event.metadata.eventType))

  /** Set state regardless of whether it already exists. Unlike [[create]], replay replaces the current state. */
  def set(f: P => S): Unit =
    register { case (_, selected, _) => F.pure(f(selected).some) }

  /** Effectful version of [[set]]. */
  def setF(f: P => F[S]): Unit =
    register { case (_, selected, _) => f(selected).map(_.some) }

  /** Update existing state without inspecting the event. Fails with [[ProjectionStateNotFound]] when state is absent. */
  def update(f: S => S): Unit =
    update((state: S, _: P) => f(state))

  /** Update existing state using the event. Fails with [[ProjectionStateNotFound]] when state is absent. */
  def update(f: (S, P) => S): Unit =
    register:
      case (Some(state), selected, _) => F.pure(f(state, selected).some)
      case (None, _, event)           => F.raiseError(ProjectionStateNotFound(event.metadata.eventType))

  /** Effectfully update existing state without inspecting the event. */
  def updateF(f: S => F[S]): Unit =
    updateF((state: S, _: P) => f(state))

  /** Effectfully update existing state using the event. */
  def updateF(f: (S, P) => F[S]): Unit =
    register:
      case (Some(state), selected, _) => f(state, selected).map(_.some)
      case (None, _, event)           => F.raiseError(ProjectionStateNotFound(event.metadata.eventType))

  /** Create or replace state while inspecting the previous state. */
  def upsert(f: (Option[S], P) => S): Unit =
    register { case (state, selected, _) => F.pure(f(state, selected).some) }

  /** Effectful version of [[upsert]]. */
  def upsertF(f: (Option[S], P) => F[S]): Unit =
    register { case (state, selected, _) => f(state, selected).map(_.some) }

  /** Delete state. Replaying a deletion remains a successful no-op. */
  def delete: Unit =
    register(_ => F.pure(none[S]))

  /** Directly transform optional state. Returning `None` deletes it. */
  def handle(f: (Option[S], P) => Option[S]): Unit =
    register { case (state, selected, _) => F.pure(f(state, selected)) }

  /** Effectful version of [[handle]]. */
  def handleF(f: (Option[S], P) => F[Option[S]]): Unit =
    register { case (state, selected, _) => f(state, selected) }

/** A [[Projection]] declared as one typed handler per concrete event. The handler registrations derive the event
  * filter, key resolution, and state transitions, so those three concerns cannot drift apart.
  *
  * Declare handlers with [[handlers]] and choose the state operation explicitly:
  *
  * {{{
  * override protected val eventHandlers = handlers:
  *   on[Created](_.id).set(event => State(event.id))
  *   on[Changed](_.id).update((state, event) => state.copy(value = event.value))
  *   on[Deleted](_.id).delete
  * }}}
  *
  * `create` rejects existing state and `update` rejects missing state. Prefix an operation with
  * [[EventHandlerDefinition.reject]] for a domain-specific or conditional error.
  *
  * Use [[onEnvelope]] or [[onManyEnvelope]] when key resolution or state transitions need event metadata.
  */
trait EventSourcedProjection[F[_]: ApplicativeThrow, A <: Event, K, S] extends Projection[F, A, K, S]:

  /** The stable handler registry for this projection. Build it with [[handlers]]. */
  protected val eventHandlers: List[EventHandler[F, A, K, S]]

  /** Collect typed handler declarations into an immutable, validated registry. */
  final protected def handlers(declare: EventHandlerCollector[F, A, K, S] ?=> Unit): List[EventHandler[F, A, K, S]] =
    given collector: EventHandlerCollector[F, A, K, S] = new EventHandlerCollector(None)
    declare
    validate(collector.result)

  /** Collect handlers that use `scope` as their default key. An individual handler can override it with
    * `on[Event](key)` or `on[Event].keyedBy(anotherScope)`.
    */
  final protected def handlersBy(scope: Scope[K])(
    declare: EventHandlerCollector[F, A, K, S] ?=> Unit,
  ): List[EventHandler[F, A, K, S]] =
    given collector: EventHandlerCollector[F, A, K, S] =
      new EventHandlerCollector(Some(ProjectionDefaultKey.single(scope)))
    declare
    validate(collector.result)

  /** Collect handlers keyed by an ordered pair of declared event scopes. Multi-valued scopes produce the ordered
    * Cartesian product of their resolved keys.
    */
  final protected def handlersBy[K1, K2](first: Scope[K1], second: Scope[K2])(
    declare: EventHandlerCollector[F, A, K, S] ?=> Unit,
  )(using tupleKey: (K1, K2) =:= K): List[EventHandler[F, A, K, S]] =
    given collector: EventHandlerCollector[F, A, K, S] =
      new EventHandlerCollector(Some(ProjectionDefaultKey.tuple(first, second)))
    declare
    validate(collector.result)

  /** Start a handler using the projection-level default declared by [[handlersBy]], or requiring an explicit
    * [[EventHandlerDefinition.keyedBy]] before a state operation.
    */
  final protected def on[E <: A: ClassTag: EventSchema](using
    collector: EventHandlerCollector[F, A, K, S],
  ): EventHandlerDefinition[F, A, E, K, S, E] =
    val schema = summon[EventSchema[E]]
    val keys =
      collector.defaultKey.map(defaultKey => (event: EventEnvelope[E]) => defaultKey.resolve(schema, event.payload))
    new EventHandlerDefinition(
      keys,
      _.payload,
      collector,
      keyValidation = () => collector.defaultKey.foreach(_.validate(schema)),
    )

  /** Handle one key derived from the event payload. */
  final protected def on[E <: A: ClassTag: EventSchema](key: E => K)(using
    collector: EventHandlerCollector[F, A, K, S],
  ): EventHandlerDefinition[F, A, E, K, S, E] =
    new EventHandlerDefinition(Some(event => key(event.payload) :: Nil), _.payload, collector)

  /** Handle several distinct keys derived from the event payload. */
  final protected def onMany[E <: A: ClassTag: EventSchema](keys: E => IterableOnce[K])(using
    collector: EventHandlerCollector[F, A, K, S],
  ): EventHandlerDefinition[F, A, E, K, S, E] =
    new EventHandlerDefinition(Some(event => keys(event.payload)), _.payload, collector)

  /** Handle one key using the full envelope, including metadata. */
  final protected def onEnvelope[E <: A: ClassTag: EventSchema](key: EventEnvelope[E] => K)(using
    collector: EventHandlerCollector[F, A, K, S],
  ): EventHandlerDefinition[F, A, E, K, S, EventEnvelope[E]] =
    new EventHandlerDefinition(Some(event => key(event) :: Nil), event => event, collector)

  /** Handle several distinct keys using the full envelope, including metadata. */
  final protected def onManyEnvelope[E <: A: ClassTag: EventSchema](keys: EventEnvelope[E] => IterableOnce[K])(using
    collector: EventHandlerCollector[F, A, K, S],
  ): EventHandlerDefinition[F, A, E, K, S, EventEnvelope[E]] =
    new EventHandlerDefinition(Some(keys), event => event, collector)

  private def validate(all: List[EventHandler[F, A, K, S]]): List[EventHandler[F, A, K, S]] =
    require(all.nonEmpty, "An EventSourcedProjection must declare at least one event handler")
    val duplicates =
      all
        .flatMap(handler => handler.acceptedEventTypes.toList.map(_ -> handler))
        .groupBy(_._1)
        .collect { case (eventType, _ :: _ :: _) => eventType.value }
        .toList
        .sorted
    require(duplicates.isEmpty, s"Duplicate event handlers: ${duplicates.mkString(", ")}")
    all

  private lazy val byType: Map[EventTypeName, EventHandler[F, A, K, S]] =
    validate(eventHandlers).flatMap(handler => handler.acceptedEventTypes.toList.map(_ -> handler)).toMap

  final override def filter: Set[EventTypeName] =
    byType.keySet

  final override def resolveKeys(event: EventEnvelope[A]): List[K] =
    byType.get(event.metadata.eventType).fold(List.empty[K])(_.resolveKeys(event))

  final override def handle(state: Option[S], event: EventEnvelope[A]): F[Option[S]] =
    byType.get(event.metadata.eventType).fold(state.pure)(_.handle(state, event))

/** An [[EventSourcedProjection]] whose read-model writes and checkpoint advance are committed atomically by an
  * [[AtomicRepository]]. This guarantee covers repository state only; external effects performed by handlers are not
  * part of that transaction.
  */
trait ExactlyOnceEventSourcedProjection[F[_], A <: Event, K, S] extends EventSourcedProjection[F, A, K, S]:

  override protected val repository: AtomicRepository[F, K, S]
