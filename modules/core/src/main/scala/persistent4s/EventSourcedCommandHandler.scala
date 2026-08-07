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

import cats.effect.Concurrent
import cats.syntax.all.*
import org.typelevel.otel4s.Attribute

/** Raised when an emitted event declares a durable scope that the command did not acquire. */
final case class CommandScopeViolation(
  eventType: EventTypeName,
  missingScopes: Set[ScopeId],
) extends IllegalArgumentException(
      s"Event ${eventType.value} uses command scopes that were not acquired: " +
        missingScopes.toList.map(_.value).sorted.mkString(", "),
    )

/** Raised when a high-level command emits an event type that it did not register with `on[Event]`. */
final case class UnregisteredCommandEvent(eventClass: String)
    extends IllegalArgumentException(
      s"No event schema is registered for emitted event $eventClass; declare an on[Event] transition or ignore it",
    )

final private case class EventDescription(
  eventType: EventTypeName,
  version: Int,
  scopes: Set[ScopeId],
)

final private case class RetryableCommandAppendConflict(conflict: IndexConflictException)
    extends RuntimeException(conflict)

/** A typed state transition registered by an [[EventSourcedCommandHandler]]. */
final class CommandEventHandler[C, S, A <: Event] private (
  private[persistent4s] val acceptedEventTypes: Set[EventTypeName],
  private[persistent4s] val version: Int,
  private val acceptsPayload: A => Boolean,
  private val describePayload: A => Option[EventDescription],
  private val run: (C, S, A) => S,
):

  private[persistent4s] def accepts(event: A): Boolean =
    acceptsPayload(event)

  private[persistent4s] def describe(event: A): Option[EventDescription] =
    describePayload(event)

  private[persistent4s] def evolve(command: C, state: S, event: A): S =
    run(command, state, event)

object CommandEventHandler:

  private[persistent4s] def make[C, S, A <: Event, E <: A](
    accepts: (C, E) => Boolean,
    transition: (S, C, E) => S,
  )(using classTag: ClassTag[E], schema: EventSchema[E]): CommandEventHandler[C, S, A] =
    new CommandEventHandler[C, S, A](
      schema.acceptedEventTypes,
      schema.version,
      event => classTag.unapply(event).isDefined,
      event =>
        classTag
          .unapply(event)
          .map(typed => EventDescription(schema.eventType, schema.version, schema.resolveScopes(typed))),
      (command, state, event) =>
        classTag.unapply(event) match
          case Some(typedEvent) if accepts(command, typedEvent) => transition(state, command, typedEvent)
          case Some(_)                                          => state
          case None                                             =>
            throw EventPayloadTypeMismatch(
              schema.eventType,
              classTag.runtimeClass.getName,
              event.getClass.getName,
            ),
    )

final private case class CommandScopeResolver[C](
  name: String,
  resolve: C => List[ScopeId],
)

final private case class ResolvedCommandSelection(
  scopes: List[ScopeId],
  tags: Set[Tag],
  scopeBased: Boolean,
)

final private case class CommandEmission[A <: Event](
  explicitTags: Option[Set[Tag]],
  event: A,
)

final private case class CommandSnapshotDefinition[C, S](
  id: SnapshotId,
  version: Int,
  every: Int,
  failureMode: SnapshotFailureMode,
  key: (C, ResolvedCommandSelection) => String,
  codec: SnapshotCodec[S],
)

/** The immutable definition collected by an [[EventSourcedCommandHandler.handler]] block. */
final class CommandBehavior[C, S, A <: Event, R] private[persistent4s] (
  private[persistent4s] val initial: S,
  private val resolveLegacyTags: Option[C => Set[Tag]],
  private val resolveHeaders: Option[C => Map[String, String]],
  private val resolveMessages: Option[(S, C, Either[R, List[A]]) => Either[Throwable, List[OutgoingMessage]]],
  private val scopes: List[CommandScopeResolver[C]],
  private val handlers: List[CommandEventHandler[C, S, A]],
  private val rejection: PartialFunction[(S, C), R],
  private val emitEvents: (S, C) => List[CommandEmission[A]],
  private[persistent4s] val snapshot: Option[CommandSnapshotDefinition[C, S]],
):

  private lazy val byType: Map[EventTypeName, CommandEventHandler[C, S, A]] =
    handlers.flatMap(handler => handler.acceptedEventTypes.toList.map(_ -> handler)).toMap

  private[persistent4s] val eventTypes: Set[EventTypeName] = byType.keySet

  private[persistent4s] val eventSchemas: Set[EventStorageSchema] =
    handlers.flatMap(handler => handler.acceptedEventTypes.map(EventStorageSchema(_, handler.version))).toSet

  private[persistent4s] val scopeNames: Set[String] = scopes.map(_.name).toSet

  private[persistent4s] def selection(command: C): ResolvedCommandSelection =
    if scopes.nonEmpty then
      val resolved = scopes.flatMap(_.resolve(command)).distinct
      require(resolved.nonEmpty, "Command behavior resolved no scope keys")
      ResolvedCommandSelection(resolved, resolved.iterator.map(_.toTag).toSet, scopeBased = true)
    else ResolvedCommandSelection(Nil, resolveLegacyTags.fold(Set.empty[Tag])(_(command)), scopeBased = false)

  private[persistent4s] def headers(command: C): Map[String, String] =
    resolveHeaders.fold(Map.empty[String, String])(_(command))

  private[persistent4s] def messages(
    state: S,
    command: C,
    outcome: Either[R, List[A]],
  ): Either[Throwable, List[OutgoingMessage]] =
    resolveMessages.fold(Right(Nil): Either[Throwable, List[OutgoingMessage]])(_(state, command, outcome))

  private[persistent4s] def evolve(command: C, state: S, event: A): S =
    handlers.find(_.accepts(event)).fold(state)(_.evolve(command, state, event))

  private[persistent4s] def evolve(command: C, state: S, event: EventEnvelope[A]): S =
    byType.get(event.metadata.eventType).fold(state)(_.evolve(command, state, event.payload))

  private[persistent4s] def validate(state: S, command: C): Either[R, Unit] =
    rejection.lift((state, command)).toLeft(())

  private[persistent4s] def describe(event: A): EventDescription =
    handlers.iterator.flatMap(_.describe(event)).nextOption().getOrElse {
      throw UnregisteredCommandEvent(event.getClass.getName)
    }

  private[persistent4s] def decide(
    state: S,
    command: C,
    selection: ResolvedCommandSelection,
  ): List[(Set[Tag], A)] =
    emitEvents(state, command).map { emission =>
      val description = describe(emission.event)
      val eventScopeTags = description.scopes.map(_.toTag)
      val missingScopes = description.scopes.filterNot(scope => selection.scopes.contains(scope))
      if missingScopes.nonEmpty then throw CommandScopeViolation(description.eventType, missingScopes)

      val tags = emission.explicitTags.getOrElse {
        if eventScopeTags.nonEmpty then eventScopeTags else selection.tags
      }
      val missingEventTags = eventScopeTags.filterNot(tags.contains)
      if missingEventTags.nonEmpty then
        throw CommandScopeViolation(description.eventType, description.scopes.filter(s => missingEventTags(s.toTag)))

      tags -> emission.event
    }

/** The registration context supplied inside an [[EventSourcedCommandHandler.handler]] block. */
@implicitNotFound("Command behavior must be declared inside `handler(initial): ...`")
final class CommandBehaviorCollector[C, S, A <: Event, R] private[persistent4s] ():

  private val eventHandlers = ListBuffer.empty[CommandEventHandler[C, S, A]]

  private val commandScopes = ListBuffer.empty[CommandScopeResolver[C]]

  private var resolveTags: Option[C => Set[Tag]] = None

  private var resolveHeaders: Option[C => Map[String, String]] = None

  private var resolveMessages: Option[(S, C, Either[R, List[A]]) => Either[Throwable, List[OutgoingMessage]]] = None

  private var rejection: PartialFunction[(S, C), R] = PartialFunction.empty

  private var emitEvents: Option[(S, C) => List[CommandEmission[A]]] = None

  private var snapshotDefinition: Option[CommandSnapshotDefinition[C, S]] = None

  private[persistent4s] def add(handler: CommandEventHandler[C, S, A]): Unit =
    eventHandlers += handler

  private[persistent4s] def setTags(resolve: C => Set[Tag]): Unit =
    require(resolveTags.isEmpty, "Command behavior must declare legacy tags exactly once")
    require(commandScopes.isEmpty, "Use either typed scopes or legacy tags, not both")
    resolveTags = Some(resolve)

  private[persistent4s] def setHeaders(resolve: C => Map[String, String]): Unit =
    require(resolveHeaders.isEmpty, "Command behavior must declare headers at most once")
    resolveHeaders = Some(resolve)

  private[persistent4s] def setMessages(
    resolve: (S, C, Either[R, List[A]]) => Either[Throwable, List[OutgoingMessage]],
  ): Unit =
    require(resolveMessages.isEmpty, "Command behavior must declare messages at most once")
    resolveMessages = Some(resolve)

  private[persistent4s] def addScope[K](scope: Scope[K], key: C => K): Unit =
    addScopes(scope, command => key(command) :: Nil)

  private[persistent4s] def addScopes[K](scope: Scope[K], keys: C => IterableOnce[K]): Unit =
    require(resolveTags.isEmpty, "Use either typed scopes or legacy tags, not both")
    require(!commandScopes.exists(_.name == scope.name), s"Command behavior already declares scope ${scope.name}")
    commandScopes += CommandScopeResolver(
      scope.name,
      command => keys(command).iterator.map(scope(_)).toList.distinct,
    )

  private[persistent4s] def resolveScopes[K](scope: Scope[K], command: C): Set[ScopeId] =
    resolveScopes(scope.name, command)

  private[persistent4s] def resolveScopes(scopeName: String, command: C): Set[ScopeId] =
    commandScopes
      .find(_.name == scopeName)
      .map(_.resolve(command).toSet)
      .getOrElse(throw new IllegalArgumentException(s"Command behavior does not declare scope $scopeName"))

  private[persistent4s] def hasScope(name: String): Boolean =
    commandScopes.exists(_.name == name)

  private[persistent4s] def hasTypedScopes: Boolean =
    commandScopes.nonEmpty

  private[persistent4s] def hasLegacyTags: Boolean =
    resolveTags.nonEmpty

  private[persistent4s] def sharedScopeNames[E](schema: EventSchema[E]): Set[String] =
    commandScopes.iterator.map(_.name).toSet.intersect(schema.scopeNames)

  private[persistent4s] def addRejection(rule: PartialFunction[(S, C), R]): Unit =
    rejection = rejection.orElse(rule)

  private[persistent4s] def setEmitter(emit: (S, C) => List[CommandEmission[A]]): Unit =
    require(emitEvents.isEmpty, "Command behavior must declare one emitter")
    emitEvents = Some(emit)

  private[persistent4s] def setSnapshot(definition: CommandSnapshotDefinition[C, S]): Unit =
    require(snapshotDefinition.isEmpty, "Command behavior must declare at most one snapshot policy")
    snapshotDefinition = Some(definition)

  private[persistent4s] def result(initial: S): CommandBehavior[C, S, A, R] =
    val allHandlers = eventHandlers.toList
    require(allHandlers.nonEmpty, "Command behavior must declare at least one event handler")
    require(commandScopes.nonEmpty || resolveTags.nonEmpty, "Command behavior must declare at least one scope")

    val duplicateEventTypes =
      allHandlers
        .flatMap(handler => handler.acceptedEventTypes.toList.map(_ -> handler))
        .groupBy(_._1)
        .collect { case (eventType, _ :: _ :: _) => eventType.value }
        .toList
        .sorted
    require(duplicateEventTypes.isEmpty, s"Duplicate command event handlers: ${duplicateEventTypes.mkString(", ")}")

    new CommandBehavior(
      initial, resolveTags, resolveHeaders, resolveMessages, commandScopes.toList, allHandlers, rejection,
      emitEvents.getOrElse(throw new IllegalArgumentException("Command behavior must declare an emitter")),
      snapshotDefinition,
    )

/** The staged definition returned by [[EventSourcedCommandHandler.on]]. Event selection is fail-closed: when the
  * command and event share exactly one typed scope, that scope is matched automatically; when they share none, the
  * handler must opt in with [[allEvents]]; and when they share several, it must choose with [[within]] or
  * [[withinAll]].
  */
final class CommandEventHandlerDefinition[C, S, A <: Event, R, E <: A] private[persistent4s] (
  collector: CommandBehaviorCollector[C, S, A, R],
  accepts: (C, E) => Boolean = (_: C, _: E) => true,
  explicitScopes: Set[String] = Set.empty,
  acceptsAllEvents: Boolean = false,
)(using classTag: ClassTag[E], schema: EventSchema[E]):

  private def updated(
    nextAccepts: (C, E) => Boolean = accepts,
    nextExplicitScopes: Set[String] = explicitScopes,
    nextAcceptsAllEvents: Boolean = acceptsAllEvents,
  ): CommandEventHandlerDefinition[C, S, A, R, E] =
    new CommandEventHandlerDefinition(collector, nextAccepts, nextExplicitScopes, nextAcceptsAllEvents)

  /** Apply this transition only when `predicate` holds. Chained predicates are combined with `&&`. */
  def when(predicate: (C, E) => Boolean): CommandEventHandlerDefinition[C, S, A, R, E] =
    updated(nextAccepts = (command, event) => accepts(command, event) && predicate(command, event))

  /** Apply this transition when a key derived from the command equals a key derived from the event. */
  def matching[K](commandKey: C => K, eventKey: E => K): CommandEventHandlerDefinition[C, S, A, R, E] =
    when((command, event) => commandKey(command) == eventKey(event))

  /** Apply this transition only to the event history selected by the command's typed `scope`. The corresponding event
    * key comes from `EventSchema[E].scopedBy(scope)(...)`, so it is declared only once.
    */
  def within[K](scope: Scope[K]): CommandEventHandlerDefinition[C, S, A, R, E] =
    require(!acceptsAllEvents, "Cannot combine allEvents with within")
    require(collector.hasScope(scope.name), s"Command behavior must declare scope ${scope.name} before using within")
    require(
      schema.scopeNames.contains(scope.name),
      s"Event ${schema.eventType.value} does not declare scope ${scope.name}",
    )
    updated(
      nextAccepts = (command, event) =>
        accepts(command, event) &&
          schema.resolveScopeIds(scope, event).exists(collector.resolveScopes(scope, command).contains),
      nextExplicitScopes = explicitScopes + scope.name,
    )

  /** Match every typed scope shared by this command and event. Use this when matching all shared scopes is deliberate;
    * ambiguous multi-scope handlers are otherwise rejected.
    */
  def withinAll: CommandEventHandlerDefinition[C, S, A, R, E] =
    require(!acceptsAllEvents, "Cannot combine allEvents with withinAll")
    require(explicitScopes.isEmpty, "Declare either individual within scopes or withinAll, not both")
    val shared = collector.sharedScopeNames(schema)
    require(
      shared.nonEmpty,
      s"Event ${schema.eventType.value} shares no typed scope with this command; use allEvents to accept it explicitly",
    )
    updated(
      nextAccepts = (command, event) =>
        accepts(command, event) && shared.forall { scopeName =>
          schema.resolveScopeIds(scopeName, event).exists(collector.resolveScopes(scopeName, command).contains)
        },
      nextExplicitScopes = shared,
    )

  /** Deliberately accept this event from the command's complete selected history without scope-key matching. This is
    * required for an event that shares no typed scope with the command.
    */
  def allEvents: CommandEventHandlerDefinition[C, S, A, R, E] =
    require(explicitScopes.isEmpty, "Cannot combine within scopes with allEvents")
    updated(nextAcceptsAllEvents = true)

  /** Evolve state without inspecting the command or event. */
  def evolve(transition: S => S): Unit =
    evolve((state: S, _: E) => transition(state))

  /** Evolve state using the typed event. */
  def evolve(transition: (S, E) => S): Unit =
    evolve((state: S, _: C, event: E) => transition(state, event))

  /** Evolve state using the current state, command and typed event. */
  def evolve(transition: (S, C, E) => S): Unit =
    collector.add(CommandEventHandler.make(resolveScopeSelection, transition))

  /** Include this event type in replay while leaving state unchanged. */
  def ignore: Unit =
    evolve(identity[S])

  private def resolveScopeSelection: (C, E) => Boolean =
    if acceptsAllEvents || explicitScopes.nonEmpty || collector.hasLegacyTags then accepts
    else if !collector.hasTypedScopes then
      throw new IllegalArgumentException(
        s"Command behavior must declare at least one scope before on[${schema.eventType.value}]",
      )
    else
      collector.sharedScopeNames(schema).toList.sorted match
        case scopeName :: Nil =>
          (command, event) =>
            accepts(command, event) &&
              schema.resolveScopeIds(scopeName, event).exists(collector.resolveScopes(scopeName, command).contains)
        case Nil =>
          throw new IllegalArgumentException(
            s"Event ${schema.eventType.value} shares no typed scope with this command; " +
              "declare allEvents to accept it explicitly",
          )
        case scopeNames =>
          throw new IllegalArgumentException(
            s"Event ${schema.eventType.value} shares multiple command scopes " +
              s"(${scopeNames.mkString(", ")}); declare within(scope) or withinAll explicitly",
          )

/** A command handler described by one typed behavior block. Domain rejections are returned as `Left[R]`; storage,
  * decoding, callback and exhausted concurrency failures remain failures in `F`.
  */
trait EventSourcedCommandHandler[C, S, A <: Event, R]:

  /** The stable command definition. Build it with [[handler]]. */
  protected val behavior: CommandBehavior[C, S, A, R]

  /** Set to `true` only for legacy tag-based handlers that deliberately need every event type. Typed scope handlers
    * always read every event in their scopes so concurrency revisions are independent from reducer filters.
    */
  protected def readAllEventTypes: Boolean = false

  /** Maximum number of retry attempts after an optimistic concurrency conflict. */
  protected def maxRetries: Int = 3

  /** Collect one command's scopes, state transitions, rejection rules, optional snapshot and event emission. */
  final protected def handler(
    initial: S,
  )(declare: CommandBehaviorCollector[C, S, A, R] ?=> Unit): CommandBehavior[C, S, A, R] =
    given collector: CommandBehaviorCollector[C, S, A, R] = new CommandBehaviorCollector
    declare
    collector.result(initial)

  /** Declare one typed history and concurrency scope. Call repeatedly for multi-scope commands. */
  final protected def scope[K](definition: Scope[K])(key: C => K)(using
    collector: CommandBehaviorCollector[C, S, A, R],
  ): Unit =
    collector.addScope(definition, key)

  /** Acquire several keys from the same typed scope, for example both accounts participating in a transfer. */
  final protected def scopeMany[K](definition: Scope[K])(keys: C => IterableOnce[K])(using
    collector: CommandBehaviorCollector[C, S, A, R],
  ): Unit =
    collector.addScopes(definition, keys)

  /** Legacy escape hatch. New handlers should use [[scope]] so consistency boundaries have one stable typed definition. */
  final protected def tags(resolve: C => Set[Tag])(using collector: CommandBehaviorCollector[C, S, A, R]): Unit =
    collector.setTags(resolve)

  /** Metadata attached to every event this command emits — a correlation id, a causation id, the request that caused
    * it. Evaluated once per attempt: every event in one attempt shares the same map, and a retry re-evaluates it, so an
    * impure resolver (a fresh id, a clock reading) yields fresh values for the attempt that actually commits.
    */
  final protected def headers(resolve: C => Map[String, String])(using
    collector: CommandBehaviorCollector[C, S, A, R],
  ): Unit =
    collector.setHeaders(resolve)

  /** Messages enqueued in the transaction that appends this command's events, so an event and the message it causes
    * become visible together or not at all. Only [[runWithMessages]] consults this; [[run]] ignores it.
    *
    * `outcome` is `Left` when a [[reject]] rule turned the command down. Answering precisely when it writes nothing is
    * the whole job of a partner that can say no: a rejection the caller never hears about is indistinguishable from a
    * partner that has died, and costs the asker its full deadline to find out.
    *
    * Returning `Left` aborts everything: no events, no messages. It exists because these messages have to be
    * serialized, and a pure function with no error channel could only throw.
    */
  final protected def messages(
    resolve: (S, C, Either[R, List[A]]) => Either[Throwable, List[OutgoingMessage]],
  )(using collector: CommandBehaviorCollector[C, S, A, R]): Unit =
    collector.setMessages(resolve)

  /** Start a typed state transition declaration. High-level handlers require an explicit stable [[EventSchema]]. A
    * single typed scope shared by the command and event is matched automatically. No shared scope requires an explicit
    * `.allEvents`, while multiple shared scopes require `.within(scope)` or `.withinAll`.
    */
  final protected def on[E <: A: ClassTag: EventSchema](using
    collector: CommandBehaviorCollector[C, S, A, R],
  ): CommandEventHandlerDefinition[C, S, A, R, E] =
    new CommandEventHandlerDefinition(collector)

  /** Reject the first matching state/command combination with a typed domain error. */
  final protected def reject(rule: PartialFunction[(S, C), R])(using
    collector: CommandBehaviorCollector[C, S, A, R],
  ): Unit =
    collector.addRejection(rule)

  /** Cache caught-up command state every `every` replayed events. Snapshot state formats are versioned independently
    * from event formats. Increment `version` whenever state, its codec, reducer logic, matching predicates or scope-key
    * encoding changes. The default key is the command's canonical set of resolved scopes, and cache failures are
    * best-effort unless [[SnapshotFailureMode.Strict]] is selected.
    */
  final protected def snapshot(
    id: String,
  )(using collector: CommandBehaviorCollector[C, S, A, R], codec: SnapshotCodec[S]): Unit =
    snapshot(SnapshotId(id))

  final protected def snapshot(
    id: SnapshotId,
    version: Int = 1,
    every: Int = 100,
    failureMode: SnapshotFailureMode = SnapshotFailureMode.BestEffort,
  )(using collector: CommandBehaviorCollector[C, S, A, R], codec: SnapshotCodec[S]): Unit =
    snapshotBy(id, version, every, failureMode)((_, selection) => canonical(selection.tags.map(_.value)))

  /** Configure a snapshot with an explicit key for state that depends on command fields beyond its scopes. */
  final protected def snapshotBy(
    id: SnapshotId,
    version: Int = 1,
    every: Int = 100,
    failureMode: SnapshotFailureMode = SnapshotFailureMode.BestEffort,
  )(
    key: C => String,
  )(using collector: CommandBehaviorCollector[C, S, A, R], codec: SnapshotCodec[S]): Unit =
    snapshotBy(id, version, every, failureMode)((command, _) => key(command))

  private def snapshotBy(
    id: SnapshotId,
    version: Int,
    every: Int,
    failureMode: SnapshotFailureMode,
  )(
    key: (C, ResolvedCommandSelection) => String,
  )(using collector: CommandBehaviorCollector[C, S, A, R], codec: SnapshotCodec[S]): Unit =
    require(version >= 1, "Snapshot version must be at least 1")
    require(every >= 1, "Snapshot frequency must be at least 1")
    collector.setSnapshot(CommandSnapshotDefinition(id, version, every, failureMode, key, codec))

  /** Emit one event. Its tags are derived from its declared event scopes, falling back to all command scopes. */
  final protected def emit(event: C => A)(using collector: CommandBehaviorCollector[C, S, A, R]): Unit =
    emit((_: S, command: C) => event(command))

  /** Emit one event using the state and command. */
  final protected def emit(event: (S, C) => A)(using collector: CommandBehaviorCollector[C, S, A, R]): Unit =
    collector.setEmitter((state, command) => CommandEmission(None, event(state, command)) :: Nil)

  /** Emit several events using their declared event scopes. */
  final protected def emitMany(events: C => IterableOnce[A])(using
    collector: CommandBehaviorCollector[C, S, A, R],
  ): Unit =
    emitMany((_: S, command: C) => events(command))

  /** Emit several events using the state and command. */
  final protected def emitMany(events: (S, C) => IterableOnce[A])(using
    collector: CommandBehaviorCollector[C, S, A, R],
  ): Unit =
    collector.setEmitter((state, command) =>
      events(state, command).iterator.map(event => CommandEmission(None, event)).toList,
    )

  /** Emit one event with explicit additional/query tags. Every durable event scope must still be present. */
  final protected def emitTagged(event: C => (Set[Tag], A))(using
    collector: CommandBehaviorCollector[C, S, A, R],
  ): Unit =
    emitTagged((_: S, command: C) => event(command))

  /** Emit one explicitly tagged event using the state and command. */
  final protected def emitTagged(event: (S, C) => (Set[Tag], A))(using
    collector: CommandBehaviorCollector[C, S, A, R],
  ): Unit =
    collector.setEmitter((state, command) =>
      val (tags, emitted) = event(state, command)
      CommandEmission(Some(tags), emitted) :: Nil,
    )

  /** Emit several independently tagged events. */
  final protected def emitManyTagged(events: C => IterableOnce[(Set[Tag], A)])(using
    collector: CommandBehaviorCollector[C, S, A, R],
  ): Unit =
    emitManyTagged((_: S, command: C) => events(command))

  /** Emit several independently tagged events using the state and command. */
  final protected def emitManyTagged(events: (S, C) => IterableOnce[(Set[Tag], A)])(using
    collector: CommandBehaviorCollector[C, S, A, R],
  ): Unit =
    collector.setEmitter((state, command) =>
      events(state, command).iterator.map((tags, event) => CommandEmission(Some(tags), event)).toList,
    )

  final def eventTypes: Set[EventTypeName] = behavior.eventTypes

  final def initial: S = behavior.initial

  final def scopes(command: C): Set[ScopeId] = behavior.selection(command).scopes.toSet

  final def tags(command: C): Set[Tag] = behavior.selection(command).tags

  final def headers(command: C): Map[String, String] = behavior.headers(command)

  final def messages(state: S, command: C, outcome: Either[R, List[A]]): Either[Throwable, List[OutgoingMessage]] =
    behavior.messages(state, command, outcome)

  final def evolve(command: C, state: S, event: A): S = behavior.evolve(command, state, event)

  final def validate(state: S, command: C): Either[R, Unit] = behavior.validate(state, command)

  final def decide(state: S, command: C): List[(Set[Tag], A)] =
    behavior.decide(state, command, behavior.selection(command))

  /** Execute a command. Expected domain failures are `Left`; infrastructure failures remain failed effects. */
  final def run[F[_]: Concurrent](command: C)(using
    runtime: CommandRuntime[F, A],
  ): F[Either[R, List[EventEnvelope[A]]]] =
    val cmdAttr = Attribute("command.type", command.getClass.getSimpleName)
    traced(runtime.telemetry, cmdAttr) {
      suspend(behavior.selection(command)).flatMap { selection =>
        withRetry(runtime.telemetry, cmdAttr, maxRetries)(attempt(command, selection))
      }
    }

  /** [[run]] plus the behavior's [[messages]], appended and enqueued in one transaction.
    *
    * If the command write no event, the rejection message is committed.
    */
  final def runWithMessages[F[_]: Concurrent](command: C)(using
    runtime: TransactionalCommandRuntime[F, A],
  ): F[Either[R, List[EventEnvelope[A]]]] =
    val cmdAttr = Attribute("command.type", command.getClass.getSimpleName)
    traced(runtime.telemetry, cmdAttr) {
      suspend(behavior.selection(command)).flatMap { selection =>
        withRetry(runtime.telemetry, cmdAttr, maxRetries)(attemptWithMessages(command, selection))
      }
    }

  private def traced[F[_], B](
    telemetry: Option[CommandTelemetry[F]],
    cmdAttr: Attribute[String],
  )(body: F[B]): F[B] =
    telemetry.fold(body) { t =>
      t.tracer
        .spanBuilder("persistent4s.commandhandler.handle")
        .addAttribute(cmdAttr)
        .build
        .surround(body)
    }

  private def countRetry[F[_]: Concurrent](
    telemetry: Option[CommandTelemetry[F]],
    cmdAttr: Attribute[String],
  ): F[Unit] =
    telemetry.traverse_(_.metrics.retries.add(1L, cmdAttr))

  /** Execute a command and translate only its typed domain rejection into an application error. Existing failed effects
    * (storage, decoding and concurrency failures) pass through unchanged.
    */
  final def runOrRaise[F[_]: Concurrent](command: C)(mapRejection: R => Throwable)(using
    runtime: CommandRuntime[F, A],
  ): F[List[EventEnvelope[A]]] =
    run(command).flatMap(_.leftMap(mapRejection).liftTo[F])

  final private case class ReplayStart(
    state: S,
    globalPosition: Long,
    eventCount: Long,
  )

  final private case class ReplayedCommand(
    state: S,
    index: Long,
    validation: Either[R, Unit],
  )

  private def retryable: PartialFunction[Throwable, Throwable] = { case conflict: IndexConflictException =>
    RetryableCommandAppendConflict(conflict)
  }

  private def readFilter(selection: ResolvedCommandSelection): EventFilter =
    val readEventTypes =
      if selection.scopeBased || readAllEventTypes then Set.empty[EventTypeName]
      else behavior.eventTypes
    EventFilter(readEventTypes, selection.tags)

  /** Everything both commit paths share: resume from a snapshot, replay, cache the caught-up state, validate. The state
    * is returned even when validation rejects, because a handler that answers a caller needs it to say why.
    */
  private def replay[F[_]: Concurrent](
    command: C,
    selection: ResolvedCommandSelection,
    filter: EventFilter,
    fingerprint: String,
  )(using runtime: CommandRuntime[F, A]): F[ReplayedCommand] =
    for
      start      <- loadSnapshot(command, selection, filter, fingerprint)
      envelopes  <- runtime.eventStore.readFrom(start.globalPosition, filter).compile.toList
      state      <- suspend(envelopes.foldLeft(start.state)((current, event) => behavior.evolve(command, current, event)))
      index       = envelopes.lastOption.map(_.metadata.globalPosition).getOrElse(start.globalPosition)
      eventCount  = start.eventCount + envelopes.size
      _          <- saveSnapshotIfDue(command, selection, fingerprint, start.eventCount, state, index, eventCount)
      validation <- suspend(behavior.validate(state, command))
    yield ReplayedCommand(state, index, validation)

  /** Scope checks, storage-schema checks and headers, in one place so the two commit paths cannot disagree. */
  private def buildEvents[F[_]: Concurrent](
    command: C,
    decided: List[(Set[Tag], A)],
  )(using runtime: CommandRuntime[F, A]): F[List[PendingEvent[A]]] =
    suspend {
      val eventHeaders = behavior.headers(command)
      decided.map { (tags, event) =>
        val description = behavior.describe(event)
        val declared = EventStorageSchema(description.eventType, description.version)
        runtime.eventStore.storageSchema(event).foreach { storage =>
          if storage != declared then throw EventSchemaMismatch(declared, storage, event.getClass.getName)
        }
        PendingEvent(payload = event, tags = tags, eventType = description.eventType, isExternal = false, id = None,
          headers = eventHeaders)
      }
    }

  private def withRetry[F[_]: Concurrent, B](
    telemetry: Option[CommandTelemetry[F]],
    cmdAttr: Attribute[String],
    retriesLeft: Int,
  )(attempt: => F[B]): F[B] =
    attempt.handleErrorWith {
      case RetryableCommandAppendConflict(_) if retriesLeft > 0 =>
        countRetry(telemetry, cmdAttr) *> withRetry(telemetry, cmdAttr, retriesLeft - 1)(attempt)
      case RetryableCommandAppendConflict(conflict) =>
        Concurrent[F].raiseError(conflict)
      case error => Concurrent[F].raiseError(error)
    }

  private def attempt[F[_]: Concurrent](
    command: C,
    selection: ResolvedCommandSelection,
  )(using runtime: CommandRuntime[F, A]): F[Either[R, List[EventEnvelope[A]]]] =
    val filter = readFilter(selection)
    val fingerprint = filterFingerprint(behavior.eventSchemas, selection.tags)

    replay(command, selection, filter, fingerprint).flatMap { replayed =>
      replayed.validation match
        case Left(rejection) => Concurrent[F].pure(Left(rejection))
        case Right(_)        =>
          for
            decided  <- suspend(behavior.decide(replayed.state, command, selection))
            events   <- buildEvents(command, decided)
            appended <- runtime.eventStore.append(filter, replayed.index, events).adaptError(retryable)
          yield Right(appended)
    }

  /** [[attempt]] with the behavior's messages enqueued in the same transaction. Interleaved rather than composed with
    * [[attempt]], because the messages are consulted on both outcomes and on the rejected one they are all that gets
    * written.
    */
  private def attemptWithMessages[F[_]: Concurrent](
    command: C,
    selection: ResolvedCommandSelection,
  )(using runtime: TransactionalCommandRuntime[F, A]): F[Either[R, List[EventEnvelope[A]]]] =
    given CommandRuntime[F, A] = runtime.plain

    val filter = readFilter(selection)
    val fingerprint = filterFingerprint(behavior.eventSchemas, selection.tags)

    replay(command, selection, filter, fingerprint).flatMap { replayed =>
      replayed.validation match
        case Left(rejection) =>
          // No events, so there is no local invariant to protect: `appendWithMessages` skips the conflict check and the
          // rejection's message is enqueued alone. Deliberately not adapted to a retryable conflict — a store that
          // conflicts here has broken the TransactionalMessages contract, and surfacing that beats retrying around it.
          for
            outgoing <- suspend(behavior.messages(replayed.state, command, Left(rejection))).rethrow
            _        <- runtime.eventStore.appendWithMessages(filter, replayed.index, outgoing)
          yield Left(rejection)
        case Right(_) =>
          for
            decided  <- suspend(behavior.decide(replayed.state, command, selection))
            outgoing <- suspend(behavior.messages(replayed.state, command, Right(decided.map(_._2)))).rethrow
            events   <- buildEvents(command, decided)
            appended <- runtime.eventStore
                          .appendWithMessages(filter, replayed.index, outgoing, events)
                          .adaptError(retryable)
          yield Right(appended)
    }

  private def loadSnapshot[F[_]: Concurrent](
    command: C,
    selection: ResolvedCommandSelection,
    filter: EventFilter,
    fingerprint: String,
  )(using runtime: CommandRuntime[F, A]): F[ReplayStart] =
    val cold = ReplayStart(behavior.initial, 0L, 0L)

    (behavior.snapshot, runtime.snapshots) match
      case (Some(definition), Some(store)) =>
        val load =
          suspend(definition.key(command, selection)).flatMap { key =>
            store.load(definition.id, key, definition.version).flatMap {
              case Some(snapshot)
                  if snapshot.filterFingerprint == fingerprint &&
                    snapshot.globalPosition >= 0L &&
                    snapshot.eventCount >= 0L =>
                runtime.eventStore.currentRevision(filter).flatMap { currentRevision =>
                  if snapshot.globalPosition > currentRevision then
                    store.delete(definition.id, key, definition.version).as(cold)
                  else
                    definition.codec.decode(snapshot.payload) match
                      case Right(state) =>
                        Concurrent[F].pure(ReplayStart(state, snapshot.globalPosition, snapshot.eventCount))
                      case Left(_) =>
                        store.delete(definition.id, key, definition.version).as(cold)
                }
              case Some(_) =>
                store.delete(definition.id, key, definition.version).as(cold)
              case None => Concurrent[F].pure(cold)
            }
          }

        definition.failureMode match
          case SnapshotFailureMode.BestEffort => load.handleError(_ => cold)
          case SnapshotFailureMode.Strict     => load
      case _ => Concurrent[F].pure(cold)

  private def saveSnapshotIfDue[F[_]: Concurrent](
    command: C,
    selection: ResolvedCommandSelection,
    fingerprint: String,
    previousCount: Long,
    state: S,
    globalPosition: Long,
    eventCount: Long,
  )(using runtime: CommandRuntime[F, A]): F[Unit] =
    (behavior.snapshot, runtime.snapshots) match
      case (Some(definition), Some(store))
          if eventCount > 0 && eventCount / definition.every > previousCount / definition.every =>
        val save =
          for
            payload <- suspend(definition.codec.encode(state))
            key     <- suspend(definition.key(command, selection))
            _       <- store.save(
                   definition.id,
                   key,
                   definition.version,
                   StoredCommandSnapshot(globalPosition, eventCount, fingerprint, payload),
                 )
          yield ()

        definition.failureMode match
          case SnapshotFailureMode.BestEffort => save.handleError(_ => ())
          case SnapshotFailureMode.Strict     => save
      case _ => Concurrent[F].unit

  private def filterFingerprint(eventSchemas: Set[EventStorageSchema], scopeTags: Set[Tag]): String =
    val eventTypes = canonical(eventSchemas.map(schema => s"${schema.eventType.value}@${schema.version}"))
    val tags = canonical(scopeTags.map(_.value))
    s"events=$eventTypes;scopes=$tags"

  private def suspend[F[_]: Concurrent, B](value: => B): F[B] =
    try Concurrent[F].pure(value)
    catch case NonFatal(error) => Concurrent[F].raiseError(error)

  private def canonical(values: Iterable[String]): String =
    EventSourcedCommandHandler.canonical(values)

object EventSourcedCommandHandler:

  private[persistent4s] def canonical(values: Iterable[String]): String =
    values.toList.sorted.map(value => s"${value.length}:$value").mkString("|")
