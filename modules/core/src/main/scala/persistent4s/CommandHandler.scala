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

import scala.util.control.NonFatal

import cats.effect.Concurrent
import cats.syntax.all.*
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Counter
import org.typelevel.otel4s.trace.Tracer

import scala.annotation.unused

/** A CommandHandler defines how a command is processed in an event-sourced system. It reads events from the store to
  * build the current state, validates the command against that state, and decides which new events to produce.
  *
  * @tparam C
  *   the command type
  * @tparam S
  *   the state type, derived by folding events
  * @tparam E
  *   the event type
  */
trait CommandHandler[C, S, E <: Event]:

  /** Which tags to read from the event store for this command. */
  def tags(command: C): Set[Tag]

  /** The event types that this handler is interested in for building the state. If not specified, all events with the
    * relevant tags will be included.
    */
  def eventTypes: Option[Set[EventTypeName]] = None

  /** The initial state before any events have been applied. */
  def initial: S

  /** Fold a single event into the current state. */
  def evolve(command: C, state: S, event: E): S

  /** Validate the command against the current state. Should raise an error if the command is invalid. */
  def validate(state: S, command: C): Either[Throwable, Unit]

  /** Produce the events that result from applying the command, each with its own set of tags. Only called after
    * validation passes.
    */
  def decide(state: S, command: C): List[(Set[Tag], E)]

  /** Internal decision hook that receives the already-resolved command tags. Implementations that default emitted
    * events to the command's read scope can override this to avoid evaluating [[tags]] twice.
    */
  protected def decide(state: S, command: C, @unused commandTags: Set[Tag]): List[(Set[Tag], E)] =
    decide(state, command)

  /** Additional metadata to attach to every event produced by this command. Override to add. Defaults to none. */
  def headers(@unused command: C): Map[String, String] = Map.empty

  /** Messages to enqueue in the same transaction that appends this command's events, so an event and the message it
    * causes become visible together or not at all. Only [[runWithMessages]] consults this; [[run]] ignores it.
    *
    * `outcome` is `Left` when [[validate]] rejected the command. A handler answering another service has to be able to
    * reply precisely when it writes nothing.
    *
    * Returning `Left` aborts everything: no events are appended and no messages are enqueued. It is there because these
    * messages have to be serialized, and a pure function with no error channel could only throw.
    */
  def messages(
    @unused state: S,
    @unused command: C,
    @unused outcome: Either[Throwable, List[E]],
  ): Either[Throwable, List[OutgoingMessage]] = Right(Nil)

  /** Maximum number of retry attempts on optimistic concurrency conflicts. Override to customize. Default is 3. */
  def maxRetries: Int = 3

  /** Execute a command using this handler and the given event store. On optimistic concurrency conflict
    * (IndexConflictException), the command is automatically retried with fresh state up to maxRetries times. The
    * command is re-validated against the new state and may still succeed.
    */
  def run[F[_]: Concurrent: Tracer](command: C)(using
    eventStore: EventStore[F, E],
    metrics: CommandHandlerMetrics[F],
  ): F[List[EventEnvelope[E]]] =
    val cmdAttr = Attribute("command.type", command.getClass.getSimpleName)
    Tracer[F]
      .spanBuilder("persistent4s.commandhandler.handle")
      .addAttribute(cmdAttr)
      .build
      .surround(runWithRetry(command, maxRetries, metrics.retries, cmdAttr))

  /** [[run]] plus [[messages]], appended and enqueued atomically.
    *
    * A rejection comes back as a `Left` rather than raised, unlike in [[run]]: the rejection may have emitted a message
    * of its own, and that message is committed, so the caller has to be able to tell "rejected, and answered" from
    * "failed, redeliver me" without one exception standing for both.
    *
    * Traced and counted exactly like [[run]]. A partner answering someone else's saga is running the same command
    * against the same store; there is no reason for it to be the one path in the library nobody can see.
    */
  def runWithMessages[F[_]: Concurrent: Tracer](command: C)(using
    eventStore: EventStore[F, E] & TransactionalMessages[F, E],
    metrics: CommandHandlerMetrics[F],
  ): F[Either[Throwable, List[EventEnvelope[E]]]] =
    val cmdAttr = Attribute("command.type", command.getClass.getSimpleName)
    Tracer[F]
      .spanBuilder("persistent4s.commandhandler.handle")
      .addAttribute(cmdAttr)
      .build
      .surround(runWithMessagesRetry(command, maxRetries, metrics.retries, cmdAttr))

  private def runWithRetry[F[_]: Concurrent](
    command: C,
    retriesLeft: Int,
    retriesCounter: Counter[F, Long],
    cmdAttr: Attribute[String],
  )(using eventStore: EventStore[F, E]): F[List[EventEnvelope[E]]] =
    attempt(command).handleErrorWith {
      case _: IndexConflictException if retriesLeft > 0 =>
        retriesCounter.add(1L, cmdAttr) *> runWithRetry(command, retriesLeft - 1, retriesCounter, cmdAttr)
      case e =>
        Concurrent[F].raiseError(e)
    }

  private def runWithMessagesRetry[F[_]: Concurrent](
    command: C,
    retriesLeft: Int,
    retriesCounter: Counter[F, Long],
    cmdAttr: Attribute[String],
  )(using
    eventStore: EventStore[F, E] & TransactionalMessages[F, E],
  ): F[Either[Throwable, List[EventEnvelope[E]]]] =
    attemptWithMessages(command).handleErrorWith {
      case _: IndexConflictException if retriesLeft > 0 =>
        retriesCounter.add(1L, cmdAttr) *> runWithMessagesRetry(command, retriesLeft - 1, retriesCounter, cmdAttr)
      case e =>
        Concurrent[F].raiseError(e)
    }

  private def attempt[F[_]: Concurrent](
    command: C,
  )(using eventStore: EventStore[F, E]): F[List[EventEnvelope[E]]] =
    for
      tags      <- suspend(tags(command))
      filter     = EventFilter(eventTypes.getOrElse(Set.empty), tags)
      envelopes <- eventStore.readFrom(0L, filter).compile.toList
      state     <- suspend(envelopes.foldLeft(initial)((s, env) => evolve(command, s, env.payload)))
      index      = envelopes.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
      result    <- suspend(validate(state, command))
      _         <- result match
             case Left(e)  => Concurrent[F].raiseError(e)
             case Right(_) => Concurrent[F].unit
      decided      <- suspend(decide(state, command, tags))
      eventHeaders <- suspend(headers(command))
      events       <- suspend(
                  decided.map((tags, event) =>
                    PendingEvent(payload = event, tags = tags, eventType = EventTypeName.fromInstance(event),
                      isExternal = false, id = None, headers = eventHeaders),
                  ),
                )
      appendedEvents <- eventStore.append(filter, index, events)
    yield appendedEvents

  /** [[attempt]] with the handler's [[messages]] enqueued in the same transaction.
    *
    * Written in the same shape as [[attempt]] rather than sharing a helper with it, because it has to interleave: the
    * messages are consulted on both the rejected and the accepted path, and on the rejected one they are all that gets
    * written. Every piece of user code goes through [[suspend]] here too — a handler that throws while deciding what to
    * answer must fail the command, not the fibre.
    */
  private def attemptWithMessages[F[_]: Concurrent](
    command: C,
  )(using
    eventStore: EventStore[F, E] & TransactionalMessages[F, E],
  ): F[Either[Throwable, List[EventEnvelope[E]]]] =
    for
      tags      <- suspend(tags(command))
      filter     = EventFilter(eventTypes.getOrElse(Set.empty), tags)
      envelopes <- eventStore.readFrom(0L, filter).compile.toList
      state     <- suspend(envelopes.foldLeft(initial)((s, env) => evolve(command, s, env.payload)))
      index      = envelopes.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
      validated <- suspend(validate(state, command))
      outcome   <- validated match
                   case Left(rejection) =>
                     // No events, so there is no local invariant to protect and `appendWithMessages` skips the
                     // conflict check — the rejection's message is enqueued on its own.
                     for
                       outgoing <- suspend(messages(state, command, Left(rejection))).rethrow
                       _        <- eventStore.appendWithMessages(filter, index, outgoing)
                     yield Left(rejection)
                   case Right(_) =>
                     for
                       decided      <- suspend(decide(state, command, tags))
                       outgoing     <- suspend(messages(state, command, Right(decided.map(_._2)))).rethrow
                       eventHeaders <- suspend(headers(command))
                       events       <-
                         suspend(
                           decided.map((tags, event) =>
                             PendingEvent(payload = event, tags = tags, eventType = EventTypeName.fromInstance(event),
                               isExternal = false, id = None, headers = eventHeaders),
                           ),
                         )
                       appended <- eventStore.appendWithMessages(filter, index, outgoing, events)
                     yield Right(appended)
    yield outcome

  private def suspend[F[_]: Concurrent, B](value: => B): F[B] =
    try Concurrent[F].pure(value)
    catch case NonFatal(error) => Concurrent[F].raiseError(error)
