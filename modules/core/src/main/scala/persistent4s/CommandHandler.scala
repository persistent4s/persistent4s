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

import cats.effect.Concurrent
import cats.syntax.all.*
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
  def run[F[_]: Concurrent](command: C)(using
    eventStore: EventStore[F, E],
  ): F[List[E]] =
    runWithRetry(command, maxRetries)

  /** [[run]] plus [[messages]], appended and enqueued atomically.
    *
    * A rejection comes back as a `Left` rather than raised, unlike in [[run]]: the rejection may have emitted a message
    * of its own, and that message is committed, so the caller has to be able to tell "rejected, and answered" from
    * "failed, redeliver me" without one exception standing for both.
    */
  def runWithMessages[F[_]: Concurrent](command: C)(using
    eventStore: EventStore[F, E] & TransactionalMessages[F, E],
  ): F[Either[Throwable, List[E]]] =
    runWithMessagesRetry(command, maxRetries)

  private def runWithRetry[F[_]: Concurrent](command: C, retriesLeft: Int)(using
    eventStore: EventStore[F, E],
  ): F[List[E]] =
    attempt(command).handleErrorWith {
      case _: IndexConflictException if retriesLeft > 0 =>
        runWithRetry(command, retriesLeft - 1)
      case e =>
        Concurrent[F].raiseError(e)
    }

  private def runWithMessagesRetry[F[_]: Concurrent](command: C, retriesLeft: Int)(using
    eventStore: EventStore[F, E] & TransactionalMessages[F, E],
  ): F[Either[Throwable, List[E]]] =
    attemptWithMessages(command).handleErrorWith {
      case _: IndexConflictException if retriesLeft > 0 =>
        runWithMessagesRetry(command, retriesLeft - 1)
      case e =>
        Concurrent[F].raiseError(e)
    }

  /** Read this command's scope and fold it into a state, returning that state alongside the filter that defines the
    * scope and the index to append against — the same filter has to be handed back to the append, or the concurrency
    * check would guard something other than what was read.
    */
  private def loadState[F[_]: Concurrent](command: C)(using
    eventStore: EventStore[F, E],
  ): F[(EventFilter, Long, S)] =
    val filter = EventFilter(eventTypes.getOrElse(Set.empty), tags(command))
    eventStore.readFrom(0L, filter).compile.toList.map { envelopes =>
      val state = envelopes.foldLeft(initial)((s, env) => evolve(command, s, env.payload))
      val index = envelopes.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
      (filter, index, state)
    }

  private def pendingEvents(command: C, decided: List[(Set[Tag], E)]): List[PendingEvent[E]] =
    val eventHeaders = headers(command)
    decided.map((tags, event) =>
      PendingEvent(payload = event, tags = tags, eventType = EventTypeName.fromInstance(event), isExternal = false,
        id = None, headers = eventHeaders),
    )

  private def attempt[F[_]: Concurrent](
    command: C,
  )(using eventStore: EventStore[F, E]): F[List[E]] =
    loadState(command).flatMap { case (filter, index, state) =>
      validate(state, command) match
        case Left(error) => Concurrent[F].raiseError(error)
        case Right(_)    => eventStore.append(filter, index, pendingEvents(command, decide(state, command)))
    }

  private def attemptWithMessages[F[_]: Concurrent](
    command: C,
  )(using eventStore: EventStore[F, E] & TransactionalMessages[F, E]): F[Either[Throwable, List[E]]] =
    loadState(command).flatMap { case (filter, index, state) =>
      validate(state, command) match
        case Left(rejection) =>
          // No events, so there is no local invariant to protect and `appendWithMessages` skips the conflict check —
          // the rejection's message is enqueued on its own.
          messages(state, command, Left(rejection)) match
            case Left(error)     => Concurrent[F].raiseError(error)
            case Right(outgoing) => eventStore.appendWithMessages(filter, index, outgoing).as(Left(rejection))
        case Right(_) =>
          val decided = decide(state, command)
          messages(state, command, Right(decided.map(_._2))) match
            case Left(error)     => Concurrent[F].raiseError(error)
            case Right(outgoing) =>
              eventStore
                .appendWithMessages(filter, index, outgoing, pendingEvents(command, decided))
                .map(Right(_))
    }
