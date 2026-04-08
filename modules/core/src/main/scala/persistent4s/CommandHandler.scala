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
trait CommandHandler[C, S, E]:

  /** Which tags to read from the event store for this command. */
  def tags(command: C): Set[Tag]

  /** The event types that this handler is interested in for building the state. If not specified, all events with the
    * relevant tags will be included.
    */
  def eventTypes: Option[Set[String]] = None

  /** The initial state before any events have been applied. */
  def initial: S

  /** Fold a single event into the current state. */
  def evolve(command: C, state: S, event: E): S

  /** Validate the command against the current state. Should raise an error if the command is invalid. */
  def validate[F[_]: Concurrent](state: S, command: C): F[Unit]

  /** Produce the events that result from applying the command, each with its own set of tags. Only called after
    * validation passes.
    */
  def decide(state: S, command: C): List[(Set[Tag], E)]

  /** Execute a command using this handler and the given event store. */
  def run[F[_]: Concurrent](command: C)(using eventStore: EventStore[F, E]): F[Unit] =
    for
      tags      <- Concurrent[F].pure(tags(command))
      envelopes <- eventStore.read(eventTypes.getOrElse(Set.empty).toList, tags).compile.toList
      state      = envelopes.foldLeft(initial)((s, env) => evolve(command, s, env.payload))
      index      = envelopes.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
      _         <- validate(state, command)
      decided    = decide(state, command)
      events     = decided.map((tags, event) => (tags, event.getClass.getSimpleName, event))
      _         <- eventStore.append(index, events)
    yield ()
