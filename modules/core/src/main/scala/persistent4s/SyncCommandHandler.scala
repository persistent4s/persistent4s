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

import cats.effect.Temporal
import cats.effect.Concurrent
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Topic
import org.typelevel.otel4s.trace.Tracer
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

/** Wraps a command execution so that running a command also waits for a specific projection to catch up before
  * returning, instead of returning as soon as the events are appended.
  *
  * The execution is supplied as a function rather than a concrete handler so that this works for both
  * [[CommandHandler]] and [[EventSourcedCommandHandler]] - see the factories in the companion. All it needs is the ids
  * of the appended events, which both paths return via [[EventEnvelope]].
  *
  * @tparam C
  *   the command type
  * @tparam E
  *   the event type
  * @tparam K
  *   the projection's key type
  * @tparam PS
  *   the projection's state type
  * @param execute
  *   runs the command and yields the appended events; any command rejection is already an error in `F`
  * @param topic
  *   the topic a [[Projector]] publishes to after persisting - see [[Projector.run]]
  * @param filter
  *   the target projection's event type filter (see Projection.filter): events the command produces outside this set
  *   are not waited on, since the projection would never publish for them
  * @param timeout
  *   how long to wait for the projection to catch up before failing
  * @param maxQueued
  *   the subscriber queue size passed to [[fs2.concurrent.Topic.subscribeAwait]]
  */
final case class SyncCommandHandler[F[_], C, E <: Event, K, PS](
  execute: C => F[List[EventEnvelope[E]]],
  topic: Topic[F, (UUID, Either[Throwable, Map[K, Option[PS]]])],
  filter: Set[EventTypeName],
  timeout: FiniteDuration,
  maxQueued: Int = 256,
):

  /** Run the command, then wait until the projection has processed and persisted every event it produced. Fails with
    * the projection's own error if any of the events fail to process, or with a timeout error if the projection doesn't
    * catch up in time. The events are appended either way - this only gates the return, it cannot undo the write.
    */
  def runSync(command: C)(using F: Temporal[F]): F[Map[K, Option[PS]]] =

    def awaitAll(
      stream: Stream[F, (UUID, Either[Throwable, Map[K, Option[PS]]])],
      ids: Set[UUID],
    ): F[Map[K, Option[PS]]] =
      stream.collect { case (id, result) if ids.contains(id) => (id, result) }.evalMap { case (id, result) =>
        result.liftTo[F].map(id -> _)
      }
        .scan((ids, Map.empty[K, Option[PS]])) { case ((remaining, acc), (id, payload)) =>
          (remaining - id, acc ++ payload)
        }
        .find { case (remaining, _) => remaining.isEmpty }
        .map(_._2)
        .compile
        .lastOrError

    topic.subscribeAwait(maxQueued).use { stream =>
      for
        envelopes <- execute(command)
        ids        = envelopes.filter(e => filter.contains(e.metadata.eventType)).map(_.metadata.id).toSet
        result    <-
          if ids.isEmpty then F.pure(Map.empty[K, Option[PS]])
          else F.timeout(awaitAll(stream, ids), timeout)
      yield result
    }

  /** Run the command without waiting for the projection. */
  def run(command: C): F[List[EventEnvelope[E]]] =
    execute(command)

object SyncCommandHandler:

  /** Wait on a projection for a [[CommandHandler]]. */
  def fromCommandHandler[F[_]: Concurrent: Tracer, C, S, E <: Event, K, PS](
    handler: CommandHandler[C, S, E],
    eventStore: EventStore[F, E],
    topic: Topic[F, (UUID, Either[Throwable, Map[K, Option[PS]]])],
    filter: Set[EventTypeName],
    timeout: FiniteDuration,
    maxQueued: Int = 256,
  )(using metrics: CommandHandlerMetrics[F]): SyncCommandHandler[F, C, E, K, PS] =
    SyncCommandHandler(
      // `run`'s context bounds and its using clause are one list, so all four are supplied together
      command => handler.run(command)(using summon[Concurrent[F]], summon[Tracer[F]], eventStore, metrics),
      topic,
      filter,
      timeout,
      maxQueued,
    )

  /** Wait on a projection for an [[EventSourcedCommandHandler]], translating its typed rejection into an error. */
  def fromEventSourced[F[_]: Concurrent, C, S, E <: Event, R, K, PS](
    handler: EventSourcedCommandHandler[C, S, E, R],
    runtime: CommandRuntime[F, E],
    mapRejection: R => Throwable,
    topic: Topic[F, (UUID, Either[Throwable, Map[K, Option[PS]]])],
    filter: Set[EventTypeName],
    timeout: FiniteDuration,
    maxQueued: Int = 256,
  ): SyncCommandHandler[F, C, E, K, PS] =
    SyncCommandHandler(
      command => handler.runOrRaise(command)(mapRejection)(using summon[Concurrent[F]], runtime),
      topic,
      filter,
      timeout,
      maxQueued,
    )
