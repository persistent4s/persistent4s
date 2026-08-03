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
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

/** Wraps a [[CommandHandler]] so that running a command also waits for a specific projection to catch up before
  * returning, instead of returning as soon as the events are appended.
  *
  * @tparam C
  *   the command type
  * @tparam S
  *   the command handler's state
  * @tparam E
  *   the event type
  * @tparam K
  *   the projection's key type
  * @tparam PS
  *   the projection's state type
  * @param handler
  *   the underlying command handler
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
final case class SyncCommandHandler[F[_], C, S, E <: Event, K, PS](
  handler: CommandHandler[C, S, E],
  topic: Topic[F, (UUID, Either[Throwable, Map[K, Option[PS]]])],
  filter: Set[EventTypeName],
  timeout: FiniteDuration,
  maxQueued: Int = 256,
):

  /** Run the command, then wait until the projection has processed and persisted every event it produced. Fails with
    * the projection's own error if any of the events fail to process, or with a timeout error if the projection doesn't
    * catch up in time. The events are appended either way - this only gates the return, it cannot undo the write.
    */
  def runSync(command: C)(using eventStore: EventStore[F, E])(using F: Temporal[F]): F[Map[K, Option[PS]]] =

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
        envelopes <- handler.run(command)
        ids        = envelopes.filter(e => filter.contains(e.metadata.eventType)).map(_.metadata.id).toSet
        result    <-
          if ids.isEmpty then F.pure(Map.empty[K, Option[PS]])
          else F.timeout(awaitAll(stream, ids), timeout)
      yield result
    }

  def run(command: C)(using eventStore: EventStore[F, E])(using F: Concurrent[F]): F[List[EventEnvelope[E]]] =
    handler.run(command)
