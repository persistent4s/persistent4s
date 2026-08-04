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

import scala.concurrent.duration.*
import fs2.Stream
import fs2.concurrent.Topic
import cats.effect.Async
import org.typelevel.log4cats.Logger
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.otel4s.metrics.Meter
import java.util.UUID

/** Drives a [[Projection]] by reading events from an [[EventStore]], tracking progress via a [[ProjectionCheckpoint]],
  * and reacting to [[EventNotification]]s. Delivery semantics (batching, retry, checkpoint frequency) are determined by
  * the implementation.
  */
trait Projector[F[_], A <: Event]:

  /** Run the projection as an infinite stream. The stream only terminates on an unrecoverable error; the caller is
    * responsible for restart logic.
    *
    * '''Single runner:''' a projection shares one checkpoint, so running this on multiple instances concurrently races
    * the checkpoint and double-applies events. Wrap it in a [[LeaderElection]] so only one instance drives a given
    * projection at a time.
    *
    * @param projection
    *   the projection to run
    * @param topic
    *   an optional topic to publish the projection state after a batch is processed. Used by [[SyncCommandHandler]] to
    *   wait for the projection to catch up before returning from a command.
    */
  def run[K, S](
    projection: Projection[F, A, K, S],
    topic: Option[Topic[F, (UUID, Either[Throwable, Map[K, Option[S]]])]] = None,
  ): Stream[F, Unit]

object Projector:

  /** Create a [[DefaultProjector]] with the given event store and checkpoint.
    *
    * @param eventStore
    *   the event store to read from, which must also implement [[EventNotification]]
    * @param checkpoint
    *   durable storage for the last processed position per projection
    * @param batchSize
    *   maximum number of events processed in a single batch (default: 100). A larger value reduces checkpoint overhead
    *   but increases memory usage and the reprocessing window after a failure.
    */
  def apply[F[_]: Async: Logger: Tracer: Meter, A <: Event](
    eventStore: EventStore[F, A] & EventNotification[F],
    checkpoint: ProjectionCheckpoint[F],
    batchSize: Int = 100,
    publishTimeout: FiniteDuration = 1.second,
  ): Projector[F, A] =
    DefaultProjector(eventStore, checkpoint, batchSize, publishTimeout = publishTimeout)
