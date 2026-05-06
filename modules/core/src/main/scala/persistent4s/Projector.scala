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

import fs2.Stream
import cats.effect.Async
import org.typelevel.otel4s.trace.Tracer
import org.typelevel.otel4s.metrics.Meter

/** A Projector is responsible for running a Projection. It manages the lifecycle of the projection, including loading
  * the last checkpoint, subscribing to event notifications, and ensuring that events are processed in the correct
  * order. The projector should handle any necessary error handling and retries to ensure that the projection processes
  * events reliably.
  *
  * Delivery semantics are projector-dependent. The default implementation processes events sequentially within each
  * batch while keeping the intermediate state in memory. When a batch completes successfully, it persists the updated
  * states and advances the checkpoint once for the last processed event. If handling an event fails, it persists the
  * successfully computed state up to the previous event and saves the checkpoint for that last fully processed event
  * before failing the stream. Unless state persistence and checkpoint persistence are committed atomically, processing
  * remains at-least-once and projection persistence should therefore be idempotent.
  *
  * @tparam F
  *   the effect type, such as IO
  * @tparam A
  *   the event type, which must extend the Event trait
  * @tparam K
  *   the key type for fetching and persisting state
  */
trait Projector[F[_], A <: Event]:

  /** Run the given projection. This should start the projection and keep it running, processing events as they come in.
    * The returned stream may replay already processed events after a failure unless the projection state updates and
    * checkpoint updates are persisted atomically. The default projector only checkpoints fully processed events.
    *
    * @param projection
    *   the projection to run
    */
  def run[K, S](projection: Projection[F, A, K, S]): Stream[F, Unit]

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
  def apply[F[_]: Async: Tracer: Meter, A <: Event](
    eventStore: EventStore[F, A] & EventNotification[F],
    checkpoint: ProjectionCheckpoint[F],
    batchSize: Int = 100,
  ): Projector[F, A] =
    DefaultProjector(eventStore, checkpoint, batchSize)
