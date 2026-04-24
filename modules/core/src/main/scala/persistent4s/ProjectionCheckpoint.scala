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

/** A trait for managing projection checkpoints. A checkpoint is a record of the last processed event's global position
  * for a given projection. This allows a projector to resume processing from the correct position after a restart or
  * failure, ensuring that events are not missed or processed multiple times. The checkpoint should be stored in a
  * durable storage to survive application restarts.
  */
trait ProjectionCheckpoint[F[_]]:

  /** Load the last checkpoint for the specified projection.
    *
    * Returns the global position of the last successfully processed event, so the projector can resume from that point
    * rather than reprocessing the entire event store. Returning `None` signals that no checkpoint exists yet, which
    * causes the projector to start from the very beginning of the event store.
    *
    * @param projectionName
    *   the name of the projection to load the checkpoint for
    * @return
    *   the global position of the last processed event, or `None` to start from the beginning
    */
  def load(projectionName: String): F[Option[Long]]

  /** Save the checkpoint for the specified projection. This should update the global position of the last processed
    * event.
    *
    * @param projectionName
    *   the name of the projection to save the checkpoint for
    * @param globalPosition
    *   the global position of the last processed event
    */
  def save(projectionName: String, globalPosition: Long): F[Unit]
