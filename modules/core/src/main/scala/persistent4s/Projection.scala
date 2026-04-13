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

/** A Projection defines how to process events from the event store. */
trait Projection[F[_], A <: Event]:

  /** The name of the projection, used for checkpointing. Each projection should have a unique name to avoid conflicts
    * with other projections.
    *
    * @return
    *   the name of the projection
    */
  def name: String

  /** The filter that determines which events this projection will process. Only events that match the filter will be
    * passed to the `handle` method.
    *
    * @return
    *   the event filter for this projection
    */
  def filter: EventFilter

  /** Handle an incoming event. This method will be called for each event that matches the projection's filter. The
    * projection should perform any necessary processing of the event, such as updating a read model.
    *
    * Important note: If the handler is not idempotent, it may be called multiple times for the same event in case of
    * retries or failures. Therefore, it's crucial to ensure that the handler can safely handle duplicate events without
    * causing inconsistent state or side effects.
    *
    * @param event
    *   the event to handle
    */
  def handle(event: EventEnvelope[A]): F[Unit]
