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

/** A Projection defines how to process events from the event store.
  *
  * @tparam F
  *   the effect type, such as IO
  * @tparam A
  *   the event type, which must extend the Event trait
  * @tparam K
  *   the key type for fetching and persisting state
  */
trait Projection[F[_], A <: Event, K]:

  type State

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

  /** Resolve keys for a given event. This method is used to determine which keys are affected by an event, and
    * therefore which state entries need to be fetched and updated. An event may affect multiple keys.
    *
    * Returning an empty list means the event is intentionally ignored by this projection: no state is fetched or
    * persisted, but the event is still acknowledged and the checkpoint advances past it.
    *
    * @param event
    *   the event for which to resolve the keys
    * @return
    *   the keys affected by this event, or an empty list to skip the event without blocking the checkpoint
    */
  def resolveKeys(event: EventEnvelope[A]): List[K]

  /** Fetch the current state for multiple keys. This method will be called before processing a batch of events to get
    * the current state for all relevant keys.
    *
    * @param keys
    *   the keys for which to fetch the state
    * @return
    *   a map of keys to their corresponding state, or `None` if no state exists for a key
    */
  def fetchStates(keys: List[K]): F[Map[K, Option[State]]]

  /** Handle an incoming event. This method will be called for each event that matches the projection's filter. The
    * projection should perform any necessary processing of the event, such as updating a read model.
    *
    * Important note: If the handler is not idempotent, it may be called multiple times for the same event in case of
    * retries or failures. Therefore, it's crucial to ensure that the handler can safely handle duplicate events without
    * causing inconsistent state or side effects.
    *
    * Returning `Some(state)` causes the projector to call `persist(key, Some(state))` for that key after the batch
    * completes. Returning `None` causes the projector to call `persist(key, None)`, signalling that the state for that
    * key should be deleted. The `persist` implementation is responsible for carrying out the actual deletion.
    *
    * @param state
    *   the current state of the projection before processing the event, or `None` if no state exists yet for the key
    * @param event
    *   the event to handle
    * @return
    *   the updated state, or `None` to request deletion of the state for this key
    */
  def handle(state: Option[State], event: EventEnvelope[A]): F[Option[State]]

  /** Persist the current state for a given key. This method will be called after processing an event to save the
    * updated state. The implementation can choose how to store the state, such as using a database or an in-memory
    * cache. Passing None indicates that the state should be removed for that key.
    *
    * @param key
    *   the key for which to persist the state
    * @param state
    *   the state to persist, if it exists
    * @return
    *   a F[Unit] that completes when the state has been persisted
    */
  def persist(key: K, state: Option[State]): F[Unit]
