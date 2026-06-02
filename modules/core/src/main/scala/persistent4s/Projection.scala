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

import cats.Applicative
import cats.syntax.all.*

/** A Projection defines how to process events from the event store.
  *
  * @tparam F
  *   the effect type, such as IO
  * @tparam A
  *   the event type, which must extend the Event trait
  * @tparam K
  *   the key type for fetching and persisting state
  * @tparam S
  *   the state type for the projection
  */
trait Projection[F[_]: Applicative, A <: Event, K, S]:

  /** Storage backend for this projection's read-model state. */
  protected val repository: Repository[F, K, S]

  /** The name of the projection, used for checkpointing.
    *
    * ⚠️ **STABLE IDENTIFIER** — Changing this name will orphan the checkpoint stored for this projection. Once set in
    * production, changing it requires manual migration of the checkpoint record.
    *
    * Each projection should have a unique name to avoid conflicts with other projections.
    *
    * @return
    *   the name of the projection
    */
  def name: String

  /** The event type filter for this projection. Only events whose type name is included in this set will be processed
    * by the projection.
    *
    * @return
    *   the set of event type names that this projection is interested in
    */
  def filter: Set[EventTypeName]

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
  def handle(state: Option[S], event: EventEnvelope[A]): F[Option[S]]

  /** Provided by the framework. Fetches current state for the given keys via [[repository]]. */
  final def fetchStates(keys: List[K]): F[Map[K, Option[S]]] =
    repository.findMany(keys)

  /** Provided by the framework. Persists a batch of state updates via [[repository]]: `None` values trigger deletion,
    * `Some` values trigger upsert.
    */
  final def persistStates(states: Map[K, Option[S]]): F[Unit] =
    val toDelete = states.collect { case (key, None) => key }.toList
    val toUpsert = states.collect { case (key, Some(state)) => key -> state }.toMap
    val deleteF = if (toDelete.nonEmpty) repository.deleteMany(toDelete) else Applicative[F].unit
    val upsertF = if (toUpsert.nonEmpty) repository.upsertMany(toUpsert) else Applicative[F].unit
    deleteF *> upsertF
