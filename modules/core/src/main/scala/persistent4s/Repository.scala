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

/** A Repository defines how a Projection fetches and persists its state. The projector will call `fetchStates` to get
  * the current state for relevant keys before processing a batch of events, and will call `upsertMany` and `deleteMany`
  * after processing a batch to save the updated state. The implementation of the repository is up to the user and can
  * use any durable storage mechanism, such as a database or a key-value store. The only requirement is that it provides
  * the specified methods for fetching and persisting state.
  *
  * @tparam F
  *   the effect type, such as IO
  * @tparam K
  *   the key type for fetching and persisting state
  * @tparam S
  *   the state type for the projection
  */
trait Repository[F[_], K, S]:

  /** Fetch the current state for a list of keys. The result is a map from key to an optional value, where None
    * indicates that no value exists for the key.
    *
    * @param keys
    *   the list of keys to fetch
    * @return
    *   a map from key to an optional value, where None indicates that no value exists for the key
    */
  def findMany(keys: List[K]): F[Map[K, Option[S]]]

  /** Persist a batch of updated states. The input is a map from key to value, where each entry represents the new state
    * for that key. The implementation should upsert these values in durable storage so that they can be retrieved later
    * by `fetchStates`.
    *
    * @param states
    *   a map from key to value, where each entry represents the new state for that key
    * @return
    *   an effect representing the completion of the operation
    */
  def upsertMany(states: Map[K, S]): F[Unit]

  /** Delete a batch of keys and their associated state. The implementation should remove these keys from durable
    * storage so that they are no longer returned by `fetchStates`.
    *
    * @param keys
    *   the list of keys to delete
    * @return
    *   an effect representing the completion of the operation
    */
  def deleteMany(keys: List[K]): F[Unit]
