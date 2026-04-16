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

import java.time.Instant

/** EventMetadata contains metadata about an event, such as its global position in the event store, its tags, its type,
  * and the timestamp of when the event was created. This metadata is used to provide context about the event and can be
  * used for filtering and querying events in the event store.
  *
  * @param globalPosition
  *   the global position of the event in the event store
  * @param tags
  *   the tags associated with the event
  * @param eventType
  *   the type of the event
  * @param timestamp
  *   the timestamp of when the event was created
  */
final case class EventMetadata(
  globalPosition: Long,
  tags: Set[Tag],
  eventType: String,
  timestamp: Instant,
)
