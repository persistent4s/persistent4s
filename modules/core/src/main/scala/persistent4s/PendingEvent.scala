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

import java.util.UUID

/** A PendingEvent is an event that is about to be appended to the event store, together with the author-supplied
  * context the store needs to persist it. Unlike [[EventMetadata]], it carries no store-assigned fields
  * (globalPosition, timestamp) — those are assigned at commit time.
  *
  * @param payload
  *   the actual event data
  * @param tags
  *   the tags associated with the event
  * @param eventType
  *   the type of the event
  * @param isExternal
  *   whether the event comes from an external domain
  * @param id
  *   an optional caller-supplied UUID
  * @param headers
  *   arbitrary author-supplied key-value metadata
  */
final case class PendingEvent[A <: Event](
  payload: A,
  tags: Set[Tag],
  eventType: EventTypeName,
  isExternal: Boolean,
  id: Option[UUID] = None,
  headers: Map[String, String] = Map.empty,
)
