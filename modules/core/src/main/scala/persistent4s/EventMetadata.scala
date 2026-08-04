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
import java.util.UUID

/** Metadata attached to every stored event.
  *
  * @param globalPosition
  *   the event's position in the global sequence
  * @param id
  *   unique identifier for the event, used for idempotent re-ingestion
  * @param tags
  *   the tags associated with the event
  * @param eventType
  *   the type of the event
  * @param isExternal
  *   whether the event originated from an external domain
  * @param timestamp
  *   the instant at which the event was recorded
  * @param headers
  *   arbitrary author-supplied key-value metadata
  * @param eventVersion
  *   the persisted schema version of the event payload
  */
final case class EventMetadata(
  globalPosition: Long,
  id: UUID,
  tags: Set[Tag],
  eventType: EventTypeName,
  isExternal: Boolean,
  timestamp: Instant,
  headers: Map[String, String],
  eventVersion: Int = 1,
)
