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

/** An EventEnvelope is a wrapper around an event that includes both the event's metadata and its payload.
  *
  * @param metadata
  *   the metadata associated with the event
  * @param payload
  *   the actual event data
  */
final case class EventEnvelope[A](
  metadata: EventMetadata,
  payload: A,
)
