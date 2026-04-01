/*
 * Copyright 2026 persistent4s
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

/** A filter for selecting which events a projection should process. A projection will only receive events that match
  * the specified event types and tags.
  *
  * @param eventTypes
  *   a set of event type names to include. If empty, all event types are included.
  * @param tags
  *   a set of tags to include. If empty, all tags are included. An event matches the filter if it has at least one of
  *   the specified tags.
  *
  * @param eventTypes
  * @param tags
  */
final case class EventFilter(
  eventTypes: Set[String] = Set.empty,
  tags: Set[Tag] = Set.empty,
)
