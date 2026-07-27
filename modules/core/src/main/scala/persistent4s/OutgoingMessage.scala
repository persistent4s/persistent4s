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

/** A self-routing message to be published to an external broker via a [[MessageOutbox]], independent of the domain
  * event log.
  *
  * @param topic
  *   destination topic
  * @param key
  *   partition key, driving per-key ordering; `None` lets the broker assign a partition
  * @param payload
  *   already-serialized message body
  * @param headers
  *   optional string headers propagated to the broker record
  */
final case class OutgoingMessage(
  topic: String,
  key: Option[String],
  payload: String,
  headers: Map[String, String] = Map.empty,
)
