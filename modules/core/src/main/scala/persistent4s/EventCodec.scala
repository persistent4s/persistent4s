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

/** A codec for serializing and deserializing events. The serialization format is left to the implementation (e.g. JSON
  * via Circe, binary, etc.).
  *
  * @tparam A
  *   the event type
  */
trait EventCodec[A <: Event]:

  /** Serialize an event to a String representation. Returns `Left` if encoding fails for any reason (e.g. the codec
    * has no encoder registered for the event's concrete type, or the underlying serializer rejects the value).
    */
  def encode(event: A): Either[Throwable, String]

  /** Deserialize an event from its type name and String representation. */
  def decode(eventType: EventTypeName, payload: String): Either[Throwable, A]
