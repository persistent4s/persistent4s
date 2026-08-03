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

  /** Serialize an event to a String representation. */
  def encode(event: A): String

  /** Resolve the durable event identifier written alongside an encoded payload. Legacy codecs retain class-name-based
    * identity; schema-aware codecs override this with the registered [[EventSchema]].
    */
  def eventType(event: A): EventTypeName =
    EventTypeName.fromInstance(event)

  /** Resolve the schema version written alongside an encoded payload. */
  def eventVersion(event: A): Int = 1

  /** Encode a payload and its storage metadata in one operation. */
  final def encodeWithSchema(event: A): EncodedEvent =
    EncodedEvent(eventType(event), eventVersion(event), encode(event))

  /** Deserialize an event from its type name and String representation. */
  def decode(eventType: EventTypeName, payload: String): Either[Throwable, A]

  /** Deserialize a versioned event. Existing codecs remain source-compatible and support their legacy version `1`;
    * schema-aware codecs override this method to evolve older payloads.
    */
  def decode(eventType: EventTypeName, version: Int, payload: String): Either[Throwable, A] =
    if version == 1 then decode(eventType, payload)
    else Left(UnsupportedEventVersion(eventType, version))
