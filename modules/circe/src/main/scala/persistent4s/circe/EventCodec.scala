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

package persistent4s.circe

import io.circe.{Json, parser}

import persistent4s.{Event, EventCodec, EventTypeName}

/** Circe-based implementation of EventCodec. Users provide circe Encoder/Decoder instances for their event types, and
  * this bridges them to the core EventCodec interface.
  */
object CirceEventCodec:

  /** Create an EventCodec from circe Encoder and Decoder instances.
    *
    * @param encodeEvent
    *   a function that encodes an event to a circe Json value
    * @param decodeEvent
    *   a function that decodes an event from its type name and a circe Json value
    */
  def make[A <: Event](
    encodeEvent: A => Json,
    decodeEvent: (EventTypeName, Json) => Either[Throwable, A],
  ): EventCodec[A] =
    new EventCodec[A]:
      def encode(event: A): String =
        encodeEvent(event).noSpaces

      def decode(eventType: EventTypeName, payload: String): Either[Throwable, A] =
        parser.parse(payload).left.map(e => e: Throwable).flatMap(json => decodeEvent(eventType, json))
