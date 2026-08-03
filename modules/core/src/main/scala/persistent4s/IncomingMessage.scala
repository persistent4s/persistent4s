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

/** A message received from an external broker - the mirror of [[OutgoingMessage]].
  *
  * @param topic
  *   the topic the message was read from
  * @param key
  *   the partition key the producer set, or `None` for a keyless record
  * @param payload
  *   the raw, still-serialized message body
  * @param headers
  *   the broker record's headers.
  */
final case class IncomingMessage(
  topic: String,
  key: Option[String],
  payload: String,
  headers: Map[String, String] = Map.empty,
):

  /** Decode [[payload]] with `decoder`. Only a [[MessageDecoder]] is asked for: reading someone else's message is no
    * reason to be able to write one, and a service that only consumes a type should not have to supply an encoder for
    * it. A full [[MessageCodec]] satisfies this too.
    */
  def as[M](using decoder: MessageDecoder[M]): Either[Throwable, M] = decoder.decode(payload)
