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

/** Serializes a message payload for sending.
  *
  * Separate from [[MessageDecoder]] because most places need only one direction, and a service often cannot supply the
  * other honestly: whoever sends a command encodes it and has no cause to read one back, while the receiver is in
  * exactly the opposite position. A payload that genuinely goes both ways has [[MessageCodec]].
  *
  * The encoded form is the wire contract. Where the reader is another service, it has to be what ''that'' service's
  * decoder expects — a sum type encoded with a constructor wrapper, which is what most derivation gives by default,
  * will not be read by a partner whose decoder expects the bare leaf.
  */
trait MessageEncoder[M]:

  def encode(message: M): Either[Throwable, String]

/** Reads a message payload that arrived from elsewhere. The mirror of [[MessageEncoder]]. */
trait MessageDecoder[M]:

  def decode(payload: String): Either[Throwable, M]

/** Both directions, for a payload the same service writes and later reads back — a saga's instance state, for instance.
  * Satisfies either half on its own wherever only one of them is asked for.
  */
trait MessageCodec[M] extends MessageEncoder[M], MessageDecoder[M]
