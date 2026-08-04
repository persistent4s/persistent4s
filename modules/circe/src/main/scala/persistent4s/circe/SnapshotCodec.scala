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

import io.circe.{Decoder, Encoder, parser}

import persistent4s.SnapshotCodec

/** Circe-backed command snapshot codecs. Import `persistent4s.circe.given` to derive one from a state's existing
  * Encoder and Decoder.
  */
object SnapshotCodec:

  def circe[S: Encoder: Decoder]: persistent4s.SnapshotCodec[S] =
    new persistent4s.SnapshotCodec[S]:
      override def encode(state: S): String =
        Encoder[S].apply(state).noSpaces

      override def decode(payload: String): Either[Throwable, S] =
        parser.decode[S](payload).left.map(error => error: Throwable)

given circeSnapshotCodec[S: Encoder: Decoder]: SnapshotCodec[S] =
  persistent4s.circe.SnapshotCodec.circe[S]
