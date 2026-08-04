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

import scala.util.Try

import io.circe.{Decoder, Encoder, parser}

import persistent4s.MessageCodec

object CirceMessageCodec:

  def derived[M](using encoder: Encoder[M], decoder: Decoder[M]): MessageCodec[M] =
    new MessageCodec[M]:
      def encode(message: M): Either[Throwable, String] =
        Try(encoder(message).noSpaces).toEither

      def decode(payload: String): Either[Throwable, M] =
        parser.parse(payload).left.map(e => e: Throwable).flatMap(_.as[M].left.map(e => e: Throwable))
