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

package persistent4s.circe

import java.util.concurrent.ConcurrentHashMap

import io.circe.{Decoder, Encoder, Json, parser}

import persistent4s.EventCodec

object CirceEventCodec:

  /** Create an EventCodec that automatically discovers Encoder/Decoder instances from companion objects via reflection.
    * Events must use `derives Encoder, Decoder` for this to work.
    *
    * The fully qualified class name is embedded in the JSON payload as a `_type` field, so that decoding can find the
    * correct class without relying on the event_type column. This means the event_type stored in the database can use
    * simple names (e.g. "StudentCreated") while the codec handles class resolution internally.
    */
  def auto[A]: EventCodec[A] =
    new EventCodec[A]:
      private val encoderCache = new ConcurrentHashMap[String, Encoder[A]]()
      private val decoderCache = new ConcurrentHashMap[String, Decoder[A]]()

      def encode(event: A): String =
        val fqcn = event.getClass.getName
        val encoder = encoderCache.computeIfAbsent(
          fqcn,
          _ => findEncoder[A](event.getClass),
        )
        encoder(event).deepMerge(Json.obj("_type" -> Json.fromString(fqcn))).noSpaces

      def decode(payload: String): Either[Throwable, A] =
        for
          json    <- parser.parse(payload).left.map(e => e: Throwable)
          fqcn    <- json.hcursor.get[String]("_type").left.map(e => e: Throwable)
          decoder  = decoderCache.computeIfAbsent(fqcn, _ => findDecoder[A](Class.forName(fqcn)))
          event   <- decoder.decodeJson(json).left.map(e => e: Throwable)
        yield event

  private def findEncoder[A](clazz: Class[?]): Encoder[A] =
    val companion = getCompanion(clazz)
    companion.getClass.getMethods
      .find(m => m.getParameterCount == 0 && classOf[Encoder[?]].isAssignableFrom(m.getReturnType))
      .map(_.invoke(companion).asInstanceOf[Encoder[A]])
      .getOrElse(throw new RuntimeException(s"No Encoder found for ${clazz.getName}. Does it derive Encoder?"))

  private def findDecoder[A](clazz: Class[?]): Decoder[A] =
    val companion = getCompanion(clazz)
    companion.getClass.getMethods
      .find(m => m.getParameterCount == 0 && classOf[Decoder[?]].isAssignableFrom(m.getReturnType))
      .map(_.invoke(companion).asInstanceOf[Decoder[A]])
      .getOrElse(throw new RuntimeException(s"No Decoder found for ${clazz.getName}. Does it derive Decoder?"))

  private def getCompanion(clazz: Class[?]): Any =
    val companionClass = Class.forName(clazz.getName + "$")
    companionClass.getField("MODULE$").get(null)
