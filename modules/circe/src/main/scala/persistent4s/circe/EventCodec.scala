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

import scala.compiletime.{constValue, erasedValue, summonFrom, summonInline}
import scala.deriving.Mirror

import io.circe.{Decoder, Encoder, Json, parser}

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

  /** Auto-derive an EventCodec for a sealed hierarchy rooted at `A`. Each concrete leaf case class must have circe
    * `Encoder` and `Decoder` instances in scope (typically via `derives Encoder, Decoder`). Intermediate sealed
    * traits/enums are supported and traversed recursively.
    *
    * Dispatch on the encode side uses `event.getClass.getSimpleName`, which matches [[EventTypeName.fromInstance]].
    */
  inline def derived[A <: Event](using m: Mirror.SumOf[A]): EventCodec[A] =
    val leaves: List[Leaf[A]] = collectLeaves[A, m.MirroredElemTypes, m.MirroredElemLabels]
    val decoders: Map[String, Decoder[A]] = leaves.iterator.map(l => l.name -> l.decoder).toMap
    val encoders: Map[String, Encoder[A]] = leaves.iterator.map(l => l.name -> l.encoder).toMap
    make[A](
      encodeEvent = event =>
        val name = event.getClass.getSimpleName
        encoders.get(name) match
          case Some(e) => e(event)
          case None    => throw new IllegalStateException(s"No circe Encoder derived for event class '$name'"),
      decodeEvent = (eventType, json) =>
        decoders.get(eventType.value) match
          case Some(d) => d.decodeJson(json).left.map(e => e: Throwable)
          case None    => Left(new RuntimeException(s"Unknown event type: ${eventType.value}")),
    )

  final private case class Leaf[A](name: String, encoder: Encoder[A], decoder: Decoder[A])

  private inline def collectLeaves[A, Elems <: Tuple, Labels <: Tuple]: List[Leaf[A]] =
    inline erasedValue[Elems] match
      case _: EmptyTuple => Nil
      case _: (t *: ts)  =>
        inline erasedValue[Labels] match
          case _: (l *: ls) =>
            val rest = collectLeaves[A, ts, ls]
            summonFrom {
              case childMirror: Mirror.SumOf[`t`] =>
                collectLeaves[A, childMirror.MirroredElemTypes, childMirror.MirroredElemLabels] ++ rest
              case _ =>
                val name = constValue[l & String]
                val enc = summonInline[Encoder[t]].asInstanceOf[Encoder[A]]
                val dec = summonInline[Decoder[t]].asInstanceOf[Decoder[A]]
                Leaf(name, enc, dec) :: rest
            }
