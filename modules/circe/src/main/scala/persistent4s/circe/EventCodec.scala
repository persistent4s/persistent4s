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

import scala.compiletime.{erasedValue, summonFrom, summonInline}
import scala.deriving.Mirror
import scala.reflect.ClassTag

import io.circe.{Decoder, Encoder, Json, parser}

import persistent4s.{Event, EventCodec, EventSchema, EventTypeName}

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
      def encode(event: A): Either[Throwable, String] =
        scala.util.Try(encodeEvent(event).noSpaces).toEither

      def decode(eventType: EventTypeName, payload: String): Either[Throwable, A] =
        parser.parse(payload).left.map(e => e: Throwable).flatMap(json => decodeEvent(eventType, json))

  /** Auto-derive an EventCodec for a sealed hierarchy rooted at `A`. Each concrete leaf case class must have circe
    * `Encoder` and `Decoder` instances in scope (typically via `derives Encoder, Decoder`). Intermediate sealed
    * traits/enums are supported and traversed recursively.
    *
    * A leaf may provide an [[EventSchema]] and [[JsonEventUpcaster]] in its companion to use a stable identifier and
    * evolve older payload versions. Leaves without a schema retain class-name-based version-1 behavior.
    */
  inline def derived[A <: Event](using m: Mirror.SumOf[A]): EventCodec[A] =
    val leaves: List[Leaf[A]] = collectLeaves[A, m.MirroredElemTypes]
    fromLeaves(leaves)

  /** Build an [[EventCodec]] for an open root event type from independently sealed event families. This allows the
    * families and their concrete events to live in separate source files while retaining automatic codec derivation
    * within each family.
    *
    * Example:
    *
    * {{ CirceEventCodec .builder[DomainEvent] .add[OrderEvent] .add[PaymentEvent] .build }}
    */
  def builder[A <: Event]: Builder[A] =
    new Builder[A](Nil)

  final class Builder[A <: Event] private[CirceEventCodec] (private val leaves: List[Leaf[A]]):

    /** Add every concrete event below the sealed event family `B`. Intermediate sealed traits are traversed
      * recursively.
      */
    inline def add[B <: A](using m: Mirror.SumOf[B]): Builder[A] =
      appended(collectLeaves[A, m.MirroredElemTypes])

    /** Create the codec from all registered event families. */
    def build: EventCodec[A] =
      fromLeaves(leaves)

    private def appended(additionalLeaves: List[Leaf[A]]): Builder[A] =
      new Builder[A](leaves ++ additionalLeaves)

  private def fromLeaves[A <: Event](leaves: List[Leaf[A]]): EventCodec[A] =
    val registrations = leaves.flatMap(leaf => leaf.acceptedEventTypes.toList.map(_ -> leaf))
    val duplicateEventTypes =
      registrations
        .groupBy(_._1)
        .collect { case (eventType, _ :: _ :: _) => eventType.value }
        .toList
        .sorted
    require(
      duplicateEventTypes.isEmpty,
      s"Duplicate event schema identifiers: ${duplicateEventTypes.mkString(", ")}",
    )

    val byEventType = registrations.toMap

    def leafFor(event: A): Leaf[A] =
      leaves.find(_.accepts(event)).getOrElse {
        throw new IllegalStateException(s"No circe Encoder derived for event class '${event.getClass.getName}'")
      }

    def parsePayload[B](payload: String)(f: Json => Either[Throwable, B]): Either[Throwable, B] =
      parser.parse(payload).left.map(error => error: Throwable).flatMap(f)

    def registered(eventType: EventTypeName): Either[Throwable, Leaf[A]] =
      byEventType
        .get(eventType)
        .toRight(new RuntimeException(s"Unknown event type: ${eventType.value}"))

    new EventCodec[A]:
      // `leafFor` throws when no encoder was derived for the event's concrete class; surfacing that as a `Left` keeps
      // the failure on the codec's declared error channel, as in `make` above.
      def encode(event: A): Either[Throwable, String] =
        scala.util.Try(leafFor(event).encoder(event).noSpaces).toEither

      override def eventType(event: A): EventTypeName =
        leafFor(event).eventType

      override def eventVersion(event: A): Int =
        leafFor(event).version

      def decode(eventType: EventTypeName, payload: String): Either[Throwable, A] =
        registered(eventType).flatMap(leaf => parsePayload(payload)(leaf.decodeCurrent))

      override def decode(eventType: EventTypeName, version: Int, payload: String): Either[Throwable, A] =
        registered(eventType).flatMap(leaf => parsePayload(payload)(leaf.decodeVersion(eventType, version, _)))

  final private case class Leaf[A](
    eventType: EventTypeName,
    version: Int,
    acceptedEventTypes: Set[EventTypeName],
    accepts: A => Boolean,
    encoder: Encoder[A],
    decoder: Decoder[A],
    upcaster: Option[JsonEventUpcaster[?]],
  ):

    def decodeCurrent(json: Json): Either[Throwable, A] =
      decoder.decodeJson(json).left.map(error => error: Throwable)

    def decodeVersion(storedEventType: EventTypeName, storedVersion: Int, json: Json): Either[Throwable, A] =
      val evolved =
        if storedVersion == version then Right(json)
        else
          upcaster match
            case Some(value) => value.upcast(storedEventType, storedVersion, version, json)
            case None        =>
              if storedVersion < 1 then Left(InvalidEventVersion(storedEventType, storedVersion))
              else if storedVersion > version then Left(FutureEventVersion(storedEventType, storedVersion, version))
              else Left(MissingEventUpcast(storedEventType, storedVersion, version))

      evolved.flatMap(decodeCurrent)

  private inline def collectLeaves[A, Elems <: Tuple]: List[Leaf[A]] =
    inline erasedValue[Elems] match
      case _: EmptyTuple => Nil
      case _: (t *: ts)  =>
        val rest = collectLeaves[A, ts]
        summonFrom {
          case childMirror: Mirror.SumOf[`t`] =>
            collectLeaves[A, childMirror.MirroredElemTypes] ++ rest
          case _ =>
            val classTag = summonInline[ClassTag[t]]
            val schema = schemaFor[t](using classTag)
            val upcaster = upcasterFor[t]
            val enc = summonInline[Encoder[t]].asInstanceOf[Encoder[A]]
            val dec = summonInline[Decoder[t]].asInstanceOf[Decoder[A]]
            Leaf(
              schema.eventType,
              schema.version,
              schema.acceptedEventTypes,
              event => classTag.unapply(event).isDefined,
              enc,
              dec,
              upcaster,
            ) :: rest
        }

  private inline def schemaFor[E](using classTag: ClassTag[E]): EventSchema[E] =
    summonFrom {
      case stable: EventSchema[E] => stable
      case _                      => EventSchema.legacy[E]
    }

  private inline def upcasterFor[E]: Option[JsonEventUpcaster[E]] =
    summonFrom {
      case value: JsonEventUpcaster[E] => Some(value)
      case _                           => None
    }
