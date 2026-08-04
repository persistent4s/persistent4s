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

import io.circe.syntax.*
import io.circe.{Decoder, Encoder, Json}

import persistent4s.{Event, EventSchema, EventTypeName}

import weaver.SimpleIOSuite

object CirceEventCodecSuite extends SimpleIOSuite:

  final case class MyEvent(value: String) extends Event derives Encoder.AsObject, Decoder

  private val codec = CirceEventCodec.make[MyEvent](
    encodeEvent = _.asJson,
    decodeEvent = (_, json) => json.as[MyEvent].left.map(identity),
  )

  private val eventType = EventTypeName.fromString("MyEvent")

  trait OpenEvent extends Event

  sealed trait FirstEventFamily extends OpenEvent

  final case class FirstEvent(value: String) extends FirstEventFamily derives Encoder.AsObject, Decoder

  sealed trait SecondEventFamily extends OpenEvent

  final case class SecondEvent(value: Int) extends SecondEventFamily derives Encoder.AsObject, Decoder

  sealed trait EvolvingEvent extends Event

  final case class NameChanged(value: String) extends EvolvingEvent derives Encoder.AsObject, Decoder

  object NameChanged:

    given EventSchema[NameChanged] =
      EventSchema("library.name-changed", version = 3).withAlias("LegacyNameChanged")

    given JsonEventUpcaster[NameChanged] =
      JsonEventUpcaster
        .builder[NameChanged]
        .step(fromVersion = 1)(json =>
          json.hcursor
            .get[String]("oldValue")
            .map(value => Json.obj("text" -> value.asJson)),
        )
        .step(fromVersion = 2)(json =>
          json.hcursor
            .get[String]("text")
            .map(value => Json.obj("value" -> value.asJson)),
        )
        .build

  final case class MissingEvolution(value: String) extends EvolvingEvent derives Encoder.AsObject, Decoder

  object MissingEvolution:

    given EventSchema[MissingEvolution] = EventSchema("library.missing-evolution", version = 2)

  sealed trait DuplicateSchemaEvent extends Event

  final case class DuplicateSchemaA(value: String) extends DuplicateSchemaEvent derives Encoder.AsObject, Decoder

  object DuplicateSchemaA:

    given EventSchema[DuplicateSchemaA] = EventSchema("library.duplicate", version = 1)

  final case class DuplicateSchemaB(value: String) extends DuplicateSchemaEvent derives Encoder.AsObject, Decoder

  object DuplicateSchemaB:

    given EventSchema[DuplicateSchemaB] = EventSchema("library.duplicate", version = 1)

  private val evolvingCodec = CirceEventCodec.derived[EvolvingEvent]

  private val openHierarchyCodec =
    CirceEventCodec
      .builder[OpenEvent]
      .add[FirstEventFamily]
      .add[SecondEventFamily]
      .build

  pureTest("encode and decode form a round-trip") {
    val event = MyEvent("hello")
    val roundTrip = codec.encode(event).flatMap(s => codec.decode(eventType, s))
    expect(roundTrip == Right(event))
  }

  pureTest("decode returns Left for invalid JSON") {
    expect(codec.decode(eventType, "not valid json").isLeft)
  }

  pureTest("decode returns Left when JSON does not match the event schema") {
    // {"wrong": "field"} is valid JSON but missing the required "value" field
    expect(codec.decode(eventType, """{"wrong": "field"}""").isLeft)
  }

  pureTest("encode produces compact JSON without whitespace") {
    codec.encode(MyEvent("hello")) match
      case Right(s) => expect(!s.contains(" ") && !s.contains("\n"))
      case Left(e)  => failure(s"encode failed: ${e.getMessage}")
  }

  pureTest("encode returns Left when the underlying encoder throws") {
    val brokenCodec = CirceEventCodec.make[MyEvent](
      encodeEvent = _ => throw new RuntimeException("boom"),
      decodeEvent = (_, json) => json.as[MyEvent].left.map(identity),
    )
    brokenCodec.encode(MyEvent("hello")) match
      case Left(e)  => expect(e.getMessage == "boom")
      case Right(_) => failure("expected Left from a throwing encoder")
  }

  pureTest("builder combines independently sealed event families") {
    val events: List[OpenEvent] = List(FirstEvent("hello"), SecondEvent(42))
    expect(
      events.forall { event =>
        openHierarchyCodec
          .encode(event)
          .flatMap(payload => openHierarchyCodec.decode(EventTypeName.fromInstance(event), payload)) == Right(event)
      },
    )
  }

  pureTest("builder preserves decoding errors for a registered event") {
    expect(openHierarchyCodec.decode(EventTypeName.of[FirstEvent], "{}").isLeft)
  }

  pureTest("builder rejects an unregistered event type") {
    expect(openHierarchyCodec.decode(EventTypeName.fromString("UnknownEvent"), "{}").isLeft)
  }

  pureTest("derived codecs encode stable event schema metadata") {
    val event = NameChanged("current")
    val encoded = evolvingCodec.encodeWithSchema(event)

    expect(encoded.map(_.eventType.value) == Right("library.name-changed")) and
      expect(encoded.map(_.version) == Right(3)) and
      expect(
        encoded.flatMap(e => evolvingCodec.decode(e.eventType, e.version, e.payload)) == Right(event),
      )
  }

  pureTest("versioned decoding applies every JSON upcast step in order") {
    val result = evolvingCodec.decode(
      EventTypeName.fromString("library.name-changed"),
      version = 1,
      payload = """{"oldValue":"before"}""",
    )

    expect(result == Right(NameChanged("before")))
  }

  pureTest("event schema aliases decode historical event identifiers") {
    val result = evolvingCodec.decode(
      EventTypeName.fromString("LegacyNameChanged"),
      version = 1,
      payload = """{"oldValue":"before"}""",
    )

    expect(result == Right(NameChanged("before")))
  }

  pureTest("the legacy decode overload treats a payload as the current registered version") {
    val event = NameChanged("current")
    val encoded = evolvingCodec.encodeWithSchema(event)

    expect(encoded.flatMap(e => evolvingCodec.decode(e.eventType, e.payload)) == Right(event))
  }

  pureTest("versioned decoding reports a missing evolution step") {
    val result = evolvingCodec.decode(
      EventTypeName.fromString("library.missing-evolution"),
      version = 1,
      payload = """{"value":"before"}""",
    )

    expect(result.left.exists(_.isInstanceOf[MissingEventUpcast]))
  }

  pureTest("versioned decoding rejects events produced by a newer application") {
    val result = evolvingCodec.decode(
      EventTypeName.fromString("library.name-changed"),
      version = 4,
      payload = """{"value":"future"}""",
    )

    expect(result.left.exists(_.isInstanceOf[FutureEventVersion]))
  }

  pureTest("an upcaster definition rejects duplicate steps") {
    val result = Try(
      JsonEventUpcaster
        .builder[NameChanged]
        .step(1)(Right(_))
        .step(1)(Right(_)),
    )

    expect(result.isFailure)
  }

  pureTest("codec construction rejects duplicate stable event identifiers") {
    expect(Try(CirceEventCodec.derived[DuplicateSchemaEvent]).isFailure)
  }
