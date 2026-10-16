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

import java.time.LocalDate

import io.circe.syntax.*
import io.circe.{Decoder, Encoder, Json, parser}

import persistent4s.{Event, EventSchema, EventTypeName}

import weaver.SimpleIOSuite

object PatchCodecSuite extends SimpleIOSuite:

  final case class NameUpdate(name: Option[Option[String]]) derives PatchCodec

  final case class MixedUpdate(
    number: Int,
    name: Option[Option[String]],
    birthdate: Option[Option[LocalDate]],
    active: Option[Option[Boolean]],
    nickname: Option[String],
    metadata: Json,
  ) derives PatchCodec

  final case class Code(value: String)

  object Code:

    given Encoder[Code] = Encoder.encodeString.contramap(_.value)

    given Decoder[Code] = Decoder.decodeString.emap { value =>
      Either.cond(value.startsWith("P-"), Code(value), "Expected a patient code")
    }

  final case class CodeUpdate(code: Option[Option[Code]]) derives PatchCodec

  final case class NestedUpdate(details: Option[Option[NameUpdate]]) derives PatchCodec

  sealed trait PatientEvent extends Event

  final case class PatientUpdated(number: Int, name: Option[Option[String]]) extends PatientEvent derives PatchCodec

  object PatientUpdated:

    given EventSchema[PatientUpdated] =
      EventSchema("patients.updated", version = 2).withAlias("OldPatientUpdated")

    given JsonEventUpcaster[PatientUpdated] =
      JsonEventUpcaster
        .builder[PatientUpdated]
        .step(fromVersion = 1)(json =>
          json.hcursor.get[String]("oldName").map { name =>
            json.mapObject(_.remove("oldName").add("name", name.asJson))
          },
        )
        .build

  private val examples: List[(String, NameUpdate)] = List(
    "{}" -> NameUpdate(None),
    """{"name":null}""" -> NameUpdate(Some(None)),
    """{"name":"Alice"}""" -> NameUpdate(Some(Some("Alice"))),
  )

  private val eventCodec = CirceEventCodec.derived[PatientEvent]

  pureTest("missing, null, and new values encode and decode separately") {
    expect(examples.forall { case (json, update) =>
      update.asJson.noSpaces == json && parser.decode[NameUpdate](json) == Right(update)
    })
  }

  pureTest("nested-option fields reject invalid values") {
    val invalid = List("""{"name":1}""", """{"name":false}""", """{"name":[]}""", """{"name":{}}""")
    expect(invalid.forall(json => parser.decode[NameUpdate](json).isLeft))
  }

  pureTest("updates require an object") {
    val invalid = List("null", "false", "1", "[]", "\"Alice\"")
    expect(invalid.forall(json => parser.decode[NameUpdate](json).isLeft))
  }

  pureTest("decoding through a cursor preserves missing and null field handling") {
    val decoder = Decoder[NameUpdate]
    val present = Json.obj("update" -> Json.obj("name" -> Json.Null)).hcursor.downField("update")
    val missing = Json.obj().hcursor.downField("update")
    val incorrect = Json.arr().hcursor.downField("update")

    expect(decoder.tryDecode(present) == Right(NameUpdate(Some(None)))) and
      expect(decoder.tryDecode(missing).isLeft) and
      expect(decoder.tryDecode(incorrect).isLeft) and
      expect(decoder.tryDecodeAccumulating(present).toEither == Right(NameUpdate(Some(None)))) and
      expect(decoder.tryDecodeAccumulating(missing).isInvalid)
  }

  pureTest("ordinary fields keep their codecs and meaningful null values") {
    val update = MixedUpdate(1, None, Some(Some(LocalDate.of(2000, 1, 2))), Some(Some(false)), None, Json.Null)
    val expected = """{"number":1,"birthdate":"2000-01-02","active":false,"nickname":null,"metadata":null}"""

    expect(update.asJson.noSpaces == expected) and
      expect(parser.decode[MixedUpdate](expected) == Right(update)) and
      expect(parser.decode[MixedUpdate]("""{"number":1,"metadata":null}""") ==
        Right(MixedUpdate(1, None, None, None, None, Json.Null)))
  }

  pureTest("required fields and invalid dates remain errors") {
    val missingNumber = """{"metadata":null}"""
    val nullNumber = """{"number":null,"metadata":null}"""
    val invalidDate = """{"number":1,"birthdate":"not-a-date","metadata":null}"""

    expect(parser.decode[MixedUpdate](missingNumber).isLeft) and
      expect(parser.decode[MixedUpdate](nullNumber).isLeft) and
      expect(parser.decode[MixedUpdate](invalidDate).isLeft)
  }

  pureTest("accumulating decoding reports every invalid field") {
    val json = Json.obj("number" -> "wrong".asJson, "name" -> 1.asJson, "metadata" -> Json.Null)
    val result = Decoder[MixedUpdate].decodeAccumulating(json.hcursor).toEither

    expect(result.left.exists(_.length == 2))
  }

  pureTest("custom scalar codecs are used inside update fields") {
    val update = CodeUpdate(Some(Some(Code("P-123"))))
    val json = """{"code":"P-123"}"""

    expect(update.asJson.noSpaces == json) and
      expect(parser.decode[CodeUpdate](json) == Right(update)) and
      expect(parser.decode[CodeUpdate]("""{"code":"invalid"}""").isLeft) and
      expect(parser.decode[CodeUpdate]("""{"code":null}""") == Right(CodeUpdate(Some(None))))
  }

  pureTest("nested update objects use their own patch codecs") {
    val nestedExamples = examples.map { case (json, update) =>
      s"""{"details":$json}""" -> NestedUpdate(Some(Some(update)))
    } ++ List("{}" -> NestedUpdate(None), """{"details":null}""" -> NestedUpdate(Some(None)))

    expect(nestedExamples.forall { case (json, update) =>
      update.asJson.noSpaces == json && parser.decode[NestedUpdate](json) == Right(update)
    })
  }

  pureTest("historical full payloads decode as supplied changes") {
    val json = """{"number":1,"name":"Alice"}"""
    expect(parser.decode[PatientUpdated](json) == Right(PatientUpdated(1, Some(Some("Alice")))))
  }

  pureTest("event codecs preserve patch semantics and explicit schemas") {
    val updates = examples.map { case (_, update) => PatientUpdated(1, update.name) }

    expect(updates.forall { update =>
      val encoded = eventCodec.encodeWithSchema(update)
      encoded.exists(event => event.eventType.value == "patients.updated" && event.version == 2) &&
      encoded.flatMap(event => eventCodec.decode(event.eventType, event.version, event.payload)) == Right(update)
    })
  }

  pureTest("event aliases and upcasters work with patch codecs") {
    val result = eventCodec.decode(
      EventTypeName.fromString("OldPatientUpdated"),
      version = 1,
      payload = """{"number":1,"oldName":"Alice"}""",
    )

    expect(result == Right(PatientUpdated(1, Some(Some("Alice")))))
  }
