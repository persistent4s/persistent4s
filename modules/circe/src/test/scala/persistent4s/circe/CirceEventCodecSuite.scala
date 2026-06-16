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

import io.circe.{Decoder, Encoder}
import io.circe.syntax.*
import persistent4s.{Event, EventTypeName}
import weaver.SimpleIOSuite

object CirceEventCodecSuite extends SimpleIOSuite:

  final case class MyEvent(value: String) extends Event derives Encoder.AsObject, Decoder

  private val codec = CirceEventCodec.make[MyEvent](
    encodeEvent = _.asJson,
    decodeEvent = (_, json) => json.as[MyEvent].left.map(identity),
  )

  private val eventType = EventTypeName.fromString("MyEvent")

  pureTest("encode and decode form a round-trip") {
    val event = MyEvent("hello")
    expect(codec.decode(eventType, codec.encode(event)) == Right(event))
  }

  pureTest("decode returns Left for invalid JSON") {
    expect(codec.decode(eventType, "not valid json").isLeft)
  }

  pureTest("decode returns Left when JSON does not match the event schema") {
    // {"wrong": "field"} is valid JSON but missing the required "value" field
    expect(codec.decode(eventType, """{"wrong": "field"}""").isLeft)
  }

  pureTest("encode produces compact JSON without whitespace") {
    val encoded = codec.encode(MyEvent("hello"))
    expect(!encoded.contains(" ") && !encoded.contains("\n"))
  }
