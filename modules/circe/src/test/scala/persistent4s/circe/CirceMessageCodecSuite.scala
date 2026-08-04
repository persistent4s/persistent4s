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
import weaver.SimpleIOSuite

object CirceMessageCodecSuite extends SimpleIOSuite:

  final case class CreateCharge(amount: Int, account: String) derives Encoder.AsObject, Decoder

  private val codec = CirceMessageCodec.derived[CreateCharge]

  pureTest("encode and decode form a round-trip") {
    val message = CreateCharge(42, "acct-1")
    val roundTrip = codec.encode(message).flatMap(codec.decode)
    expect(roundTrip == Right(message))
  }

  pureTest("decode returns Left for invalid JSON") {
    expect(codec.decode("not valid json").isLeft)
  }

  pureTest("decode returns Left when JSON does not match the schema") {
    // valid JSON, but missing the required fields
    expect(codec.decode("""{"wrong": "field"}""").isLeft)
  }

  pureTest("encode produces compact JSON without whitespace") {
    codec.encode(CreateCharge(42, "acct-1")) match
      case Right(s) => expect(!s.contains(" ") && !s.contains("\n"))
      case Left(e)  => failure(s"encode failed: ${e.getMessage}")
  }
