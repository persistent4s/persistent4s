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
import persistent4s.SnapshotCodec
import persistent4s.circe.CirceSnapshotCodec.given
import weaver.SimpleIOSuite

object CirceSnapshotCodecSuite extends SimpleIOSuite:

  final case class TestState(count: Int, label: String) derives Encoder, Decoder

  pureTest("encode then decode round-trips correctly") {
    val state   = TestState(42, "hello")
    val codec   = summon[SnapshotCodec[TestState]]
    val encoded = codec.encode(state)
    expect(codec.decode(encoded) == Right(state))
  }

  pureTest("encode produces compact JSON containing field names") {
    val encoded = summon[SnapshotCodec[TestState]].encode(TestState(1, "test"))
    expect(encoded.contains("count")) && expect(encoded.contains("label"))
  }

  pureTest("decode returns Left for invalid JSON") {
    expect(summon[SnapshotCodec[TestState]].decode("not valid json {{{").isLeft)
  }

  pureTest("decode returns Left when JSON does not match the state schema") {
    expect(summon[SnapshotCodec[TestState]].decode("""{"wrong_field": true}""").isLeft)
  }
