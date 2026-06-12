/*
 * Copyright 2026 Bastien Jolidon
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

package persistent4s

import weaver.SimpleIOSuite
import java.util.UUID

object TagSuite extends SimpleIOSuite:

  pureTest("value returns category:id format") {
    expect(Tag("student", "42").value == "student:42")
  }

  pureTest("UUID constructor converts UUID to string id") {
    val uuid = UUID.fromString("00000000-0000-0000-0000-000000000001")
    expect(Tag("student", uuid).value == s"student:$uuid")
  }

  pureTest("fromString parses a valid category:id string") {
    expect(Tag.fromString("student:42") == Some(Tag("student", "42")))
  }

  pureTest("fromString returns None when string has no colon") {
    expect(Tag.fromString("nocolon") == None)
  }

  pureTest("fromString returns None for empty string") {
    expect(Tag.fromString("") == None)
  }

  pureTest("fromString treats extra colons as part of the id") {
    // split(limit=2): "a:b:c" → category="a", id="b:c"
    expect(Tag.fromString("a:b:c") == Some(Tag("a", "b:c")))
  }

  pureTest("fromString and value are a round-trip") {
    val tag = Tag("user", "abc-123")
    expect(Tag.fromString(tag.value) == Some(tag))
  }
