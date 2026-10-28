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

package persistent4s

import scala.math.Ordering.Implicits.infixOrderingOps

import weaver.SimpleIOSuite

object ReadCursorSuite extends SimpleIOSuite:

  pureTest("cursors are ordered by commit group before position") {
    expect(ReadCursor(1L, 50L) < ReadCursor(2L, 10L)) and
      expect(ReadCursor(2L, 10L) < ReadCursor(2L, 11L)) and
      expect(ReadCursor(2L, 10L) <= ReadCursor(2L, 10L))
  }

  pureTest("Start precedes every event's cursor") {
    expect(ReadCursor.Start < ReadCursor(0L, 0L)) and expect(ReadCursor.Start < ReadCursor(1L, 0L))
  }

  pureTest("sorting cursors follows delivery order") {
    val cursors = List(ReadCursor(3L, 1L), ReadCursor(1L, 9L), ReadCursor(1L, 2L), ReadCursor.Start)
    expect(cursors.sorted == List(ReadCursor.Start, ReadCursor(1L, 2L), ReadCursor(1L, 9L), ReadCursor(3L, 1L)))
  }
