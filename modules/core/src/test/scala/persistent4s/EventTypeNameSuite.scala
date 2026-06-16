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

import weaver.SimpleIOSuite

object EventTypeNameSuite extends SimpleIOSuite:

  sealed trait MyEvent extends Event

  final case class MyEventA(x: Int) extends MyEvent

  case object MyEventB extends MyEvent

  pureTest("of derives the simple class name from the type parameter") {
    expect(EventTypeName.of[MyEventA].value == "MyEventA")
  }

  pureTest("fromInstance derives the simple class name from a runtime instance") {
    expect(EventTypeName.fromInstance(MyEventA(1)).value == "MyEventA")
  }

  pureTest("fromInstance appends $ for case objects due to JVM module class naming") {
    expect(EventTypeName.fromInstance(MyEventB).value == "MyEventB$")
  }

  pureTest("fromString wraps a raw string and value extracts it back") {
    expect(EventTypeName.fromString("SomeEvent").value == "SomeEvent")
  }

  pureTest("of and fromInstance agree for case classes") {
    expect(EventTypeName.of[MyEventA] == EventTypeName.fromInstance(MyEventA(1)))
  }

  pureTest("of and fromInstance agree for case objects (both include the trailing $)") {
    expect(EventTypeName.of[MyEventB.type] == EventTypeName.fromInstance(MyEventB))
  }
