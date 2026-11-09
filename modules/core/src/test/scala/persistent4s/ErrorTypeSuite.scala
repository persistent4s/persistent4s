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

import java.util.UUID

import weaver.SimpleIOSuite

object ErrorTypeSuite extends SimpleIOSuite:

  private val cause = new IllegalStateException("out of memory")

  pureTest("the default names a failure by its class") {
    expect(ErrorType.default.name(cause) == "java.lang.IllegalStateException") and
      expect(ErrorType.default.name(IndexConflictException(1L, 2L)) == "persistent4s.IndexConflictException")
  }

  pureTest("projection failures are named after the error that caused them") {
    val nested = ProjectionExecutionFailed("orders", ProjectionEventFailed("orders", UUID.randomUUID(), cause))
    expect(ErrorType.default.name(nested) == "java.lang.IllegalStateException")
  }

  pureTest("custom names apply to the underlying error and fall back to its class") {
    val errorType = ErrorType.from { case _: IllegalStateException => "53200" }
    expect(errorType.name(ProjectionEventFailed("orders", UUID.randomUUID(), cause)) == "53200") and
      expect(errorType.name(new RuntimeException("boom")) == "java.lang.RuntimeException")
  }
