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

object Persistent4sFailureSuite extends SimpleIOSuite:

  private val cause = new RuntimeException("connection lost")

  pureTest("library failures are marked") {
    val failures: List[Throwable] = List(
      IndexConflictException(1L, 2L),
      ProjectionCheckpointConflict("orders", ReadCursor.Start),
      ProjectionStateNotFound(EventTypeName.fromString("OrderPlaced")),
      ProjectionExecutionFailed("orders", cause),
      EventEncodingFailed("OrderPlaced", cause),
    )
    forEach(failures)(failure => expect(failure.isInstanceOf[Persistent4sFailure]))
  }

  pureTest("wrapping failures keep their cause and message") {
    val failure = EventEncodingFailed("OrderPlaced", cause)
    expect(failure.getCause eq cause) and
      expect(failure.getMessage == "EventCodec failed to encode event: connection lost")
  }

  pureTest("application errors are not marked") {
    expect(!cause.isInstanceOf[Persistent4sFailure])
  }
