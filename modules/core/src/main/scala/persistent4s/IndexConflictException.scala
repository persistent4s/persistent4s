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

/** An IndexConflictException is thrown when there is a conflict between the expected index and the actual index in the
  * event store. This typically occurs when multiple clients are trying to append events to the event store
  * concurrently, and the expected index provided by one client does not match the actual index in the event store due
  * to another client having already appended events. The exception contains both the expected index and the actual
  * index to provide context about the conflict.
  *
  * @param expectedIndex
  *   the expected index that the client provided when attempting to append events
  * @param actualIndex
  *   the actual index in the event store at the time of the append attempt, which did not match the expected index
  */
final case class IndexConflictException(
  expectedIndex: Long,
  actualIndex: Long,
) extends RuntimeException(
      s"Index conflict: expected $expectedIndex, actual $actualIndex",
    )
