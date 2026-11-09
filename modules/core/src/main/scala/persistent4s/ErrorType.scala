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

import scala.annotation.tailrec

/** Names a failure for the `error.type` metric attribute. Names must come from a small, fixed set, such as class names or
  * database error codes, so that they can be used as metric labels.
  */
trait ErrorType:

  /** @param error
    *   the failure to name
    * @return
    *   a short name with bounded cardinality
    */
  def name(error: Throwable): String

object ErrorType:

  /** Names a failure by the class of its underlying error, looking through persistent4s wrappers. */
  val default: ErrorType = from(PartialFunction.empty)

  /** Names some failures with `named` and the others by class name. Both look through persistent4s wrappers, so a
    * projection failure is named after the error that caused it.
    *
    * @param named
    *   names for the underlying errors it is defined for
    */
  def from(named: PartialFunction[Throwable, String]): ErrorType =
    error => named.applyOrElse(underlying(error), _.getClass.getName)

  @tailrec
  private def underlying(error: Throwable): Throwable = error match
    case ProjectionEventFailed(_, _, cause)  => underlying(cause)
    case ProjectionExecutionFailed(_, cause) => underlying(cause)
    case other                               => other
