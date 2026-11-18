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

/** Marks a failure raised by persistent4s itself, so applications can tell it apart from their own errors, e.g. to
  * report it separately. Failures that wrap another error, such as a database error during a projection, keep it as
  * their cause.
  */
trait Persistent4sFailure extends Throwable

/** Raised when an [[EventCodec]] cannot encode an event before it is stored or published.
  *
  * @param eventClass
  *   class name of the event that could not be encoded
  * @param cause
  *   the codec's error
  */
final case class EventEncodingFailed(eventClass: String, cause: Throwable)
    extends RuntimeException(s"EventCodec failed to encode event: ${cause.getMessage}", cause)
    with Persistent4sFailure
