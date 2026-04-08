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

import fs2.Stream

/** A trait for event stores that can provide a stream of notifications when new events are added. This allows
  * projectors to react to new events without polling the event store.
  */
trait EventNotification[F[_]]:

  /** A stream that emits a unit value whenever a new event is added to the event store. Projectors can subscribe to
    * this stream to be notified of new events and trigger re-processing. The stream should never complete, and should
    * emit a value for every new event added to the store.
    *
    * @return
    *   a stream of notifications for new events
    */
  def notification: Stream[F, Unit]
