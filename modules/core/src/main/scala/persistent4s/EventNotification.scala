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

enum EventStoreNotification:

  /** Emitted when new events are appended to the store. */
  case EventsAppended

  /** Emitted when a projection is paused */
  case PauseProjection(name: String)

  /** Emitted when a projection is resumed */
  case ResumeProjection(name: String)

  /** Emitted when a projection's checkpoint is changed */
  case UpdateCheckpointIndex(name: String, index: Long)

  case UnknownNotification

/** A trait for event stores that can provide a stream of notifications when new events are added or to communicate
  * other relevant changes such as projection failure or pause. This allows projectors to react to new events without
  * polling the event store.
  */
trait EventNotification[F[_]]:

  /** A stream that emits a unit value to signal that new events have been appended to the store. Projectors subscribe
    * to this stream to avoid polling. The stream should never complete.
    *
    * Implementations are only required to emit at least one signal per [[EventStore.append]] call, not necessarily one
    * per individual event. Consumers must therefore treat each emission as "there may be new events" and re-read from
    * their last checkpoint, rather than assuming exactly one emission per event. The [[DefaultProjector]] already
    * handles this correctly by coalescing multiple signals into a single catch-up pass.
    *
    * @return
    *   an infinite stream of wake-up signals
    */
  def notification(projectionName: String): Stream[F, EventStoreNotification]
