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

/** Subscribes to a topic and streams [[IncomingMessage]]s. */
trait MessageSubscriber[F[_]]:

  /** Stream messages from `topic`, each paired with its acknowledge action.
    *
    * @param fromBeginning
    *   where to start when this subscription has no recorded position yet: the earliest retained record if true, only
    *   records arriving from now on if false. It has no effect once a position has been acknowledged — a resumed
    *   subscription always continues from there, whichever value is passed.
    */
  def subscribe(topic: String, fromBeginning: Boolean): Stream[F, (IncomingMessage, F[Unit])]
