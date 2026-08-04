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

/** Subscribes to a topic and streams [[EventEnvelope]]s.
  *
  * Each emitted element is paired with an acknowledge action (`F[Unit]`) that must be invoked after successfully
  * processing the envelope.
  */
trait EventSubscriber[F[_], A <: Event]:

  /** Stream decoded envelopes from `topic`, each paired with an acknowledge action.
    *
    * @param fromBeginning
    *   if true, start from the earliest available record when no prior position is known.
    */
  def subscribe(topic: String, fromBeginning: Boolean): Stream[F, (EventEnvelope[A], F[Unit])]
