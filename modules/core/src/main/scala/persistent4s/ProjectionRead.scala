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

import cats.effect.Sync
import cats.syntax.all.*

import fs2.Stream

/** One page of events read by [[EventStore.readForProjection]].
  *
  * @param events
  *   the next events in delivery order; none of them can later be preceded by an event that is not yet visible
  * @param heldBack
  *   `true` when visible events exist that the store withheld because an earlier write may still commit before them. No
  *   notification announces when that write resolves (a rollback announces nothing), so a projector seeing this must
  *   re-read on its own until it clears.
  */
final case class ProjectionRead[A <: Event](events: List[EventEnvelope[A]], heldBack: Boolean)

object ProjectionRead:

  /** For stores whose positions only become visible in order, so position order is already a safe delivery order and
    * nothing is ever held back.
    */
  def byPosition[F[_]: Sync, A <: Event](events: Stream[F, EventEnvelope[A]]): F[ProjectionRead[A]] =
    events.compile.toList.map(ProjectionRead(_, heldBack = false))
