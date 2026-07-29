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

/** Capability of an [[EventStore]] that can enqueue [[OutgoingMessage]]s into a [[MessageOutbox]] in the same
  * transaction that appends events, so the events and the messages they cause become visible together or not at all.
  */
trait TransactionalMessages[F[_], A <: Event]:

  /** [[EventStore.append]] plus an atomic message enqueue. */
  def appendWithMessages(
    eventFilter: EventFilter,
    expectedIndex: Long,
    messages: List[OutgoingMessage],
    events: List[(Option[UUID], Set[Tag], EventTypeName, Boolean, A)]*,
  ): F[List[A]]

  /** [[EventStore.appendUnchecked]] plus an atomic message enqueue. */
  def appendUncheckedWithMessages(
    messages: List[OutgoingMessage],
    events: List[(Option[UUID], Set[Tag], EventTypeName, Boolean, A)]*,
  ): F[List[A]]
