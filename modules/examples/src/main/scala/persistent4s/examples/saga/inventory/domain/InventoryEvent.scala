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

package persistent4s.examples.saga.inventory.domain

import java.util.UUID

import io.circe.{Decoder, Encoder}

import persistent4s.{Event, Tag}

sealed trait InventoryEvent extends Event

/** Stock arriving. Unconditional — it depends on nothing that is already in the log. */
final case class ItemRestocked(itemId: UUID, amount: Int) extends InventoryEvent derives Encoder, Decoder

/** Stock committed to an order.
  *
  * `orderId` belongs to the orders service and means nothing here beyond identifying which request this satisfied. It
  * is what makes honouring a request idempotent: delivery is at-least-once, so the handler folds its own log and
  * recognises an `orderId` it has already reserved for instead of reserving twice.
  */
final case class StockReserved(itemId: UUID, orderId: UUID, amount: Int) extends InventoryEvent derives Encoder, Decoder

final case class StockReleased(itemId: UUID, orderId: UUID, amount: Int) extends InventoryEvent derives Encoder, Decoder

object InventoryTags:

  /** The tag every item's events carry, and so the scope every stock decision reads and appends under. Defined once on
    * purpose: it is the boundary that stops two orders from taking the same last unit, and two spellings of it would
    * split that boundary in half without anything failing.
    */
  def item(itemId: UUID): Tag = Tag("item", itemId)
