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

package persistent4s.examples.saga.inventory.domain.item

import java.util.UUID

import io.circe.Encoder

import persistent4s.examples.saga.inventory.domain.{InventoryEvent, ItemRestocked, StockReserved}

final case class Reservation(orderId: UUID, amount: Int) derives Encoder.AsObject

final case class ItemStock(itemId: UUID, available: Int, reservations: List[Reservation]) derives Encoder.AsObject

object ItemStock:

  /** What is left of an item, and who holds the rest.
    *
    * This is the whole read side of the inventory service. The arithmetic is the same one [[ReserveStockHandler]] folds
    * to make its decision, so a projection here would be a second copy of it maintained separately — the interesting
    * status field in this example belongs to the orders service, not to stock.
    */
  def fold(itemId: UUID, events: List[InventoryEvent]): ItemStock =
    events.foldLeft(ItemStock(itemId, available = 0, reservations = Nil)) { (stock, event) =>
      event match
        case ItemRestocked(_, amount)          => stock.copy(available = stock.available + amount)
        case StockReserved(_, orderId, amount) =>
          stock.copy(
            available = stock.available - amount,
            reservations = stock.reservations :+ Reservation(orderId, amount),
          )
    }
