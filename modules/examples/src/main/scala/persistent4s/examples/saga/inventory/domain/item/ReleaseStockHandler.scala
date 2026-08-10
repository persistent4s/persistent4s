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

import persistent4s.EventSourcedCommandHandler
import persistent4s.examples.saga.contract.ReleaseStock
import persistent4s.examples.saga.inventory.domain.{InventoryEvent, InventoryScopes, StockReleased, StockReserved}

final case class ReleaseState(amount: Option[Int])

/** The compensation half of the saga: give back whatever this order is holding.
  *
  * Its rejection type is `Unit` because it turns nothing down. Releasing an order that holds nothing is not an error —
  * it is the ordinary outcome of a compensation that arrives twice, or of one for a reservation that was refused — and
  * it says so by emitting no events rather than by failing. `emitMany` is what allows that: an empty list is a
  * successful command that wrote nothing.
  *
  * The handler object is not nested inside its command the way [[RestockItem]]'s is, because [[ReleaseStock]] belongs
  * to the contract both services share, not to inventory.
  */
object ReleaseStockHandler extends EventSourcedCommandHandler[ReleaseStock, ReleaseState, InventoryEvent, Unit]:

  override protected val behavior = handler(ReleaseState(None)):
    scope(InventoryScopes.Item)(_.itemId)

    // The item is matched automatically as the one shared scope; `matching` adds the order on top. The order is
    // deliberately not a scope of its own — the invariant being protected is per item, and scoping by order as well
    // would put unrelated items into one concurrency boundary.
    on[StockReserved]
      .matching(_.orderId, _.orderId)
      .evolve((_, event) => ReleaseState(Some(event.amount)))

    on[StockReleased]
      .matching(_.orderId, _.orderId)
      .evolve(_ => ReleaseState(None))

    emitMany: (state, command) =>
      state.amount.toList.map(amount => StockReleased(command.itemId, command.orderId, amount))
