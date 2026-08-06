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

import persistent4s.examples.saga.contract.ReleaseStock
import persistent4s.CommandHandler
import persistent4s.Tag
import persistent4s.examples.saga.inventory.domain.{InventoryEvent, InventoryTags, StockReserved, StockReleased}

final case class ReleaseState(amount: Option[Int])

object ReleaseStockHandler extends CommandHandler[ReleaseStock, ReleaseState, InventoryEvent]:

  override def tags(command: ReleaseStock): Set[Tag] = Set(InventoryTags.item(command.itemId))

  override def evolve(command: ReleaseStock, state: ReleaseState, event: InventoryEvent): ReleaseState =
    event match
      case StockReserved(_, orderId, amount) if orderId == command.orderId => ReleaseState(Some(amount))
      case StockReleased(_, orderId, amount) if orderId == command.orderId => ReleaseState(None)
      case _                                                               => state

  def initial: ReleaseState = ReleaseState(None)

  def validate(state: ReleaseState, command: ReleaseStock): Either[Throwable, Unit] = Right(())

  def decide(state: ReleaseState, command: ReleaseStock): List[(Set[Tag], InventoryEvent)] =
    state.amount match
      case Some(amt) =>
        List(Set(InventoryTags.item(command.itemId)) -> StockReleased(command.itemId, command.orderId, amt))
      case None => Nil
