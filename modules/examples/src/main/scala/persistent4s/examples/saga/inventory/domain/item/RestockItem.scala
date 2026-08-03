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

import persistent4s.{CommandHandler, EventTypeName, Tag}
import persistent4s.examples.saga.inventory.domain.{InventoryEvent, InventoryTags, ItemRestocked}

final case class RestockItem(itemId: UUID, amount: Int)

/** The ordinary case, kept next to [[ReserveStockHandler]] for the contrast: a command whose invariant is entirely local
  * needs no state, no messages and no saga — just `run`.
  *
  * The state is `Unit` because nothing in the log can make a restock invalid. That does mean the handler still pays for a
  * read it has no use for: `CommandHandler` always folds the command's scope, and there is no way to say "read nothing".
  *
  * Narrowing `eventTypes` to `ItemRestocked` is not an optimisation, though — it is what keeps this command out of the
  * reservation's concurrency boundary. The filter defines the scope the optimistic-concurrency check guards, so:
  *   - a concurrent reservation does not bounce a restock, because `StockReserved` is not in this scope;
  *   - a concurrent restock *does* bounce a reservation, because [[ReserveStockHandler]]'s scope includes both — which
  *     is right, since stock arriving is news a reservation wants to re-read before giving up.
  */
object RestockItemHandler extends CommandHandler[RestockItem, Unit, InventoryEvent]:

  override def eventTypes: Option[Set[EventTypeName]] = Some(Set(EventTypeName.of[ItemRestocked]))

  def tags(command: RestockItem): Set[Tag] = Set(InventoryTags.item(command.itemId))

  def initial: Unit = ()

  def evolve(command: RestockItem, state: Unit, event: InventoryEvent): Unit = ()

  def validate(state: Unit, command: RestockItem): Either[Throwable, Unit] =
    if command.amount <= 0 then Left(new IllegalArgumentException("Restock amount must be positive"))
    else Right(())

  def decide(state: Unit, command: RestockItem): List[(Set[Tag], InventoryEvent)] =
    List(Set(InventoryTags.item(command.itemId)) -> ItemRestocked(command.itemId, command.amount))
