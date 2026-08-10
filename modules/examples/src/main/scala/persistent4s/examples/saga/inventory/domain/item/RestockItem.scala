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

import persistent4s.EventSourcedCommandHandler
import persistent4s.examples.saga.inventory.domain.{InventoryEvent, InventoryScopes, ItemRestocked}

final case class RestockItem(itemId: UUID, amount: Int)

object RestockItem:

  enum Error:

    case NotPositive(amount: Int)

  object Handler extends EventSourcedCommandHandler[RestockItem, Unit, InventoryEvent, Error]:

    override protected val behavior = handler(initial = ()):
      scope(InventoryScopes.Item)(_.itemId)

      on[ItemRestocked].ignore

      reject:
        case (_, command) if command.amount <= 0 => Error.NotPositive(command.amount)

      emit: command =>
        ItemRestocked(command.itemId, command.amount)
