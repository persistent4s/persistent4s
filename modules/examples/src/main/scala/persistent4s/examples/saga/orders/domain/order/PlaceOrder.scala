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

package persistent4s.examples.saga.orders.domain.order

import java.util.UUID

import persistent4s.EventSourcedCommandHandler
import persistent4s.examples.saga.orders.domain.{CustomerRegistered, OrderEvent, OrderPlaced, OrdersScopes}

final case class PlaceOrder(orderId: UUID, customerId: UUID, itemId: UUID, amount: Int, price: Int)

/** The command the whole example exists to show: it can only check half of what matters.
  *
  * Reading `{customer, order}` gives it everything the orders service knows — the customer is real, this order is new,
  * the amount makes sense — and none of what actually decides whether the order can be honoured. Stock lives in another
  * service's log, behind another service's concurrency boundary, and no read this handler could perform would settle
  * it: two orders can both see the same last unit and only one append can win.
  *
  * So it appends [[OrderPlaced]], which is a promise to find out rather than a promise to deliver, and the saga takes
  * it from there. Everything a synchronous check would have given up front — a definite answer at request time —
  * becomes a compensation later.
  */
object PlaceOrder:

  final case class State(customerExists: Boolean = false, orderExists: Boolean = false)

  enum Error:

    case NoSuchCustomer(customerId: UUID)

    case AlreadyPlaced(orderId: UUID)

    case NotPositive(amount: Int)

  object Handler extends EventSourcedCommandHandler[PlaceOrder, State, OrderEvent, Error]:

    override protected val behavior = handler(State()):
      // Two scopes, and they are the read set as well as the concurrency boundary. Unlike the old `eventTypes` filter,
      // a scope handler reads *everything* in its scopes, so the saga's own `OrderConfirmed`/`OrderCancelled` now pass
      // through here. Neither is registered below, so neither changes the state — but both now share this command's
      // boundary, and a saga settling an order while a `PlaceOrder` for the same id is in flight costs it a retry.
      scope(OrdersScopes.Customer)(_.customerId)
      scope(OrdersScopes.Order)(_.orderId)

      // Each event shares exactly one of the two scopes, so each is matched on its own key with nothing written down.
      // The old handler spelled the second one out as `if placed.orderId == command.orderId`, and needed to: an
      // unguarded match would have read a customer's previous order as this one and rejected every order after their
      // first.
      on[CustomerRegistered].evolve(state => state.copy(customerExists = true))

      on[OrderPlaced].evolve(state => state.copy(orderExists = true))

      reject:
        case (state, command) if !state.customerExists => Error.NoSuchCustomer(command.customerId)
        case (state, command) if state.orderExists     => Error.AlreadyPlaced(command.orderId)
        case (_, command) if command.amount <= 0       => Error.NotPositive(command.amount)

      // Goes out tagged `order:O` only, never `customer:C` — emission tags come from the event's declared scopes when
      // it has any, and [[OrdersScopes.Order]] explains why this event declares just the one.
      emit: command =>
        OrderPlaced(command.orderId, command.customerId, command.itemId, command.amount, command.price)
