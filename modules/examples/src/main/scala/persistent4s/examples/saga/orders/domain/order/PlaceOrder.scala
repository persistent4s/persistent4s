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

import persistent4s.{CommandHandler, EventTypeName, Tag}
import persistent4s.examples.saga.orders.domain.{CustomerRegistered, OrderPlaced, OrderEvent, OrdersTags}

final case class PlaceOrder(orderId: UUID, customerId: UUID, itemId: UUID, amount: Int, price: Int)

final case class PlaceOrderState(customerExists: Boolean, orderExists: Boolean)

/** The command the whole example exists to show: it can only check half of what matters.
  *
  * Reading `{customer:C, order:O}` gives it everything the orders service knows — the customer is real, this order is
  * new, the amount makes sense — and none of what actually decides whether the order can be honoured. Stock lives in
  * another service's log, behind another service's concurrency boundary, and no read this handler could perform would
  * settle it: two orders can both see the same last unit and only one append can win.
  *
  * So it appends [[OrderPlaced]], which is a promise to find out rather than a promise to deliver, and the saga takes
  * it from there. Everything a synchronous check would have given up front — a definite answer at request time —
  * becomes a compensation later.
  */
object PlaceOrderHandler extends CommandHandler[PlaceOrder, PlaceOrderState, OrderEvent]:

  override def eventTypes: Option[Set[EventTypeName]] =
    Some(Set(EventTypeName.of[CustomerRegistered], EventTypeName.of[OrderPlaced]))

  def tags(command: PlaceOrder): Set[Tag] =
    Set(OrdersTags.customer(command.customerId), OrdersTags.order(command.orderId))

  def initial: PlaceOrderState = PlaceOrderState(customerExists = false, orderExists = false)

  def evolve(command: PlaceOrder, state: PlaceOrderState, event: OrderEvent): PlaceOrderState =
    event match
      case _: CustomerRegistered => state.copy(customerExists = true)
      // Guarded on the id rather than the type: order events carry only their order's tag today, so nothing else can
      // arrive here — but if they ever also carried the customer's, an unguarded match would read a customer's previous
      // order as this one and reject every order after their first.
      case placed: OrderPlaced if placed.orderId == command.orderId => state.copy(orderExists = true)
      case _                                                        => state

  def validate(state: PlaceOrderState, command: PlaceOrder): Either[Throwable, Unit] =
    if !state.customerExists then Left(new IllegalStateException(s"No such customer: ${command.customerId}"))
    else if state.orderExists then Left(new IllegalStateException(s"Order already placed: ${command.orderId}"))
    else if command.amount <= 0 then Left(new IllegalArgumentException("Order amount must be positive"))
    else Right(())

  def decide(state: PlaceOrderState, command: PlaceOrder): List[(Set[Tag], OrderEvent)] =
    List(
      Set(OrdersTags.order(command.orderId)) ->
        OrderPlaced(command.orderId, command.customerId, command.itemId, command.amount, command.price),
    )
