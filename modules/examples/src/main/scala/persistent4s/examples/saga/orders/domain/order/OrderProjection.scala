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

import cats.effect.*
import cats.syntax.all.*

import persistent4s.*
import persistent4s.examples.saga.orders.domain.{
  CustomerRegistered,
  OrderCancelled,
  OrderConfirmed,
  OrderPlaced,
  OrderEvent,
}

/** The read model an order-status API is served from — and the reason a saga forces you to have one.
  *
  * Three of the four events this folds are about the same order, but only the first is written by a request the client
  * made. `OrderConfirmed` and `OrderCancelled` arrive from the saga, seconds later and out of band, which is why
  * `status` exists at all: there is no row shape in which "the order was accepted" and "the stock is ours" are the same
  * fact.
  */
final class OrderProjection[F[_]: Async] private (
  protected val repository: Repository[F, UUID, OrderView],
) extends Projection[F, OrderEvent, UUID, OrderView]:

  override val name: String = "order-projection"

  override val filter: Set[EventTypeName] = Set(
    EventTypeName.of[OrderPlaced],
    EventTypeName.of[OrderConfirmed],
    EventTypeName.of[OrderCancelled],
  )

  override def resolveKeys(event: EventEnvelope[OrderEvent]): List[UUID] = event.payload match
    case OrderPlaced(orderId, _, _, _, _) => List(orderId)
    case OrderConfirmed(orderId)          => List(orderId)
    case OrderCancelled(orderId, _)       => List(orderId)
    // Unreachable through `filter`, and deliberately no key rather than an error: customers are not orders, and an
    // empty list is how a projection says "not mine" without blocking the checkpoint.
    case _: CustomerRegistered => Nil

  override def handle(state: Option[OrderView], event: EventEnvelope[OrderEvent]): F[Option[OrderView]] =
    (state, event.payload) match
      case (None, OrderPlaced(orderId, customerId, itemId, amount, price)) =>
        OrderView(orderId, customerId, itemId, amount, OrderStatus.Placed, reason = None).some.pure[F]

      // Already recorded. `handle` has to tolerate seeing an event twice, and rebuilding this row from the event would
      // reset a status the saga has since decided — an order that was confirmed would silently go back to Placed.
      case (Some(existing), _: OrderPlaced) => existing.some.pure[F]

      case (Some(existing), OrderConfirmed(_)) =>
        existing.copy(status = OrderStatus.Confirmed, reason = None).some.pure[F]

      case (Some(existing), OrderCancelled(_, reason)) =>
        existing.copy(status = OrderStatus.Cancelled, reason = Some(reason)).some.pure[F]

      // A decision about an order that was never placed. Events arrive in append order, so the saga's own terminal
      // events cannot outrun their trigger — reaching here means the log and this table disagree about reality.
      case _ =>
        Async[F].raiseError(new RuntimeException(s"Unexpected event ${event.payload} for state $state"))

object OrderProjection:

  def make[F[_]: Async](repository: Repository[F, UUID, OrderView]): F[OrderProjection[F]] =
    Async[F].pure(new OrderProjection(repository))
