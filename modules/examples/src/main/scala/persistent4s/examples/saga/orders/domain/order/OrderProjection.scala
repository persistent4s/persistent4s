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

import cats.ApplicativeThrow

import persistent4s.*
import persistent4s.examples.saga.orders.domain.{OrderCancelled, OrderConfirmed, OrderEvent, OrderPlaced, OrdersScopes}

/** The read model an order-status API is served from — and the reason a saga forces you to have one. */
final class OrderProjection[F[_]: ApplicativeThrow](
  protected val repository: AtomicRepository[F, UUID, OrderView],
) extends ExactlyOnceEventSourcedProjection[F, OrderEvent, UUID, OrderView]:

  override val name: String = "order-projection"

  override protected val eventHandlers = handlersBy(OrdersScopes.Order):

    on[OrderPlaced].upsert: (existing, event) =>
      existing.getOrElse(
        OrderView(event.orderId, event.customerId, event.itemId, event.amount, OrderStatus.Placed, reason = None),
      )

    on[OrderConfirmed].update: (state, _) =>
      state.copy(status = OrderStatus.Confirmed, reason = None)

    on[OrderCancelled].update: (state, event) =>
      state.copy(status = OrderStatus.Cancelled, reason = Some(event.reason))
