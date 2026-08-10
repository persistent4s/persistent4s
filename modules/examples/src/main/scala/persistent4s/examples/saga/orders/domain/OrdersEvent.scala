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

package persistent4s.examples.saga.orders.domain

import java.util.UUID

import io.circe.{Decoder, Encoder}

import persistent4s.{Event, EventSchema}

sealed trait OrderEvent extends Event

final case class CustomerRegistered(customerId: UUID, name: String) extends OrderEvent derives Encoder, Decoder

object CustomerRegistered:

  given EventSchema[CustomerRegistered] =
    EventSchema[CustomerRegistered]("orders.customer-registered")
      .scopedBy(OrdersScopes.Customer)(_.customerId)

/** An order that exists but is not yet honoured. This is the event the saga triggers on. */
final case class OrderPlaced(orderId: UUID, customerId: UUID, itemId: UUID, amount: Int, price: Int) extends OrderEvent
    derives Encoder,
      Decoder

object OrderPlaced:

  given EventSchema[OrderPlaced] =
    EventSchema[OrderPlaced]("orders.order-placed")
      .scopedBy(OrdersScopes.Order)(_.orderId)

/** The stock was reserved. Written by the saga, from inventory's reply — never by inventory itself. */
final case class OrderConfirmed(orderId: UUID) extends OrderEvent derives Encoder, Decoder

object OrderConfirmed:

  given EventSchema[OrderConfirmed] =
    EventSchema[OrderConfirmed]("orders.order-confirmed")
      .scopedBy(OrdersScopes.Order)(_.orderId)

/** The stock was refused, or nobody answered in time. Also written by the saga. */
final case class OrderCancelled(orderId: UUID, reason: String) extends OrderEvent derives Encoder, Decoder

object OrderCancelled:

  given EventSchema[OrderCancelled] =
    EventSchema[OrderCancelled]("orders.order-cancelled")
      .scopedBy(OrdersScopes.Order)(_.orderId)
