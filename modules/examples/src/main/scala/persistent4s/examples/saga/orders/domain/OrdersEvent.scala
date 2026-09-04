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

import persistent4s.{Event, Tag}

sealed trait OrderEvent extends Event

final case class CustomerRegistered(customerId: UUID, name: String) extends OrderEvent derives Encoder, Decoder

/** An order that exists but is not yet honoured.
  *
  * This is the event the saga triggers on, and the reason the read model needs a status: at the moment it is written
  * nobody knows whether the stock is there. It is a promise to find out, not a promise to deliver.
  */
final case class OrderPlaced(orderId: UUID, customerId: UUID, itemId: UUID, amount: Int, price: Int) extends OrderEvent
    derives Encoder,
      Decoder

/** The stock was reserved. Written by the saga, from inventory's reply — never by inventory itself. */
final case class OrderConfirmed(orderId: UUID) extends OrderEvent derives Encoder, Decoder

/** The stock was refused, or nobody answered in time. Also written by the saga. */
final case class OrderCancelled(orderId: UUID, reason: String) extends OrderEvent derives Encoder, Decoder

object OrdersTags:

  def customer(customerId: UUID): Tag = Tag("customer", customerId)

  /** The tag every event about one order carries — including the two the saga appends later, which is how a
    * confirmation lands in the same scope the order was placed in.
    *
    * Order events deliberately do '''not''' also carry their customer's tag. They could, and a query "all orders of
    * this customer" would then be one read — but the customer tag is in the concurrency scope of every `PlaceOrder`, so
    * every one of a customer's past orders would become a conflict source for their next one.
    */
  def order(orderId: UUID): Tag = Tag("order", orderId)
