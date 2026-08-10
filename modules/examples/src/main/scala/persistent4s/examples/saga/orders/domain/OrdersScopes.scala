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

import persistent4s.Scope

object OrdersScopes:

  val Customer: Scope[UUID] = Scope[UUID]("customer")

  /** The scope every event about one order belongs to — including the two the saga appends later, which is how a
    * confirmation lands in the same history the order was placed in.
    *
    * Order events deliberately do '''not''' also declare [[Customer]]. They could, and a query "all orders of this
    * customer" would then be one read — but the customer scope is read by every `PlaceOrder`, so every one of a
    * customer's past orders would become a conflict source for their next one.
    */
  val Order: Scope[UUID] = Scope[UUID]("order")
