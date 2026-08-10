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

package persistent4s.examples.saga.orders.domain.customer

import java.util.UUID

import persistent4s.EventSourcedCommandHandler
import persistent4s.examples.saga.orders.domain.{CustomerRegistered, OrderEvent, OrdersScopes}

final case class RegisterCustomer(customerId: UUID, name: String)

object RegisterCustomer:

  enum Error:

    case AlreadyRegistered(customerId: UUID)

    case BlankName

  object Handler extends EventSourcedCommandHandler[RegisterCustomer, Boolean, OrderEvent, Error]:

    override protected val behavior = handler(initial = false):
      scope(OrdersScopes.Customer)(_.customerId)

      on[CustomerRegistered].evolve(_ => true)

      reject:
        case (true, command)                        => Error.AlreadyRegistered(command.customerId)
        case (_, command) if command.name.isBlank() => Error.BlankName

      emit: command =>
        CustomerRegistered(command.customerId, command.name)
