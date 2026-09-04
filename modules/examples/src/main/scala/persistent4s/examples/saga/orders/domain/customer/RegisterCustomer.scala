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

import persistent4s.{CommandHandler, EventTypeName, Tag}
import persistent4s.examples.saga.orders.domain.{CustomerRegistered, OrderEvent, OrdersTags}

final case class RegisterCustomer(customerId: UUID, name: String)

final case class RegisterCustomerState(exists: Boolean)

object RegisterCustomerHandler extends CommandHandler[RegisterCustomer, RegisterCustomerState, OrderEvent]:

  override def eventTypes: Option[Set[EventTypeName]] = Some(Set(EventTypeName.of[CustomerRegistered]))

  def tags(command: RegisterCustomer): Set[Tag] = Set(OrdersTags.customer(command.customerId))

  def initial: RegisterCustomerState = RegisterCustomerState(exists = false)

  def evolve(command: RegisterCustomer, state: RegisterCustomerState, event: OrderEvent): RegisterCustomerState =
    event match
      case _: CustomerRegistered => state.copy(exists = true)
      case _                     => state

  def validate(state: RegisterCustomerState, command: RegisterCustomer): Either[Throwable, Unit] =
    if state.exists then Left(new IllegalStateException(s"Customer already registered: ${command.customerId}"))
    else if command.name.isBlank then Left(new IllegalArgumentException("Customer name must not be blank"))
    else Right(())

  def decide(state: RegisterCustomerState, command: RegisterCustomer): List[(Set[Tag], OrderEvent)] =
    List(
      Set(OrdersTags.customer(command.customerId)) -> CustomerRegistered(command.customerId, command.name),
    )
