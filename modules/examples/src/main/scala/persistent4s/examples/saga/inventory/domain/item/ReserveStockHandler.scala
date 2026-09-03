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

import persistent4s.{PendingReply, SagaCommandHandler, SagaHeaders, Tag}
import persistent4s.examples.saga.contract.{ReserveStock, PartnerReply}
import persistent4s.examples.saga.inventory.domain.{InventoryEvent, InventoryTags, ItemRestocked, StockReserved}
import persistent4s.examples.saga.inventory.domain.StockReleased
import persistent4s.RequestContext

/** What one item's log says about a single incoming request.
  *
  * @param available
  *   everything restocked minus everything reserved
  * @param alreadyReserved
  *   how much this very `orderId` was already given, if the request has been honoured before. Folded per command, which
  *   is why the state is cheap: recognising a redelivery does not need a list of every order ever served.
  */
final case class StockState(available: Int, alreadyReserved: Option[Int])

/** Answers the orders service's `reserve-stock` saga.
  *
  * This is the whole reason the request has to travel: the invariant it protects — never promise stock twice — lives
  * here, in this log, under this item's optimistic-concurrency scope. No amount of checking on the orders side could
  * replace it, because two orders can pass the same check at the same time and only one append can win.
  *
  * A '''case class''' rather than an object because the address to answer arrives with the request, which is what
  * [[SagaCommandHandler.request]] carries — one instance per request, which is free.
  */
final case class ReserveStockHandler(request: RequestContext)
    extends SagaCommandHandler[ReserveStock, StockState, InventoryEvent]:

  def tags(command: ReserveStock): Set[Tag] = Set(InventoryTags.item(command.itemId))

  def initial: StockState = StockState(available = 0, alreadyReserved = None)

  def evolve(command: ReserveStock, state: StockState, event: InventoryEvent): StockState =
    event match
      case ItemRestocked(_, amount)          => state.copy(available = state.available + amount)
      case StockReserved(_, orderId, amount) =>
        val taken = state.copy(available = state.available - amount)
        if orderId == command.orderId then taken.copy(alreadyReserved = Some(amount)) else taken
      case StockReleased(_, orderId, amount) =>
        val added = state.copy(available = state.available + amount)
        if orderId == command.orderId then added.copy(alreadyReserved = None) else added

  def validate(state: StockState, command: ReserveStock): Either[Throwable, Unit] =
    if state.alreadyReserved.isDefined then Right(())
    else if request.hasExpired then
      Left(
        // Both instants, because this text is the whole of what the caller learns: it travels back in the reply and
        // lands in the order's `reason` field, where "expired" alone leaves nobody able to say by how much.
        new IllegalStateException(
          s"request expired at ${request.message.headers.getOrElse(SagaHeaders.ExpiresAt, "?")}, " +
            s"now ${request.receivedAt}",
        ),
      )
    else if command.amount <= 0 then Left(new IllegalArgumentException("Reservation amount must be positive"))
    else if state.available < command.amount then
      Left(
        new IllegalStateException(
          s"insufficient stock: ${state.available} available, ${command.amount} requested",
        ),
      )
    else Right(())

  def decide(state: StockState, command: ReserveStock): List[(Set[Tag], InventoryEvent)] =
    if state.alreadyReserved.isDefined then Nil
    else
      List(
        Set(InventoryTags.item(command.itemId)) ->
          StockReserved(command.itemId, command.orderId, command.amount),
      )

  /** Always answers, and the rejection carries its reason: a caller that hears nothing has to wait out its whole
    * deadline to learn what a single message could have told it immediately.
    */
  override def reply(
    state: StockState,
    command: ReserveStock,
    outcome: Either[Throwable, List[InventoryEvent]],
  ): Option[PendingReply] =
    Some(
      PendingReply(
        outcome.fold(rejection => PartnerReply.reject(rejection.getMessage), _ => PartnerReply.accept),
      ),
    )
