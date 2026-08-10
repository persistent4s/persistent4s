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

import java.time.Instant

import persistent4s.{EventSourcedSagaCommandHandler, PendingReply, RequestContext, SagaHeaders}
import persistent4s.examples.saga.contract.{PartnerReply, ReserveStock}
import persistent4s.examples.saga.inventory.domain.{
  InventoryEvent,
  InventoryScopes,
  ItemRestocked,
  StockReleased,
  StockReserved,
}

/** What one item's log says about a single incoming request.
  *
  * @param available
  *   everything restocked minus everything reserved
  * @param alreadyReserved
  *   how much this very `orderId` was already given, if the request has been honoured before. Folded per command, which
  *   is why the state is cheap: recognising a redelivery does not need a list of every order ever served.
  */
final case class StockState(available: Int, alreadyReserved: Option[Int]):

  /** Whether this exact request has already been served. Every rejection rule below is skipped when it has. */
  def honoured: Boolean = alreadyReserved.isDefined

/** Answers the orders service's `reserve-stock` saga.
  *
  * This is the whole reason the request has to travel: the invariant it protects — never promise stock twice — lives
  * here, in this log, under this item's optimistic-concurrency scope. No amount of checking on the orders side could
  * replace it, because two orders can pass the same check at the same time and only one append can win.
  *
  * A '''case class''' rather than an object because the address to answer arrives with the request, which is what
  * [[EventSourcedSagaCommandHandler.request]] carries. One instance per request, and with it one `behavior` — the
  * expiry rule below closes over that request, so the definition cannot be shared the way every other handler's is.
  */
final case class ReserveStockHandler(request: RequestContext)
    extends EventSourcedSagaCommandHandler[ReserveStock, StockState, InventoryEvent, ReserveStockHandler.Error]:

  import ReserveStockHandler.Error

  override protected val behavior = handler(StockState(available = 0, alreadyReserved = None)):
    scope(InventoryScopes.Item)(_.itemId)

    on[ItemRestocked].evolve((state, event) => state.copy(available = state.available + event.amount))

    // Deliberately not `matching(_.orderId, _.orderId)`, unlike [[ReleaseStockHandler]]: every reservation on this
    // item moves `available`, whoever it was for, and only the flag is about this order. Matching on the order here
    // would leave the arithmetic reading a stock nobody else had ever touched.
    on[StockReserved].evolve: (state, command, event) =>
      val taken = state.copy(available = state.available - event.amount)
      if event.orderId == command.orderId then taken.copy(alreadyReserved = Some(event.amount)) else taken

    on[StockReleased].evolve: (state, command, event) =>
      val added = state.copy(available = state.available + event.amount)
      if event.orderId == command.orderId then added.copy(alreadyReserved = None) else added

    // Every rule is guarded on `!state.honoured`, and that guard is the idempotency this request depends on. Old
    // `validate` said it once, by answering `Right` before it checked anything else; a rejection rule can only say
    // "no", so it has to be said three times. A redelivery has already had its own amount taken out of `available`,
    // so re-judging it would refuse the very request it succeeded at, and the saga would hear a rejection for stock
    // it holds.
    reject:
      case (state, _) if !state.honoured && request.hasExpired =>
        Error.Expired(request.message.headers.getOrElse(SagaHeaders.ExpiresAt, "?"), request.receivedAt)

      case (state, command) if !state.honoured && command.amount <= 0 =>
        Error.NotPositive(command.amount)

      case (state, command) if !state.honoured && state.available < command.amount =>
        Error.InsufficientStock(state.available, command.amount)

    // Nothing to write for a request already served, and nothing to fail either — the reply still goes out, which is
    // what makes a redelivery cost the caller one message instead of its whole deadline.
    emitMany: (state, command) =>
      if state.honoured then Nil
      else List(StockReserved(command.itemId, command.orderId, command.amount))

    // Always answers, and the rejection carries its reason: a caller that hears nothing has to wait out its whole
    // deadline to learn what a single message could have told it immediately. `reply` is `messages` with the
    // correlation headers filled in from the request, so this handler never touches `SagaHeaders` itself.
    reply: (_, _, outcome) =>
      Some(PendingReply(outcome.fold(error => PartnerReply.reject(error.message), _ => PartnerReply.accept)))

object ReserveStockHandler:

  enum Error:

    case Expired(expiresAt: String, receivedAt: Instant)

    case NotPositive(amount: Int)

    case InsufficientStock(available: Int, requested: Int)

    /** The whole of what the caller learns: this text travels back in the reply and lands in the order's `reason`
      * field. [[Expired]] carries both instants because "expired" alone leaves nobody able to say by how much.
      */
    def message: String = this match
      case Expired(expiresAt, receivedAt)          => s"request expired at $expiresAt, now $receivedAt"
      case NotPositive(amount)                     => s"reservation amount must be positive, got $amount"
      case InsufficientStock(available, requested) => s"insufficient stock: $available available, $requested requested"
