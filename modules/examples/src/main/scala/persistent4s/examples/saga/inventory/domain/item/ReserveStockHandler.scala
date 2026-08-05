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

import scala.util.Try

import persistent4s.{CommandHandler, IncomingMessage, MessageCodec, OutgoingMessage, SagaHeaders, Tag}
import persistent4s.examples.saga.contract.{RequestHeaders, ReserveStock, PartnerReply}
import persistent4s.examples.saga.inventory.domain.{InventoryEvent, InventoryTags, ItemRestocked, StockReserved}
import persistent4s.examples.saga.inventory.domain.StockReleased

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
  * A '''case class''' rather than an object because the address to answer arrives with the request. `messages` is pure
  * and gets only the state and the command, so the origin message has to be closed over instead of passed in — which
  * also means one instance per request, which is free.
  *
  * @param receivedAt
  *   when this request was picked up. Passed in rather than read here so [[validate]] stays pure while still being able
  *   to judge whether the request is stale: the clock is read once, in the consumer, and travels as data.
  */
final case class ReserveStockHandler(origin: IncomingMessage, receivedAt: Instant)
    extends CommandHandler[ReserveStock, StockState, InventoryEvent]:

  import ReserveStockHandler.replyCodec

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
    else if expired then
      Left(
        new IllegalStateException(
          s"request expired at ${origin.headers.getOrElse(RequestHeaders.ExpiresAt, "?")}, now $receivedAt",
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

  /** Whether the request's stated expiry has passed.
    *
    * No header means no expiry — a caller that never set one gets the old behaviour, which is right for a plain
    * fire-and-forget command that nobody is waiting on.
    *
    * An *unreadable* header, on the other hand, counts as expired. The header exists to bound how stale a request may
    * be; if it cannot be parsed then staleness is unknown, and honouring it anyway would quietly restore the very leak
    * it was added to narrow. Declining is also the loud option: the reason travels back and lands in the order's
    * `reason` field.
    */
  private def expired: Boolean =
    origin.headers.get(RequestHeaders.ExpiresAt) match
      case None        => false
      case Some(value) => Try(Instant.parse(value)).toOption.forall(receivedAt.isAfter)

  def decide(state: StockState, command: ReserveStock): List[(Set[Tag], InventoryEvent)] =
    if state.alreadyReserved.isDefined then Nil
    else
      List(
        Set(InventoryTags.item(command.itemId)) ->
          StockReserved(command.itemId, command.orderId, command.amount),
      )

  override def messages(
    state: StockState,
    command: ReserveStock,
    outcome: Either[Throwable, List[InventoryEvent]],
  ): List[OutgoingMessage] =
    val reply = outcome.fold(
      rejection => PartnerReply.reject(rejection.getMessage),
      _ => PartnerReply.accept,
    )

    val payload = replyCodec
      .encode(reply)
      .fold(error => throw new IllegalStateException("StockReservationReply must be encodable", error), identity)

    SagaHeaders.reply(origin, payload, key = Some(command.orderId.toString)).toList

object ReserveStockHandler:

  private val replyCodec: MessageCodec[PartnerReply] = summon[MessageCodec[PartnerReply]]
