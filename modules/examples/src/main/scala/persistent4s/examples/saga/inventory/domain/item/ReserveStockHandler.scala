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

import persistent4s.{CommandHandler, EventTypeName, IncomingMessage, MessageCodec, OutgoingMessage, SagaHeaders, Tag}
import persistent4s.examples.saga.contract.{RequestHeaders, ReserveStock, StockReservationReply}
import persistent4s.examples.saga.inventory.domain.{
  InventoryEvent,
  InventoryTags,
  ItemRestocked,
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

  /** Both event types move stock, so both belong in the scope the concurrency check guards. A restock landing between
    * this handler's read and its append is a conflict worth retrying for — the retry re-reads and may now say yes.
    */
  override def eventTypes: Option[Set[EventTypeName]] =
    Some(Set(EventTypeName.of[ItemRestocked], EventTypeName.of[StockReserved]))

  def tags(command: ReserveStock): Set[Tag] = Set(InventoryTags.item(command.itemId))

  def initial: StockState = StockState(available = 0, alreadyReserved = None)

  def evolve(command: ReserveStock, state: StockState, event: InventoryEvent): StockState =
    event match
      case ItemRestocked(_, amount)          => state.copy(available = state.available + amount)
      case StockReserved(_, orderId, amount) =>
        val taken = state.copy(available = state.available - amount)
        if orderId == command.orderId then taken.copy(alreadyReserved = Some(amount)) else taken

  /** A request already honoured is valid, not a duplicate to be rejected — the sender is owed the same answer as the
    * first time, and history is honoured before any policy below it. [[decide]] is what makes it write nothing.
    *
    * Then [[RequestHeaders.ExpiresAt]]: past its expiry the request is declined without looking at stock at all, because
    * the caller has said it will no longer be listening. This is what keeps a late delivery — the partner was down, or
    * the record sat on the topic — from reserving for an order that has since been cancelled.
    *
    * It narrows that window rather than closing it, and it barely narrows one case at all: a request this handler
    * '''declined'''. The saga compensates as soon as it reads a rejection, so its instance is terminal within
    * milliseconds while the request stays honourable for the rest of its window — and a rejection writes no event, so a
    * redelivery inside that window is judged afresh and says yes if stock arrived meanwhile. That reserves stock for an
    * order cancelled precisely for lack of it. Reproduced deliberately; see the README.
    *
    * Closing it needs the refusal to leave a trace here, so a later request for the same order collides with it. Note
    * where that lands: a refusal cannot be recorded from [[validate]], because a rejection is *defined* as writing no
    * events, so it would have to become a command that succeeds and [[decide]]s a refusal event instead.
    */
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
    * An *unreadable* header, on the other hand, counts as expired. The header exists to bound how stale a request may be;
    * if it cannot be parsed then staleness is unknown, and honouring it anyway would quietly restore the very leak it was
    * added to narrow. Declining is also the loud option: the reason travels back and lands in the order's `reason` field.
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

  /** The reply, enqueued in the same transaction as the reservation it reports.
    *
    * Emitted on '''both''' paths, and that is the point of `runWithMessages`: a rejection writes no event but still owes
    * an answer. Without it the saga would learn nothing until its deadline expired, and every out-of-stock order would
    * cost 30 seconds before being cancelled for the wrong stated reason.
    */
  override def messages(
    state: StockState,
    command: ReserveStock,
    outcome: Either[Throwable, List[InventoryEvent]],
  ): List[OutgoingMessage] =
    val reply = outcome.fold(
      rejection => StockReservationReply.reject(rejection.getMessage),
      _ => StockReservationReply.accept,
    )
    // `messages` is pure, so an encoding failure has nowhere to go — `Saga` sidesteps this by keeping requests typed and
    // letting the runner encode them. Circe on two fields cannot fail, so this asserts the invariant rather than
    // pretending to handle it: quietly returning no message would drop the reply and leave the saga to time out.
    val payload = replyCodec
      .encode(reply)
      .fold(error => throw new IllegalStateException("StockReservationReply must be encodable", error), identity)
    // Keyed by the order, not by the item the request was keyed on: one saga instance's traffic belongs on one
    // partition. `SagaHeaders.reply` defaults to echoing the request's key, which is only the right default when the
    // request was keyed by the saga key — here it is keyed by item, so requests for one item stay ordered.
    //
    // Empty when the request carries no reply address, i.e. it did not come from a saga. Nothing to answer, so the
    // reservation just happens; the consumer logs it rather than letting it pass unremarked.
    SagaHeaders.reply(origin, payload, key = Some(command.orderId.toString)).toList

object ReserveStockHandler:

  private val replyCodec: MessageCodec[StockReservationReply] = summon[MessageCodec[StockReservationReply]]
