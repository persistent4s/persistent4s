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

package persistent4s.examples.saga.orders.saga

import java.util.UUID

import scala.concurrent.duration.*

import io.circe.{Decoder, Encoder}

import persistent4s.*
import persistent4s.circe.CirceMessageCodec
import persistent4s.examples.saga.contract.{RequestHeaders, ReserveStock, StockReservationReply, Topics}
import persistent4s.examples.saga.orders.domain.{OrderCancelled, OrderConfirmed, OrderPlaced, OrdersEvent, OrdersTags}

/** What an instance carries while it waits.
  *
  * A single-step saga barely needs one — [[SagaContext]] already hands the decision functions the instance's key, which
  * here is the order id, so `onReply` could work from that alone. The item and amount are kept because they are what a
  * compensating `ReleaseStock` would need, and because `saga_instances.data` is the first place anyone looks when an
  * instance is stuck.
  */
final case class ReserveStockState(orderId: UUID, itemId: UUID, amount: Int) derives Encoder.AsObject, Decoder

/** Turns "an order was placed" into "the stock is ours, or the order is off".
  *
  * The three decision functions are pure and total: given a trigger, a reply, or a deadline, each returns the events to
  * append and where the instance goes next. Nothing here touches Kafka, the outbox, the instance table or a transaction —
  * [[SagaRunner]] owns all of that, which is why this file can be read as domain logic rather than plumbing.
  *
  * Note what the saga writes: [[OrderConfirmed]] and [[OrderCancelled]], events of ''this'' service's log. Inventory's
  * reply never becomes an event. The two services share the [[StockReservationReply]] DTO and nothing else, so inventory
  * can rename or restructure its own events freely and this file does not care.
  */
object ReserveStockSaga extends Saga[OrdersEvent, ReserveStockState, ReserveStock, StockReservationReply]:

  val name: String = "reserve-stock"

  val triggers: Set[EventTypeName] = Set(EventTypeName.of[OrderPlaced])

  /** Long enough that a busy inventory service is not written off, short enough that a client is not left guessing. This
    * is the only bound on how long an order can sit at `Placed`.
    */
  private val ReplyTimeout: FiniteDuration = 30.seconds

  def start(event: EventEnvelope[OrdersEvent]): Option[SagaStart[ReserveStockState, ReserveStock]] =
    event.payload match
      case OrderPlaced(orderId, _, itemId, amount) =>
        Some(
          SagaStart(
            // The instance id is derived from this key, so replaying the trigger lands on the same row and starts
            // nothing twice.
            key = orderId.toString,
            data = ReserveStockState(orderId, itemId, amount),
            request = List(
              SagaRequest(
                topic = Topics.InventoryCommands,
                // Keyed by item, not by order: requests for one item then share a partition and reach inventory in
                // order, which is what makes contention for the last unit a contest inventory can actually see. The
                // reply is keyed by order instead — the partner chooses that, since only it knows both ids.
                key = Some(itemId.toString),
                payload = ReserveStock(orderId, itemId, amount),
                // Tell the partner when to stop caring, so a request delivered late is declined instead of honoured for
                // an order this saga has already cancelled.
                //
                // Computed from the trigger event's own timestamp, because these functions are pure and cannot read a
                // clock. That makes the stated expiry very slightly *earlier* than the instance's real deadline, which
                // the runner stamps as `now + timeout` when the instance is created — and earlier is the safe side: the
                // partner gives up a hair before this saga does, never after.
                headers = Map(
                  RequestHeaders.ExpiresAt -> event.metadata.timestamp.plusMillis(ReplyTimeout.toMillis).toString,
                ),
              ),
            ),
            timeout = Some(ReplyTimeout),
          ),
        )
      // Unreachable — the trigger loop only reads `triggers` — but `start` is offered an envelope of the whole log's
      // event type, so the match has to be total.
      case _ => None

  def onReply(
    ctx: SagaContext,
    state: ReserveStockState,
    reply: StockReservationReply,
  ): SagaDecision[OrdersEvent, ReserveStockState, ReserveStock] =
    if reply.accepted then
      SagaDecision.completed(events = List(orderTag(state.orderId) -> OrderConfirmed(state.orderId)))
    else
      SagaDecision.compensated(events =
        List(
          orderTag(state.orderId) ->
            OrderCancelled(state.orderId, reply.reason.getOrElse("inventory declined the reservation")),
        ),
      )

  def onTimeout(ctx: SagaContext, state: ReserveStockState): SagaDecision[OrdersEvent, ReserveStockState, ReserveStock] =
    SagaDecision.compensated(events =
      List(orderTag(state.orderId) -> OrderCancelled(state.orderId, "inventory did not answer in time")),
    )

  /** A decision's events are tagged like any other event about this order, so the projection picks them up and a later
    * read of the order's scope sees them. The tag does no concurrency work here: the runner appends a decision's events
    * unchecked, and what stops a second confirmation is the instance's step guard plus the deterministic event ids.
    */
  private def orderTag(orderId: UUID): Set[Tag] = Set(OrdersTags.order(orderId))

  val stateCodec: MessageCodec[ReserveStockState] = CirceMessageCodec.derived[ReserveStockState]

  /** Both come from the contract's own givens — the only codec this saga has to derive is the one for its private state. */
  val requestCodec: MessageCodec[ReserveStock] = summon[MessageCodec[ReserveStock]]

  val replyCodec: MessageCodec[StockReservationReply] = summon[MessageCodec[StockReservationReply]]
