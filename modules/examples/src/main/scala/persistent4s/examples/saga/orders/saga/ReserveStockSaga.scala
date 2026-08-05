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
import persistent4s.examples.saga.contract.{RequestHeaders, ReserveStock, ReleaseStock, Topics}
import persistent4s.examples.saga.orders.domain.{OrderCancelled, OrderConfirmed, OrderPlaced, OrdersTags}
import persistent4s.examples.saga.contract.PartnerReply
import persistent4s.examples.saga.contract.{AuthorizePayment, CancelPayment}
import persistent4s.examples.saga.orders.domain.OrderEvent

/** What an instance carries while it waits.
  *
  * A single-step saga barely needs one — [[SagaContext]] already hands the decision functions the instance's key, which
  * here is the order id, so `onReply` could work from that alone. The item and amount are kept because they are what a
  * compensating `ReleaseStock` would need, and because `saga_instances.data` is the first place anyone looks when an
  * instance is stuck.
  */
final case class OrderState(
  orderId: UUID,
  customerId: UUID,
  itemId: UUID,
  amount: Int,
  price: Int,
  stockSuccess: Option[Boolean],
  paymentSuccess: Option[Boolean],
) derives Encoder.AsObject,
      Decoder

object ReserveStockSaga extends Saga[OrderEvent, OrderState, OrderRequest, PartnerReply]:

  val name: String = "place-order"

  val triggers: Set[EventTypeName] = Set(EventTypeName.of[OrderPlaced])

  private val StockOrdinal = 0

  private val PaymentOrdinal = 1

  val NoAttribution = "reply did not say which request it answered"

  private val ReplyTimeout: FiniteDuration = 30.seconds

  override def start(event: EventEnvelope[OrderEvent]): Option[SagaStart[OrderState, OrderRequest]] =
    event.payload match
      case OrderPlaced(orderId, customerId, itemId, amount, price) =>
        Some(
          SagaStart(
            key = orderId.toString,
            data = OrderState(orderId, customerId, itemId, amount, price, None, None),
            request = List(
              SagaRequest(
                topic = Topics.InventoryCommands,
                key = Some(itemId.toString),
                payload = ReserveStock(orderId, itemId, amount),
                headers = Map(
                  RequestHeaders.ExpiresAt -> event.metadata.timestamp.plusMillis(ReplyTimeout.toMillis).toString,
                  RequestHeaders.Kind      -> ReserveStock.Kind,
                ),
              ),
              SagaRequest(
                topic = Topics.PaymentCommands,
                key = Some(customerId.toString),
                payload = AuthorizePayment(orderId, customerId, price),
                headers = Map(
                  RequestHeaders.ExpiresAt -> event.metadata.timestamp.plusMillis(ReplyTimeout.toMillis).toString,
                  RequestHeaders.Kind      -> AuthorizePayment.Kind,
                ),
              ),
            ),
            timeout = Some(ReplyTimeout),
          ),
        )
      case _ => None

  override def onReply(
    ctx: SagaContext,
    state: OrderState,
    reply: SagaReply[PartnerReply],
  ): SagaDecision[OrderEvent, OrderState, OrderRequest] =
    reply.answering.map(_.ordinal) match
      case Some(StockOrdinal)   => settle(state.copy(stockSuccess = Some(reply.payload.accepted)))
      case Some(PaymentOrdinal) => settle(state.copy(paymentSuccess = Some(reply.payload.accepted)))
      case Some(other)          =>
        SagaDecision.failed(s"reply named request ordinal $other, which this saga never sent")
      case None =>
        SagaDecision.failed(NoAttribution)

  private def settle(state: OrderState): SagaDecision[OrderEvent, OrderState, OrderRequest] =
    (state.stockSuccess, state.paymentSuccess) match
      case (Some(true), Some(true)) =>
        SagaDecision.completed(events = List(orderTag(state.orderId) -> OrderConfirmed(state.orderId)))
      case (Some(_), Some(_)) =>
        SagaDecision.compensated(
          events = List(orderTag(state.orderId) -> OrderCancelled(state.orderId, "a partner declined")),
          messages = undoRequests(state),
        )
      case _ => SagaDecision.continue(state, timeout = Some(ReplyTimeout))

  override def onTimeout(
    ctx: SagaContext,
    state: OrderState,
  ): SagaDecision[OrderEvent, OrderState, OrderRequest] =
    SagaDecision.compensated(
      events =
        List(orderTag(state.orderId) -> OrderCancelled(state.orderId, "At least one partner did not answer on time")),
      messages = undoRequests(state),
    )

  private def undoRequests(state: OrderState): List[SagaRequest[OrderRequest]] =
    List(
      Option.unless(state.stockSuccess.contains(false))(
        SagaRequest(
          Topics.InventoryCommands,
          Some(state.itemId.toString),
          ReleaseStock(state.orderId, state.itemId),
          Map(
            RequestHeaders.Kind -> ReleaseStock.Kind,
          ),
        ),
      ),
      Option.unless(state.paymentSuccess.contains(false))(
        SagaRequest(
          Topics.PaymentCommands,
          Some(state.customerId.toString),
          CancelPayment(state.orderId, state.customerId),
          Map(
            RequestHeaders.Kind -> CancelPayment.Kind,
          ),
        ),
      ),
    ).flatten

  private def orderTag(orderId: UUID): Set[Tag] = Set(OrdersTags.order(orderId))

  val stateCodec: MessageCodec[OrderState] = CirceMessageCodec.derived[OrderState]

  val requestEncoder: MessageEncoder[OrderRequest] = OrderRequest.encoder

  val replyDecoder: MessageDecoder[PartnerReply] = summon[MessageCodec[PartnerReply]]
