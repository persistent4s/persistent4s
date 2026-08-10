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
import persistent4s.examples.saga.contract.{ReserveStock, ReleaseStock, Topics}
import persistent4s.examples.saga.orders.domain.{OrderCancelled, OrderConfirmed, OrderPlaced}
import persistent4s.examples.saga.contract.PartnerReply
import persistent4s.examples.saga.contract.{AuthorizePayment, CancelPayment}
import persistent4s.examples.saga.orders.domain.OrderEvent
import persistent4s.examples.saga.orders.domain.OrdersScopes

/** What one partner answered.
  *
  * A sum rather than a `Boolean` beside an `Option[String]`, because a reason exists exactly when a partner declined
  * and that pair could also represent "accepted, and here is why it failed". Carrying the words rather than the verdict
  * is the point: they end up in the order's `reason` field, and the customer learns "insufficient stock: 3 available,
  * 99 requested" instead of "a partner declined".
  */
enum PartnerOutcome derives Encoder.AsObject, Decoder:

  case Accepted

  case Declined(reason: String)

  def declined: Boolean = this match
    case Declined(_) => true
    case Accepted    => false

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
  stock: Option[PartnerOutcome],
  payment: Option[PartnerOutcome],
) derives Encoder.AsObject,
      Decoder

object ReserveStockSaga extends Saga[OrderEvent, OrderState, OrderRequest, PartnerReply]:

  val name: String = "place-order"

  val triggers: Set[EventTypeName] = summon[EventSchema[OrderPlaced]].acceptedEventTypes

  private object Requests:

    val Stock = "stock"

    val Payment = "payment"

    val Release = "release"

    val Cancel = "cancel"

  val NoAttribution = "reply did not say which request it answered"

  /** Stands in when a partner says no without saying why. [[PartnerReply.reject]] always carries a reason, but the wire
    * format allows one without, and an order cancelled for no stated reason is the least useful row in the table.
    */
  val NoReason = "no reason given"

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
                label = Requests.Stock,
                topic = Topics.InventoryCommands,
                key = Some(itemId.toString),
                payload = ReserveStock(orderId, itemId, amount),
              ),
              SagaRequest(
                label = Requests.Payment,
                topic = Topics.PaymentCommands,
                key = Some(customerId.toString),
                payload = AuthorizePayment(orderId, customerId, price),
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
    val outcome =
      if reply.payload.accepted then PartnerOutcome.Accepted
      else PartnerOutcome.Declined(reply.payload.reason.getOrElse(NoReason))

    reply.answering.map(_.label) match
      case Some(Requests.Stock)   => settle(state.copy(stock = Some(outcome)))
      case Some(Requests.Payment) => settle(state.copy(payment = Some(outcome)))
      case Some(other)            =>
        SagaDecision.failed(s"reply named request `$other`, which this saga never sent")
      case None =>
        SagaDecision.failed(NoAttribution)

  private def settle(state: OrderState): SagaDecision[OrderEvent, OrderState, OrderRequest] =
    (state.stock, state.payment) match
      case (Some(PartnerOutcome.Accepted), Some(PartnerOutcome.Accepted)) =>
        SagaDecision.completed(events = List(orderTag(state.orderId) -> OrderConfirmed(state.orderId)))
      case (Some(stock), Some(payment)) =>
        SagaDecision.compensated(
          events = List(orderTag(state.orderId) -> OrderCancelled(state.orderId, declineReason(stock, payment))),
          messages = undoRequests(state),
        )
      case _ => SagaDecision.continue(state)

  /** The declining partner's own words, carried across the service boundary into the order's `reason` field. Both
    * partners can decline the same order, and then both reasons travel — dropping either would leave the row claiming a
    * single cause for a decision that had two.
    */
  private def declineReason(outcomes: PartnerOutcome*): String =
    outcomes.collect { case PartnerOutcome.Declined(reason) => reason }.mkString("; ")

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
      Option.unless(state.stock.exists(_.declined))(
        SagaRequest(
          Requests.Release,
          Topics.InventoryCommands,
          Some(state.itemId.toString),
          ReleaseStock(state.orderId, state.itemId),
        ),
      ),
      Option.unless(state.payment.exists(_.declined))(
        SagaRequest(
          Requests.Cancel,
          Topics.PaymentCommands,
          Some(state.customerId.toString),
          CancelPayment(state.orderId, state.customerId),
        ),
      ),
    ).flatten

  private def orderTag(orderId: UUID): Set[Tag] = Set(OrdersScopes.Order(orderId).toTag)

  val stateCodec: MessageCodec[OrderState] = CirceMessageCodec.derived[OrderState]

  val replyDecoder: MessageDecoder[PartnerReply] = summon[MessageCodec[PartnerReply]]
