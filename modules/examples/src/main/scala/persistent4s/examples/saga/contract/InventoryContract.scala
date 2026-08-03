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

package persistent4s.examples.saga.contract

import java.util.UUID

import io.circe.{Decoder, Encoder}

import persistent4s.MessageCodec
import persistent4s.circe.CirceMessageCodec

// Everything the orders and inventory services agree on — and deliberately nothing more.
//
// These are message DTOs, not events. Neither service can name the other's event types, so neither can be coupled to
// how the other models its log: a reply arrives as one of these, and the saga turns it into an event of its own. The
// two services share a conversation, not a schema.

/** Reserve `amount` of `itemId` on behalf of `orderId`.
  *
  * `orderId` means nothing to the inventory service beyond being an opaque correlation id — it exists so inventory can
  * fold its own log and recognise a request it has already honoured. Delivery is at-least-once, so it *will* see the
  * same request twice.
  */
final case class ReserveStock(orderId: UUID, itemId: UUID, amount: Int) derives Encoder.AsObject, Decoder

object ReserveStock:

  given MessageCodec[ReserveStock] = CirceMessageCodec.derived[ReserveStock]

/** Inventory's answer. It carries no order id: correlating a reply with the instance that is waiting for it is the
  * runner's job, done with the `persistent4s.sagaId` header the partner echoes back, so the payload does not repeat it.
  */
final case class StockReservationReply(accepted: Boolean, reason: Option[String])
    derives Encoder.AsObject, Decoder

object StockReservationReply:

  val accept: StockReservationReply = StockReservationReply(accepted = true, reason = None)

  def reject(reason: String): StockReservationReply = StockReservationReply(accepted = false, reason = Some(reason))

  given MessageCodec[StockReservationReply] = CirceMessageCodec.derived[StockReservationReply]

/** Headers the two services agree on, beyond the reserved `persistent4s.*` ones the runner stamps itself. */
object RequestHeaders:

  /** The instant after which a request must no longer be honoured, as an ISO-8601 string.
    *
    * This makes the caller's deadline a '''shared fact''' instead of one side's private opinion. Without it the only
    * thing bounding staleness is the saga's own timer, which the partner cannot see: a request delivered late — the
    * partner was down, or the record sat on the topic — is honoured in full, and by then the saga may have compensated
    * and will drop the reply. The reservation is then held for an order that no longer exists, and nothing in this
    * example ever releases it.
    *
    * The cost is an assumption of bounded clock skew between the two services, which is a far weaker thing to ask for
    * than a distributed transaction.
    *
    * What it does '''not''' fix, measured rather than guessed:
    *   - A request honoured just before its expiry whose reply arrives just after the saga gave up. Narrow — the width of
    *     a reply in flight.
    *   - A request the partner '''declined''', then redelivered. The saga compensates the moment it reads a rejection, so
    *     its instance is terminal within milliseconds while the request stays honourable for the rest of its 30 seconds.
    *     Any redelivery inside that window is judged afresh — with no memory of the refusal, since a rejection writes no
    *     event — and is accepted if stock has arrived meanwhile. Nearly the whole window is exposed, so the expiry buys
    *     very little here.
    *
    * Closing the second case needs the refusal itself to leave a trace, so that a later request for the same order
    * collides with it: a terminal per-order record on the partner's side, not a deadline.
    */
  val ExpiresAt = "expiresAt"

/** Topics both services have to agree on.
  *
  * `OrdersReplies` is a detail the orders service owns and only *announces*: the runner stamps it onto every request as
  * `persistent4s.replyTo`, and inventory answers whatever address it was given rather than looking it up here. The
  * constant is shared for the reader's benefit, and so the reply consumer has something to subscribe to.
  */
object Topics:

  val InventoryCommands = "inventory.commands"

  val OrdersReplies = "orders.replies"
