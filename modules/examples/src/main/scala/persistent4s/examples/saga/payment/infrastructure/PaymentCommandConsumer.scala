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

package persistent4s.examples.saga.payment.infrastructure

import cats.effect.Async
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.log4cats.Logger

import persistent4s.{MessagePublisher, MessageSubscriber}
import persistent4s.examples.saga.contract.{AuthorizePayment, CancelPayment, PartnerReply}
import persistent4s.SagaParticipant
import persistent4s.RequestContext

/** A stub payment partner — just enough to exercise the fan-out saga end to end, not a real payments domain.
  *
  * No state, no event store, no [[persistent4s.CommandHandler]]: authorising is a pure function of `price`, so there is
  * nothing here a redelivery could compute differently, and nothing a cancellation needs to look up before doing
  * nothing. That is also why replies go out through a plain [[MessagePublisher]] rather than a transactional outbox —
  * there is no append to make atomic with the send, unlike inventory's `StockReserved`.
  */
object PaymentCommandConsumer:

  def stream[F[_]: Async: Logger](
    subscriber: MessageSubscriber[F],
    publisher: MessagePublisher[F],
    topic: String,
  ): Stream[F, Unit] =
    SagaParticipant[F]
      .replying[AuthorizePayment, PartnerReply](publisher)((ctx, command) => authorize(ctx, command))
      .on[CancelPayment]((_, command) => cancel(command))
      .subscribe(subscriber, topic)

  /** Accepts an even price, declines an odd one — arbitrary on purpose, just something a caller can pick by hand to
    * choose which branch of the saga runs.
    *
    * Returns the answer rather than sending it: encoding it, addressing it and publishing it are all the same in every
    * partner, so `replying` does them. What is left is the only part that belongs to payment.
    */
  private def authorize[F[_]: Async: Logger](
    ctx: RequestContext,
    command: AuthorizePayment,
  ): F[PartnerReply] =
    val answer =
      if ctx.hasExpired then PartnerReply.reject("request expired")
      else if command.price % 2 == 0 then PartnerReply.accept
      else PartnerReply.reject(s"price ${command.price} is odd")
    Logger[F]
      .info(
        s"AuthorizePayment: order ${command.orderId}, customer ${command.customerId}, price ${command.price} -> " +
          answer.reason.fold("accepted")(reason => s"declined: $reason"),
      )
      .as(answer)

  /** Does nothing but log — there is no authorization record here to look up, so there is nothing to release, and
    * nothing a redelivered cancellation could double-undo either. No reply: this only ever arrives as part of a
    * terminal decision, and the runner drops a reply to an instance that is no longer pending.
    */
  private def cancel[F[_]: Logger](command: CancelPayment): F[Unit] =
    Logger[F].info(s"CancelPayment: order ${command.orderId}, customer ${command.customerId} — no-op")
