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

import persistent4s.{IncomingMessage, MessageCodec, MessagePublisher, MessageSubscriber, SagaHeaders}
import persistent4s.examples.saga.contract.{AuthorizePayment, CancelPayment, PartnerReply, RequestHeaders}

/** A stub payment partner — just enough to exercise the fan-out saga end to end, not a real payments domain.
  *
  * No state, no event store, no [[persistent4s.CommandHandler]]: authorising is a pure function of `price`, so there is
  * nothing here a redelivery could compute differently, and nothing a cancellation needs to look up before doing
  * nothing. That is also why replies go out through a plain [[MessagePublisher]] rather than a transactional outbox —
  * there is no append to make atomic with the send, unlike inventory's `StockReserved`.
  */
object PaymentCommandConsumer:

  private val replyCodec: MessageCodec[PartnerReply] = summon[MessageCodec[PartnerReply]]

  def stream[F[_]: Async: Logger](
    subscriber: MessageSubscriber[F],
    publisher: MessagePublisher[F],
    topic: String,
  ): Stream[F, Unit] =
    subscriber.subscribe(topic, fromBeginning = true).evalMap { case (message, ack) =>
      handle(publisher, message) *> ack
    }

  private def handle[F[_]: Async: Logger](publisher: MessagePublisher[F], message: IncomingMessage): F[Unit] =
    message.headers.get(RequestHeaders.Kind) match
      case Some(AuthorizePayment.Kind) =>
        message.as[AuthorizePayment] match
          case Left(error)    => decodeFailed(message, error)
          case Right(command) => authorize(publisher, message, command)
      case Some(CancelPayment.Kind) =>
        message.as[CancelPayment] match
          case Left(error)    => decodeFailed(message, error)
          case Right(command) => cancel(command)
      case other =>
        Logger[F].error(
          s"payment does not recognise command kind '$other' from '${message.topic}', dropping it: ${message.payload}",
        )

  private def decodeFailed[F[_]: Logger](message: IncomingMessage, error: Throwable): F[Unit] =
    Logger[F].error(error)(
      s"payment could not decode a command from '${message.topic}', dropping it: ${message.payload}",
    )

  /** Accepts an even price, declines an odd one — arbitrary on purpose, just something a caller can pick by hand to
    * choose which branch of the saga runs.
    */
  private def authorize[F[_]: Async: Logger](
    publisher: MessagePublisher[F],
    message: IncomingMessage,
    command: AuthorizePayment,
  ): F[Unit] =
    val reply =
      if command.price % 2 == 0 then PartnerReply.accept
      else PartnerReply.reject(s"price ${command.price} is odd")
    for
      _ <- Logger[F].info(
             s"AuthorizePayment: order ${command.orderId}, customer ${command.customerId}, price ${command.price} -> " +
               reply.reason.fold("accepted")(reason => s"declined: $reason"),
           )
      // `messages` on a CommandHandler asserts the same way for the same reason: this is pure code with nowhere to
      // report an encoding failure, and PartnerReply on two fields cannot fail to encode.
      payload = replyCodec
                  .encode(reply)
                  .fold(error => throw new IllegalStateException("PartnerReply must be encodable", error), identity)
      _ <- SagaHeaders.reply(message, payload, key = Some(command.orderId.toString)) match
             case Some(out) => publisher.publish(out)
             case None      =>
               Logger[F].warn(s"AuthorizePayment for order ${command.orderId} is not addressed; nobody will be answered")
    yield ()

  /** Does nothing but log — there is no authorization record here to look up, so there is nothing to release, and
    * nothing a redelivered cancellation could double-undo either. No reply: this only ever arrives as part of a
    * terminal decision, and the runner drops a reply to an instance that is no longer pending.
    */
  private def cancel[F[_]: Logger](command: CancelPayment): F[Unit] =
    Logger[F].info(s"CancelPayment: order ${command.orderId}, customer ${command.customerId} — no-op")
