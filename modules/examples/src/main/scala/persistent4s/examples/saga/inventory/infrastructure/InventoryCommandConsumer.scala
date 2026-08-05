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

package persistent4s.examples.saga.inventory.infrastructure

import cats.effect.{Async, Clock}
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.log4cats.Logger

import persistent4s.{EventStore, IncomingMessage, MessageSubscriber, SagaHeaders, TransactionalMessages}
import persistent4s.examples.saga.contract.ReserveStock
import persistent4s.examples.saga.inventory.domain.InventoryEvent
import persistent4s.examples.saga.inventory.domain.item.ReserveStockHandler
import persistent4s.examples.saga.contract.RequestHeaders
import persistent4s.examples.saga.contract.ReleaseStock
import persistent4s.examples.saga.inventory.domain.item.ReleaseStockHandler

/** The whole partner side of the saga: read a command, run the handler, acknowledge.
  *
  * There is no saga machinery here at all. Answering a saga needs nothing but a [[MessageSubscriber]], a handler that
  * emits its reply through [[SagaHeaders.reply]], and a message outbox to carry it — which is the point. A service can
  * take part in someone else's saga without knowing that a saga is what it is taking part in.
  */
object InventoryCommandConsumer:

  /** What `runWithMessages` needs: a store that appends events and enqueues messages in one transaction. */
  private type MessagingStore[F[_]] = EventStore[F, InventoryEvent] & TransactionalMessages[F, InventoryEvent]

  def stream[F[_]: Async: Logger](
    subscriber: MessageSubscriber[F],
    store: MessagingStore[F],
    topic: String,
  ): Stream[F, Unit] =
    subscriber.subscribe(topic, fromBeginning = true).evalMap { case (message, ack) =>
      handle(store, message) *> ack
    }

  /** Mirrors the ack policy [[persistent4s.SagaRunner]] applies to replies, for the same reasons.
    *
    * A decode failure is permanent — a redelivery would fail identically — so it is logged and acked rather than left
    * to block the partition forever. Everything else propagates, `ack` never runs, and the broker redelivers: the
    * handler is idempotent, so a second attempt is safe, and if it keeps failing the requester's deadline compensates.
    */
  private def handle[F[_]: Async: Logger](store: MessagingStore[F], message: IncomingMessage): F[Unit] =
    message.headers.get(RequestHeaders.Kind) match
      case Some(ReserveStock.Kind) =>
        message.as[ReserveStock] match
          case Left(error)    => decodeFailed(message, error)
          case Right(command) => reserve(store, message, command)
      case Some(ReleaseStock.Kind) =>
        message.as[ReleaseStock] match
          case Left(error)    => decodeFailed(message, error)
          case Right(command) => release(store, command)
      case other =>
        Logger[F].error(
          s"inventory does not recognise command kind '$other' from '${message.topic}', dropping it: ${message.payload}'",
        )

  private def decodeFailed[F[_]: Logger](message: IncomingMessage, error: Throwable): F[Unit] =
    Logger[F].error(error)(
      s"inventory could not decode a command from '${message.topic}', dropping it: ${message.payload}",
    )

  private def reserve[F[_]: Async: Logger](
    store: MessagingStore[F],
    message: IncomingMessage,
    command: ReserveStock,
  ): F[Unit] =
    given MessagingStore[F] = store
    for
      // The handler answers whoever asked, and answers nobody if the request is not addressed. That is legitimate — a
      // plain fire-and-forget command — but it is also what a misconfigured caller looks like, so say it out loud.
      //
      // All three headers, not just the address: `SagaHeaders.reply` needs the correlation pair as well, and returns
      // nothing if any one of them is missing. Checking only `replyTo` would stay quiet about the case most likely to be
      // a bug — an addressed request whose correlation is incomplete, which gets silently dropped on the floor.
      _ <-
        if addressed(message) then Async[F].unit
        else
          Logger[F].warn(
            s"command for order ${command.orderId} is not fully addressed " +
              s"(needs ${SagaHeaders.ReplyTo}, ${SagaHeaders.Name} and ${SagaHeaders.Id}); " +
              "honouring it as fire-and-forget, nobody will be answered",
          )
      // Read once, here, and handed to the handler as data: judging whether the request is stale is a decision, and
      // decisions in a `CommandHandler` are pure.
      receivedAt <- Clock[F].realTimeInstant
      result     <- ReserveStockHandler(message, receivedAt).runWithMessages[F](command)
      _          <- result match
             // Accepted, but nothing written: this request had already been honoured, so it got the same answer again.
             case Right(Nil) =>
               Logger[F].info(s"order ${command.orderId} was already reserved; re-answered without reserving again")
             case Right(_) =>
               Logger[F].info(
                 s"reserved ${command.amount} of item ${command.itemId} for order ${command.orderId}",
               )
             // Not a failure — a decision, already on its way back to the asker.
             case Left(rejection) =>
               Logger[F].info(s"declined order ${command.orderId}: ${rejection.getMessage}")
    yield ()

  /** Whether [[SagaHeaders.reply]] will be able to build an answer to this message. */
  private def addressed(message: IncomingMessage): Boolean =
    List(SagaHeaders.ReplyTo, SagaHeaders.Name, SagaHeaders.Id).forall(message.headers.contains)

  private def release[F[_]: Async: Logger](store: MessagingStore[F], command: ReleaseStock): F[Unit] =
    given MessagingStore[F] = store
    ReleaseStockHandler.run[F](command).flatMap { events =>
      if events.isEmpty then Logger[F].info(s"order ${command.orderId} had nothing reserved to release; no-op")
      else Logger[F].info(s"released stock for order ${command.orderId}")
    }
