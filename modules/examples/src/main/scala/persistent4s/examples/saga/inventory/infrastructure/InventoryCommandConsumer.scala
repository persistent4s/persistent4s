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

object InventoryCommandConsumer:

  private type MessagingStore[F[_]] = EventStore[F, InventoryEvent] & TransactionalMessages[F, InventoryEvent]

  def stream[F[_]: Async: Logger](
    subscriber: MessageSubscriber[F],
    store: MessagingStore[F],
    topic: String,
  ): Stream[F, Unit] =
    subscriber.subscribe(topic, fromBeginning = true).evalMap { case (message, ack) =>
      handle(store, message) *> ack
    }

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
      _ <-
        if addressed(message) then Async[F].unit
        else
          Logger[F].warn(
            s"command for order ${command.orderId} is not fully addressed " +
              s"(needs ${SagaHeaders.ReplyTo}, ${SagaHeaders.Name} and ${SagaHeaders.Id}); " +
              "honouring it as fire-and-forget, nobody will be answered",
          )
      receivedAt <- Clock[F].realTimeInstant
      result     <- ReserveStockHandler(message, receivedAt).runWithMessages[F](command)
      _          <- result match
             case Right(Nil) =>
               Logger[F].info(s"order ${command.orderId} was already reserved; re-answered without reserving again")
             case Right(_) =>
               Logger[F].info(
                 s"reserved ${command.amount} of item ${command.itemId} for order ${command.orderId}",
               )
             case Left(rejection) =>
               Logger[F].info(s"declined order ${command.orderId}: ${rejection.getMessage}")
    yield ()

  private def addressed(message: IncomingMessage): Boolean =
    List(SagaHeaders.ReplyTo, SagaHeaders.Name, SagaHeaders.Id).forall(message.headers.contains)

  private def release[F[_]: Async: Logger](store: MessagingStore[F], command: ReleaseStock): F[Unit] =
    given MessagingStore[F] = store
    ReleaseStockHandler.run[F](command).flatMap { events =>
      if events.isEmpty then Logger[F].info(s"order ${command.orderId} had nothing reserved to release; no-op")
      else Logger[F].info(s"released stock for order ${command.orderId}")
    }
