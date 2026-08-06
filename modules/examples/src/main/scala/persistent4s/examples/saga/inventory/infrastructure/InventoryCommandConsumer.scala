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

import cats.effect.Async
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.log4cats.Logger

import persistent4s.{EventStore, MessageSubscriber, TransactionalMessages}
import persistent4s.examples.saga.contract.ReserveStock
import persistent4s.examples.saga.inventory.domain.InventoryEvent
import persistent4s.examples.saga.inventory.domain.item.ReserveStockHandler
import persistent4s.examples.saga.contract.ReleaseStock
import persistent4s.examples.saga.inventory.domain.item.ReleaseStockHandler
import persistent4s.SagaParticipant
import persistent4s.RequestContext

object InventoryCommandConsumer:

  private type MessagingStore[F[_]] = EventStore[F, InventoryEvent] & TransactionalMessages[F, InventoryEvent]

  def stream[F[_]: Async: Logger](
    subscriber: MessageSubscriber[F],
    store: MessagingStore[F],
    topic: String,
  ): Stream[F, Unit] =
    given MessagingStore[F] = store
    SagaParticipant[F]
      .on[ReserveStock]((ctx, command) => reserve(ctx, command))
      .on[ReleaseStock]((_, command) => release(command))
      .subscribe(subscriber, topic)

  private def reserve[F[_]: Async: Logger](ctx: RequestContext, command: ReserveStock)(using
    MessagingStore[F],
  ): F[Unit] =
    ReserveStockHandler(ctx).runWithMessages[F](command).flatMap {
      case Right(Nil) =>
        Logger[F].info(s"order ${command.orderId} was already reserved; re-answered without reserving again")
      case Right(_) =>
        Logger[F].info(s"reserved ${command.amount} of item ${command.itemId} for order ${command.orderId}")
      case Left(rejection) => Logger[F].info(s"declined order ${command.orderId}: ${rejection.getMessage}")
    }

  private def release[F[_]: Async: Logger](command: ReleaseStock)(using MessagingStore[F]): F[Unit] =
    ReleaseStockHandler.run[F](command).flatMap { events =>
      if events.isEmpty then Logger[F].info(s"order ${command.orderId} had nothing reserved to release; no-op")
      else Logger[F].info(s"released stock for order ${command.orderId}")
    }
