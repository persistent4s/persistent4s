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

import persistent4s.{CommandRuntime, MessageSubscriber, RequestContext, SagaParticipant, TransactionalCommandRuntime}
import persistent4s.examples.saga.contract.{ReleaseStock, ReserveStock}
import persistent4s.examples.saga.inventory.domain.InventoryEvent
import persistent4s.examples.saga.inventory.domain.item.{ReleaseStockHandler, ReserveStockHandler}

object InventoryCommandConsumer:

  /** `commands` is an ordinary parameter rather than a `using` one on purpose: [[CommandRuntime]] has a `given` derived
    * from any in-scope `EventStore`, so an implicit one would silently resolve to a runtime with no snapshots and no
    * telemetry the moment this argument was forgotten.
    */
  def stream[F[_]: Async: Logger](
    subscriber: MessageSubscriber[F],
    topic: String,
    commands: TransactionalCommandRuntime[F, InventoryEvent],
  ): Stream[F, Unit] =
    SagaParticipant[F]
      .on[ReserveStock]((ctx, command) => reserve(ctx, command, commands))
      .on[ReleaseStock]((_, command) => release(command, commands.plain))
      .subscribe(subscriber, topic)

  /** The transactional runtime, because the reply has to be enqueued in the transaction that appends the reservation:
    * "the stock is reserved" and "I told them it is reserved" become true together or not at all.
    */
  private def reserve[F[_]: Async: Logger](
    ctx: RequestContext,
    command: ReserveStock,
    commands: TransactionalCommandRuntime[F, InventoryEvent],
  ): F[Unit] =
    commands.execute(ReserveStockHandler(ctx), command).flatMap {
      case Right(Nil) =>
        Logger[F].info(s"order ${command.orderId} was already reserved; re-answered without reserving again")
      case Right(_) =>
        Logger[F].info(s"reserved ${command.amount} of item ${command.itemId} for order ${command.orderId}")
      case Left(rejection) => Logger[F].info(s"declined order ${command.orderId}: ${rejection.message}")
    }

  /** `plain`, because a compensation answers nobody: the saga moved to Compensated when it sent this, and is not
    * waiting on a reply. Nothing to enqueue means no reason to reach for the transactional path.
    */
  private def release[F[_]: Async: Logger](
    command: ReleaseStock,
    commands: CommandRuntime[F, InventoryEvent],
  ): F[Unit] =
    commands.execute(ReleaseStockHandler, command).flatMap {
      case Right(events) if events.nonEmpty => Logger[F].info(s"released stock for order ${command.orderId}")
      case _                                => Logger[F].info(s"order ${command.orderId} had nothing reserved to release; no-op")
    }
