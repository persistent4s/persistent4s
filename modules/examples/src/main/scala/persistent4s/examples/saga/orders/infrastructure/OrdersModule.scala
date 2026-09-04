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

package persistent4s.examples.saga.orders.infrastructure

import cats.effect.*
import fs2.io.net.Network
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import pureconfig.ConfigSource
import skunk.*

given Tracer[IO] = Tracer.Implicits.noop

given Meter[IO] = Meter.Implicits.noop

given Logger[IO] = Slf4jLogger.getLogger[IO]

import persistent4s.*
import persistent4s.circe.CirceEventCodec
import persistent4s.examples.saga.contract.Topics
import persistent4s.examples.saga.orders.domain.OrderEvent
import persistent4s.examples.saga.orders.domain.order.{OrderProjection, OrderRepository}
import persistent4s.examples.saga.orders.saga.ReserveStockSaga
import persistent4s.kafka.{KafkaConsumerConfig, KafkaMessageProducerConfig, KafkaModule}
import persistent4s.postgres.{PostgresConfig, PostgresModule, PostgresSagaRepository}

final class OrdersModule private (
  val store: EventStore[IO, OrderEvent] & EventNotification[IO],
  val orderRepository: OrderRepository[IO],
  val sagaRepository: PostgresSagaRepository[IO],
  val commandMetrics: CommandHandlerMetrics[IO],
)

/** Wiring for the service that hosts the saga.
  *
  * `enableSaga = true` is doing two things: it creates `saga_instances`, and it brings the message outbox with it,
  * because a saga that cannot enqueue a request atomically with the events that caused it is not a saga. The event
  * outbox stays off — the orders log never leaves this service either.
  *
  * Three background loops end up running: the projector (read model), the message relay (requests out), and the saga
  * runner, which is itself three loops — trigger, reply and timer.
  */
object OrdersModule:

  val eventCodec: EventCodec[OrderEvent] = CirceEventCodec.derived[OrderEvent]

  private val pgConfigPath = "persistent4s.orders.postgres"

  private val kafkaConfigPath = "persistent4s.saga.kafka"

  /** This service's identity as a Kafka consumer. [[SagaRunner.replyGroupId]] derives a distinct group per saga from
    * it, which matters as soon as there is more than one: sagas sharing a group would have the reply topic's partitions
    * split between them and each would silently skip the other's replies.
    */
  val serviceGroupId = "orders-service"

  def make: Resource[IO, OrdersModule] =
    for
      components <- PostgresModule.make[IO, OrderEvent](eventCodec, pgConfigPath, enableSaga = true)
      store       = components.eventStore
      checkpoint  = components.checkpoint
      sagaRepo   <- Resource.eval(
                    IO.fromOption(components.sagaRepository)(
                      new IllegalStateException("Saga repository missing despite enableSaga = true"),
                    ),
                  )
      messageOutbox <- Resource.eval(
                         IO.fromOption(components.messageOutbox)(
                           new IllegalStateException("Message outbox missing despite enableSaga = true"),
                         ),
                       )
      pgConfig  <- Resource.eval(loadPgConfig)
      bootstrap <- Resource.eval(loadKafkaBootstrap)
      viewPool  <- Session
                    .Builder[IO]
                    .withHost(pgConfig.host)
                    .withPort(pgConfig.port)
                    .withUserAndPassword(pgConfig.user, pgConfig.password)
                    .withDatabase(pgConfig.database)
                    .pooled(pgConfig.maxConnections)
      orderRepo  = OrderRepository.make[IO](viewPool)
      orderProj <- Resource.eval(OrderProjection.make[IO](orderRepo))
      projector  = DefaultProjector[IO, OrderEvent](store, checkpoint)
      _         <- projector.run(orderProj).compile.drain.background
      // Drains the requests the saga enqueued. Nothing reaches inventory without it.
      relay   <- KafkaModule.messageRelay[IO](messageOutbox, KafkaMessageProducerConfig(bootstrapServers = bootstrap))
      _       <- relay.run().background
      replies <- KafkaModule.messageSubscriber[IO](
                   KafkaConsumerConfig(
                     bootstrapServers = bootstrap,
                     groupId = SagaRunner.replyGroupId(serviceGroupId, ReserveStockSaga.name),
                   ),
                 )
      metrics <- Resource.eval(CommandHandlerMetrics.make[IO])
      // The *transactional* store: the runner enqueues a saga's requests in the same transaction that appends the
      // events causing them, which only the raw PostgreSQL store can do. Everything else here — the projector, the
      // command handlers behind the routes — goes through the instrumented `eventStore` above.
      runner = SagaRunner[IO, OrderEvent](
                 store = components.transactionalStore, checkpoint = checkpoint, repository = sagaRepo,
                 replies = replies, replyTopic = Topics.OrdersReplies,
               )
      // `run` ends only on an unrecoverable error, and one loop failing takes the other two with it. Restarting is the
      // caller's job — a real service would supervise this; an example settles for making the death loud instead of
      // letting the process go on serving HTTP with a silently dead saga.
      _ <- runner
             .run(ReserveStockSaga, components.leaderElection)
             .compile
             .drain
             .handleErrorWith(error => Logger[IO].error(error)("saga runner stopped; sagas will no longer advance"))
             .background
    yield new OrdersModule(store, orderRepo, sagaRepo, metrics)

  private def loadPgConfig: IO[PostgresConfig] =
    IO.delay(ConfigSource.default.at(pgConfigPath).load[PostgresConfig]).flatMap {
      case Right(c) => IO.pure(c)
      case Left(e)  => IO.raiseError(new RuntimeException(e.prettyPrint()))
    }

  private def loadKafkaBootstrap: IO[String] =
    IO.delay(ConfigSource.default.at(s"$kafkaConfigPath.bootstrap-servers").load[String]).flatMap {
      case Right(s) => IO.pure(s)
      case Left(e)  => IO.raiseError(new RuntimeException(e.prettyPrint()))
    }
