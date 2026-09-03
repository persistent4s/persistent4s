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

import cats.effect.*
import fs2.io.net.Network
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer
import pureconfig.ConfigSource

given Tracer[IO] = Tracer.Implicits.noop

given Meter[IO] = Meter.Implicits.noop

given Logger[IO] = Slf4jLogger.getLogger[IO]

import persistent4s.*
import persistent4s.circe.CirceEventCodec
import persistent4s.examples.saga.contract.Topics
import persistent4s.examples.saga.inventory.domain.InventoryEvent
import persistent4s.kafka.{KafkaConsumerConfig, KafkaMessageProducerConfig, KafkaModule}
import persistent4s.postgres.PostgresModule

final class InventoryModule private (
  val store: EventStore[IO, InventoryEvent] & EventNotification[IO],
  val commandMetrics: CommandHandlerMetrics[IO],
)

/** Wiring for the service that answers the saga.
  *
  * Notice what is '''absent''': no `SagaRepository`, no `SagaRunner`, no checkpoint, no leader election. Taking part in
  * someone else's saga needs a message outbox and a subscription, nothing more. Notice also that the event outbox is
  * off — inventory publishes no events at all. Its log stays private, and the only thing that leaves this service is a
  * reply.
  */
object InventoryModule:

  val eventCodec: EventCodec[InventoryEvent] = CirceEventCodec.derived[InventoryEvent]

  private val pgConfigPath = "persistent4s.inventory.postgres"

  private val kafkaConfigPath = "persistent4s.saga.kafka"

  /** This service's own subscription to the command topic. Unrelated to the saga's reply group: scaling inventory means
    * adding members here, and Kafka splits the command topic's partitions between them.
    */
  val commandGroupId = "inventory-service.commands"

  def make: Resource[IO, InventoryModule] =
    for
      components <- PostgresModule.make[IO, InventoryEvent](eventCodec, pgConfigPath, enableMessageOutbox = true)
      store       = components.eventStore
      outbox     <- Resource.eval(
                  IO.fromOption(components.messageOutbox)(
                    new IllegalStateException("Message outbox missing despite enableMessageOutbox = true"),
                  ),
                )
      bootstrap <- Resource.eval(loadKafkaBootstrap)
      // Carries replies out of the transaction that wrote them. Without it a reservation would commit and its answer
      // would never leave, and the asking saga would sit until its deadline.
      relay      <- KafkaModule.messageRelay[IO](outbox, KafkaMessageProducerConfig(bootstrapServers = bootstrap))
      _          <- relay.run().background
      subscriber <- KafkaModule.messageSubscriber[IO](
                      KafkaConsumerConfig(bootstrapServers = bootstrap, groupId = commandGroupId),
                    )
      metrics <- Resource.eval(CommandHandlerMetrics.make[IO])
      // The *transactional* store, not the instrumented one: replying inside the appending transaction is what
      // `TransactionalMessages` adds, and only the raw PostgreSQL store implements it. Appends on this path therefore
      // bypass the otel4s instrumentation, which is the trade `Components.transactionalStore` documents.
      _ <- InventoryCommandConsumer
             .stream[IO](subscriber, components.transactionalStore, Topics.InventoryCommands, metrics)
             .compile
             .drain
             .background
    yield new InventoryModule(store, metrics)

  private def loadKafkaBootstrap: IO[String] =
    IO.delay(ConfigSource.default.at(s"$kafkaConfigPath.bootstrap-servers").load[String]).flatMap {
      case Right(s) => IO.pure(s)
      case Left(e)  => IO.raiseError(new RuntimeException(e.prettyPrint()))
    }
