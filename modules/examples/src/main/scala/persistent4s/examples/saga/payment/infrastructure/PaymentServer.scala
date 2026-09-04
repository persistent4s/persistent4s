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

import cats.effect.*
import org.typelevel.log4cats.Logger
import org.typelevel.log4cats.slf4j.Slf4jLogger
import pureconfig.ConfigSource

import persistent4s.{MessagePublisher, MessageSubscriber}
import persistent4s.examples.saga.contract.Topics
import persistent4s.kafka.{KafkaConsumerConfig, KafkaMessageProducerConfig, KafkaModule}

/** No HTTP, no Postgres: this service exists only to answer the saga on Kafka, so there is nothing else to wire.
  *
  * Compare the size of this against `InventoryModule` — the difference ''is'' the point. A partner with no invariant to
  * protect and no state to persist needs a subscriber and a publisher, nothing more.
  */
object PaymentServer extends IOApp.Simple:

  given Logger[IO] = Slf4jLogger.getLogger[IO]

  private val kafkaConfigPath = "persistent4s.saga.kafka"

  /** This service's own subscription to the command topic — unrelated to the saga's reply group, same reasoning as
    * `InventoryModule.commandGroupId`.
    */
  private val commandGroupId = "payment-service.commands"

  def run: IO[Unit] =
    resources.use { case (subscriber, publisher) =>
      PaymentCommandConsumer.stream[IO](subscriber, publisher, Topics.PaymentCommands).compile.drain
    }

  private def resources: Resource[IO, (MessageSubscriber[IO], MessagePublisher[IO])] =
    for
      bootstrap  <- Resource.eval(loadKafkaBootstrap)
      subscriber <- KafkaModule.messageSubscriber[IO](
                      KafkaConsumerConfig(bootstrapServers = bootstrap, groupId = commandGroupId),
                    )
      publisher <- KafkaModule.messagePublisher[IO](KafkaMessageProducerConfig(bootstrapServers = bootstrap))
    yield (subscriber, publisher)

  private def loadKafkaBootstrap: IO[String] =
    IO.delay(ConfigSource.default.at(s"$kafkaConfigPath.bootstrap-servers").load[String]).flatMap {
      case Right(s) => IO.pure(s)
      case Left(e)  => IO.raiseError(new RuntimeException(e.prettyPrint()))
    }
