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

package persistent4s.kafka

import scala.concurrent.duration.*

import cats.effect.Async
import cats.syntax.all.*
import persistent4s.{Event, Outbox}
import persistent4s.EventPublisher

/** Drains an [[Outbox]] into Kafka via an [[EventPublisher]], acking each published entry.
  *
  * A long-running process: start once at startup; `run` only returns if the outbox stream ends or an unrecoverable
  * error escapes. Restart-safe, since progress lives in the outbox table.
  *
  * '''Single instance:''' run at most one relay per outbox/topic. Two concurrent relays would interleave records on the
  * partition, breaking ordering and possibly publishing an event twice. Not enforced yet (a future Postgres advisory
  * lock may make it self-enforcing).
  *
  * '''Producer:''' the [[EventPublisher]] must use `enable.idempotence=true` (Kafka's default since 3.0), otherwise
  * retries can reorder records within a partition.
  *
  * @param outbox
  *   source of unpublished events (e.g. `PostgresOutbox`)
  * @param publisher
  *   the Kafka sink
  * @param topic
  *   destination topic for every entry this relay drains (the `Outbox` API drains everything, so today one relay maps
  *   to one outbox and one topic)
  * @param batchSize
  *   max entries fetched from the outbox per round-trip
  */
final class KafkaRelay[F[_]: Async, A <: Event] private (
  outbox: Outbox[F, A],
  publisher: EventPublisher[F, A],
  topic: String,
  batchSize: Int,
):

  /** Run the relay once. Returns when the outbox stream completes or fails. On a publish error it fails fast without
    * acking the failing batch, so the next run reprocesses it.
    */
  def runOnce: F[Unit] =
    outbox
      .stream(batchSize)
      .chunks
      .evalMap { chunk =>
        val envelopes = chunk.toList
        publisher.publish(topic, envelopes) *>
          outbox.markPublished(envelopes.map(_.metadata.globalPosition))
      }
      .compile
      .drain

  /** Run with exponential-backoff supervision: restart [[runOnce]] after a failure, doubling the delay up to
    * `maxDelay`. Never terminates unless the outbox stream completes.
    *
    * TODO: log the error.
    */
  def run(
    initialDelay: FiniteDuration = 1.second,
    maxDelay: FiniteDuration = 30.seconds,
  ): F[Unit] =
    Backoff.retryWithBackoff(runOnce, initialDelay, maxDelay)

object KafkaRelay:

  def apply[F[_]: Async, A <: Event](
    outbox: Outbox[F, A],
    publisher: EventPublisher[F, A],
    topic: String,
    batchSize: Int = 128,
  ): KafkaRelay[F, A] = new KafkaRelay(outbox, publisher, topic, batchSize)
