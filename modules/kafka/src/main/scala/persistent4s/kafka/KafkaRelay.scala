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

import cats.effect.{Async, Temporal}
import cats.syntax.all.*
import persistent4s.{Event, Outbox}
import persistent4s.EventPublisher

/** Drains an [[Outbox]] into Kafka via an [[EventPublisher]] and acknowledges published entries.
  *
  * Lifecycle: a long-running process. Start once at application startup; `run` only terminates if its underlying outbox
  * stream terminates or an unrecoverable error escapes. Restart-safe — progress lives in the outbox table.
  *
  * ==Single-instance requirement==
  *
  * At most '''one''' relay instance per service must be running against the same outbox/topic at any time. The
  * [[EventSubscriber]] ordering guarantee relies on a single producer feeding the partition: two concurrent relays
  * would interleave records in the broker in nondeterministic order, breaking the per-tag ordering guarantee and
  * potentially publishing the same event twice.
  *
  * This is currently a deployment constraint — the library does not enforce it. A future iteration may add a Postgres
  * advisory lock acquired at relay startup to make it self-enforcing.
  *
  * ==Producer configuration==
  *
  * The supplied [[EventPublisher]] must be backed by a producer configured with `enable.idempotence=true` (the default
  * since Kafka 3.0). Without it, retries on transient broker errors can reorder records within the partition and break
  * the ordering guarantee.
  *
  * @param outbox
  *   the source of unpublished events (implementation-agnostic; e.g. `PostgresOutbox`)
  * @param publisher
  *   the Kafka sink
  * @param topic
  *   destination topic; every outbox entry drained by this relay is published here. To fan out an outbox across
  *   multiple topics, run multiple relays — typically one per topic, each filtering the outbox entries it cares about
  *   (the current `Outbox` API drains everything, so today this means "one relay = one outbox = one topic").
  * @param batchSize
  *   maximum number of entries fetched from the outbox per round-trip
  */
final class KafkaRelay[F[_]: Async, A <: Event] private (
  outbox: Outbox[F, A],
  publisher: EventPublisher[F, A],
  topic: String,
  batchSize: Int,
):

  /** Run the relay once. Returns when the input stream completes or fails. Fails fast on any publish error without
    * acking the failing envelope, so the next run reprocesses it.
    */
  def runOnce: F[Unit] =
    outbox
      .stream(batchSize)
      .evalMap { envelope =>
        publisher.publish(topic, envelope) *>
          outbox.markPublished(envelope.metadata.globalPosition)
      }
      .compile
      .drain

  /** Run the relay with exponential-backoff supervision. Restarts [[runOnce]] after a delay whenever it fails, doubling
    * the delay up to `maxDelay`. Never terminates unless the outbox stream itself completes.
    *
    * TODO: log the error and current delay before each retry once a logging library is integrated.
    */
  def run(
    initialDelay: FiniteDuration = 1.second,
    maxDelay: FiniteDuration = 30.seconds,
  ): F[Unit] =
    runOnce.handleErrorWith { _ =>
      Temporal[F].sleep(initialDelay) *> run((initialDelay * 2).min(maxDelay), maxDelay)
    }

object KafkaRelay:

  def apply[F[_]: Async, A <: Event](
    outbox: Outbox[F, A],
    publisher: EventPublisher[F, A],
    topic: String,
    batchSize: Int = 128,
  ): KafkaRelay[F, A] = new KafkaRelay(outbox, publisher, topic, batchSize)
