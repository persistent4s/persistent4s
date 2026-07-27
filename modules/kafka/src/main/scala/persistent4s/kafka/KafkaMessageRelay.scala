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
import fs2.Stream

import persistent4s.{MessageOutbox, MessagePublisher}

/** Drains a [[MessageOutbox]] into Kafka via a [[MessagePublisher]]. Long-running: start once, `run` only returns if an
  * unrecoverable error escapes. Restart-safe (progress lives in the outbox table).
  */
final class KafkaMessageRelay[F[_]: Async] private (
  outbox: MessageOutbox[F],
  publisher: MessagePublisher[F],
  batchSize: Int,
  pollInterval: FiniteDuration,
):

  def runOnce: F[Unit] =
    def drainLoop: F[Unit] =
      outbox.drainBatch(batchSize)(entries => publisher.publish(entries.map(_._2))).flatMap { published =>
        if published >= batchSize then drainLoop
        else waitForSignal *> drainLoop
      }
    drainLoop

  private def waitForSignal: F[Unit] =
    outbox.notifications.merge(Stream.awakeEvery[F](pollInterval).void).head.compile.drain

  def run(
    initialDelay: FiniteDuration = 1.second,
    maxDelay: FiniteDuration = 30.seconds,
  ): F[Unit] =
    Backoff.retryWithBackoff(runOnce, initialDelay, maxDelay)

object KafkaMessageRelay:

  def apply[F[_]: Async](
    outbox: MessageOutbox[F],
    publisher: MessagePublisher[F],
    batchSize: Int = 128,
    pollInterval: FiniteDuration = 1.second,
  ): KafkaMessageRelay[F] = new KafkaMessageRelay(outbox, publisher, batchSize, pollInterval)
