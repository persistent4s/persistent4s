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

import cats.effect.Temporal
import cats.syntax.all.*

private[kafka] object Backoff:

  /** Supervise `action` with exponential backoff: on failure, sleep then restart, doubling the delay up to `maxDelay`.
    * The delay resets to `initialDelay` after a run that lasted at least `maxDelay` (i.e. one that had recovered), so
    * failures far apart in time don't compound. Returns only if `action` completes normally.
    */
  def retryWithBackoff[F[_]: Temporal](
    action: F[Unit],
    initialDelay: FiniteDuration,
    maxDelay: FiniteDuration,
  ): F[Unit] =
    def loop(delay: FiniteDuration): F[Unit] =
      Temporal[F].monotonic.flatMap { start =>
        action.handleErrorWith { _ =>
          Temporal[F].monotonic.flatMap { end =>
            val effectiveDelay = if end - start >= maxDelay then initialDelay else delay
            Temporal[F].sleep(effectiveDelay) *> loop((effectiveDelay * 2).min(maxDelay))
          }
        }
      }
    loop(initialDelay)
