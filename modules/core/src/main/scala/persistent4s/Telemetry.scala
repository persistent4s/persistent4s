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

package persistent4s

import cats.effect.Async
import cats.syntax.all.*
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.Histogram

private[persistent4s] object Telemetry:

  def timed[F[_]: Async, A](hist: Histogram[F, Double], attrs: Attribute[?]*)(fa: F[A]): F[Either[Throwable, A]] =
    for
      start  <- Async[F].monotonic
      result <- fa.attempt
      end    <- Async[F].monotonic
      _      <- hist.record((end - start).toNanos.toDouble / 1e6, attrs*)
    yield result
