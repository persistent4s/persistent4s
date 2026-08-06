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

import cats.Functor
import cats.syntax.all.*
import org.typelevel.otel4s.metrics.{Counter, Meter}
import org.typelevel.otel4s.trace.Tracer

/** Metrics used by [[CommandHandler.run]]. Build once per application (e.g. alongside the event store) and provide as a
  * given. The retries counter must not be recreated on every command.
  */
final case class CommandHandlerMetrics[F[_]](retries: Counter[F, Long])

object CommandHandlerMetrics:

  def make[F[_]: Meter: Functor]: F[CommandHandlerMetrics[F]] =
    Meter[F]
      .counter[Long]("persistent4s.commandhandler.retries")
      .withDescription("Number of command handler retry attempts on conflict")
      .withUnit("{retries}")
      .create
      .map(CommandHandlerMetrics(_))

/** Tracing and metrics for the [[EventSourcedCommandHandler]] path.
  *
  * Carried as a value on [[CommandRuntime]] rather than as a context bound on `run`, so that adding observability does
  * not ripple through `CommandRuntime.execute`, `SyncCommandHandler.fromEventSourced` and every call site — and so
  * `CommandRuntime.eventStoreOnly` and the `fromEventStore` given stay as lightweight as they are.
  */
final case class CommandTelemetry[F[_]](
  tracer: Tracer[F],
  metrics: CommandHandlerMetrics[F],
)

object CommandTelemetry:

  def make[F[_]: Meter: Functor: Tracer]: F[CommandTelemetry[F]] =
    CommandHandlerMetrics.make[F].map(CommandTelemetry(Tracer[F], _))
