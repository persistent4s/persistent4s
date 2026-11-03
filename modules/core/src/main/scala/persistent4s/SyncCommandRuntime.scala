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

import java.util.UUID
import java.util.concurrent.TimeoutException

import scala.concurrent.duration.FiniteDuration

import cats.effect.{Concurrent, Resource, Temporal}
import cats.syntax.all.*

import fs2.concurrent.Topic

/** Shared command execution that waits for one projection before returning.
  *
  * Configure this once at startup and provide it to services as a given instance. Notifications must come from the same
  * application instance; they are published only after the projection has saved its changes.
  */
final class SyncCommandRuntime[F[_], A <: Event] private (
  runtime: CommandRuntime[F, A],
  subscribed: Resource[F, List[EventEnvelope[A]] => F[Unit]],
):

  /** Runs a command and waits for its accepted events to reach the configured projection.
    *
    * Rejections and commands without matching events return immediately. Command and projection failures keep their
    * original error. A timeout does not undo saved events.
    *
    * @param handler
    *   command definition
    * @param command
    *   requested change
    * @return
    *   the rejection or saved events, after the projection catches up
    */
  def execute[C, S, R](
    handler: EventSourcedCommandHandler[C, S, A, R],
    command: C,
  )(using F: Concurrent[F]): F[Either[R, List[EventEnvelope[A]]]] =
    subscribed.use { await =>
      handler.run(command)(using F, runtime).flatTap(_.fold(_ => F.unit, await))
    }

object SyncCommandRuntime:

  /** Waits for notifications from a projection running in this application instance.
    *
    * The subscription is opened before each command runs, so even immediate notifications are received.
    *
    * @param runtime
    *   event store and command snapshots
    * @param topic
    *   notifications published after the projection saves its changes
    * @param filter
    *   event types handled by that projection; empty means every type
    * @param timeout
    *   maximum wait after the command saves its events
    * @param maxQueued
    *   notifications buffered for each command
    * @param timeoutError
    *   error raised only when waiting for the projection times out
    * @return
    *   a runtime shared by all handlers using this event type and projection
    */
  def fromTopic[F[_]: Temporal, A <: Event, K, PS](
    runtime: CommandRuntime[F, A],
    topic: Topic[F, (UUID, Either[Throwable, Map[K, Option[PS]]])],
    filter: Set[EventTypeName],
    timeout: FiniteDuration,
    maxQueued: Int = 256,
    timeoutError: => Throwable = new TimeoutException("Projection did not catch up in time"),
  ): SyncCommandRuntime[F, A] =
    val F = Temporal[F]
    val subscribed = topic.subscribeAwait(maxQueued).map { stream => (envelopes: List[EventEnvelope[A]]) =>
      val ids = envelopes.iterator
        .filter(event => filter.isEmpty || filter.contains(event.metadata.eventType))
        .map(_.metadata.id)
        .toSet
      if ids.isEmpty then F.unit
      else
        F.timeoutTo(
          SyncCommandHandler.awaitAll(stream, ids).void,
          timeout,
          F.unit.flatMap(_ => F.raiseError[Unit](timeoutError)),
        )
    }
    new SyncCommandRuntime(runtime, subscribed)
