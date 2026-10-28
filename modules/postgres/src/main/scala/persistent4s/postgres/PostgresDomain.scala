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

package persistent4s.postgres

import java.util.UUID
import java.util.concurrent.TimeoutException

import scala.concurrent.duration.FiniteDuration

import cats.effect.*
import cats.syntax.all.*

import fs2.Stream
import fs2.concurrent.Topic
import org.typelevel.log4cats.Logger
import org.typelevel.otel4s.metrics.Meter
import org.typelevel.otel4s.trace.Tracer

import persistent4s.{
  CommandRuntime,
  DefaultProjector,
  DuplicateProjectionNames,
  Event,
  EventNotification,
  EventStore,
  LeaderElectedProjector,
  Projection,
  ProjectionRuntime,
  Projector,
  SyncCommandRuntime,
}

/** The event store, command runtime and projection runtimes of one event type, built by [[PostgresModule.domain]] on
  * top of the module's shared pool, checkpoints, leader election and notification listener.
  *
  * Projection names must be unique across every domain of the module, since checkpoints and leader election leases are
  * keyed by name alone: starting a projection whose name is already running in this module fails with
  * [[persistent4s.DuplicateProjectionNames]].
  *
  * @param module
  *   the module this domain was built from
  * @param eventStore
  *   the instrumented store; use this for ordinary appends and reads
  * @param transactionalStore
  *   the same store, untyped-down to its PostgreSQL implementation, for the dual-write entry points
  *   (`appendWithMessages`/`appendUncheckedWithMessages`) that commit events and outgoing messages in one transaction.
  *   Appends made through it bypass the otel4s instrumentation on [[eventStore]].
  * @param outbox
  *   present only when the domain was built with `outbox = true`
  */
final class PostgresDomain[F[_]: Async: Logger: Tracer: Meter, A <: Event] private (
  val module: PostgresModule[F],
  val eventStore: EventStore[F, A] & EventNotification[F],
  val transactionalStore: PostgresEventStore[F, A],
  val outbox: Option[PostgresOutbox[F, A]],
  runningProjections: Ref[F, Set[String]],
):

  val commandRuntime: CommandRuntime[F, A] =
    CommandRuntime(eventStore, Some(module.snapshotStore))

  def projectionRuntime: ProjectionRuntime[F, A] =
    ProjectionRuntime(exclusive(DefaultProjector(eventStore, module.checkpoint)))

  /** Like [[projectionRuntime]], but each projection runs under the module's leader election so that several instances
    * of the same service pointed at this database never race a projection's shared checkpoint.
    */
  def leaderElectedProjectionRuntime: ProjectionRuntime[F, A] =
    ProjectionRuntime(
      exclusive(LeaderElectedProjector(DefaultProjector(eventStore, module.checkpoint), module.leaderElection)),
    )

  /** Starts a projection and configures direct `handler.runSync` calls for it.
    *
    * Provide the returned runtime to services with `using`. Commands subscribe before saving and wait for their events
    * to reach this projection. Notifications stay within this process, so use one application instance. Releasing the
    * resource stops the projection.
    *
    * @param projection
    *   read model that commands wait for
    * @param timeout
    *   maximum wait after a command saves its events
    * @param timeoutError
    *   error raised when the projection wait expires; command and projection errors remain unchanged
    * @return
    *   shared runtime for synchronous commands, with managed projection shutdown
    */
  def startSync[K, S](
    projection: Projection[F, A, K, S],
    timeout: FiniteDuration,
    timeoutError: => Throwable = new TimeoutException("Projection did not catch up in time"),
  ): Resource[F, SyncCommandRuntime[F, A]] =
    for
      updates <- Resource.eval(Topic[F, (UUID, Either[Throwable, Map[K, Option[S]]])])
      _       <- leaderElectedProjectionRuntime.startAll(_.run(projection, Some(updates)))
    yield SyncCommandRuntime.fromTopic(commandRuntime, updates, projection.filter, timeout, timeoutError = timeoutError)

  /** Claims each projection's name in the module for as long as it runs. The claim is taken before leader election, so
    * a duplicate fails on every instance rather than only on the one that wins the lease.
    */
  private def exclusive(underlying: Projector[F, A]): Projector[F, A] =
    new Projector[F, A]:
      override def run[K, S](
        projection: Projection[F, A, K, S],
        topic: Option[Topic[F, (UUID, Either[Throwable, Map[K, Option[S]]])]],
      ): Stream[F, Unit] =
        val name = projection.name
        val claim =
          runningProjections.modify { running =>
            if running.contains(name) then (running, DuplicateProjectionNames(List(name)).raiseError[F, Unit])
            else (running + name, Async[F].unit)
          }.flatten
        Stream.bracket(claim)(_ => runningProjections.update(_ - name)) >> underlying.run(projection, topic)

object PostgresDomain:

  private[postgres] def apply[F[_]: Async: Logger: Tracer: Meter, A <: Event](
    module: PostgresModule[F],
    eventStore: EventStore[F, A] & EventNotification[F],
    transactionalStore: PostgresEventStore[F, A],
    outbox: Option[PostgresOutbox[F, A]],
    runningProjections: Ref[F, Set[String]],
  ): PostgresDomain[F, A] =
    new PostgresDomain(module, eventStore, transactionalStore, outbox, runningProjections)
