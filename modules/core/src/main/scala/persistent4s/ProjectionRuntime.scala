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

import scala.collection.mutable.ListBuffer

import cats.effect.{Async, Fiber, Outcome, Resource}
import cats.syntax.all.*

import fs2.Stream

/** Raised before startup when two registered projections share a checkpoint name. */
final case class DuplicateProjectionNames(names: List[String])
    extends IllegalArgumentException(s"Duplicate projection names: ${names.mkString(", ")}")

/** Raised when a projection stream completes even though a running projector is expected to be long-lived. */
final case class ProjectionTerminatedUnexpectedly(projectionName: String)
    extends RuntimeException(s"Projection $projectionName terminated unexpectedly")

/** Adds the projection name to an unexpected stream failure. */
final case class ProjectionExecutionFailed(projectionName: String, cause: Throwable)
    extends RuntimeException(s"Projection $projectionName failed: ${cause.getMessage}", cause)

final private[persistent4s] case class RegisteredProjection[F[_]](
  name: String,
  stream: Stream[F, Unit],
)

/** Type-safe collector used by [[ProjectionRuntime.startAll]]. Each call captures a projection's key and state types
  * before the resulting homogeneous streams are started together.
  */
final class ProjectionRegistrations[F[_], A <: Event] private[persistent4s] (
  projector: Projector[F, A],
):

  private val registrations = ListBuffer.empty[RegisteredProjection[F]]

  /** Register one projection. */
  def run[K, S](projection: Projection[F, A, K, S]): Unit =
    registrations += RegisteredProjection(projection.name, projector.run(projection))

  private[persistent4s] def result: List[RegisteredProjection[F]] = registrations.toList

/** A running group of projections. Unexpected termination is observable through [[await]] or [[outcome]]. */
final class RunningProjections[F[_]] private[persistent4s] (
  fiber: Fiber[F, Throwable, Unit],
)(using F: Async[F]):

  /** Wait until the group terminates, raising its named projection failure. */
  def await: F[Unit] =
    fiber.join.flatMap {
      case Outcome.Succeeded(result) => result
      case Outcome.Errored(error)    => F.raiseError(error)
      case Outcome.Canceled()        => F.unit
    }

  /** Inspect the underlying group outcome without changing it. */
  def outcome: F[Outcome[F, Throwable, Unit]] = fiber.join

/** Resource-safe startup for a heterogeneous group of projections. */
final class ProjectionRuntime[F[_]: Async, A <: Event] private (
  projector: Projector[F, A],
):

  /** Start all registered projections concurrently.
    *
    * Duplicate names fail resource acquisition. Releasing the resource cancels the group and waits for stream
    * finalizers. If any projection fails or completes unexpectedly, its siblings are canceled and the named failure is
    * exposed by the returned [[RunningProjections]].
    */
  def startAll(configure: ProjectionRegistrations[F, A] => Unit): Resource[F, RunningProjections[F]] =
    Resource.eval {
      Async[F].delay {
        val registrations = new ProjectionRegistrations(projector)
        configure(registrations)
        validate(registrations.result)
      }
    }.flatMap { registrations =>
      Resource
        .make(Async[F].start(run(registrations)))(_.cancel)
        .map(new RunningProjections(_))
    }

  private def validate(registrations: List[RegisteredProjection[F]]): List[RegisteredProjection[F]] =
    require(registrations.nonEmpty, "At least one projection must be registered")
    val duplicates =
      registrations
        .groupBy(_.name)
        .collect { case (name, _ :: _ :: _) => name }
        .toList
        .sorted
    if duplicates.nonEmpty then throw DuplicateProjectionNames(duplicates)
    registrations

  private def run(registrations: List[RegisteredProjection[F]]): F[Unit] =
    Stream
      .emits(registrations)
      .covary[F]
      .map { registration =>
        registration.stream
          .handleErrorWith(error => Stream.raiseError[F](ProjectionExecutionFailed(registration.name, error))) ++ Stream
          .raiseError[F](ProjectionTerminatedUnexpectedly(registration.name))
      }
      .parJoinUnbounded
      .compile
      .drain

object ProjectionRuntime:

  def apply[F[_]: Async, A <: Event](projector: Projector[F, A]): ProjectionRuntime[F, A] =
    new ProjectionRuntime(projector)
