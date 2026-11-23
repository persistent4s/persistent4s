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

import scala.concurrent.duration.FiniteDuration

import cats.Applicative
import cats.effect.{Async, Ref}
import cats.syntax.all.*
import fs2.concurrent.Topic
import org.typelevel.log4cats.Logger

/** Answers the commands waiting on a paused projection. While it is paused, each event it would process is published as
  * a [[ProjectionPaused]] failure as soon as the store returns it, so a waiting command fails at once instead of after
  * its timeout. Only events after the projection's cursor, or after the last answered one, are read.
  */
final private[persistent4s] class PausedWaiters[F[_]: Async: Logger, A <: Event, K, S] private (
  projectionName: String,
  filter: EventFilter,
  eventStore: EventStore[F, A],
  topic: Topic[F, (UUID, Either[Throwable, Map[K, Option[S]]])],
  publishTimeout: FiniteDuration,
  answered: Ref[F, Option[ReadCursor]],
):

  /** Leaves the stored events unanswered when the projection starts paused: no command waits on them yet. */
  def skipStored: F[Unit] =
    eventStore.currentRevision(filter).flatMap(eventStore.cursorAt).flatMap(cursor => answered.set(Some(cursor)))

  /** Forgets the answered events once the projection runs again; it then processes and publishes them as usual. */
  def reset: F[Unit] = answered.set(None)

  /** Answers the next events the paused projection does not process.
    *
    * @return
    *   the events read, so the caller reads on after a full page and rechecks held-back events; `None` when the read
    *   fails, which only leaves the waiting commands to their timeout
    */
  def answer(state: ProjectionCheckpointState, maxEvents: Int): F[Option[ProjectionRead[A]]] =
    val reason = state.error.flatMap(_.linesIterator.nextOption())
    (for
      from <- answered.get.map(_.getOrElse(state.cursor))
      read <- eventStore.readForProjection(from, filter, maxEvents)
      _    <- read.events.traverse_ { event =>
                val paused = ProjectionPaused(projectionName, event.metadata.id, reason)
                Async[F].timeoutTo(topic.publish1((event.metadata.id, Left(paused))).void, publishTimeout, Applicative[F].unit)
              }
      _    <- read.events.lastOption.traverse_(event => answered.set(Some(event.metadata.cursor)))
    yield Option(read)).handleErrorWith { error =>
      Logger[F].warn(error)(s"Could not answer the commands waiting on paused projection $projectionName").as(None)
    }

private[persistent4s] object PausedWaiters:

  /** Whether a notification lets the projection run again, after which its answered events are processed as usual. */
  def resumes(notification: EventStoreNotification): Boolean = notification match
    case EventStoreNotification.ResumeProjection(_) | EventStoreNotification.UpdateCheckpointIndex(_, _) => true
    case _                                                                                                => false

  def make[F[_]: Async: Logger, A <: Event, K, S](
    projectionName: String,
    filter: EventFilter,
    eventStore: EventStore[F, A],
    topic: Topic[F, (UUID, Either[Throwable, Map[K, Option[S]]])],
    publishTimeout: FiniteDuration,
  ): F[PausedWaiters[F, A, K, S]] =
    Ref.of[F, Option[ReadCursor]](None).map(new PausedWaiters(projectionName, filter, eventStore, topic, publishTimeout, _))
