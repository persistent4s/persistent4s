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

import cats.effect.Async

import fs2.Stream
import fs2.concurrent.Topic

/** A [[Projector]] that runs each projection under [[LeaderElection]], keyed by the projection's own name.
  *
  * A projection shares one checkpoint across every instance of a horizontally scaled service, so running the same
  * projection on several instances races that checkpoint and double-applies events. Wrapping the projector means at
  * most one instance drives a given projection at a time, and another takes over automatically when the leader stops.
  *
  * Standing by is silent: on a non-leader instance the returned stream simply produces nothing until leadership is
  * acquired, so it neither completes nor fails, and grouping it with [[ProjectionRuntime.startAll]] behaves as usual.
  *
  * Leadership is taken per projection name rather than once for the whole group, so distinct projections can be driven
  * by different instances.
  */
final case class LeaderElectedProjector[F[_]: Async, A <: Event](
  underlying: Projector[F, A],
  leaderElection: LeaderElection[F],
) extends Projector[F, A]:

  override def run[K, S](
    projection: Projection[F, A, K, S],
    topic: Option[Topic[F, (UUID, Either[Throwable, Map[K, Option[S]]])]] = None,
  ): Stream[F, Unit] =
    Stream.eval(
      leaderElection.runAsLeader(projection.name)(underlying.run(projection, topic).compile.drain),
    )
