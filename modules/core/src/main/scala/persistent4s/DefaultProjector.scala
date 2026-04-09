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
import fs2.Stream

final case class DefaultProjector[F[_]: Async, A](
  eventStore: EventStore[F, A] & EventNotification[F],
  checkpoint: ProjectionCheckpoint[F],
) extends Projector[F, A]:

  override def run(projection: Projection[F, A]): Stream[F, Unit] =
    val processEvents: Stream[F, Unit] =
      Stream
        .eval(checkpoint.load(projection.name))
        .flatMap { lastPosition =>
          eventStore.readFrom(lastPosition.getOrElse(-1L), projection.filter)
        }
        .evalMap { envelope =>
          projection.handle(envelope) *>
            checkpoint.save(projection.name, envelope.metadata.globalPosition)
        }

    (Stream.emit(()) ++ eventStore.notification).flatMap(_ => processEvents)
