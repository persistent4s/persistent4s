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
import cats.Applicative

final case class DefaultProjector[F[_]: Async, A <: Event](
  eventStore: EventStore[F, A] & EventNotification[F],
  checkpoint: ProjectionCheckpoint[F],
  batchSize: Int = 100,
) extends Projector[F, A]:

  // TODO add error handling
  override def run[K](projection: Projection[F, A, K]): Stream[F, Unit] = {

    def processBatch(batch: List[EventEnvelope[A]]): F[Unit] =
      if (batch.isEmpty) Applicative[F].unit
      else {
        val keyedEvents = batch.flatMap { event =>
          projection.resolveKeys(event).map(_ -> event)
        }
        val grouped = keyedEvents.groupBy(_._1)
        val keys = grouped.keySet

        for {
          existing <- keys.toList.foldLeftM(Map.empty[K, projection.State]) { (acc, key) =>
                        projection.fetchState(key).map {
                          case Some(state) => acc.updated(key, state)
                          case None        => acc
                        }
                      }

          finalStates <- grouped.toList.foldLeftM(Map.empty[K, projection.State]) { case (acc, (key, pairs)) =>
                           val eventsForKey = pairs.map(_._2)
                           val state0 = existing.getOrElse(key, projection.initialState)

                           eventsForKey
                             .foldLeftM(state0)(projection.handle)
                             .map(stateN => acc.updated(key, stateN))
                         }

          _ <- finalStates.toList.traverse_ { case (key, state) =>
                 projection.persist(key, state)
               }
          _ <- checkpoint.save(projection.name, batch.last.metadata.globalPosition)
        } yield ()
      }

    def processEvents: Stream[F, Unit] =
      Stream
        .eval(checkpoint.load(projection.name))
        .flatMap { lastPosition =>
          eventStore.readFrom(lastPosition.getOrElse(-1L), projection.filter)
        }
        .chunkN(batchSize)
        .evalMap(chunk => processBatch(chunk.toList))

    (Stream.emit(()) ++ eventStore.notification).flatMap(_ => processEvents)
  }
