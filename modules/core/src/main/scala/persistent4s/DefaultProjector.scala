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

import cats.Applicative
import cats.effect.Async
import cats.effect.Deferred
import cats.effect.Ref
import cats.syntax.all.*
import fs2.Stream

final case class DefaultProjector[F[_]: Async, A <: Event](
  eventStore: EventStore[F, A] & EventNotification[F],
  checkpoint: ProjectionCheckpoint[F],
  batchSize: Int = 100,
) extends Projector[F, A]:

  final private case class WakeupState(
    pending: Boolean,
    signal: Deferred[F, Unit],
  )

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

    def markPending(wakeupState: Ref[F, WakeupState]): F[Unit] =
      wakeupState.modify {
        case current @ WakeupState(true, _) =>
          current -> Applicative[F].unit
        case WakeupState(false, signal) =>
          WakeupState(pending = true, signal) -> signal.complete(()).void
      }.flatten

    def awaitWork(wakeupState: Ref[F, WakeupState]): F[Unit] =
      Deferred[F, Unit].flatMap { nextSignal =>
        wakeupState.modify {
          case WakeupState(true, _) =>
            WakeupState(pending = false, nextSignal) -> Applicative[F].unit
          case current @ WakeupState(false, signal) =>
            current -> (signal.get *> awaitWork(wakeupState))
        }.flatten
      }

    Stream.eval(Deferred[F, Unit]).flatMap { initialSignal =>
      Stream.eval(Ref.of[F, WakeupState](WakeupState(pending = true, initialSignal))).flatMap { wakeupState =>
        val notifications =
          eventStore.notification.evalMap(_ => markPending(wakeupState)).drain

        val projector =
          Stream
            .repeatEval(awaitWork(wakeupState))
            .flatMap(_ => processEvents)

        projector.concurrently(notifications)
      }
    }
  }
