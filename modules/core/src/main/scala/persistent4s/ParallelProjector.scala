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
import cats.Parallel
import cats.effect.Async
import cats.effect.Deferred
import cats.effect.Ref
import cats.syntax.all.*
import fs2.Stream

final case class ParallelProjector[F[_]: Async: Parallel, A <: Event](
  eventStore: EventStore[F, A] & EventNotification[F],
  checkpoint: ProjectionCheckpoint[F],
  batchSize: Int = 100,
) extends Projector[F, A]:

  final private case class WakeupState(
    pending: Boolean,
    signal: Deferred[F, Unit],
  )

  final private case class BatchProgress[K, S](
    stateCache: Map[K, Option[S]],
    dirtyKeys: Set[K],
    lastProcessedPosition: Option[Long],
  )

  // TODO How should we handle failure? What do we do if the process dies?
  override def run[K](projection: Projection[F, A, K]): Stream[F, Unit] = {

    def persistProgress(progress: BatchProgress[K, projection.State]): F[Unit] =
      val statesToPersist = progress.dirtyKeys.map { key =>
        key -> progress.stateCache.getOrElse(key, None)
      }.toMap
      projection.persistStates(statesToPersist) *> progress.lastProcessedPosition
        .traverse_(checkpoint.save(projection.name, _))
        .void

    def processEvent(
      progress: BatchProgress[K, projection.State],
      event: EventEnvelope[A],
    ): F[BatchProgress[K, projection.State]] =
      val eventKeys = projection.resolveKeys(event)

      eventKeys
        .foldLeftM(progress.stateCache) { (stateCache, key) =>
          projection.handle(stateCache.getOrElse(key, None), event).map { stateN =>
            stateCache.updated(key, stateN)
          }
        }
        .map { stateCacheN =>
          progress.copy(
            stateCache = stateCacheN,
            dirtyKeys = progress.dirtyKeys ++ eventKeys,
            lastProcessedPosition = Some(event.metadata.globalPosition),
          )
        }

    def processBatch(batch: List[EventEnvelope[A]]): F[Unit] =
      if (batch.isEmpty) Applicative[F].unit
      else {
        val keyToEvents: Map[K, List[EventEnvelope[A]]] =
          batch.foldLeft(Map.empty[K, List[EventEnvelope[A]]]) { (acc, event) =>
            projection.resolveKeys(event).foldLeft(acc) { (m, key) =>
              m.updated(key, m.getOrElse(key, Nil) :+ event)
            }
          }

        for {
          initialStates <- projection.fetchStates(keyToEvents.keySet.toList)

          parallelResult <- keyToEvents.toList.parTraverse { case (key, events) =>
                              events
                                .foldLeftM(initialStates.getOrElse(key, None)) { (state, event) =>
                                  projection.handle(state, event)
                                }
                                .map(key -> _)
                            }.attempt

          _ <- parallelResult match {
                 case Right(keyStatesList) =>
                   projection.persistStates(keyStatesList.toMap) *>
                     checkpoint.save(projection.name, batch.last.metadata.globalPosition)

                 case Left(_) =>
                   val progress0 = BatchProgress[K, projection.State](
                     stateCache = initialStates,
                     dirtyKeys = Set.empty,
                     lastProcessedPosition = None,
                   )
                   batch
                     .foldLeftM(progress0) { (progress, event) =>
                       processEvent(progress, event).handleErrorWith { error =>
                         persistProgress(progress) *> Async[F].raiseError(error)
                       }
                     }
                     .flatMap(persistProgress)
               }
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
