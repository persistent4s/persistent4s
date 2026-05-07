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
import persistent4s.EventStoreNotification.*

/** A [[Projector]] implementation that processes events for distinct keys in parallel within each batch.
  *
  * Within a batch, events are grouped by key. Each key's event sequence is folded independently and all keys are
  * processed concurrently via `Parallel`. If every key succeeds the resulting states and checkpoint are persisted in
  * one shot. If any key fails the batch falls back to the sequential strategy of [[DefaultProjector]]: events are
  * replayed in order and progress is saved up to the last successfully processed position before the error is
  * re-raised.
  *
  * The notification, pause/resume, and checkpoint-reset mechanics are identical to [[DefaultProjector]].
  *
  * @param eventStore
  *   the event store to read from, which must also implement [[EventNotification]]
  * @param checkpoint
  *   durable storage for the last processed position per projection
  * @param batchSize
  *   maximum number of events processed in a single batch (default: 100)
  * @param maxBatchPerPass
  *   maximum number of batches read in a single wake-up pass (default: 10)
  */
final case class ParallelProjector[F[_]: Async: Parallel, A <: Event](
  eventStore: EventStore[F, A] & EventNotification[F],
  checkpoint: ProjectionCheckpoint[F],
  batchSize: Int = 100,
  maxBatchPerPass: Int = 10,
) extends Projector[F, A]:

  final private case class Work(
    pendingEvents: Boolean,
    notif: Option[EventStoreNotification],
  )

  final private case class WakeupState(
    pending: Boolean,
    processNotif: Option[EventStoreNotification],
    signal: Deferred[F, Unit],
  )

  final private case class BatchProgress[K, S](
    stateCache: Map[K, Option[S]],
    dirtyKeys: Set[K],
    lastProcessedPosition: Option[Long],
  )

  override def run[K, S](projection: Projection[F, A, K, S]): Stream[F, Unit] = {

    def persistProgress(
      progress: BatchProgress[K, S],
      projectionState: Ref[F, ProjectionCheckpointState],
      error: Option[Throwable] = None,
    ): F[Unit] =
      val statesToPersist = progress.dirtyKeys.map { key =>
        key -> progress.stateCache.getOrElse(key, None)
      }.toMap
      for {
        current <- projectionState.get
        next     = current.copy(
                 globalPosition = progress.lastProcessedPosition.getOrElse(current.globalPosition),
                 running = if (error.isEmpty) current.running else false,
                 error = error.map(formatError),
               )
        _ <- projection.persistStates(statesToPersist)
        _ <- checkpoint.save(next)
        _ <- projectionState.set(next)
      } yield ()

    def processEvent(
      progress: BatchProgress[K, S],
      event: EventEnvelope[A],
    ): F[BatchProgress[K, S]] =
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

    def processBatch(batch: List[EventEnvelope[A]], projectionState: Ref[F, ProjectionCheckpointState]): F[Unit] =
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
                   val finalProgress = BatchProgress[K, S](
                     stateCache = keyStatesList.toMap,
                     dirtyKeys = keyStatesList.map(_._1).toSet,
                     lastProcessedPosition = Some(batch.last.metadata.globalPosition),
                   )
                   persistProgress(finalProgress, projectionState)

                 case Left(_) =>
                   val progress0 = BatchProgress[K, S](
                     stateCache = initialStates,
                     dirtyKeys = Set.empty,
                     lastProcessedPosition = None,
                   )
                   batch
                     .foldLeftM(progress0) { (progress, event) =>
                       processEvent(progress, event).handleErrorWith { error =>
                         persistProgress(progress, projectionState, Some(error)) *> Async[F].raiseError(error)
                       }
                     }
                     .flatMap(progress => persistProgress(progress, projectionState))
               }
        } yield ()
      }

    def processEvents(projectionState: Ref[F, ProjectionCheckpointState]): F[Int] =
      val passLimit = batchSize * maxBatchPerPass

      for {
        state  <- projectionState.get
        events <- eventStore
                    .readFrom(
                      state.globalPosition,
                      EventFilter(projection.filter, Set.empty),
                      Some(passLimit),
                    )
                    .compile
                    .toList
        _ <- events.grouped(batchSize).toList.traverse_(batch => processBatch(batch, projectionState))
      } yield events.size

    def notificationHandler(wakeupState: Ref[F, WakeupState], notification: EventStoreNotification): F[Unit] =
      notification match {
        case EventsAppended      => markPending(wakeupState)
        case UnknownNotification => Applicative[F].unit
        case other               => markNotification(wakeupState, other)
      }

    def markNotification(wakeupState: Ref[F, WakeupState], notification: EventStoreNotification): F[Unit] =
      wakeupState.modify {
        case WakeupState(false, _, signal) =>
          WakeupState(
            pending = false,
            processNotif = Some(notification),
            signal = signal,
          ) -> signal.complete(()).void

        case WakeupState(true, _, signal) =>
          WakeupState(
            pending = true,
            processNotif = Some(notification),
            signal = signal,
          ) -> Applicative[F].unit
      }.flatten

    def markPending(wakeupState: Ref[F, WakeupState]): F[Unit] =
      wakeupState.modify {
        case current @ WakeupState(true, _, _) =>
          current -> Applicative[F].unit
        case WakeupState(false, notif, signal) =>
          WakeupState(pending = true, notif, signal) -> signal.complete(()).void
      }.flatten

    def awaitWork(wakeupState: Ref[F, WakeupState]): F[Work] =
      Deferred[F, Unit].flatMap { nextSignal =>
        wakeupState.modify {
          case WakeupState(false, None, signal) =>
            WakeupState(false, None, signal) -> (signal.get *> awaitWork(wakeupState))

          case WakeupState(pending, notif, _) =>
            WakeupState(
              pending = false,
              processNotif = None,
              signal = nextSignal,
            ) -> Applicative[F].pure(Work(pendingEvents = pending, notif = notif))
        }.flatten
      }

    def processNotification(
      notif: EventStoreNotification,
      projectionState: Ref[F, ProjectionCheckpointState],
      wakeupState: Ref[F, WakeupState],
    ): F[Unit] =

      def saveState(update: ProjectionCheckpointState => ProjectionCheckpointState): F[Unit] =
        for {
          current <- projectionState.get
          next     = update(current)
          _       <- checkpoint.save(next)
          _       <- projectionState.set(next)
        } yield ()

      notif match {
        case PauseProjection(_)              => saveState(_.copy(running = false))
        case ResumeProjection(_)             => saveState(_.copy(running = true, error = None)) *> markPending(wakeupState)
        case UpdateCheckpointIndex(_, index) =>
          saveState(_.copy(globalPosition = index, error = None)) *> markPending(wakeupState)
        case _ => Applicative[F].unit
      }

    def formatError(e: Throwable): String =
      s"${e.getClass.getSimpleName}: ${e.getMessage}\n${e.getStackTrace.mkString("\n")}"

    def pauseWithError(projectionState: Ref[F, ProjectionCheckpointState])(error: Throwable): F[Unit] =
      for {
        current <- projectionState.get
        next     = current.copy(running = false, error = Some(formatError(error)))
        _       <- checkpoint.save(next).handleErrorWith(_ => Applicative[F].unit)
        _       <- projectionState.set(next)
      } yield ()

    Stream.eval {
      for {
        maybeState      <- checkpoint.load(projection.name)
        initialState     = maybeState.getOrElse(ProjectionCheckpointState(projection.name, -1L, true, None))
        projectionState <- Ref.of[F, ProjectionCheckpointState](initialState)
        initialSignal   <- Deferred[F, Unit]
        wakeupState     <- Ref.of[F, WakeupState](
                         WakeupState(
                           pending = true,
                           processNotif = None,
                           signal = initialSignal,
                         ),
                       )
      } yield (projectionState, wakeupState)
    }.flatMap { case (projectionState, wakeupState) =>
      val notifications =
        eventStore
          .notification(projection.name)
          .evalMap(notification => notificationHandler(wakeupState, notification))
          .drain

      val passLimit = batchSize * maxBatchPerPass

      val projector =
        Stream
          .repeatEval(awaitWork(wakeupState))
          .evalMap { work =>
            (for {
              _         <- work.notif.traverse_(notif => processNotification(notif, projectionState, wakeupState))
              state     <- projectionState.get
              processed <- if (state.running && work.pendingEvents)
                             processEvents(projectionState)
                           else
                             Applicative[F].pure(0)
              _ <-
                if processed == passLimit then markPending(wakeupState)
                else Applicative[F].unit
            } yield ()).handleErrorWith(pauseWithError(projectionState))
          }

      projector.concurrently(notifications)
    }
  }
