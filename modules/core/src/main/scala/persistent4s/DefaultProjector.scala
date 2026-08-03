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

import scala.concurrent.duration.*
import cats.Applicative
import cats.effect.Async
import cats.effect.Deferred
import cats.effect.Ref
import cats.syntax.all.*
import fs2.Stream
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.{Counter, Histogram, Meter}
import org.typelevel.otel4s.trace.Tracer
import persistent4s.EventStoreNotification.*
import fs2.concurrent.Topic
import java.util.UUID

/** The default [[Projector]] implementation.
  *
  * Events are read in chunks of up to `batchSize` and processed sequentially within each chunk. For each chunk, all
  * distinct keys are looked up once via [[Projection.fetchStates]], the events are folded in order, and the resulting
  * states are persisted together with a single checkpoint advance. This amortizes the I/O cost of checkpointing over
  * many events.
  *
  * On handler failure mid-batch, the successfully computed states up to the failing event are persisted and the
  * checkpoint is advanced to the last fully processed position before the error is re-raised. The stream then
  * terminates; the caller is responsible for restarting it (e.g. via `Stream.retry` or a supervisor).
  *
  * New events are detected via [[EventNotification]]. Notifications are coalesced: if multiple events arrive while a
  * batch is being processed, only one additional pass is triggered rather than one per notification.
  *
  * @param eventStore
  *   the event store to read from, which must also implement [[EventNotification]]
  * @param checkpoint
  *   durable storage for the last processed position per projection
  * @param batchSize
  *   maximum number of events processed in a single batch (default: 100). A larger value reduces checkpoint overhead
  *   but increases memory usage and the reprocessing window after a failure.
  */
final case class DefaultProjector[F[_]: Async: Tracer: Meter, A <: Event](
  eventStore: EventStore[F, A] & EventNotification[F],
  checkpoint: ProjectionCheckpoint[F],
  batchSize: Int = 100,
  maxBatchPerPass: Int = 10,
  publishTimeout: FiniteDuration = 1.second,
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
    processedEvents: List[EventEnvelope[A]] = Nil,
  )

  // TODO How should we handle failure? What do we do if the process dies?
  // Answer: On failure, push the error in the checkpoint state and wait on the Defer until the dev restart or fix the issue.

  // TODO Projection can miss some notifications due to connexion issue or restart.
  // A solution would be to store user requests in postgres and keep track of the last processed request in the checkpoint.
  // The projector can then reprocess all events since the last processed request to catch up on missed notifications.
  // Also note that currently, only the last user intent is stored if multiple notifications arrive during a batch processing.

  override def run[K, S](
    projection: Projection[F, A, K, S],
    topic: Option[Topic[F, (UUID, Either[Throwable, Map[K, Option[S]]])]] = None,
  ): Stream[F, Unit] = {

    Stream
      .eval(makeInstruments)
      .flatMap { (batchSizeHist, batchDurationHist, pausedCounter) =>
        val projAttr = Attribute("projection.name", projection.name)

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
            atomicCommit = projection.persistStatesAtomically(statesToPersist, current.globalPosition, next)
            _           <- atomicCommit.getOrElse(projection.persistStates(statesToPersist) *> checkpoint.save(next))
            _           <- projectionState.set(next)
            _           <- publishResults(progress)
          } yield ()

        def publishResults(progress: BatchProgress[K, S]): F[Unit] =
          topic.traverse_ { t =>
            progress.processedEvents.traverse_ { event =>
              val keys = projection.resolveKeys(event)
              val payload = keys.map(k => k -> progress.stateCache.getOrElse(k, None)).toMap
              Async[F]
                .timeoutTo(t.publish1((event.metadata.id, Right(payload))).void, publishTimeout, Applicative[F].unit)
            }
          }

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
                processedEvents = progress.processedEvents :+ event,
              )
            }

        def processBatch(batch: List[EventEnvelope[A]], projectionState: Ref[F, ProjectionCheckpointState]): F[Unit] =
          if (batch.isEmpty) Applicative[F].unit
          else {
            val batchSz = batch.size.toLong

            val body: F[Unit] =
              val keys = batch.flatMap(event => projection.resolveKeys(event)).toSet
              for {
                initialStates <- projection.fetchStates(keys.toList)

                progress0 = BatchProgress[K, S](
                              stateCache = initialStates,
                              dirtyKeys = Set.empty,
                              lastProcessedPosition = None,
                            )

                finalProgress <-
                  batch.foldLeftM(progress0) { (progress, event) =>
                    processEvent(progress, event).handleErrorWith { error =>
                      persistProgress(progress, projectionState, Some(error)) *>
                        topic.traverse_(t =>
                          Async[F].timeoutTo(
                            t.publish1((event.metadata.id, Left(error))).void,
                            publishTimeout,
                            Applicative[F].unit,
                          ),
                        ) *>
                        Async[F].raiseError(error)
                    }
                  }

                _ <- persistProgress(finalProgress, projectionState)
              } yield ()

            Tracer[F]
              .spanBuilder("persistent4s.projector.batch")
              .addAttribute(projAttr)
              .addAttribute(Attribute("batch.size", batchSz))
              .build
              .surround(
                for
                  result <- Telemetry.timed(batchDurationHist, projAttr)(body)
                  _      <- batchSizeHist.record(batchSz, projAttr)
                  _      <- result.fold(Async[F].raiseError, Async[F].pure)
                yield (),
              )
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
            case EventsAppended =>
              markPending(wakeupState)
            case UnknownNotification =>
              Applicative[F].unit
            case other => markNotification(wakeupState, other)
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
            case PauseProjection(_) =>
              saveState(_.copy(running = false))
            case ResumeProjection(_) =>
              saveState(_.copy(running = true, error = None)) *> markPending(wakeupState)
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
            _       <- pausedCounter.add(1L, projAttr)
          } yield ()

        def retryAfterConflict(
          projectionState: Ref[F, ProjectionCheckpointState],
          wakeupState: Ref[F, WakeupState],
        ): F[Unit] =
          checkpoint
            .load(projection.name)
            .map(_.getOrElse(ProjectionCheckpointState(projection.name, -1L, true, None)))
            .flatMap(projectionState.set) *> markPending(wakeupState)

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
                } yield ()).handleErrorWith {
                  case _: ProjectionCheckpointConflict => retryAfterConflict(projectionState, wakeupState)
                  case error                           => pauseWithError(projectionState)(error)
                }
              }

          projector.concurrently(notifications)
        }
      }
  }

  private def makeInstruments: F[(Histogram[F, Long], Histogram[F, Double], Counter[F, Long])] =
    (
      Meter[F]
        .histogram[Long]("persistent4s.projector.batch.size")
        .withDescription("Number of events per processed batch")
        .withUnit("{events}")
        .create,
      Meter[F]
        .histogram[Double]("persistent4s.projector.batch.duration")
        .withDescription("Time to process one batch")
        .withUnit("ms")
        .create,
      Meter[F]
        .counter[Long]("persistent4s.projector.paused")
        .withDescription("Number of times a projection was paused due to an unrecoverable error")
        .withUnit("{pauses}")
        .create,
    ).tupled
