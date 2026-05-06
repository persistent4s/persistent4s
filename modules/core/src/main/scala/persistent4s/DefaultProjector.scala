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
import org.typelevel.otel4s.Attribute
import org.typelevel.otel4s.metrics.{Histogram, Meter}
import org.typelevel.otel4s.trace.Tracer

/** The default [[Projector]] implementation.
  *
  * Events are read in chunks of up to `batchSize` and processed sequentially within each chunk. For each chunk, all
  * distinct keys are looked up once via [[Projection.fetchState]], the events are folded in order, and the resulting
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
  override def run[K, S](projection: Projection[F, A, K, S]): Stream[F, Unit] = {

    Stream
      .eval(makeInstruments)
      .flatMap { (batchSizeHist, batchDurationHist) =>
        val projAttr = Attribute("projection.name", projection.name)

        def persistProgress(progress: BatchProgress[K, S]): F[Unit] =
          val statesToPersist = progress.dirtyKeys.map { key =>
            key -> progress.stateCache.getOrElse(key, None)
          }.toMap
          projection.persistStates(statesToPersist) *> progress.lastProcessedPosition
            .traverse_(checkpoint.save(projection.name, _))
            .void

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

        def processBatch(batch: List[EventEnvelope[A]]): F[Unit] =
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

                finalProgress <- batch.foldLeftM(progress0) { (progress, event) =>
                                   processEvent(progress, event).handleErrorWith { error =>
                                     persistProgress(progress) *> Async[F].raiseError(error)
                                   }
                                 }

                _ <- persistProgress(finalProgress)
              } yield ()

            Tracer[F]
              .spanBuilder("persistent4s.projector.batch")
              .addAttribute(projAttr)
              .addAttribute(Attribute("batch.size", batchSz))
              .build
              .surround(
                for
                  start  <- Async[F].monotonic
                  result <- body.attempt
                  end    <- Async[F].monotonic
                  _      <- batchSizeHist.record(batchSz, projAttr)
                  _      <- batchDurationHist.record((end - start).toNanos.toDouble / 1e6, projAttr)
                  _      <- result.fold(Async[F].raiseError, Async[F].pure)
                yield (),
              )
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
  }

  private def makeInstruments: F[(Histogram[F, Long], Histogram[F, Double])] =
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
    ).tupled
