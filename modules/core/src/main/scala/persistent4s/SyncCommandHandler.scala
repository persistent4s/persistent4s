package persistent4s

import cats.effect.Temporal
import cats.syntax.all.*
import fs2.Stream
import fs2.concurrent.Topic
import java.util.UUID
import scala.concurrent.duration.FiniteDuration

/** Wraps a [[CommandHandler]] so that running a command also waits for a specific projection to catch up before
  * returning, instead of returning as soon as the events are appended.
  *
  * @tparam C
  *   the command type
  * @tparam S
  *   the command handler's state
  * @tparam E
  *   the event type
  * @tparam K
  *   the projection's key type
  * @tparam PS
  *   the projection's state type
  * @param handler
  *   the underlying command handler
  * @param topic
  *   the topic a [[Projector]] publishes to after persisting - see [[Projector.run]]
  * @param timeout
  *   how long to wait for the projection to catch up before failing
  * @param maxQueued
  *   the subscriber queue size passed to [[fs2.concurrent.Topic.subscribeAwait]]
  */
final case class SyncCommandHandler[F[_], C, S, E <: Event, K, PS](
  handler: CommandHandler[C, S, E],
  topic: Topic[F, (UUID, Either[Throwable, Map[K, Option[PS]]])],
  timeout: FiniteDuration,
  maxQueued: Int = 256,
):

  /** Run the command, then wait until the projection has processed and persisted every event it produced. Fails with
    * the projection's own error if any of the events fail to process, or with a timeout error if the projection doesn't
    * catch up in time. The events are appended either way - this only gates the return, it cannot undo the write.
    */
  def runSync(command: C)(using F: Temporal[F], eventStore: EventStore[F, E]): F[Map[K, Option[PS]]] =

    def awaitAll(
      stream: Stream[F, (UUID, Either[Throwable, Map[K, Option[PS]]])],
      ids: Set[UUID],
    ): F[Map[K, Option[PS]]] =
      stream.collect { case (id, result) if ids.contains(id) => (id, result) }.evalMap { case (id, result) =>
        result.liftTo[F].map(id -> _)
      }
        .scan((ids, Map.empty[K, Option[PS]])) { case ((remaining, acc), (id, payload)) =>
          (remaining - id, acc ++ payload)
        }
        .find { case (remaining, _) => remaining.isEmpty }
        .map(_._2)
        .compile
        .lastOrError

    topic.subscribeAwait(maxQueued).use { stream =>
      for
        envelopes <- handler.run(command)
        ids        = envelopes.map(_.metadata.id).toSet
        result    <-
          if ids.isEmpty then F.pure(Map.empty[K, Option[PS]])
          else F.timeout(awaitAll(stream, ids), timeout)
      yield result
    }
