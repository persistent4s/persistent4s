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

package persistent4s.postgres

import scala.concurrent.duration.*

import cats.effect.*
import cats.effect.std.Supervisor
import cats.syntax.all.*

import fs2.Stream
import fs2.concurrent.Topic

import org.typelevel.log4cats.Logger

import skunk.Session
import skunk.data.{Identifier, Notification}

import persistent4s.Backoff

/** Shares one PostgreSQL `LISTEN` session between every notification consumer of the process.
  *
  * A consumer that runs `LISTEN` itself keeps a pooled session for as long as it listens, so a pool smaller than the
  * number of projections and relays leaves none for their reads. The hub instead keeps a single session listening on a
  * fixed set of channels and broadcasts each notification to the subscribers of its channel.
  *
  * The session is opened by the first [[listenR]] and kept until the hub is released. If it fails, the hub reconnects
  * with exponential backoff. Notifications sent while no `LISTEN` is registered are lost, so the hub publishes
  * [[PostgresNotificationHub.Message.CatchUp]] to every subscriber once `LISTEN` is registered, on the first connection
  * and after each reconnection; subscribers must then re-read what they track. Nothing is published while the hub is
  * disconnected: the database may well be unreachable then, and a projection whose read fails pauses until resumed. The
  * short maximum reconnection delay bounds how long a recovered database goes unnoticed.
  *
  * Subscribers share the connection: one that stops pulling while its queue is full delays every other subscriber.
  */
final class PostgresNotificationHub[F[_]: Async: Logger] private (
  pool: Resource[F, Session[F]],
  topics: Map[Identifier, Topic[F, PostgresNotificationHub.Message]],
  supervisor: Supervisor[F],
  started: Ref[F, Boolean],
  maxQueued: Int,
  retryInitialDelay: FiniteDuration,
  retryMaxDelay: FiniteDuration,
):

  import PostgresNotificationHub.*

  /** Subscribe to `channel`. The subscription is registered once the resource is acquired: every notification sent on
    * `channel` afterwards is either delivered as a [[Message.Payload]] or followed by a [[Message.CatchUp]].
    *
    * @throws IllegalArgumentException
    *   if the hub was not built to listen on `channel`
    */
  def listenR(channel: Identifier): Resource[F, Stream[F, Message]] =
    topics.get(channel) match
      case Some(topic) => topic.subscribeAwait(maxQueued) <* Resource.eval(start)
      case None        =>
        Resource.raiseError[F, Stream[F, Message], Throwable](
          new IllegalArgumentException(s"The notification hub does not listen on channel ${channel.value}"),
        )

  private val start: F[Unit] =
    Async[F].uncancelable { _ =>
      started.getAndSet(true).flatMap(alreadyStarted => supervisor.supervise(run).void.unlessA(alreadyStarted))
    }

  private def run: F[Unit] =
    Backoff.retryWithBackoff(
      listenOnce.handleErrorWith { error =>
        Logger[F].warn(error)(s"PostgreSQL notification listener failed, reconnecting") *> error.raiseError[F, Unit]
      },
      retryInitialDelay,
      retryMaxDelay,
    )

  /** Listen on every channel with one session until it fails; never completes normally. */
  private def listenOnce: F[Unit] =
    pool.use { session =>
      topics.toList.traverse { case (channel, topic) =>
        listen(session, channel).map(_.evalMap(notification => topic.publish1(Message.Payload(notification.value))))
      }.use { forwarders =>
        catchUp *>
          Stream.emits(forwarders).parJoinUnbounded.compile.drain *>
          ListenerStopped.raiseError[F, Unit]
      }
    }

  /** `LISTEN` on `channel`, ignoring a failed `UNLISTEN` on release. It only fails once the session is broken, which is
    * when the listener is already failing with its own error; the runtime would otherwise report it on every
    * reconnection. Nothing is left registered either way: the pool discards a broken session and runs `UNLISTEN *` when
    * recycling a healthy one.
    */
  private def listen(session: Session[F], channel: Identifier): Resource[F, Stream[F, Notification[String]]] =
    Resource
      .make(session.channel(channel).listenR(maxQueued).allocated) { case (_, unlisten) =>
        unlisten.handleError(_ => ())
      }
      .map { case (notifications, _) => notifications }

  private def catchUp: F[Unit] =
    topics.values.toList.traverse_(_.publish1(Message.CatchUp))

object PostgresNotificationHub:

  /** What a subscriber receives from the hub. */
  enum Message:

    /** The payload of a notification sent on the subscribed channel. */
    case Payload(value: String)

    /** Notifications may have been missed: the hub just registered `LISTEN`, on its first connection or after losing
      * it.
      */
    case CatchUp

  /** Raised when the notification streams of the listening session end without an error. */
  case object ListenerStopped extends RuntimeException("PostgreSQL notification listener stopped")

  val DefaultMaxQueued: Int = 1024

  val DefaultRetryInitialDelay: FiniteDuration = 500.millis

  val DefaultRetryMaxDelay: FiniteDuration = 5.seconds

  /** Build a hub listening on `channels`. No session is used until the first [[PostgresNotificationHub.listenR]];
    * releasing the resource stops the listener and ends every subscriber stream.
    *
    * @param pool
    *   sessions to listen with; the hub holds one of them while it is running
    * @param channels
    *   the channels subscribers may listen on (default: the event store and message outbox channels)
    * @param maxQueued
    *   notifications buffered per subscriber before the listener waits for it
    * @param retryInitialDelay
    *   delay before the first reconnection attempt
    * @param retryMaxDelay
    *   upper bound of the doubling delay between reconnection attempts, and so of how long subscribers go without a
    *   catch-up once the database is reachable again
    */
  def make[F[_]: Async: Logger](
    pool: Resource[F, Session[F]],
    channels: Set[Identifier] = Set(PostgresEventStore.NotificationChannel, PostgresMessageOutbox.NotificationChannel),
    maxQueued: Int = DefaultMaxQueued,
    retryInitialDelay: FiniteDuration = DefaultRetryInitialDelay,
    retryMaxDelay: FiniteDuration = DefaultRetryMaxDelay,
  ): Resource[F, PostgresNotificationHub[F]] =
    for
      topics     <- channels.toList.traverse(channel => Resource.eval(Topic[F, Message]).map(channel -> _)).map(_.toMap)
      _          <- Resource.onFinalize(topics.values.toList.traverse_(_.close))
      supervisor <- Supervisor[F](await = false)
      started    <- Resource.eval(Ref.of[F, Boolean](false))
    yield new PostgresNotificationHub(pool, topics, supervisor, started, maxQueued, retryInitialDelay, retryMaxDelay)
