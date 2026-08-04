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

import cats.MonadThrow
import cats.syntax.all.*
import fs2.Stream

/** Transactional outbox for ephemeral messages that are not domain events */
trait MessageOutbox[F[_]]:

  /** Enqueue raw messages. */
  def enqueue(messages: List[OutgoingMessage]): F[Unit]

  /** Claim up to `batchSize` unpublished entries, hand them to `publish`, and remove them on success. Returns the
    * number processed.
    */
  def drainBatch(batchSize: Int)(publish: List[(Long, OutgoingMessage)] => F[Unit]): F[Int]

  /** Wake-up signal emitted when new entries may be available; `Stream.empty` if the implementation can't signal. */
  def notifications: Stream[F, Unit]

object MessageOutbox:

  extension [F[_]: MonadThrow](outbox: MessageOutbox[F])

    /** Typed convenience: encode `message` via its [[MessageCodec]] and enqueue it to `topic`. Encoding failures are
      * raised into `F`.
      */
    def send[M](topic: String, key: Option[String], message: M, headers: Map[String, String] = Map.empty)(using
      codec: MessageCodec[M],
    ): F[Unit] =
      codec.encode(message) match
        case Right(payload) => outbox.enqueue(List(OutgoingMessage(topic, key, payload, headers)))
        case Left(error)    => error.raiseError[F, Unit]
