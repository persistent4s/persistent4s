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

package persistent4s.examples.courses.enrollment.domain.courseview

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import fs2.Stream

import persistent4s.EventEnvelope
import persistent4s.kafka.{EventSubscriber, KafkaConsumerConfig, KafkaModule}
import persistent4s.EventCodec
import persistent4s.EventStore
import persistent4s.examples.courses.enrollment.domain.SchoolEvent

/** Subscribes to `catalog.events`, applies each event to the local `course_view` table, then commits the offset.
  *
  * At-least-once: the upsert/delete is idempotent so re-deliveries are safe. On a decode or DB error the fs2 stream
  * fails — the supervising fiber terminates and the server crashes loudly. A production deployment would route to a DLQ
  * instead; for an example this fail-fast behavior is intentional.
  */
object CatalogEventConsumer:

  def stream[F[_]: Async](
    consumerCfg: KafkaConsumerConfig,
    store: EventStore[F, SchoolEvent],
    codec: EventCodec[SchoolEvent],
    topic: String,
  ): Resource[F, Stream[F, Unit]] =
    KafkaModule.subscriber[F, SchoolEvent](consumerCfg, codec).map { subscriber =>
      consume(subscriber, store, topic)
    }

  private def consume[F[_]: Async](
    subscriber: EventSubscriber[F, SchoolEvent],
    store: EventStore[F, SchoolEvent],
    topic: String,
  ): Stream[F, Unit] =
    subscriber
      .subscribe(topic, fromBeginning = true)
      .evalMap { case (envelope, offset) =>
        // At-least-once: apply (idempotent) before committing. Per-message commit is the simplest correct pattern;
        // batching would be a perf optimization not worth the example's complexity.
        val tags = envelope.metadata.tags
        /* store.append(¨
         eventFilter = None,

        ) *> */
        offset.commit
      }

  private def apply[F[_]: Async](
    envelope: EventEnvelope[SchoolEvent],
  ): F[Unit] = ???
