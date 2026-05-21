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
import persistent4s.examples.courses.catalog.domain.{CapacityChanged, CatalogEvent, CourseClosed, CourseOpened}
import persistent4s.kafka.{EventSubscriber, KafkaConsumerConfig, KafkaModule}
import persistent4s.EventCodec

/** Subscribes to `catalog.events`, applies each event to the local `course_view` table, then commits the offset.
  *
  * At-least-once: the upsert/delete is idempotent so re-deliveries are safe. On a decode or DB error the fs2 stream
  * fails — the supervising fiber terminates and the server crashes loudly. A production deployment would route to a DLQ
  * instead; for an example this fail-fast behavior is intentional.
  */
object CatalogEventConsumer:

  def stream[F[_]: Async](
    consumerCfg: KafkaConsumerConfig,
    codec: EventCodec[CatalogEvent],
    repo: CourseViewRepository[F],
    topic: String,
  ): Resource[F, Stream[F, Unit]] =
    KafkaModule.subscriber[F, CatalogEvent](consumerCfg, codec).map { subscriber =>
      consume(subscriber, repo, topic)
    }

  private def consume[F[_]: Async](
    subscriber: EventSubscriber[F, CatalogEvent],
    repo: CourseViewRepository[F],
    topic: String,
  ): Stream[F, Unit] =
    subscriber
      .subscribe(topic, fromBeginning = true)
      .evalMap { case (envelope, offset) =>
        // At-least-once: apply (idempotent) before committing. Per-message commit is the simplest correct pattern;
        // batching would be a perf optimization not worth the example's complexity.
        apply(envelope, repo) *> offset.commit
      }

  private def apply[F[_]: Async](
    envelope: EventEnvelope[CatalogEvent],
    repo: CourseViewRepository[F],
  ): F[Unit] = envelope.payload match
    case CourseOpened(id, code, title, capacity, instructor) =>
      repo.upsert(CourseView(id, code, title, capacity, instructor, isOpen = true))
    case CapacityChanged(id, newCapacity) =>
      repo.find(id).flatMap {
        case Some(existing) => repo.upsert(existing.copy(capacity = newCapacity))
        case None           =>
          // CapacityChanged before CourseOpened locally — shouldn't happen under single-partition + per-tag ordering;
          // skip rather than crash so a later replay can self-heal once the missing CourseOpened arrives.
          Async[F].unit
      }
    case CourseClosed(id) =>
      repo.find(id).flatMap {
        case Some(existing) => repo.upsert(existing.copy(isOpen = false))
        case None           => Async[F].unit
      }
