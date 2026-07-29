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

package persistent4s.examples.courses.enrollment.domain.course

import cats.effect.{Async, Resource}
import cats.syntax.all.*
import fs2.Stream

import persistent4s.kafka.{KafkaConsumerConfig, KafkaModule}
import persistent4s.EventSubscriber
import persistent4s.EventCodec
import persistent4s.EventStore
import persistent4s.PendingEvent
import persistent4s.examples.courses.enrollment.domain.SchoolEvent

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
      store.appendUnchecked(
        List(
          PendingEvent(
            payload = envelope.payload,
            tags = envelope.metadata.tags,
            eventType = envelope.metadata.eventType,
            isExternal = true,
            id = Some(envelope.metadata.id),
            headers = envelope.metadata.headers,
          ),
        ),
      ) *>
        offset
    }
