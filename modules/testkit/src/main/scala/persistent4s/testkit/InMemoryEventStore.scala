/*
 * Copyright 2026 persistent4s
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

package persistent4s.testkit

import persistent4s.{EventStore, EventNotification, Tag, EventEnvelope, IndexConflictException, EventMetadata}
import cats.effect.*
import fs2.Stream
import fs2.concurrent.Topic
import cats.implicits.*
import cats.*
import persistent4s.EventFilter

/** An in-memory implementation of the EventStore trait for testing purposes. This implementation use Ref to store
  * events in memory and does not persist them to any external storage. It is intended for use in unit tests where you
  * want to verify the behavior of your command handlers and event processing logic without relying on an actual
  * database or event store.
  */
final case class InMemoryEventStore[F[_]: Monad: Async, A] private (
  store: Ref[F, Vector[EventEnvelope[A]]],
  topic: Topic[F, Unit],
) extends EventStore[F, A]
    with EventNotification[F]:

  def getEvents: F[Vector[EventEnvelope[A]]] = store.get

  override def append(expectedIndex: Long, events: List[(Set[Tag], String, A)]*): F[Unit] =
    store.modify { currentEvents =>
      val incomingTags = events.flatten.map(_._1).flatten.toSet
      val relevantEvents = currentEvents.filter(env => env.metadata.tags.exists(incomingTags.contains))
      val actualIndex = relevantEvents.lastOption.map(_.metadata.globalPosition).getOrElse(0L)

      if (actualIndex != expectedIndex) {
        (currentEvents, Left(new IndexConflictException(expectedIndex, actualIndex)))
      } else {
        val lastGlobalPosition = currentEvents.lastOption.map(_.metadata.globalPosition).getOrElse(0L)
        val newEvents = events.flatten.zipWithIndex.map { case ((tags, eventType, event), index) =>
          EventEnvelope(
            EventMetadata(
              globalPosition = lastGlobalPosition + index.toLong + 1L,
              tags = tags,
              eventType = eventType,
              timestamp = java.time.Instant.now(),
            ),
            event,
          )
        }
        (currentEvents ++ newEvents, Right(()))
      }
    }.flatMap {
      case Left(error) => Async[F].raiseError(error)
      case Right(_)    => topic.publish1(()).void
    }

  override def notification: Stream[F, Unit] =
    topic.subscribe(1)

  override def readFrom(fromPosition: Long, eventFilter: EventFilter): Stream[F, EventEnvelope[A]] =
    Stream
      .eval(store.get)
      .flatMap(events => Stream.emits(events))
      .filter { env =>
        val matchesTags = eventFilter.tags.isEmpty || env.metadata.tags.exists(eventFilter.tags.contains)
        val matchesTypes = eventFilter.eventTypes.isEmpty || eventFilter.eventTypes.contains(env.metadata.eventType)
        matchesTags && matchesTypes && env.metadata.globalPosition > fromPosition
      }

object InMemoryEventStore:

  def make[F[_]: Async, A]: F[InMemoryEventStore[F, A]] =
    for
      store <- Ref.of[F, Vector[EventEnvelope[A]]](Vector.empty)
      topic <- Topic[F, Unit]
    yield InMemoryEventStore(store, topic)
