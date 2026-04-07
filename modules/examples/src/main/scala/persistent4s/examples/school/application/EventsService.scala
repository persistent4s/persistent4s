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

package persistent4s.examples.school.application

import cats.effect.IO

import persistent4s.examples.school.api.{Event, EventsService, GetEventsOutput}
import persistent4s.examples.school.domain.SchoolEvent
import persistent4s.examples.school.infrastructure.implicits.given
import persistent4s.testkit.InMemoryEventStore

class EventsServiceImpl extends EventsService[IO]:

  private val store = summon[InMemoryEventStore[IO, SchoolEvent]]

  def getEvents(): IO[GetEventsOutput] =
    store.getEvents.map { events =>
      GetEventsOutput(
        events.toList.map { env =>
          Event(
            globalPosition = env.metadata.globalPosition, tags = env.metadata.tags.toList.map(_.value),
            eventType = env.metadata.eventType, timestamp = env.metadata.timestamp.toString,
            payload = env.payload.toString,
          )
        },
      )
    }
