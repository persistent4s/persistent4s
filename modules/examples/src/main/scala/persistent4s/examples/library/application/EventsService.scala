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

package persistent4s.examples.library.application

import cats.effect.IO

import persistent4s.EventFilter
import persistent4s.examples.library.api.*
import persistent4s.examples.library.infrastructure.LibraryModule
import smithy4s.Timestamp

class EventsServiceImpl(module: LibraryModule) extends EventsService[IO]:

  def getEvents(): IO[GetEventsOutput] =
    module.store
      .readFrom(0, EventFilter())
      .compile
      .toList
      .map { events =>
        GetEventsOutput(
          events.map { e =>
            EventItem(
              globalPosition = e.metadata.globalPosition,
              eventType = e.metadata.eventType,
              tags = e.metadata.tags.map(_.value).toList,
              timestamp = Timestamp.fromInstant(e.metadata.timestamp),
            )
          },
        )
      }
