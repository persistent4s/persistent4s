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

package persistent4s.examples.school.infrastructure

import cats.effect.IO
import cats.effect.unsafe.implicits.global

import persistent4s.EventStore
import persistent4s.examples.school.domain.SchoolEvent
import persistent4s.testkit.InMemoryEventStore

object implicits:

  private lazy val sharedStore: InMemoryEventStore[IO, SchoolEvent] =
    InMemoryEventStore.make[IO, SchoolEvent].unsafeRunSync()

  given InMemoryEventStore[IO, SchoolEvent] = sharedStore

  given EventStore[IO, SchoolEvent] = sharedStore