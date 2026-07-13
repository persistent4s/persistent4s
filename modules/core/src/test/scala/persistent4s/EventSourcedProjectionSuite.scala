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

import cats.effect.IO
import cats.syntax.all.*
import weaver.SimpleIOSuite

import java.time.Instant
import java.util.UUID

object EventSourcedProjectionSuite extends SimpleIOSuite:

  // ---------------------------------------------------------------------------
  // Test domain
  // ---------------------------------------------------------------------------

  sealed trait E extends Event

  final case class Created(id: String, value: Int) extends E

  final case class Incremented(id: String) extends E

  final case class FannedOut(ids: List[String]) extends E

  final case class Ignored(id: String) extends E // intentionally has no handler

  private val noopRepository: Repository[IO, String, Int] = new Repository[IO, String, Int]:
    def findMany(keys: List[String]): IO[Map[String, Option[Int]]] = IO.pure(Map.empty)
    def persist(upserts: Map[String, Int], deletes: List[String]): IO[Unit] = IO.unit

  private val projection: EventSourcedProjection[IO, E, String, Int] =
    new EventSourcedProjection[IO, E, String, Int]:
      val name = "test"
      protected val repository: Repository[IO, String, Int] = noopRepository
      val handlers = List(
        on[Created](_.id)((_, e) => e.value.some),
        on[Incremented](_.id)((state, _) => state.map(_ + 1)),
        onMany[FannedOut](_.ids)((state, _) => IO.pure(state.map(_ + 10))),
      )

  private def env(payload: E): EventEnvelope[E] =
    EventEnvelope(
      EventMetadata(1L, UUID.randomUUID(), Set.empty, EventTypeName.fromInstance(payload), isExternal = false,
        Instant.now()),
      payload,
    )

  // ---------------------------------------------------------------------------
  // Tests
  // ---------------------------------------------------------------------------

  pureTest("filter is derived from the handlers and excludes unregistered event types") {
    expect(
      projection.filter == Set(EventTypeName.of[Created], EventTypeName.of[Incremented], EventTypeName.of[FannedOut]),
    ) and expect(!projection.filter.contains(EventTypeName.of[Ignored]))
  }

  pureTest("resolveKeys dispatches by event type and yields no keys for unregistered events") {
    expect(projection.resolveKeys(env(Created("a", 1))) == List("a")) and
      expect(projection.resolveKeys(env(Incremented("b"))) == List("b")) and
      expect(projection.resolveKeys(env(Ignored("c"))) == Nil)
  }

  pureTest("onMany fans a single event out to every resolved key") {
    expect(projection.resolveKeys(env(FannedOut(List("a", "b", "c")))) == List("a", "b", "c"))
  }

  test("handle routes each event to its own handler") {
    for
      created <- projection.handle(None, env(Created("a", 7)))
      incr    <- projection.handle(Some(7), env(Incremented("a")))
      fanned  <- projection.handle(Some(1), env(FannedOut(List("a"))))
    yield expect(created == Some(7)) and expect(incr == Some(8)) and expect(fanned == Some(11))
  }

  test("handle leaves state untouched for events with no registered handler") {
    projection.handle(Some(42), env(Ignored("a"))).map(r => expect(r == Some(42)))
  }
