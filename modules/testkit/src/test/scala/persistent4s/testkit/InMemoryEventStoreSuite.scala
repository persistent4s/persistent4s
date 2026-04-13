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

package persistent4s.testkit

import cats.syntax.all.*
import cats.effect.IO
import persistent4s.{Event, EventFilter, EventTypeName, IndexConflictException, Tag}
import weaver.SimpleIOSuite

object InMemoryEventStoreSuite extends SimpleIOSuite:

  private val student1 = Tag("student", "1")

  private val student2 = Tag("student", "2")

  private val course1 = Tag("course", "1")

  sealed trait TestEvent extends Event

  object TestEvent:

    final case class StudentCreated(studentId: String) extends TestEvent

    final case class StudentDeleted(studentId: String) extends TestEvent

    final case class CourseCreated(courseId: String) extends TestEvent

    final case class EnrollmentCreated(studentId: String, courseId: String) extends TestEvent

  private def appendOne(
    store: InMemoryEventStore[IO, TestEvent],
    expectedIndex: Long,
    tags: Set[Tag],
    eventType: EventTypeName,
    event: TestEvent,
  ): IO[Unit] =
    store.append(EventFilter(Set.empty, tags), expectedIndex, List((tags, eventType, event)))

  private def isConflict(result: Either[Throwable, Unit]): Boolean =
    result match
      case Left(_: IndexConflictException) => true
      case _                               => false

  test("append stores an event in memory") {
    for
      store <- InMemoryEventStore.make[IO, TestEvent]
      event  = TestEvent.StudentCreated("1")
      _     <- appendOne(store, 0L, Set(student1), EventTypeName.of[TestEvent.StudentCreated], event)
      saved <- store.getEvents
    yield expect.all(
      saved.length == 1,
      saved.head.metadata.globalPosition == 1L,
      saved.head.metadata.tags == Set(student1),
      saved.head.metadata.eventType == EventTypeName.of[TestEvent.StudentCreated],
      saved.head.payload == event,
    )
  }

  test("concurrent appends with the same tag allow only one success") {
    for
      store <- InMemoryEventStore.make[IO, TestEvent]
      first  = appendOne(
                store,
                0L,
                Set(student1),
                EventTypeName.of[TestEvent.StudentCreated],
                TestEvent.StudentCreated("1"),
              ).attempt
      second = appendOne(
                 store,
                 0L,
                 Set(student1),
                 EventTypeName.of[TestEvent.StudentDeleted],
                 TestEvent.StudentDeleted("1"),
               ).attempt
      results      <- (first, second).parTupled
      events       <- store.getEvents
      successCount  = List(results._1, results._2).count(_.isRight)
      conflictCount = List(results._1, results._2).count(isConflict)
    yield expect.all(
      successCount == 1,
      conflictCount == 1,
      events.length == 1,
    )
  }

  test("concurrent appends with overlapping tag sets allow only one success") {
    for
      store <- InMemoryEventStore.make[IO, TestEvent]
      first  = appendOne(
                store,
                0L,
                Set(student1, course1),
                EventTypeName.of[TestEvent.EnrollmentCreated],
                TestEvent.EnrollmentCreated("1", "1"),
              ).attempt
      second = appendOne(
                 store,
                 0L,
                 Set(student1),
                 EventTypeName.of[TestEvent.StudentCreated],
                 TestEvent.StudentCreated("1"),
               ).attempt
      results      <- (first, second).parTupled
      events       <- store.getEvents
      successCount  = List(results._1, results._2).count(_.isRight)
      conflictCount = List(results._1, results._2).count(isConflict)
    yield expect.all(
      successCount == 1,
      conflictCount == 1,
      events.length == 1,
    )
  }

  test("many concurrent appends with distinct tags all succeed and keep unique global positions") {
    val numberOfEvents = 50

    for
      store   <- InMemoryEventStore.make[IO, TestEvent]
      results <- (1 to numberOfEvents).toList.parTraverse { index =>
                   appendOne(
                     store,
                     0L,
                     Set(Tag("student", index.toString)),
                     EventTypeName.of[TestEvent.StudentCreated],
                     TestEvent.StudentCreated(index.toString),
                   ).attempt
                 }
      events   <- store.getEvents
      positions = events.map(_.metadata.globalPosition)
    yield expect.all(
      results.forall(_.isRight),
      events.length == numberOfEvents,
      positions.distinct.length == numberOfEvents,
      positions.sorted == (1L to numberOfEvents.toLong).toVector,
    )
  }

  test("read filters events by tags and event types") {
    for
      store <- InMemoryEventStore.make[IO, TestEvent]
      _     <-
        appendOne(store, 0L, Set(student1), EventTypeName.of[TestEvent.StudentCreated], TestEvent.StudentCreated("1"))
      _ <-
        appendOne(store, 0L, Set(student2), EventTypeName.of[TestEvent.StudentCreated], TestEvent.StudentCreated("2"))
      _ <-
        appendOne(store, 1L, Set(student1), EventTypeName.of[TestEvent.StudentDeleted], TestEvent.StudentDeleted("1"))
      _        <- appendOne(store, 0L, Set(course1), EventTypeName.of[TestEvent.CourseCreated], TestEvent.CourseCreated("1"))
      filtered <-
        store.readFrom(0L, EventFilter(Set(EventTypeName.of[TestEvent.StudentCreated]), Set(student1))).compile.toList
    yield expect.all(
      filtered.length == 1,
      filtered.head.metadata.tags == Set(student1),
      filtered.head.metadata.eventType == EventTypeName.of[TestEvent.StudentCreated],
      filtered.head.payload == TestEvent.StudentCreated("1"),
    )
  }
