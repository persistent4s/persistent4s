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

import java.util.UUID

import scala.util.Try

import weaver.SimpleIOSuite

object EventSchemaSuite extends SimpleIOSuite:

  final case class TestEvent(value: String, entityId: UUID = new UUID(0L, 0L)) extends Event

  private val legacyCodec = new EventCodec[TestEvent]:
    def encode(event: TestEvent): String = event.value

    def decode(eventType: EventTypeName, payload: String): Either[Throwable, TestEvent] =
      Right(TestEvent(payload))

  pureTest("an event schema exposes a stable id, version, and historical aliases") {
    val schema = EventSchema[TestEvent]("library.test-event", version = 3).withAlias("TestEvent")

    expect(schema.eventType.value == "library.test-event") and
      expect(schema.version == 3) and
      expect(schema.acceptedEventTypes.map(_.value) == Set("library.test-event", "TestEvent"))
  }

  pureTest("event schemas resolve typed scope mappings by definition or stable name") {
    val books = Scope[UUID]("library.book")
    val values = Scope[String]("library.value")
    val event = TestEvent("ABC", UUID.fromString("2dcc657a-6ca1-431a-8e62-60a5c583e9df"))
    val schema =
      EventSchema[TestEvent]("library.test-event")
        .scopedBy(books)(_.entityId)
        .scopedBy(values)(_.value)

    expect(schema.scopeNames == Set("library.book", "library.value")) and
      expect(schema.resolveScope(books, event).contains(books(event.entityId))) and
      expect(schema.resolveScope("library.value", event).contains(values("ABC"))) and
      expect(schema.resolveScope("library.unknown", event).isEmpty) and
      expect(schema.resolveScopes(event) == Set(books(event.entityId), values("ABC")))
  }

  pureTest("adding a scope mapping is immutable and duplicate scope names are rejected") {
    val books = Scope[UUID]("library.book")
    val original = EventSchema[TestEvent]("library.test-event")
    val scoped = original.scopedBy(books)(_.entityId)

    expect(original.scopeNames.isEmpty) and
      expect(scoped.scopeNames == Set("library.book")) and
      expect(Try(scoped.scopedBy(books)(_.entityId)).isFailure)
  }

  pureTest("event schemas reject empty identifiers and non-positive versions") {
    expect(Try(EventSchema[TestEvent]("", version = 1)).isFailure) and
      expect(Try(EventSchema[TestEvent]("library.test-event", version = 0)).isFailure)
  }

  pureTest("legacy schemas retain JVM class-name identity at version one") {
    val schema = EventSchema.legacy[TestEvent]

    expect(schema.eventType == EventTypeName.of[TestEvent]) and expect(schema.version == 1)
  }

  pureTest("legacy codecs expose additive schema metadata without implementation changes") {
    val event = TestEvent("payload")
    val encoded = legacyCodec.encodeWithSchema(event)

    expect(encoded.eventType == EventTypeName.of[TestEvent]) and
      expect(encoded.version == 1) and
      expect(encoded.payload == "payload")
  }

  pureTest("legacy codecs reject unknown payload versions") {
    val result = legacyCodec.decode(EventTypeName.of[TestEvent], version = 2, payload = "payload")

    expect(result.left.exists(_.isInstanceOf[UnsupportedEventVersion]))
  }
