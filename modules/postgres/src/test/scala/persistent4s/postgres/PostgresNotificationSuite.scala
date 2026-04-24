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

package persistent4s.postgres

import cats.effect.IO
import persistent4s.EventStoreNotification
import persistent4s.EventStoreNotification.*
import weaver.SimpleIOSuite

object PostgresNotificationSuite extends SimpleIOSuite:

  // ---------------------------------------------------------------------------
  // Round-trip: encode then decode must return the original value
  // ---------------------------------------------------------------------------

  test("round-trip: EventsAppended") {
    IO.pure(expect(PostgresNotification.decode(PostgresNotification.encode(EventsAppended)) == EventsAppended))
  }

  test("round-trip: PauseProjection") {
    val n = PauseProjection("my-projection")
    IO.pure(expect(PostgresNotification.decode(PostgresNotification.encode(n)) == n))
  }

  test("round-trip: ResumeProjection") {
    val n = ResumeProjection("my-projection")
    IO.pure(expect(PostgresNotification.decode(PostgresNotification.encode(n)) == n))
  }

  test("round-trip: UpdateCheckpointIndex") {
    val n = UpdateCheckpointIndex("my-projection", 42L)
    IO.pure(expect(PostgresNotification.decode(PostgresNotification.encode(n)) == n))
  }

  test("round-trip: UnknownNotification encodes then decodes back to UnknownNotification") {
    IO.pure(
      expect(PostgresNotification.decode(PostgresNotification.encode(UnknownNotification)) == UnknownNotification),
    )
  }

  // ---------------------------------------------------------------------------
  // Encode: known wire strings
  // ---------------------------------------------------------------------------

  test("encode EventsAppended produces the expected wire string") {
    IO.pure(expect(PostgresNotification.encode(EventsAppended) == "events_appended"))
  }

  test("encode PauseProjection produces the expected wire string") {
    IO.pure(expect(PostgresNotification.encode(PauseProjection("proj")) == "pause_projection:proj"))
  }

  test("encode ResumeProjection produces the expected wire string") {
    IO.pure(expect(PostgresNotification.encode(ResumeProjection("proj")) == "resume_projection:proj"))
  }

  test("encode UpdateCheckpointIndex produces the expected wire string") {
    IO.pure(expect(PostgresNotification.encode(UpdateCheckpointIndex("proj", 7L)) == "update_checkpoint_index:proj:7"))
  }

  // ---------------------------------------------------------------------------
  // Decode: known wire strings
  // ---------------------------------------------------------------------------

  test("decode 'events_appended' produces EventsAppended") {
    IO.pure(expect(PostgresNotification.decode("events_appended") == EventsAppended))
  }

  test("decode 'pause_projection:proj' produces PauseProjection") {
    IO.pure(expect(PostgresNotification.decode("pause_projection:proj") == PauseProjection("proj")))
  }

  test("decode 'resume_projection:proj' produces ResumeProjection") {
    IO.pure(expect(PostgresNotification.decode("resume_projection:proj") == ResumeProjection("proj")))
  }

  test("decode 'update_checkpoint_index:proj:99' produces UpdateCheckpointIndex") {
    IO.pure(expect(PostgresNotification.decode("update_checkpoint_index:proj:99") == UpdateCheckpointIndex("proj", 99L)))
  }

  test("decode an unrecognised string produces UnknownNotification") {
    IO.pure(expect(PostgresNotification.decode("something_completely_unknown") == UnknownNotification))
  }

  // ---------------------------------------------------------------------------
  // Edge cases for projection names that contain colons
  // ---------------------------------------------------------------------------

  test("round-trip: PauseProjection with colons in name") {
    val n = PauseProjection("ns:sub:proj")
    IO.pure(expect(PostgresNotification.decode(PostgresNotification.encode(n)) == n))
  }

  test("round-trip: ResumeProjection with colons in name") {
    val n = ResumeProjection("ns:sub:proj")
    IO.pure(expect(PostgresNotification.decode(PostgresNotification.encode(n)) == n))
  }

  test("round-trip: UpdateCheckpointIndex with colons in projection name") {
    // encode produces "update_checkpoint_index:ns:sub:proj:42"
    // decode uses lastIndexOf(':') so it correctly splits at the final colon
    val n = UpdateCheckpointIndex("ns:sub:proj", 42L)
    IO.pure(expect(PostgresNotification.decode(PostgresNotification.encode(n)) == n))
  }

  test("decode UpdateCheckpointIndex with colons in name reconstructs the full name") {
    val encoded = "update_checkpoint_index:ns:sub:proj:42"
    IO.pure(expect(PostgresNotification.decode(encoded) == UpdateCheckpointIndex("ns:sub:proj", 42L)))
  }

  // ---------------------------------------------------------------------------
  // Malformed inputs decode to UnknownNotification
  // ---------------------------------------------------------------------------

  test("decode 'update_checkpoint_index:proj' with no index produces UnknownNotification") {
    // missing second colon so lastIndexOf(':') resolves on the prefix colon, giving a non-numeric index fragment
    IO.pure(
      expect(PostgresNotification.decode("update_checkpoint_index:proj") == UnknownNotification),
    )
  }

  test("decode 'update_checkpoint_index:proj:notanumber' produces UnknownNotification") {
    IO.pure(
      expect(PostgresNotification.decode("update_checkpoint_index:proj:notanumber") == UnknownNotification),
    )
  }

  test("decode empty string produces UnknownNotification") {
    IO.pure(expect(PostgresNotification.decode("") == UnknownNotification))
  }