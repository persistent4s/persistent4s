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

object ScopeSuite extends SimpleIOSuite:

  final case class ISBN(value: String)

  given ScopeKeyEncoder[ISBN] = ScopeKeyEncoder.instance(_.value.replace("-", ""))

  pureTest("a typed scope resolves a UUID to a stable scope id") {
    val id = UUID.fromString("90a7a7c2-c19f-461f-a1e8-65f622455ef0")
    val books = Scope[UUID]("library.book")

    expect(books(id) == ScopeId.fromString(s"library.book:$id").get) and
      expect(books(id).name == "library.book") and
      expect(books(id).key == id.toString)
  }

  pureTest("UUID scope keys round-trip through their stable codec") {
    val id = UUID.fromString("90a7a7c2-c19f-461f-a1e8-65f622455ef0")
    val books = Scope[UUID]("library.book")

    expect(books.decode(books(id)) == Right(id))
  }

  pureTest("custom key encoders make domain key serialization explicit") {
    val books = Scope[ISBN]("library.isbn")

    expect(books(ISBN("978-1-234")).value == "library.isbn:9781234")
  }

  pureTest("an encode-only scope reports that its typed key cannot be recovered") {
    val books = Scope[ISBN]("library.isbn")
    val result = books.decode(books(ISBN("978-1-234")))

    result match
      case Left(error: IllegalStateException) =>
        expect(error.getMessage.contains("defined without a key decoder"))
      case other => failure(s"Expected an encode-only scope error, got $other")
  }

  pureTest("scope ids preserve colons inside the encoded key") {
    val parsed = ScopeId.fromString("tenant:region:user-1")

    expect(parsed.exists(_.name == "tenant")) and
      expect(parsed.exists(_.key == "region:user-1"))
  }

  pureTest("a resolved scope can bridge to the legacy tag representation") {
    val scopeId = Scope[String]("book")("42")

    expect(scopeId.toTag == Tag("book", "42"))
  }

  pureTest("invalid scope definitions and empty encoded keys are rejected") {
    expect(Try(Scope[String]("")).isFailure) and
      expect(Try(Scope[String]("invalid:name")).isFailure) and
      expect(Try(Scope[String]("book")(" ".trim)).isFailure)
  }
