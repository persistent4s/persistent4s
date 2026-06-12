/*
 * Copyright 2026 Bastien Jolidon
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

import cats.effect.IO
import weaver.SimpleIOSuite

object InMemoryDBSuite extends SimpleIOSuite:

  // ── Fixture ────────────────────────────────────────────────────────────────

  private case class User(name: String, age: Int)

  private case class Product(sku: String, price: Double, inStock: Boolean)

  private val alice = User("alice", 30)

  private val bob = User("bob", 25)

  private val charlie = User("charlie", 35)

  private def makeUsers: IO[Table[IO, User]] =
    Table[IO, User]

  // ── Table: INSERT ──────────────────────────────────────────────────────────

  test("insert adds a single row") {
    for
      t   <- makeUsers
      _   <- t.insert(alice)
      all <- t.all
    yield expect(all == Vector(alice))
  }

  test("insertAll adds multiple rows in order") {
    for
      t   <- makeUsers
      _   <- t.insertAll(List(alice, bob, charlie))
      all <- t.all
    yield expect(all == Vector(alice, bob, charlie))
  }

  test("all returns empty vector when table is empty") {
    for
      t   <- makeUsers
      all <- t.all
    yield expect(all.isEmpty)
  }

  // ── Table: SELECT (filter / find / count / exists) ─────────────────────────

  test("filter returns only matching rows") {
    for
      t      <- makeUsers
      _      <- t.insertAll(List(alice, bob, charlie))
      result <- t.filter(_.age >= 30)
    yield expect(result == Vector(alice, charlie))
  }

  test("filter returns empty vector when nothing matches") {
    for
      t      <- makeUsers
      _      <- t.insert(alice)
      result <- t.filter(_.age > 100)
    yield expect(result.isEmpty)
  }

  test("find returns the first matching row") {
    for
      t      <- makeUsers
      _      <- t.insertAll(List(alice, bob, charlie))
      result <- t.find(_.age < 30)
    yield expect(result == Some(bob))
  }

  test("find returns None when nothing matches") {
    for
      t      <- makeUsers
      _      <- t.insert(alice)
      result <- t.find(_.name == "nobody")
    yield expect(result.isEmpty)
  }

  test("count returns total number of rows") {
    for
      t <- makeUsers
      _ <- t.insertAll(List(alice, bob, charlie))
      n <- t.count
    yield expect(n == 3)
  }

  test("count with predicate returns matching row count") {
    for
      t <- makeUsers
      _ <- t.insertAll(List(alice, bob, charlie))
      n <- t.count(_.age >= 30)
    yield expect(n == 2)
  }

  test("exists returns true when a matching row is present") {
    for
      t      <- makeUsers
      _      <- t.insertAll(List(alice, bob))
      result <- t.exists(_.name == "bob")
    yield expect(result)
  }

  test("exists returns false when no matching row is present") {
    for
      t      <- makeUsers
      _      <- t.insert(alice)
      result <- t.exists(_.name == "nobody")
    yield expect(!result)
  }

  // ── Table: sortBy / page ───────────────────────────────────────────────────

  test("sortBy orders rows by the given field") {
    for
      t      <- makeUsers
      _      <- t.insertAll(List(charlie, alice, bob))
      result <- t.sortBy(_.age)
    yield expect(result == Vector(bob, alice, charlie))
  }

  test("page returns the correct slice") {
    for
      t      <- makeUsers
      _      <- t.insertAll(List(alice, bob, charlie))
      result <- t.page(offset = 1, limit = 2)
    yield expect(result == Vector(bob, charlie))
  }

  test("page with offset beyond size returns empty vector") {
    for
      t      <- makeUsers
      _      <- t.insertAll(List(alice, bob))
      result <- t.page(offset = 5, limit = 2)
    yield expect(result.isEmpty)
  }

  // ── Table: UPDATE ──────────────────────────────────────────────────────────

  test("update transforms matching rows and returns updated count") {
    for
      t   <- makeUsers
      _   <- t.insertAll(List(alice, bob, charlie))
      n   <- t.update(_.age >= 30)(u => u.copy(age = u.age + 1))
      all <- t.all
    yield expect.all(
      n == 2,
      all.find(_.name == "alice").exists(_.age == 31),
      all.find(_.name == "charlie").exists(_.age == 36),
      all.find(_.name == "bob").exists(_.age == 25),
    )
  }

  test("update returns 0 when nothing matches") {
    for
      t <- makeUsers
      _ <- t.insert(alice)
      n <- t.update(_.age > 100)(u => u.copy(age = 0))
    yield expect(n == 0)
  }

  // ── Table: DELETE ──────────────────────────────────────────────────────────

  test("delete removes matching rows and returns deleted count") {
    for
      t   <- makeUsers
      _   <- t.insertAll(List(alice, bob, charlie))
      n   <- t.delete(_.age < 30)
      all <- t.all
    yield expect.all(
      n == 1,
      !all.exists(_.name == "bob"),
      all.size == 2,
    )
  }

  test("delete returns 0 when nothing matches") {
    for
      t <- makeUsers
      _ <- t.insertAll(List(alice, bob))
      n <- t.delete(_.name == "nobody")
    yield expect(n == 0)
  }

  test("delete removes all rows when predicate is always true") {
    for
      t   <- makeUsers
      _   <- t.insertAll(List(alice, bob, charlie))
      n   <- t.delete(_ => true)
      all <- t.all
    yield expect.all(n == 3, all.isEmpty)
  }

  // ── Table: TRUNCATE ────────────────────────────────────────────────────────

  test("truncate removes all rows") {
    for
      t   <- makeUsers
      _   <- t.insertAll(List(alice, bob, charlie))
      _   <- t.truncate
      all <- t.all
    yield expect(all.isEmpty)
  }

  test("truncate on empty table is a no-op") {
    for
      t   <- makeUsers
      _   <- t.truncate
      all <- t.all
    yield expect(all.isEmpty)
  }

  // ── Table: multiple row types ──────────────────────────────────────────────

  test("table works with a different row type") {
    val laptop = Product("LAPTOP-01", 999.99, inStock = true)
    val mouse = Product("MOUSE-01", 29.99, inStock = false)
    for
      t      <- Table[IO, Product]
      _      <- t.insertAll(List(laptop, mouse))
      result <- t.filter(_.inStock)
    yield expect(result == Vector(laptop))
  }

  // ── InMemoryDB ─────────────────────────────────────────────────────────────

  test("createTable returns a working table") {
    for
      db    <- InMemoryDB[IO]
      users <- db.createTable[User]("users")
      _     <- users.insert(alice)
      all   <- users.all
    yield expect(all == Vector(alice))
  }

  test("createTable registers the table name") {
    for
      db    <- InMemoryDB[IO]
      _     <- db.createTable[User]("users")
      _     <- db.createTable[Product]("products")
      names <- db.tableNames
    yield expect(names == Set("users", "products"))
  }

  test("createTable with existing name replaces the table") {
    for
      db       <- InMemoryDB[IO]
      original <- db.createTable[User]("users")
      _        <- original.insert(alice)
      fresh    <- db.createTable[User]("users")
      all      <- fresh.all
    yield expect(all.isEmpty)
  }

  test("dropTable removes the table from the database") {
    for
      db    <- InMemoryDB[IO]
      _     <- db.createTable[User]("users")
      _     <- db.dropTable("users")
      names <- db.tableNames
    yield expect(names.isEmpty)
  }

  test("dropTable on non-existent table is a no-op") {
    for
      db    <- InMemoryDB[IO]
      _     <- db.dropTable("ghost")
      names <- db.tableNames
    yield expect(names.isEmpty)
  }

  test("two tables in the same database are independent") {
    for
      db       <- InMemoryDB[IO]
      users    <- db.createTable[User]("users")
      products <- db.createTable[Product]("products")
      _        <- users.insert(alice)
      _        <- products.insert(Product("SKU-1", 10.0, inStock = true))
      uCount   <- users.count
      pCount   <- products.count
    yield expect.all(uCount == 1, pCount == 1)
  }
