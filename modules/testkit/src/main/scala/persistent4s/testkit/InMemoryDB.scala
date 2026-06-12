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

import cats.effect.Ref
import cats.effect.Sync
import cats.syntax.all.*

/** A thread-safe, in-memory table holding rows of type [[R]], analogous to a PostgreSQL table. Intended for tests and
  * experiments — no data is persisted.
  *
  * The "schema" is expressed through the Scala type [[R]] (typically a case class whose fields are the columns). Create
  * an instance via [[Table.apply]].
  *
  * Supported operations (inspired by SQL):
  *   - [[insert]] / [[insertAll]] — INSERT
  *   - [[all]] — SELECT *
  *   - [[filter]] — SELECT … WHERE
  *   - [[find]] — SELECT … WHERE … LIMIT 1
  *   - [[count]] / [[count(pred)]] — SELECT COUNT(*)
  *   - [[exists]] — SELECT EXISTS
  *   - [[sortBy]] — SELECT … ORDER BY
  *   - [[page]] — SELECT … LIMIT … OFFSET
  *   - [[update]] — UPDATE … SET … WHERE
  *   - [[delete]] — DELETE … WHERE
  *   - [[truncate]] — TRUNCATE
  *
  * @tparam F
  *   the effect type (needs a `Sync` instance, e.g. `cats.effect.IO`)
  * @tparam R
  *   the row type; typically a case class whose fields represent columns
  */
final class Table[F[_]: Sync, R] private (state: Ref[F, Vector[R]]):

  // ── INSERT ────────────────────────────────────────────────────────────────

  /** INSERT one row. */
  def insert(row: R): F[Unit] =
    state.update(_ :+ row)

  /** INSERT multiple rows. */
  def insertAll(rows: Iterable[R]): F[Unit] =
    state.update(_ ++ rows)

  // ── SELECT ────────────────────────────────────────────────────────────────

  /** SELECT * — all rows in insertion order. */
  def all: F[Vector[R]] =
    state.get

  /** SELECT … WHERE — rows matching `predicate`. */
  def filter(predicate: R => Boolean): F[Vector[R]] =
    state.get.map(_.filter(predicate))

  /** SELECT … WHERE … LIMIT 1 — first matching row, or `None`. */
  def find(predicate: R => Boolean): F[Option[R]] =
    state.get.map(_.find(predicate))

  /** SELECT COUNT(*) — total number of rows. */
  def count: F[Int] =
    state.get.map(_.size)

  /** SELECT COUNT(*) WHERE — number of rows matching `predicate`. */
  def count(predicate: R => Boolean): F[Int] =
    state.get.map(_.count(predicate))

  /** SELECT EXISTS — `true` if at least one row matches `predicate`. */
  def exists(predicate: R => Boolean): F[Boolean] =
    state.get.map(_.exists(predicate))

  /** SELECT … ORDER BY — all rows sorted by `field`. */
  def sortBy[B: Ordering](field: R => B): F[Vector[R]] =
    state.get.map(_.sortBy(field))

  /** SELECT … LIMIT … OFFSET — a page of rows (0-based offset). */
  def page(offset: Int, limit: Int): F[Vector[R]] =
    state.get.map(_.slice(offset, offset + limit))

  // ── UPDATE ────────────────────────────────────────────────────────────────

  /** UPDATE … SET … WHERE — apply `transform` to every row matching `predicate`.
    *
    * @return
    *   the number of rows that were updated
    */
  def update(predicate: R => Boolean)(transform: R => R): F[Int] =
    state.modify { rows =>
      var n = 0
      val updated = rows.map { r =>
        if predicate(r) then { n += 1; transform(r) }
        else r
      }
      (updated, n)
    }

  // ── DELETE ────────────────────────────────────────────────────────────────

  /** DELETE … WHERE — remove every row matching `predicate`.
    *
    * @return
    *   the number of rows that were removed
    */
  def delete(predicate: R => Boolean): F[Int] =
    state.modify { rows =>
      val (removed, kept) = rows.partition(predicate)
      (kept, removed.size)
    }

  // ── TRUNCATE ─────────────────────────────────────────────────────────────

  /** TRUNCATE — remove all rows. */
  def truncate: F[Unit] =
    state.set(Vector.empty)

object Table:

  /** Creates a new, empty [[Table]] inside the effect `F`. */
  def apply[F[_]: Sync, R]: F[Table[F, R]] =
    Ref.of[F, Vector[R]](Vector.empty).map(new Table(_))

// ── InMemoryDB ───────────────────────────────────────────────────────────────

/** An in-memory database that manages a set of named [[Table]]s, analogous to a PostgreSQL database. Intended for tests
  * and experiments — no data is persisted.
  *
  * Tables are created with [[createTable]] and dropped with [[dropTable]]. Each table is typed by its row type `R`, so
  * all operations on it remain fully type-safe. The returned [[Table]] reference is the primary handle for INSERT /
  * SELECT / UPDATE / DELETE operations.
  *
  * Example usage:
  *
  * {{{
  * case class User(name: String, age: Int)
  *
  * for
  *   db    <- InMemoryDB[IO]
  *   users <- db.createTable[User]("users")
  *   _     <- users.insert(User("alice", 30))
  *   all   <- users.all
  * yield all
  * }}}
  *
  * @tparam F
  *   the effect type (needs a `Sync` instance, e.g. `cats.effect.IO`)
  */
final class InMemoryDB[F[_]: Sync] private (tables: Ref[F, Map[String, Table[F, Any]]]):

  /** CREATE TABLE — create a new empty table named `name` with rows of type `R`.
    *
    * If a table with that name already exists it is replaced.
    */
  def createTable[R](name: String): F[Table[F, R]] =
    for
      table <- Table[F, R]
      _     <- tables.update(_.updated(name, table.asInstanceOf[Table[F, Any]]))
    yield table

  /** DROP TABLE — remove the table named `name` and discard all its rows. No-op if the table does not exist. */
  def dropTable(name: String): F[Unit] =
    tables.update(_.removed(name))

  /** Returns the names of all currently existing tables. */
  def tableNames: F[Set[String]] =
    tables.get.map(_.keySet)

object InMemoryDB:

  /** Creates a new, empty [[InMemoryDB]] inside the effect `F`. */
  def apply[F[_]: Sync]: F[InMemoryDB[F]] =
    Ref.of[F, Map[String, Table[F, Any]]](Map.empty).map(new InMemoryDB(_))
