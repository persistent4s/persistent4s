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

import java.util.UUID

import scala.compiletime.testing.{Error, typeCheckErrors}

import cats.effect.{IO, Resource}

import skunk.codec.all.{int4, text, uuid}
import skunk.implicits.*
import weaver.SimpleIOSuite

object PostgresDerivationSuite extends SimpleIOSuite:

  final private case class BookState(
    bookId: UUID,
    displayTitle: String,
    HTTPStatus: Int,
    description: Option[String],
  ) derives PostgresRow

  final private case class BorrowingState(
    bookId: UUID,
    memberId: UUID,
    returned: Boolean,
  ) derives PostgresRow

  final private case class KeyOnlyState(id: UUID) derives PostgresRow

  final private case class ShelfCode(value: String)

  private given PostgresField[ShelfCode] =
    PostgresField(text.imap(ShelfCode(_))(_.value))

  final private case class CustomFieldState(id: UUID, shelfCode: ShelfCode)

  final private case class ExplicitRowState(id: UUID, label: String)

  private given PostgresRow[ExplicitRowState] =
    PostgresRow("id" -> "legacy_id", "label" -> "legacy_label")(
      (uuid *: text).to[ExplicitRowState],
    )

  private val bookId = UUID.fromString("10000000-0000-0000-0000-000000000001")

  private val memberId = UUID.fromString("20000000-0000-0000-0000-000000000002")

  test("PostgresRow derives ordered snake_case metadata and a round-tripping codec") {
    val row = PostgresRow[BookState]
    val state = BookState(bookId, "The Left Hand of Darkness", 200, None)
    val encoded = row.codec.encode(state).map(_.map(_.value))

    IO.pure(
      expect.all(
        row.fields == List(
          PostgresRow.Field("bookId", "book_id"),
          PostgresRow.Field("displayTitle", "display_title"),
          PostgresRow.Field("HTTPStatus", "http_status"),
          PostgresRow.Field("description", "description"),
        ),
        row.codec.types.map(_.name) == List("uuid", "text", "int4", "text"),
        row.codec.decode(0, encoded) == Right(state),
      ),
    )
  }

  test("PostgresTable derives a plain state and honors a custom scalar field codec") {
    val table = PostgresTable.derived[CustomFieldState]("custom_fields").key(_.id)
    val state = CustomFieldState(bookId, ShelfCode("A-42"))
    val encoded = table.row.codec.encode(state).map(_.map(_.value))

    IO.pure(
      expect.all(
        table.columns == List("id", "shelf_code"),
        table.row.codec.types.map(_.name) == List("uuid", "text"),
        table.row.codec.decode(0, encoded) == Right(state),
      ),
    )
  }

  test("an explicit PostgresRow takes precedence over automatic derivation") {
    val table = PostgresTable.derived[ExplicitRowState]("explicit_rows").key(_.id)

    IO.pure(
      expect.all(
        table.columns == List("legacy_id", "legacy_label"),
        table.keyColumns == List("legacy_id"),
      ),
    )
  }

  test("a scalar key derives key metadata and batched CRUD SQL") {
    val table = PostgresTable.derived[BookState]("library.books").key(_.bookId)

    IO.pure(
      expect.all(
        table.name == "library.books",
        table.columns == List("book_id", "display_title", "http_status", "description"),
        table.keyColumns == List("book_id"),
        table.keyOf(BookState(bookId, "Dune", 200, None)) == bookId,
        table.findManyQuery(2).sql ==
          "SELECT \"book_id\", \"display_title\", \"http_status\", \"description\" FROM \"library\".\"books\" " +
          "WHERE \"book_id\" IN ($1, $2)",
        table.deleteManyCommand(2).sql ==
          "DELETE FROM \"library\".\"books\" WHERE \"book_id\" IN ($1, $2)",
        table.upsertManyCommand(2).sql ==
          "INSERT INTO \"library\".\"books\" (\"book_id\", \"display_title\", \"http_status\", \"description\") " +
          "VALUES ($1, $2, $3, $4), ($5, $6, $7, $8) ON CONFLICT (\"book_id\") DO UPDATE SET " +
          "\"display_title\" = EXCLUDED.\"display_title\", \"http_status\" = EXCLUDED.\"http_status\", " +
          "\"description\" = EXCLUDED.\"description\"",
      ),
    )
  }

  test("a composite key derives tuple predicates and conflict metadata") {
    val table =
      PostgresTable
        .derived[BorrowingState]("borrowings")
        .key(state => (state.bookId, state.memberId))
    val state = BorrowingState(bookId, memberId, returned = false)

    IO.pure(
      expect.all(
        table.keyColumns == List("book_id", "member_id"),
        table.keyOf(state) == (bookId, memberId),
        table.findManyQuery(2).sql ==
          "SELECT \"book_id\", \"member_id\", \"returned\" FROM \"borrowings\" " +
          "WHERE (\"book_id\", \"member_id\") IN (($1, $2), ($3, $4))",
        table.deleteManyCommand(2).sql ==
          "DELETE FROM \"borrowings\" WHERE (\"book_id\", \"member_id\") IN (($1, $2), ($3, $4))",
        table.upsertManyCommand(1).sql ==
          "INSERT INTO \"borrowings\" (\"book_id\", \"member_id\", \"returned\") VALUES ($1, $2, $3) " +
          "ON CONFLICT (\"book_id\", \"member_id\") DO UPDATE SET \"returned\" = EXCLUDED.\"returned\"",
      ),
    )
  }

  test("column overrides apply consistently to row, key, and generated SQL") {
    val table =
      PostgresTable
        .derived[BookState]("books")
        .key(_.bookId)
        .column(_.bookId, "id")
        .column(_.displayTitle, "title")

    IO.pure(
      expect.all(
        table.columns == List("id", "title", "http_status", "description"),
        table.keyColumns == List("id"),
        table.findManyQuery(1).sql ==
          "SELECT \"id\", \"title\", \"http_status\", \"description\" FROM \"books\" WHERE \"id\" IN ($1)",
        table.upsertManyCommand(1).sql ==
          "INSERT INTO \"books\" (\"id\", \"title\", \"http_status\", \"description\") " +
          "VALUES ($1, $2, $3, $4) ON CONFLICT (\"id\") DO UPDATE SET " +
          "\"title\" = EXCLUDED.\"title\", \"http_status\" = EXCLUDED.\"http_status\", " +
          "\"description\" = EXCLUDED.\"description\"",
      ),
    )
  }

  test("derived filters use mapped columns for equality, IN, and null predicates") {
    val table =
      PostgresTable
        .derived[BookState]("library.books")
        .key(_.bookId)
        .column(_.bookId, "id")
        .column(_.displayTitle, "title")
    val combined = table.filterQuery(
      List(
        table.filterValuesPredicate("displayTitle", text, List("Dune", "Foundation"), includeNull = false),
        table.filterValuePredicate("HTTPStatus", int4, 200),
        table.filterNullPredicate("description"),
      ),
    )
    val nullableIn = table.filterQuery(
      List(
        table.filterValuesPredicate(
          "description",
          text.opt,
          List(Some("classic"), Some("science-fiction")),
          includeNull = true,
        ),
      ),
    )

    IO.pure(
      expect.all(
        table.filterValueQuery("displayTitle", text).sql ==
          "SELECT \"id\", \"title\", \"http_status\", \"description\" FROM \"library\".\"books\" " +
          "WHERE \"title\" = $1",
        table.filterValuesQuery("displayTitle", text, 2, includeNull = false).sql ==
          "SELECT \"id\", \"title\", \"http_status\", \"description\" FROM \"library\".\"books\" " +
          "WHERE \"title\" IN ($1, $2)",
        table.filterValuesQuery("description", text.opt, 2, includeNull = true).sql ==
          "SELECT \"id\", \"title\", \"http_status\", \"description\" FROM \"library\".\"books\" " +
          "WHERE (\"description\" IN ($1, $2) OR \"description\" IS NULL)",
        table.filterNullQuery("description").sql ==
          "SELECT \"id\", \"title\", \"http_status\", \"description\" FROM \"library\".\"books\" " +
          "WHERE \"description\" IS NULL",
        table.filterNotNullQuery("description").sql ==
          "SELECT \"id\", \"title\", \"http_status\", \"description\" FROM \"library\".\"books\" " +
          "WHERE \"description\" IS NOT NULL",
        combined.fragment.sql ==
          "SELECT \"id\", \"title\", \"http_status\", \"description\" FROM \"library\".\"books\" " +
          "WHERE \"title\" IN ($1, $2) AND \"http_status\" = $3 AND \"description\" IS NULL",
        nullableIn.fragment.sql ==
          "SELECT \"id\", \"title\", \"http_status\", \"description\" FROM \"library\".\"books\" " +
          "WHERE (\"description\" IN ($1, $2) OR \"description\" IS NULL)",
      ),
    )
  }

  test("an all-key row derives an idempotent DO NOTHING upsert") {
    val table = PostgresTable.derived[KeyOnlyState]("processed_keys").key(_.id)

    IO.pure(
      expect(
        table.upsertManyCommand(2).sql ==
          "INSERT INTO \"processed_keys\" (\"id\") VALUES ($1), ($2) ON CONFLICT (\"id\") DO NOTHING",
      ),
    )
  }

  test("ordered queries use mapped columns and bind cursor, limit, and long offset values") {
    val table = PostgresTable
      .derived[BookState]("library.books")
      .key(_.bookId)
      .column(_.bookId, "id")
      .column(_.displayTitle, "title")
    val repository = DerivedPostgresRepository[IO, UUID, BookState](Resource.eval(IO.never), table)
    val query = repository
      .filterBy(_.displayTitle)
      .is("Dune")
      .and(_.HTTPStatus)
      .greaterThan(Some(200))
      .orderByDescending(_.displayTitle)
      .orderBy(_.bookId)
      .limit(20)
      .offset(3000000000L)
    val applied = table.filterQuery(query.plan)

    IO.pure(
      expect.all(
        applied.fragment.sql ==
          "SELECT \"id\", \"title\", \"http_status\", \"description\" FROM \"library\".\"books\" " +
          "WHERE \"title\" = $1 AND \"http_status\" > $2 ORDER BY \"title\" DESC, \"id\" ASC LIMIT $3 OFFSET $4",
        applied.fragment.encoder.encode(applied.argument).map(_.map(_.value)) ==
          List(Some("Dune"), Some("200"), Some("20"), Some("3000000000")),
        applied.fragment.encoder.types.map(_.name) == List("text", "int4", "int4", "int8"),
      ),
    )
  }

  test("unfiltered and omitted-filter queries retain SQL ordering and pagination") {
    val table = PostgresTable.derived[BookState]("books").key(_.bookId)
    val repository = DerivedPostgresRepository[IO, UUID, BookState](Resource.eval(IO.never), table)
    val base = repository.query.orderBy(_.bookId)
    val page = table.filterQuery(base.limit(2).offset(4).plan)
    val omitted = table.filterQuery(
      repository.filterBy(_.HTTPStatus).greaterThan(Option.empty[Int]).orderBy(_.bookId).limit(2).plan,
    )

    IO.pure(
      expect.all(
        page.fragment.sql ==
          "SELECT \"book_id\", \"display_title\", \"http_status\", \"description\" FROM \"books\" " +
          "ORDER BY \"book_id\" ASC LIMIT $1 OFFSET $2",
        page.fragment.encoder.encode(page.argument).map(_.map(_.value)) == List(Some("2"), Some("4")),
        omitted.fragment.sql ==
          "SELECT \"book_id\", \"display_title\", \"http_status\", \"description\" FROM \"books\" " +
          "ORDER BY \"book_id\" ASC LIMIT $1",
        base.plan.limit.isEmpty,
        base.plan.offset.isEmpty,
        base.limit(1).limit(2).plan.limit.contains(2),
        base.offset(1).offset(2).plan.offset.contains(2L),
      ),
    )
  }

  test("paged IN queries combine every chunk before applying a single SQL limit") {
    val table = PostgresTable.derived[BookState]("books").key(_.bookId).withBatchSize(2)
    val repository = DerivedPostgresRepository[IO, UUID, BookState](Resource.eval(IO.never), table)
    val query = repository
      .filterBy(_.displayTitle)
      .in(List("Dune", "Foundation", "Solaris"))
      .and(_.HTTPStatus)
      .in(List(200, 201, 202))
      .orderBy(_.bookId)
      .limit(2)
      .offset(1)
    val applied = table.filterQuery(query.plan)

    IO.pure(
      expect.all(
        applied.fragment.sql ==
          "SELECT \"book_id\", \"display_title\", \"http_status\", \"description\" FROM \"books\" " +
          "WHERE (\"display_title\" IN ($1, $2) OR \"display_title\" IN ($3)) " +
          "AND (\"http_status\" IN ($4, $5) OR \"http_status\" IN ($6)) " +
          "ORDER BY \"book_id\" ASC LIMIT $7 OFFSET $8",
        applied.fragment.encoder.encode(applied.argument).map(_.map(_.value)) ==
          List("Dune", "Foundation", "Solaris", "200", "201", "202", "2", "1").map(Some(_)),
      ),
    )
  }

  test("paged SQL queries reject plans exceeding PostgreSQL's parameter limit") {
    val table = PostgresTable.derived[BookState]("books").key(_.bookId)
    val repository = DerivedPostgresRepository[IO, UUID, BookState](Resource.eval(IO.never), table)
    val query = repository.filterBy(_.HTTPStatus).in(0 until Short.MaxValue.toInt).limit(1)

    IO(table.filterQuery(query.plan)).attempt.map { result =>
      expect(
        result.left.exists(error =>
          error.isInstanceOf[IllegalArgumentException] && error.getMessage.contains("exceeding the protocol limit"),
        ),
      )
    }
  }

  test("query bounds reject negatives and a zero limit returns without acquiring a session") {
    val table = PostgresTable.derived[BookState]("books").key(_.bookId)
    val pool = Resource.eval(IO.raiseError[skunk.Session[IO]](new AssertionError("A zero limit accessed PostgreSQL")))
    val repository = DerivedPostgresRepository[IO, UUID, BookState](pool, table)

    for
      negativeLimit  <- IO(repository.query.limit(-1)).attempt
      negativeOffset <- IO(repository.query.offset(-1)).attempt
      empty          <- repository.query.limit(0).run
    yield expect.all(
      negativeLimit.left.exists(_.isInstanceOf[IllegalArgumentException]),
      negativeOffset.left.exists(_.isInstanceOf[IllegalArgumentException]),
      empty.isEmpty,
    )
  }

  test("derivation rejects an unsupported state field") {
    val errors: List[Error] = typeCheckErrors("""
      import persistent4s.postgres.*
      final case class UnsupportedState(id: java.util.UUID, labels: List[String]) derives PostgresRow
    """)

    IO.pure(
      expect.all(
        errors.nonEmpty,
        errors.exists(_.message.contains("field [labels] has unsupported type")),
      ),
    )
  }

  test("key derivation rejects computed and optional selectors") {
    val computedErrors = typeCheckErrors("""
      import persistent4s.postgres.*
      final case class ComputedKeyState(id: java.util.UUID) derives PostgresRow
      PostgresTable.derived[ComputedKeyState]("states").key(state => state.id.toString)
    """)
    val optionalErrors = typeCheckErrors("""
      import persistent4s.postgres.*
      final case class OptionalKeyState(id: Option[java.util.UUID]) derives PostgresRow
      PostgresTable.derived[OptionalKeyState]("states").key(_.id)
    """)

    IO.pure(
      expect.all(
        computedErrors.exists(_.message.contains("must select a state field directly")),
        optionalErrors.exists(_.message.contains("key field cannot be optional")),
      ),
    )
  }

  test("column overrides reject computed and multi-field selectors") {
    val computedErrors = typeCheckErrors("""
      import persistent4s.postgres.*
      final case class ColumnState(id: java.util.UUID, title: String) derives PostgresRow
      PostgresTable.derived[ColumnState]("states").key(_.id).column(_.title.toUpperCase, "title")
    """)
    val multiFieldErrors = typeCheckErrors("""
      import persistent4s.postgres.*
      final case class ColumnState(id: java.util.UUID, title: String) derives PostgresRow
      PostgresTable.derived[ColumnState]("states").key(_.id).column(state => (state.id, state.title), "invalid")
    """)

    IO.pure(
      expect.all(
        computedErrors.exists(_.message.contains("must select a state field directly")),
        multiFieldErrors.exists(_.message.contains("must select one state field")),
      ),
    )
  }

  test("derived repository filters reject non-direct field selectors") {
    val computedErrors = typeCheckErrors("""
      import cats.effect.IO
      import persistent4s.postgres.*
      final case class FilterState(id: java.util.UUID, title: String) derives PostgresRow
      def invalid(repository: DerivedPostgresRepository[IO, java.util.UUID, FilterState]) =
        repository.filterBy(state => state.title.toUpperCase).is("DUNE")
    """)
    val storedSelectorErrors = typeCheckErrors("""
      import cats.effect.IO
      import persistent4s.postgres.*
      final case class FilterState(id: java.util.UUID, title: String) derives PostgresRow
      val titleSelector: FilterState => String = _.title
      def invalid(repository: DerivedPostgresRepository[IO, java.util.UUID, FilterState]) =
        repository.filterBy(titleSelector).is("Dune")
    """)

    IO.pure(
      expect.all(
        computedErrors.exists(_.message.contains("must select exactly one state field directly")),
        storedSelectorErrors.exists(_.message.contains("must be an inline lambda")),
      ),
    )
  }

  test("derived repository filter chains preserve field types and require direct selectors") {
    val validErrors = typeCheckErrors("""
      import cats.effect.IO
      import persistent4s.postgres.*
      final case class FilterState(
        id: java.util.UUID,
        title: String,
        revision: Int,
        active: Boolean,
        note: Option[String],
      ) derives PostgresRow
      def valid(repository: DerivedPostgresRepository[IO, java.util.UUID, FilterState]): IO[List[FilterState]] =
        repository
          .filterBy(_.title)
          .in(Some(List("Dune", "Foundation")))
          .and(_.revision)
          .is(Option.empty[Int])
          .and(_.active)
          .is(Some(false))
          .and(_.note)
          .is(None)
          .run
    """)
    val computedSelectorErrors = typeCheckErrors("""
      import cats.effect.IO
      import persistent4s.postgres.*
      final case class FilterState(id: java.util.UUID, title: String) derives PostgresRow
      def invalid(repository: DerivedPostgresRepository[IO, java.util.UUID, FilterState]) =
        repository.filterBy(_.id).is(java.util.UUID.randomUUID()).and(state => state.title.toUpperCase).is("DUNE").run
    """)
    val storedSelectorErrors = typeCheckErrors("""
      import cats.effect.IO
      import persistent4s.postgres.*
      final case class FilterState(id: java.util.UUID, title: String) derives PostgresRow
      val titleSelector: FilterState => String = _.title
      def invalid(repository: DerivedPostgresRepository[IO, java.util.UUID, FilterState]) =
        repository.filterBy(_.id).is(java.util.UUID.randomUUID()).and(titleSelector).is("Dune").run
    """)
    val wrongValueErrors = typeCheckErrors("""
      import cats.effect.IO
      import persistent4s.postgres.*
      final case class FilterState(id: java.util.UUID, revision: Int) derives PostgresRow
      def invalid(repository: DerivedPostgresRepository[IO, java.util.UUID, FilterState]) =
        repository.filterBy(_.revision).is("one").run
    """)
    val scalarNullErrors = typeCheckErrors("""
      import cats.effect.IO
      import persistent4s.postgres.*
      final case class FilterState(id: java.util.UUID, title: String) derives PostgresRow
      def invalid(repository: DerivedPostgresRepository[IO, java.util.UUID, FilterState]) =
        repository.filterBy(_.title).isNull.run
    """)

    IO.pure(
      expect.all(
        validErrors.isEmpty,
        computedSelectorErrors.exists(_.message.contains("must select exactly one state field directly")),
        storedSelectorErrors.exists(_.message.contains("must be an inline lambda")), wrongValueErrors.nonEmpty,
        scalarNullErrors.nonEmpty,
      ),
    )
  }

  test("query ordering requires direct fields and cursor filters preserve field types") {
    val validErrors = typeCheckErrors("""
      import cats.effect.IO
      import persistent4s.postgres.*
      final case class OrderedState(id: java.util.UUID, title: String, revision: Int) derives PostgresRow
      def valid(repository: DerivedPostgresRepository[IO, java.util.UUID, OrderedState]): IO[List[OrderedState]] =
        repository.query.orderBy(_.title).orderByDescending(_.id).limit(20).offset(50L)
          .and(_.revision).greaterThan(Some(3)).run
    """)
    val computedErrors = typeCheckErrors("""
      import cats.effect.IO
      import persistent4s.postgres.*
      final case class OrderedState(id: java.util.UUID, title: String) derives PostgresRow
      def invalid(repository: DerivedPostgresRepository[IO, java.util.UUID, OrderedState]) =
        repository.query.orderBy(_.title.toUpperCase).run
    """)
    val storedErrors = typeCheckErrors("""
      import cats.effect.IO
      import persistent4s.postgres.*
      final case class OrderedState(id: java.util.UUID, title: String) derives PostgresRow
      val title: OrderedState => String = _.title
      def invalid(repository: DerivedPostgresRepository[IO, java.util.UUID, OrderedState]) =
        repository.query.orderByDescending(title).run
    """)
    val wrongValueErrors = typeCheckErrors("""
      import cats.effect.IO
      import persistent4s.postgres.*
      final case class OrderedState(id: java.util.UUID, revision: Int) derives PostgresRow
      def invalid(repository: DerivedPostgresRepository[IO, java.util.UUID, OrderedState]) =
        repository.filterBy(_.revision).greaterThan("one").run
    """)

    IO.pure(
      expect.all(
        validErrors.isEmpty,
        computedErrors.exists(_.message.contains("must select exactly one state field directly")),
        storedErrors.exists(_.message.contains("must be an inline lambda")),
        wrongValueErrors.nonEmpty,
      ),
    )
  }
