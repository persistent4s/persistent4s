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

import scala.compiletime.testing.typeCheckErrors

import cats.effect.{IO, Resource}

import skunk.codec.all.{int4, text, varchar}
import weaver.SimpleIOSuite

object PostgresFlattenedRowSuite extends SimpleIOSuite:

  final case class Address(city: String, postalCode: Option[String])

  final case class Person(id: Long, home: Address, work: Option[Address], active: Boolean)

  final case class OptionalAddress(city: Option[String], postalCode: Option[String])

  final case class Contact(id: Long, address: Option[OptionalAddress], language: String)

  final case class Organization(id: Long, contact: Option[Contact])

  final case class Scalar(value: String)

  final case class WithScalar(id: Long, scalar: Scalar, optional: Option[Scalar])

  final case class NestedBeforeKey(address: Address, id: Long)

  final case class Wide(
    a01: Int,
    a02: Int,
    a03: Int,
    a04: Int,
    a05: Int,
    a06: Int,
    a07: Int,
    a08: Int,
    a09: Int,
    a10: Int,
    a11: Int,
    a12: Int,
    a13: Int,
    a14: Int,
    a15: Int,
    a16: Int,
    a17: Int,
    a18: Int,
    a19: Int,
    a20: Int,
    a21: Int,
    a22: Int,
    a23: Int,
    a24: Int,
    a25: Int,
    address: OptionalAddress,
  )

  private given PostgresField[Scalar] = PostgresField(text.imap(Scalar(_))(_.value))

  private def lowerCase(path: String): String = path.replace(".", "").toLowerCase(java.util.Locale.ROOT)

  test("flattens required and optional products with ordered field paths") {
    val row = PostgresRow.flattened[Person](lowerCase)
    val value = Person(7L, Address("Sion", Some("1950")), Some(Address("Lausanne", None)), true)
    val encoded = row.codec.encode(value).map(_.map(_.value))
    IO.pure(
      expect.all(
        row.fields == List(
          PostgresRow.Field("id", "id"),
          PostgresRow.Field("home.city", "homecity"),
          PostgresRow.Field("home.postalCode", "homepostalcode"),
          PostgresRow.Field("work.city", "workcity"),
          PostgresRow.Field("work.postalCode", "workpostalcode"),
          PostgresRow.Field("active", "active"),
        ),
        row.codec.types.map(_.name) == List("int8", "text", "text", "text", "text", "bool"),
        row.codec.decode(0, encoded) == Right(value),
        row.codec.sql.runA(4).value == "$4, $5, $6, $7, $8, $9",
      ),
    )
  }

  test("absent optional products encode as null columns and decode as absent") {
    val row = PostgresRow.flattened[Person](lowerCase)
    val value = Person(7L, Address("Sion", None), None, false)
    val encoded = row.codec.encode(value).map(_.map(_.value))
    IO.pure(
      expect.all(
        encoded.slice(3, 5) == List(None, None),
        row.codec.decode(0, encoded) == Right(value),
      ),
    )
  }

  test("an optional product with only null fields normalizes to None") {
    val row = PostgresRow.flattened[Contact](lowerCase)
    val value = Contact(7L, Some(OptionalAddress(None, None)), "FR")
    val encoded = row.codec.encode(value).map(_.map(_.value))
    IO.pure(expect(row.codec.decode(0, encoded) == Right(value.copy(address = None))))
  }

  test("partially present optional products retain nullable fields") {
    val row = PostgresRow.flattened[Contact](lowerCase)
    val value = Contact(7L, Some(OptionalAddress(None, Some("1950"))), "FR")
    IO.pure(expect(row.codec.decode(0, row.codec.encode(value).map(_.map(_.value))) == Right(value)))
  }

  test("nested optional products decode independently") {
    val row = PostgresRow.flattened[Organization](lowerCase)
    val values = List(
      Organization(1L, None),
      Organization(2L, Some(Contact(7L, None, "FR"))),
      Organization(3L, Some(Contact(8L, Some(OptionalAddress(Some("Sion"), None)), "DE"))),
    )
    IO.pure(
      expect(values.forall(value => row.codec.decode(0, row.codec.encode(value).map(_.map(_.value))) == Right(value))),
    )
  }

  test("an explicit scalar PostgresField takes precedence over product flattening") {
    val row = PostgresRow.flattened[WithScalar](lowerCase)
    val value = WithScalar(7L, Scalar("required"), Some(Scalar("optional")))
    IO.pure(
      expect.all(
        row.fields.map(_.scalaName) == List("id", "scalar", "optional"),
        row.codec.decode(0, row.codec.encode(value).map(_.map(_.value))) == Right(value),
      ),
    )
  }

  test("typed codec overrides apply to direct and nested optional scalar fields") {
    val row = PostgresRow.flattened[Contact](
      lowerCase,
      PostgresRow.columnCodec[Contact](_.language)(varchar),
      PostgresRow.columnCodec[Contact](_.address.flatMap(_.postalCode))(varchar.opt),
    )
    val value = Contact(7L, Some(OptionalAddress(Some("Sion"), Some("1950"))), "FR")
    IO.pure(
      expect.all(
        row.codec.types.map(_.name) == List("int8", "text", "varchar", "varchar"),
        row.codec.decode(0, row.codec.encode(value).map(_.map(_.value))) == Right(value),
      ),
    )
  }

  test("overridden codecs participate in both encoding and decoding") {
    val row = PostgresRow.flattened[Contact](
      lowerCase,
      PostgresRow.columnCodec[Contact](_.language)(int4.imap(_.toString)(_.toInt)),
    )
    val value = Contact(7L, None, "42")
    IO.pure(
      expect.all(
        row.codec.types.last.name == "int4",
        row.codec.decode(0, row.codec.encode(value).map(_.map(_.value))) == Right(value),
      ),
    )
  }

  test("a missing required value in a partially populated product fails decoding") {
    val row = PostgresRow.flattened[Person](lowerCase)
    val values = List(Some("7"), Some("Sion"), None, None, Some("1000"), Some("t"))
    IO.pure(expect(row.codec.decode(5, values).left.toOption.exists(_.offset == 8)))
  }

  test("products with more than 22 fields round-trip with nested columns") {
    val row = PostgresRow.flattened[Wide](lowerCase)
    val value = Wide(
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9,
      10,
      11,
      12,
      13,
      14,
      15,
      16,
      17,
      18,
      19,
      20,
      21,
      22,
      23,
      24,
      25,
      OptionalAddress(Some("Sion"), None),
    )
    IO.pure(
      expect.all(
        row.fields.size == 27,
        row.codec.decode(0, row.codec.encode(value).map(_.map(_.value))) == Right(value),
      ),
    )
  }

  test("table keys after a flattened product use the correct column and codec") {
    given PostgresRow[NestedBeforeKey] = PostgresRow.flattened[NestedBeforeKey](lowerCase)
    val table = PostgresTable.derived[NestedBeforeKey]("contacts").key(_.id)
    val value = NestedBeforeKey(Address("Sion", None), 7L)
    IO.pure(
      expect.all(
        table.keyColumns == List("id"),
        table.keyOf(value) == 7L,
        table.findManyQuery(1).encoder.types.map(_.name) == List("int8"),
      ),
    )
  }

  test("derived filters accept nested fields through Option.map and flatMap") {
    given PostgresRow[Person] = PostgresRow.flattened[Person](lowerCase)
    val table = PostgresTable.derived[Person]("people").key(_.id)
    val repository = DerivedPostgresRepository[IO, Long, Person](Resource.eval(IO.never), table)
    val query = repository
      .filterBy(_.home.city)
      .is("Sion")
      .and(_.work.map(_.city))
      .is(Some("Lausanne"))
      .and(_.work.flatMap(_.postalCode))
      .isNull
      .orderBy(_.home.city)
      .orderBy(_.id)
      .withLimit(5000, maxLimit = 200)
      .withOffset(2)
    val statement = table.filterQuery(query.plan)
    IO.pure(
      expect.all(
        statement.fragment.sql.endsWith(
          "WHERE \"homecity\" = $1 AND \"workcity\" = $2 AND \"workpostalcode\" IS NULL " +
            "ORDER BY \"homecity\" ASC, \"id\" ASC LIMIT $3 OFFSET $4",
        ),
        statement.fragment.encoder.encode(statement.argument).map(_.map(_.value)) ==
          List(Some("Sion"), Some("Lausanne"), Some("200"), Some("2")),
      ),
    )
  }

  test("nested selections and grouped counts use flattened columns") {
    given PostgresRow[Person] = PostgresRow.flattened[Person](lowerCase)
    val table = PostgresTable.derived[Person]("people").key(_.id)
    val repository = DerivedPostgresRepository[IO, Long, Person](Resource.eval(IO.never), table)
    val query = repository.filterBy(_.active).is(true).withLimit(10).withOffset(2)
    val selection = query.select(_.work.flatMap(_.postalCode)).statement
    val grouped = table.countByQuery("home.city", text, query.plan)
    val validGrouping = typeCheckErrors("""
      import persistent4s.postgres.*
      import cats.effect.{IO, Resource}
      final case class Address(city: String)
      final case class Person(id: Long, address: Address)
      given PostgresRow[Person] = PostgresRow.flattened[Person](_.replace(".", "_"))
      val table = PostgresTable.derived[Person]("people").key(_.id)
      val repository = DerivedPostgresRepository[IO, Long, Person](Resource.eval(IO.never), table)
      val count: IO[Map[String, Long]] = repository.query.countBy(_.address.city)
    """)
    IO.pure(
      expect.all(
        selection.fragment.sql == "SELECT \"workpostalcode\" FROM \"people\" WHERE \"active\" = $1 LIMIT $2 OFFSET $3",
        grouped.fragment.sql == "SELECT \"homecity\", count(*) FROM \"people\" WHERE \"active\" = $1 GROUP BY \"homecity\"",
        validGrouping.isEmpty,
      ),
    )
  }

  test("composite keys can contain required fields of a nested product") {
    given PostgresRow[Person] = PostgresRow.flattened[Person](lowerCase)
    val table = PostgresTable.derived[Person]("people").key(person => (person.id, person.home.city))
    val value = Person(7L, Address("Sion", None), None, false)
    IO.pure(
      expect.all(
        table.keyColumns == List("id", "homecity"),
        table.keyOf(value) == (7L, "Sion"),
        table.findManyQuery(1).encoder.types.map(_.name) == List("int8", "text"),
      ),
    )
  }

  test("flattened codecs reject missing or extra database columns") {
    val row = PostgresRow.flattened[Contact](lowerCase)
    val encoded = row.codec.encode(Contact(7L, None, "FR")).map(_.map(_.value))
    IO.pure(
      expect.all(
        row.codec.decode(0, encoded.drop(1)).isLeft,
        row.codec.decode(0, encoded :+ None).isLeft,
      ),
    )
  }

  test("unknown or duplicate codec overrides fail when the mapping is built") {
    for
      unknown <-
        IO(
          PostgresRow.flattened[Contact](lowerCase, PostgresRow.columnCodecAt[Contact, String]("missing", text)),
        ).attempt
      duplicate <- IO(
                     PostgresRow.flattened[Contact](
                       lowerCase,
                       PostgresRow.columnCodec[Contact](_.language)(text),
                       PostgresRow.columnCodec[Contact](_.language)(varchar),
                     ),
                   ).attempt
    yield expect.all(unknown.isLeft, duplicate.isLeft)
  }

  test("flattened column collisions and multi-column overrides are rejected") {
    for
      collision <- IO(PostgresRow.flattened[Contact](_ => "same_column")).attempt
      wide      <- IO(PostgresRow.columnCodecAt[Contact, (String, Int)]("language", text.product(int4))).attempt
    yield expect.all(collision.isLeft, wide.isLeft)
  }

  test("codec overrides reject an incompatible field type at compile time") {
    val errors = typeCheckErrors("""
      import persistent4s.postgres.*
      import skunk.codec.all.int4
      final case class Row(name: String)
      PostgresRow.columnCodec[Row](_.name)(int4)
    """)
    IO.pure(expect(errors.nonEmpty))
  }

  test("codec overrides reject computed selectors and optional wrappers of required leaves") {
    val computed = typeCheckErrors("""
      import persistent4s.postgres.*
      import skunk.codec.all.text
      final case class Row(name: String)
      PostgresRow.columnCodec[Row](_.name.toLowerCase)(text)
    """)
    val wrapped = typeCheckErrors("""
      import persistent4s.postgres.*
      import skunk.codec.all.text
      final case class Address(city: String)
      final case class Row(address: Option[Address])
      PostgresRow.columnCodec[Row](_.address.map(_.city))(text.opt)
    """)
    IO.pure(expect.all(computed.nonEmpty, wrapped.nonEmpty))
  }

  test("recursive or unsupported nested types report derivation errors") {
    val recursive = typeCheckErrors("""
      import persistent4s.postgres.*
      final case class Row(id: Long, next: Option[Row])
      PostgresRow.flattened[Row](_.replace(".", "_"))
    """)
    val unsupported = typeCheckErrors("""
      import persistent4s.postgres.*
      final case class Row(id: Long, values: List[String])
      PostgresRow.flattened[Row](identity)
    """)
    IO.pure(expect.all(recursive.nonEmpty, unsupported.nonEmpty))
  }

  test("default derivation still requires flat scalar fields") {
    val errors = typeCheckErrors("""
      import persistent4s.postgres.*
      final case class Address(city: String)
      final case class Row(id: Long, address: Address) derives PostgresRow
    """)
    IO.pure(expect(errors.nonEmpty))
  }
