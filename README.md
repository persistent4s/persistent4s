# persistent4s

A purely functional event sourcing library for Scala, built on [Typelevel](https://typelevel.org/) libraries (cats-effect, fs2, skunk).

> **Early stage project** — persistent4s is in active prototyping. APIs will change, modules may be added or removed, and nothing is stable yet. Feedback and ideas are welcome via [issues](https://github.com/antoniojimeneznieto/persistent4s/issues).

## Overview

persistent4s is **aggregateless**: commands and projections fold the events they actually need, declaring the scopes
they touch instead of loading an aggregate root.

- **Typed scopes** — declare durable history and concurrency boundaries once, including multi-scope and multi-key commands.
- **Command handling** — fold the state a command needs, return typed rejections, and append under an optimistic-concurrency check.
- **Evolvable events** — stable event IDs, payload versions, aliases, and Circe JSON upcasters.
- **Bounded replay** — versioned command snapshots backed by PostgreSQL, with event history remaining the source of truth.
- **Projections** — exactly-once handlers, per-key parallel folding, and leader election so a horizontally scaled service keeps one writer per checkpoint.
- **Atomic read models** — PostgreSQL repositories can commit projection state and its checkpoint in one transaction.
- **Read-your-writes** — optionally wait for a projection to catch up before a command returns.
- **Transactional outbox** — events and outgoing messages enqueued in the same transaction that appends, then relayed to Kafka.
- **Sagas** — orchestration across services: requests leave through the outbox, replies arrive as messages, and no event of one service ever enters another's log.

## Modules

| Module | Artifact | Description |
|---|---|---|
| core | `persistent4s-core` | Pure abstractions — `EventStore[F]`, command handling, projections, domain types |
| postgres | `persistent4s-postgres` | PostgreSQL implementation via [Skunk](https://github.com/tpolecat/skunk) |
| circe | `persistent4s-circe` | JSON serialization via [Circe](https://github.com/circe/circe) |
| kafka | `persistent4s-kafka` | Event and message relaying via [fs2-kafka](https://github.com/fd4s/fs2-kafka) |
| monitoring | `persistent4s-monitoring` | Developer-facing HTTP page for projection checkpoints — pause, resume, reindex |

## Getting started

```scala
libraryDependencies ++= Seq(
  "io.github.persistent4s" %% "persistent4s-core"       % "<version>",
  "io.github.persistent4s" %% "persistent4s-postgres"   % "<version>",
  "io.github.persistent4s" %% "persistent4s-circe"      % "<version>",
  "io.github.persistent4s" %% "persistent4s-kafka"      % "<version>",
  "io.github.persistent4s" %% "persistent4s-monitoring" % "<version>",
)
```

Runnable examples live in `modules/examples`, each with its own `docker-compose.yml` and README.

## Event definitions

Events only need their fields and JSON codecs:

```scala
import io.circe.{Decoder, Encoder}
import persistent4s.Event
import persistent4s.circe.CirceEventCodec

sealed trait PatientEvent extends Event

final case class PatientCreated(number: Int, name: String)
    extends PatientEvent derives Encoder, Decoder

val eventCodec = CirceEventCodec.derived[PatientEvent]
```

The default event name is `PatientCreated`, with version `1`. An explicit `EventSchema`
still takes precedence when you need a fixed name, aliases, versions, or shared scope definitions.
Renaming an event class changes its default stored name; use an explicit schema to keep the old name.

For events without declared scopes, command handlers select their history with `scope(...)`
and accept those events with `on[PatientCreated].allEvents`. Projections provide the key directly,
for example `on[PatientCreated](_.number).create(...)`. Scope keys are never guessed from fields.

### Partial updates

Use `PatchCodec` for update events with fields of type `Option[Option[A]]`:

```scala
import persistent4s.circe.PatchCodec

final case class PatientUpdated(
  number: Int,
  name: Option[Option[String]] = None,
) extends PatientEvent derives PatchCodec
```

`PatchCodec` supplies both Circe `Encoder` and `Decoder`. It uses the case class's field names.

| Scala value | JSON field | Meaning |
| --- | --- | --- |
| `None` | omitted | Keep the current value |
| `Some(None)` | `"name": null` | Clear the value |
| `Some(Some("Alice"))` | `"name": "Alice"` | Set the value |

Ordinary `Option[A]` fields keep Circe's usual behavior. Commands decide which fields may be
cleared and omit supplied values that already match the current state. Event handlers apply only
the supplied fields. `CirceEventCodec.derived` uses patch codecs like any other event codecs,
including custom schemas, aliases, and older-version readers.

## Releases

The GitHub release workflow publishes `persistent4s` to Maven Central.
Applications use a published version through sbt; no private repository or local
publication is required. Library changes must be released before applications
can depend on them.

## Derived PostgreSQL projection repositories

Projection states can use generated Skunk persistence without database annotations or handwritten CRUD:

```scala
final case class BookState(
  bookId: UUID,
  title: String,
  availableCopies: Int,
)

object BookRepository:
  val table =
    PostgresTable
      .derived[BookState]("books")
      .key(_.bookId)

final class BookRepository(pool: Resource[IO, Session[IO]])
    extends DerivedPostgresRepository[IO, UUID, BookState](pool, BookRepository.table):

  def findWithCopies(copies: Int): IO[List[BookState]] =
    filterBy(_.availableCopies).is(copies).run

  def findByIds(ids: List[UUID]): IO[List[BookState]] =
    filterBy(_.bookId).in(ids).run
```

`PostgresTable.derived` preserves case-class field order, maps field names to `snake_case`, and generates batched
`find`, `findMany`, `all`, full-row upsert, and delete operations. Table identity and keys remain explicit because they
are part of the durable database contract. Composite keys select a tuple of state fields:

```scala
PostgresTable
  .derived[BorrowingState]("borrowings")
  .key(state => (state.bookId, state.memberId))

// Option fields expose null-aware filtering.
borrowingRepository.filterBy(_.returnedAt).isNull.run
```

Filters are immutable query builders, so conditions can be composed before executing them once:

```scala
repository.filterBy(_.author).in(authors).and(_.availableCopies).is(Some(copies)).run
```

Optional filter inputs make frontend parameters straightforward: `None` omits that condition, while `Some(value)` or
`Some(values)` adds it. Passing an empty collection directly to `in` matches no rows. If every optional condition is
omitted, `run` loads all rows unless a limit is supplied.

Apply ordering and pagination before `run` so PostgreSQL does the work:

```scala
repository.query.orderBy(_.bookId).withLimit(100).withOffset(200).run

repository.filterBy(_.author).is(author)
  .orderBy(_.bookId)
  .withLimit(50, maxLimit = 300)
  .withOffset(200)
  .run
```

`query` starts without filters. `orderBy` sorts ascending; `orderByDescending` sorts descending. Repeated ordering
calls add tie breakers. Include a unique field for stable pages. `withLimit` caps the requested count at 200 by default;
`withLimit(5000, maxLimit = 300)` returns at most 300 rows. `withOffset` skips rows in SQL. Negative counts and offsets
fail with `PaginationError.NegativeLimit` and `PaginationError.NegativeOffset` when `run` executes, before opening a
database connection. Both extend `IllegalArgumentException`. A negative maximum is a configuration error and fails
with `IllegalArgumentException`. A zero limit or maximum returns no rows. `limit` and `offset` remain aliases.

For deep pages, `filterBy(_.bookId).greaterThan(lastBookId)` can continue from an indexed unique value instead of
counting skipped rows. A missing optional `greaterThan` input omits that condition, which selects the first page.

Ordered or paged `IN` filters use one SQL statement, so the order and limit apply to the complete result. Plans exceeding
the PostgreSQL parameter limit are rejected. Unordered, unbounded `IN` filters retain batched execution. Calling `run`
without `withLimit` still loads all matches; sorting or taking rows from its returned list happens in application memory.

The generated repository uses the existing `PostgresAtomicRepository`, so projection-state writes and checkpoint
updates run on the same Skunk session and transaction. Schema migrations, constraints, indexes, and application queries
remain explicit. SQL identifiers are validated and double-quoted exactly as declared. Use
`.column(_.scalaField, "database_column")` for legacy names, define a `PostgresField[A]` for a custom scalar codec, or
replace generated callbacks with `DerivedPostgresRepository.customized`. Concrete repositories can continue to use
`useSession` and the derived `rowCodec` for specialized Skunk queries; `all` may also be overridden.

Generated inserts and partial updates can also use an existing connection and transaction:

```scala
repository.insert(session, book) // false if this key already exists; existing rows stay unchanged

repository.update(bookId)
  .setWhenDefined(_.title)(newTitle)
  .set(_.availableCopies)(remainingCopies)
  .whereEqual(_.availableCopies)(previousCopies)
  .run(session)
```

The update returns `true` when a row changed, or `false` when the key is missing or a condition does not match.
`whereEqual` and `whereLessThan` add SQL conditions to the same update statement. `setWhenDefined` skips `None`;
for nullable fields, `Some(None)` clears the field and `Some(Some(value))` sets it. `set` always assigns the supplied value.
Key assignments, repeated assignments to one field, and empty updates are rejected. Other database constraint errors
remain errors. Composite keys and column overrides work for these writes too.

`insert(session, row)` and `run(session)` leave transaction ownership with the caller, so an incoming event's receipt
and table changes can commit or roll back together. `insert(row)` and `run` use a pooled connection for a single write.

## PostgreSQL telemetry

`PostgresModule.make` and `makeWithConfig` use Skunk 2.0.0-RC3. Supply `TracerProvider[F]` and
`MeterProvider[F]` from the same telemetry SDK as the existing `Tracer[F]` and `Meter[F]`.
Tests and examples that disable telemetry can supply the providers' explicit `noop` instances.

Database spans keep the caller's trace and use SQL summaries such as `SELECT patients`.
Pool and PostgreSQL protocol spans are disabled. Query attributes omit bound parameter values;
the SQL analyzer removes literal values from unparameterized query text. Skunk's exception details
and queries mixing literals with parameters can still contain values, so applications must sanitize
those fields before export. Database duration and pool wait metrics remain available. Applications
control sampling, including whether background database operations should start their own traces,
through their telemetry SDK.

## Design principles

- **Purely functional** — built on cats-effect `IO` and fs2 `Stream`
- **Backend agnostic** — core abstractions have no database dependency
- **Observable** — built-in support for tracing and metrics via [otel4s](https://github.com/typelevel/otel4s)
- **Minimal** — pull in only the modules you need
