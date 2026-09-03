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
omitted, `run` loads all rows.

The generated repository uses the existing `PostgresAtomicRepository`, so projection-state writes and checkpoint
updates run on the same Skunk session and transaction. Schema migrations, constraints, indexes, and application queries
remain explicit. SQL identifiers are validated and double-quoted exactly as declared. Use
`.column(_.scalaField, "database_column")` for legacy names, define a `PostgresField[A]` for a custom scalar codec, or
replace generated callbacks with `DerivedPostgresRepository.customized`. Concrete repositories can continue to use
`useSession` and the derived `rowCodec` for specialized Skunk queries; `all` may also be overridden.

## Design principles

- **Purely functional** — built on cats-effect `IO` and fs2 `Stream`
- **Backend agnostic** — core abstractions have no database dependency
- **Observable** — built-in support for tracing and metrics via [otel4s](https://github.com/typelevel/otel4s)
- **Minimal** — pull in only the modules you need
