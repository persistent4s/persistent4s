# persistent4s

A purely functional event sourcing library for Scala, built on [Typelevel](https://typelevel.org/) libraries (cats-effect, fs2, skunk).

> **Early stage project** — persistent4s is in active prototyping. APIs will change, modules may be added or removed, and nothing is stable yet. Feedback and ideas are welcome via [issues](https://github.com/antoniojimeneznieto/persistent4s/issues).

## Overview

persistent4s provides both **aggregateless** and **aggregate-based** event sourcing patterns, letting you choose the right level of abstraction for your use case.

- **Aggregateless** — work directly with event streams. Append events, read streams, build projections. No aggregate boilerplate.
- **Aggregate** — optional layer on top, with state folding and command handling for when you need it.

## Modules

| Module | Artifact | Description |
|---|---|---|
| core | `persistent4s-core` | Pure abstractions — `EventStore[F]`, domain types, aggregate support |
| postgres | `persistent4s-postgres` | PostgreSQL implementation via [Skunk](https://github.com/tpolecat/skunk) |
| circe | `persistent4s-circe` | JSON serialization via [Circe](https://github.com/circe/circe) |
| kafka | `persistent4s-kafka` | Event publishing/subscribing via [fs2-kafka](https://github.com/fd4s/fs2-kafka) |
| testkit | `persistent4s-testkit` | In-memory EventStore and test helpers for unit testing |

## Getting started

```scala
libraryDependencies ++= Seq(
  "io.github.antoniojimeneznieto" %% "persistent4s-core"     % "<version>",
  "io.github.antoniojimeneznieto" %% "persistent4s-postgres"  % "<version>",
  "io.github.antoniojimeneznieto" %% "persistent4s-circe"     % "<version>",
  "io.github.antoniojimeneznieto" %% "persistent4s-kafka"     % "<version>",
  "io.github.antoniojimeneznieto" %% "persistent4s-testkit"   % "<version>" % Test,
)
```

## Design principles

- **Purely functional** — built on cats-effect `IO` and fs2 `Stream`
- **Backend agnostic** — core abstractions have no database dependency
- **Observable** — built-in support for tracing and metrics via [otel4s](https://github.com/typelevel/otel4s)
- **Minimal** — pull in only the modules you need
