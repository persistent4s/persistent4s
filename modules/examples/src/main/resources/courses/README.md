# Courses Kafka Example

A two-service event-sourcing example built on `persistent4s` that demonstrates the **Kafka** module:

- **catalog-service** (port `8183`, monitoring `9091`) owns courses. Publishes events to Kafka topic `catalog.events`.
- **enrollment-service** (port `8184`, monitoring `9092`) owns students and enrollments. Subscribes to `catalog.events`
  and append incoming course events in the local event store. Rebuild the (`course_view`) and uses it together with its own domain state to validate
  enrollments. Publishes its own events to `enrollment.events`.

The enrollment command is a DCB highlight: it reads events filtered by `{student:S, course:C}` (a multi-tag scope) and
appends under the same scope's optimistic-concurrency boundary — two concurrent enrollments into the same near-full
course cannot both succeed.

## Running

From the repository root in one terminal, bring the infra up:

```bash
cd modules/examples/src/main/resources/courses
docker compose up -d
docker compose ps    # wait for "(healthy)" on the three Postgres/Kafka containers
```

Then, in two separate terminals at the repo root:

```bash
# Terminal A
sbt "examples/runMain persistent4s.examples.courses.catalog.infrastructure.CatalogServer"

# Terminal B
sbt "examples/runMain persistent4s.examples.courses.enrollment.infrastructure.EnrollmentServer"
```

Smithy docs are at <http://localhost:8183/docs> and <http://localhost:8184/docs>. Projection monitoring UIs are at
<http://localhost:9091> and <http://localhost:9092>. The Kafka UI (topics, messages, consumer groups, lag) is at
<http://localhost:8090>.

