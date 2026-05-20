# Courses Kafka Example

A two-service event-sourcing example built on `persistent4s` that demonstrates the **Kafka** module:

- **catalog-service** (port `8183`, monitoring `9091`) owns courses. Publishes events to Kafka topic `catalog.events`.
- **enrollment-service** (port `8184`, monitoring `9092`) owns students and enrollments. Subscribes to `catalog.events`
  to maintain a local read-model (`course_view`) and uses it together with its own event-store state to validate
  enrollments. Publishes its own events to `enrollment.events`.

The enrollment command is a DCB highlight: it reads events filtered by `{student:S, course:C}` (a multi-tag scope) and
appends under the same scope's optimistic-concurrency boundary — two concurrent enrollments into the same near-full
course cannot both succeed.

## Running

From the repository root in one terminal, bring the infra up:

```bash
cd modules/examples/src/main/resources/courses
docker compose up -d
docker compose ps    # wait for "(healthy)" on all three containers
```

Then, in two separate terminals at the repo root:

```bash
# Terminal A
sbt "examples/runMain persistent4s.examples.courses.catalog.infrastructure.CatalogServer"

# Terminal B
sbt "examples/runMain persistent4s.examples.courses.enrollment.infrastructure.EnrollmentServer"
```

Smithy docs are at <http://localhost:8183/docs> and <http://localhost:8184/docs>. Projection monitoring UIs are at
<http://localhost:9091> and <http://localhost:9092>.

## Demo script

```bash
# 1. Open a course in catalog (capacity 1 to force the full-capacity case)
COURSE_ID=$(curl -s -X POST http://localhost:8183/courses \
  -H 'Content-Type: application/json' \
  -d '{"code":"CS101","title":"Intro to CS","capacity":1,"instructor":"Ada Lovelace"}' \
  | jq -r .courseId)
echo "course: $COURSE_ID"

# 2. Wait a moment for the Kafka relay → enrollment consumer → course_view to settle (~1s)
sleep 2

# 3. Confirm enrollment service has the course in its local view
curl -s http://localhost:8184/course-view/$COURSE_ID | jq

# 4. Register two students
S1=$(curl -s -X POST http://localhost:8184/students \
  -H 'Content-Type: application/json' \
  -d '{"name":"Alice","email":"alice@example.com"}' | jq -r .studentId)
S2=$(curl -s -X POST http://localhost:8184/students \
  -H 'Content-Type: application/json' \
  -d '{"name":"Bob","email":"bob@example.com"}' | jq -r .studentId)
echo "alice: $S1   bob: $S2"

# 5. Enroll Alice — succeeds
curl -s -X POST http://localhost:8184/enrollments \
  -H 'Content-Type: application/json' \
  -d "{\"studentId\":\"$S1\",\"courseId\":\"$COURSE_ID\"}" -o /dev/null -w "alice enrol: %{http_code}\n"

# 6. Enroll Bob — fails (capacity 1, Alice has the seat)
curl -s -X POST http://localhost:8184/enrollments \
  -H 'Content-Type: application/json' \
  -d "{\"studentId\":\"$S2\",\"courseId\":\"$COURSE_ID\"}" -w "bob enrol: %{http_code}\n"

# 7. Drop Alice
curl -s -X POST http://localhost:8184/enrollments/$S1/$COURSE_ID/drop -o /dev/null -w "alice drop: %{http_code}\n"

# 8. Enroll Bob again — now succeeds
curl -s -X POST http://localhost:8184/enrollments \
  -H 'Content-Type: application/json' \
  -d "{\"studentId\":\"$S2\",\"courseId\":\"$COURSE_ID\"}" -o /dev/null -w "bob enrol again: %{http_code}\n"

# 9. Inspect state
curl -s http://localhost:8184/enrollments | jq
curl -s http://localhost:8183/events    | jq    # catalog events
curl -s http://localhost:8184/events    | jq    # enrollment events
```

You can also tail Kafka directly:

```bash
docker exec persistent4s-courses-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic catalog.events --from-beginning --timeout-ms 3000
docker exec persistent4s-courses-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic enrollment.events --from-beginning --timeout-ms 3000
```

## Teardown

```bash
docker compose down   # no named volumes — data is gone, next `up` is fresh
```
