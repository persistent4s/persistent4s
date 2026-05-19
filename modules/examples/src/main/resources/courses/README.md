# Courses Kafka Example

Two-service event-sourcing example demonstrating `persistent4s-kafka`:

- **catalog-service** owns courses; publishes events to Kafka topic `catalog.events`.
- **enrollment-service** owns students/enrollments; subscribes to `catalog.events` to maintain a local read-model, and publishes its own events to `enrollment.events`.

See [`docker-compose.yml`](docker-compose.yml) for infra.
The full demo walkthrough is filled in once the services are wired up.
