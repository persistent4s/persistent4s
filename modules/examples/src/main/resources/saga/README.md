# Saga Example — orders + inventory + payment

An **orchestration saga**: `PlaceOrder` depends on invariants that belong to other services, so it cannot validate them
locally. It checks what orders owns (the customer exists, the amount is positive), appends a provisional `OrderPlaced`,
and a saga chases the rest.

1. `OrderPlaced` triggers the saga, which fans out two requests in parallel — `ReserveStock` to inventory and
   `AuthorizePayment` to payment.
2. Each partner decides on its own log and enqueues its reply in the same transaction that appends its events.
3. Both accept → `OrderConfirmed`. Either rejects, or the deadline passes → `OrderCancelled`, plus `ReleaseStock` /
   `CancelPayment` to undo whatever did succeed.

An order therefore has a `status` — `Placed`, then `Confirmed` or `Cancelled`. No event of one service ever reaches
another: the services share only DTOs, and the saga translates a reply into an event of its *own* log.

| service | port | role |
|---|---|---|
| orders | `8183` | owns customers and orders, hosts the saga |
| inventory | `8184` | owns stock per item, answers `ReserveStock` |
| payment | — | answers `AuthorizePayment`, stateless, no HTTP |

## Running

Bring up the infrastructure — take the courses example down first if it is running, since this one reuses its ports:

```bash
cd modules/examples/src/main/resources/saga
docker compose up -d
docker compose ps    # wait for "(healthy)"
```

Then start the three services, each in its own terminal at the repo root:

```bash
sbt "examples/runMain persistent4s.examples.saga.orders.infrastructure.OrdersServer"
sbt "examples/runMain persistent4s.examples.saga.inventory.infrastructure.InventoryServer"
sbt "examples/runMain persistent4s.examples.saga.payment.infrastructure.PaymentServer"
```

## Trying it out

Swagger UI, with the ids prefilled and `Try it out` enabled:

- orders — <http://localhost:8183/docs>
- inventory — <http://localhost:8184/docs>

Register a customer, restock an item, then place an order and read it back to watch the status settle. Asking for more
than is in stock shows the compensation; stopping the inventory service before placing an order shows the timeout.

Kafka UI (topics, messages, consumer groups) is at <http://localhost:8090>. To start again from empty logs:

```bash
docker compose down -v && docker compose up -d
```
