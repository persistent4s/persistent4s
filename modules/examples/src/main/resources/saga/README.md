# Saga Example — orders + inventory

Two services demonstrating the **orchestration saga**: a command that cannot be validated locally, because the
invariant it depends on belongs to another service.

- **orders-service** (port `8183`) owns customers and orders, and hosts the `reserve-stock` saga.
- **inventory-service** (port `8184`) owns stock per item, and answers the saga's requests.

Placing an order needs stock, and stock is inventory's invariant — so `PlaceOrder` cannot check it. It validates what it
does own (the customer exists, the amount is positive), appends `OrderPlaced`, and lets the saga chase the rest:

```
POST /orders          OrderPlaced ──trigger──► saga starts, sends ReserveStock ──► inventory.commands
                                                                                        │
                                        inventory folds the item's log, appends StockReserved
                                        and enqueues its reply in the same transaction
                                                                                        │
              OrderConfirmed ◄──── saga applies the reply ◄──── orders.replies ◄────────┘
              OrderCancelled ◄──── reply was "rejected", or the deadline passed first
```

**`OrderPlaced` is provisional.** The order exists seconds before anyone knows whether it can be honoured, so the read
model carries a `status` — `Placed`, then `Confirmed` or `Cancelled`. That is the trade a saga makes: an upfront check
you cannot perform, exchanged for a compensation you can.

## What each side needs from the library

| | orders | inventory |
|---|---|---|
| `PostgresModule` | `enableSaga = true` | `enableMessageOutbox = true` |
| tables it adds | `saga_instances`, `message_outbox` | `message_outbox` |
| Kafka | message relay out, `MessageSubscriber` on `orders.replies` | `MessageSubscriber` on `inventory.commands`, message relay out |

Neither service enables the **event** outbox. No event of one service ever reaches the other — the two share only the
`ReserveStock` / `StockReservationReply` DTOs, and the saga translates a reply into an event of its *own* log. That is
the difference from the courses example, where the enrollment service imports catalog events into its own store.

## Running

Bring the infrastructure up (and take the courses example down first if it is running — this one deliberately reuses its
ports):

```bash
cd modules/examples/src/main/resources/saga
docker compose up -d
docker compose ps    # wait for "(healthy)"
```

Then, in two terminals at the repo root:

```bash
# Terminal A
sbt "examples/runMain persistent4s.examples.saga.orders.infrastructure.OrdersServer"

# Terminal B
sbt "examples/runMain persistent4s.examples.saga.inventory.infrastructure.InventoryServer"

# Terminal C
sbt "examples/runMain persistent4s.examples.saga.payment.infrastructure.PaymentServer"
```

Swagger UI is at <http://localhost:8183/docs> and <http://localhost:8184/docs> — the whole walkthrough below can be
clicked through there instead, with the ids already filled in. Kafka UI (topics, messages, consumer groups) is at
<http://localhost:8090>.

Unlike the other examples these routes are hand-written, so the specification is too: `orders-openapi.yaml` and
`inventory-openapi.yaml` next to this file, served at `/openapi.yaml` and rendered by the `swagger-ui-dist` webjar that
`smithy4s-http4s-swagger` already brings onto the classpath.

To start from nothing — the walkthrough below assumes empty logs — recreate the volumes:

```bash
docker compose down -v && docker compose up -d
```

### Endpoints

| | |
|---|---|
| `POST /customers` | `{"customerId","name"}` → `201` |
| `POST /orders` | `{"orderId","customerId","itemId","amount"}` → `202` |
| `GET /orders` | every order with its status |
| `GET /orders/{id}` | one order, or `404` |
| `GET /orders/{id}/saga` | the saga instance behind it — status, step, deadline, serialized state |
| `POST /items/{id}/restock` | `{"amount"}` → the item's stock |
| `GET /items/{id}` | available, and who holds the rest |

`GET /orders/{id}/saga` is the machinery on display: the instance id is not stored on the order, it is *derived* from the
saga name and the order id, which is how replaying a trigger event finds the same row instead of starting a second one.

## Walkthrough

```bash
CUSTOMER=11111111-1111-1111-1111-111111111111
ITEM=22222222-2222-2222-2222-222222222222

curl -X POST localhost:8183/customers \
  -H 'content-type: application/json' \
  -d "{\"customerId\":\"$CUSTOMER\",\"name\":\"Ada\"}"

curl -X POST localhost:8184/items/$ITEM/restock \
  -H 'content-type: application/json' -d '{"amount":5}'
```

**1. Happy path.** The POST returns `202 Accepted`, not `201` — nothing is confirmed yet.

```bash
ORDER=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa
curl -X POST localhost:8183/orders -H 'content-type: application/json' \
  -d "{\"orderId\":\"$ORDER\",\"customerId\":\"$CUSTOMER\",\"itemId\":\"$ITEM\",\"amount\":2}"
# {"orderId":"aaaa…","status":"Placed","outcomeAt":"/orders/aaaa…"}

curl localhost:8183/orders/$ORDER         # 404 if you are very quick — see below
sleep 1
curl localhost:8183/orders/$ORDER         # {"status":"Confirmed", ...}
curl localhost:8183/orders/$ORDER/saga    # {"status":"Completed","step":0, ...}
curl localhost:8184/items/$ITEM           # available: 3
```

Two honest caveats about that first read:

- **It can 404.** The event is committed but the projector is milliseconds behind, so the row may not exist yet. That is
  ordinary CQRS, nothing to do with the saga — and it is why `POST /orders` answers from the command it accepted rather
  than from the read model, which would have to 404 for an order that certainly exists.
- **Catching `Placed` is luck.** The whole round trip — request out, reservation, reply back, `OrderConfirmed` appended,
  projection updated — takes a few hundred milliseconds on a local broker. To watch an order actually *sit* in `Placed`,
  use scenario 3: with inventory stopped it stays there for the full 30 seconds.

**2. Rejection.** Ask for more than is left. Inventory answers "no", writes no `StockReserved`, and the saga compensates
with `OrderCancelled` — the reason travels from inventory's validation all the way into the orders log.

```bash
ORDER=bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb
curl -X POST localhost:8183/orders -H 'content-type: application/json' \
  -d "{\"orderId\":\"$ORDER\",\"customerId\":\"$CUSTOMER\",\"itemId\":\"$ITEM\",\"amount\":99}"

sleep 1
curl localhost:8183/orders/$ORDER    # {"status":"Cancelled","reason":"insufficient stock: 3 available, 99 requested"}
```

**3. Timeout.** Stop the inventory service (Ctrl-C in terminal B) and place an order. Nothing answers, the deadline
passes, and the timer loop compensates. The request is still sitting in Kafka, so restarting inventory will process it —
and its reply will be ignored, because the instance is no longer pending.

```bash
ORDER=cccccccc-cccc-cccc-cccc-cccccccccccc
curl -X POST localhost:8183/orders -H 'content-type: application/json' \
  -d "{\"orderId\":\"$ORDER\",\"customerId\":\"$CUSTOMER\",\"itemId\":\"$ITEM\",\"amount\":1}"

sleep 35
curl localhost:8183/orders/$ORDER    # {"status":"Cancelled","reason":"inventory did not answer in time"}
```

Restart inventory afterwards and watch it *decline* the request it finds waiting on the topic:

```
declined order cccc… : request expired at 2026-…:08.770Z, now 2026-…:32.100Z
```

That is the `expiresAt` header the saga stamps on every request, computed from the trigger event's own timestamp so the
decision functions stay pure. It makes the caller's deadline a shared fact rather than one side's private opinion — without
it, inventory would honour this request in full and hold stock for an order that no longer exists, with nothing in this
example ever releasing it.

**4. Redelivery.** Replay a `ReserveStock` record from the Kafka UI. Inventory folds its own log, sees a
`StockReserved` already carrying that `orderId`, and re-answers without reserving twice. Stock does not move.

**4b. The leak that is still open.** Redelivery of a request that was *declined* is a different story, and the expiry does
not save it. Point an order at an item with no stock, let it be cancelled, restock the item, then replay that request
before its 30 seconds are up:

```bash
ORDER=dddddddd-dddd-dddd-dddd-dddddddddddd   # against an item with 0 available
# … order is Cancelled "insufficient stock: 0 available, 1 requested"
curl -X POST localhost:8184/items/$ITEM/restock -d '{"amount":5}' -H 'content-type: application/json'
# … replay the request record from the Kafka UI

curl localhost:8183/orders/$ORDER   # Cancelled, "insufficient stock: 0 available, 1 requested"
curl localhost:8184/items/$ITEM     # available: 4 — with 1 reserved for that cancelled order
```

A rejection writes no event, so nothing in inventory's log remembers it, and the saga went terminal the moment it read
that rejection — leaving nearly the whole expiry window exposed. Closing this needs the refusal itself to be recorded, so
a later request for the same order collides with it. Note the invariant that never breaks either way: a *confirmation*
always implies a reservation, so the system can hold stock nobody wants but can never sell stock it does not have.

**5. Concurrency.** Restock to exactly 1 and fire two orders for it at once. Both are accepted locally — neither
`PlaceOrder` can see the other — and inventory's optimistic-concurrency check on the item tag decides which one wins.
One order ends `Confirmed`, the other `Cancelled`. This is why the request has to be sent to the service that owns the
invariant rather than checked by the caller.

```bash
curl -X POST localhost:8184/items/$ITEM/restock -H 'content-type: application/json' -d '{"amount":1}'
# ... place two orders for amount 1 with & between them, then read both
```

## Where to look

```sql
-- orders_events
SELECT saga_key, status, step, deadline FROM saga_instances;  -- one row per order; terminal once the reply lands
SELECT event_type, payload FROM events ORDER BY sequence_number;

-- inventory_events
SELECT event_type, payload FROM events ORDER BY sequence_number;
```

`message_outbox` on either side is where a request or a reply waits for its relay, and the relay *deletes* each row once
Kafka has it — so an empty table is the healthy state, and a row sitting there means the relay is stopped or the broker
is unreachable. To catch one in flight, stop the service that owns it and place an order.

