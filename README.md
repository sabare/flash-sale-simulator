# Flash Sale Simulator

A backend-focused simulation of a high-demand flash sale. The goal is to show
how a system can accept a very large burst of purchase attempts while selling
only the available inventory, without overwhelming its primary database.

## The problem

Imagine one million customers trying to buy a product with only 100 units in
stock. The system must:

- never oversell;
- prevent one user from claiming the product twice;
- return a fast response when stock is gone;
- tolerate client retries without creating duplicate orders; and
- continue processing accepted orders even if downstream services are slow.

## Target architecture

```text
Client -> Load balancer -> Stateless API -> Redis atomic reservation
                                           |
                                           v
                                      Order queue -> Worker -> Database
```

Redis acts as the fast, atomic inventory gate. Only successful reservations
are placed on the queue; workers persist them as orders asynchronously. This
keeps the database out of the hot path during the traffic spike.

## Build milestones

1. Create a small API with a health endpoint and a sale/product model.
2. Add atomic inventory reservation in Redis.
3. Add one-purchase-per-user and idempotency protection.
4. Add an asynchronous order worker and durable order storage.
5. Add rate limiting, reservation expiry, metrics, and load tests.

## Core invariant

For a product, successful reservations must never exceed its configured stock.

## Local services

- API service
- Redis
- PostgreSQL
- Background order worker

The MVP API uses Python and FastAPI.

## Run locally

This MVP uses Python 3.11+ and FastAPI.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
docker compose up -d redis
uvicorn app.main:app --reload
```

In another terminal, verify the service:

```bash
curl http://127.0.0.1:8000/health
curl http://127.0.0.1:8000/products/limited-headphones
```

Run tests from the repository with `.venv/bin/python -m pytest -q`. API tests
use a fake gateway; inventory tests execute the production Lua script using
fakeredis with Lua support. No running Redis is needed for these tests. These
checks do not replace real Redis integration tests or performance benchmarks.
Worker tests also exercise database and acknowledgment failures. Four PostgreSQL
tests are skipped unless `TEST_DATABASE_URL` points to a disposable test database:

```bash
TEST_DATABASE_URL='postgresql://user:password@localhost/test_database' \
  .venv/bin/python -m pytest -q
```

These tests create and remove randomly named schemas in that database. They
verify concurrent duplicate delivery, conflicting orders, and commit-before-ack
redelivery using real PostgreSQL and emulated Redis.

## Current API

Reserve one item with a user identifier:

```bash
curl -X POST http://127.0.0.1:8000/sales/limited-headphones/reservations \
  -H "content-type: application/json" \
  -H "Idempotency-Key: reserve-user-123-001" \
  -d '{"user_id":"user-123"}'
```

The endpoint returns `201` while stock remains and `409` when it is sold out.
The stock counter lives in Redis and is decremented by a Lua script, so the
check-and-decrement is atomic even when multiple API instances receive traffic.
The same user can reserve a given product only once; duplicate attempts return
`409` without decrementing stock.
Send an `Idempotency-Key` on every request. Repeating the same request with the
same key returns the original `201` response and does not reserve another unit.
Replay records last 24 hours and are scoped to a product and user. After expiry,
the user's permanent claim still blocks a second reservation.
If Redis is unavailable, the API returns `503` instead of risking an
incorrect sale.

Look up the order using the `reservation_id` from the reservation response:

```bash
curl http://127.0.0.1:8000/orders/REPLACE_WITH_RESERVATION_UUID
```

`GET /orders/{reservation_id}` returns `200` with the reservation ID, product ID,
user ID, status, and creation timestamp once the worker has saved the order.
New orders have status `pending_payment`. A `404` means no order exists yet:
the reservation may still be queued, or the ID may be unknown. After a successful
reservation, wait briefly and retry; `404` alone does not prove sale failure.
Malformed UUIDs return `422`; unavailable/unconfigured PostgreSQL returns `503`.
On first startup, lookup may return `503` until the worker initializes the schema.
This demo has no authentication; possession of a reservation UUID permits lookup.

## Reservation queue

Successful new reservations append a `reservation.created` event to the Redis
Stream `flash-sale:reservations` in the same Lua script as the stock decrement
and claim. Retries and rejected attempts do not add events. Each event contains
`schema_version`, `reservation_id`, `product_id`, and `user_id`.

Inspect queued events after making a reservation:

```bash
docker compose exec redis redis-cli XRANGE flash-sale:reservations - + COUNT 10
```

The worker reads events through the `order-workers` consumer group, including
events queued before its first startup. It saves an order in PostgreSQL with
status `pending_payment`, then acknowledges the event. The reservation UUID is
the order's primary key: repeated delivery reuses the existing row, without
resetting its status or creation time. Conflicting payloads are rejected.
PostgreSQL also enforces one order per product/user for this single-sale demo.

A `201` API response confirms a reservation; order creation happens later. It
does not indicate payment. The stream is intentionally untrimmed until we
implement safe retention after processing.

Failures are logged and left pending. Each worker loop first reclaims up to
10 events idle for at least 60 seconds using `XAUTOCLAIM`, then reads new work.
This recovers its own failed events and events owned by crashed workers. The
pending-list scan cursor advances across loops so later entries are reached.
Reclaiming resets the idle timer, spacing retries by at least the idle threshold.

Configure `RECLAIM_IDLE_MS` on the worker to change the positive threshold in
milliseconds. Choose a value longer than normal batch processing time: a slow
live worker can otherwise overlap with a recovering worker. PostgreSQL's unique
constraints make repeated order writes safe. Delivery can occur more than once;
this does not promise exactly-once message delivery.

Malformed or permanently failing events stay pending and retry at that interval;
a retry limit and dead-letter queue are still future work. Missing stream entries
are logged as errors because recovery cannot reconstruct deleted event data.

The prototype targets a single Redis server and trusts caller-supplied user IDs.
It has no authentication or crash-durability guarantee yet. Losing Redis state
can lose both claims and events and cause stock to be reseeded. Lua prevents
interleaving requests but does not provide rollback for runtime errors; the
script validates stock and queue type before writing. See
[Redis scripting documentation](https://redis.io/docs/latest/develop/programmability/eval-intro/).

## Run with Docker

Docker Compose starts the API, Redis, PostgreSQL, and the worker. The API uses
Redis for stock, claims, replay responses, and queued reservation events. The
worker writes orders, and the API reads them for status lookups; reservation
requests do not access PostgreSQL.
The bundled database credentials are for local development. PostgreSQL has no
published host port, and its data lives in the named `postgres_data` volume.

```bash
docker compose up --build
```

Send the reservation request above, then inspect worker logs and saved orders:

```bash
docker compose logs --tail=50 worker
docker compose exec postgres psql -U flashsale -d flashsale -c \
  "SELECT reservation_id, product_id, user_id, status, created_at FROM orders;"
docker compose exec redis redis-cli XPENDING flash-sale:reservations order-workers
```

The worker bootstraps the initial table from `app/orders.sql`. This is an initial
schema, not a migration framework. To run a worker outside Docker, configure
`DATABASE_URL` and `REDIS_URL` and run `python -m app.worker`. Each process gets
a unique consumer name unless `WORKER_NAME` is set (use a unique value per worker).
The implementation uses a short database transaction and connection per event;
connection pooling and batching can be measured and added later.

Stop the services with:

```bash
docker compose down
```

This retains PostgreSQL's named volume. Redis persistence is not configured yet;
recreating Redis can reset the sale while old orders remain in PostgreSQL.

Implementation references: [Redis consumer groups](https://redis.io/docs/latest/commands/xreadgroup/)
[pending-event recovery](https://redis.io/docs/latest/commands/xautoclaim/),
and [Psycopg transactions](https://www.psycopg.org/psycopg3/docs/basic/usage.html).
