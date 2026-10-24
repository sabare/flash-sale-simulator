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

## Local services (planned)

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

## Reservation queue

Successful new reservations append a `reservation.created` event to the Redis
Stream `flash-sale:reservations` in the same Lua script as the stock decrement
and claim. Retries and rejected attempts do not add events. Each event contains
`schema_version`, `reservation_id`, `product_id`, and `user_id`.

Inspect queued events after making a reservation:

```bash
docker compose exec redis redis-cli XRANGE flash-sale:reservations - + COUNT 10
```

This milestone implements the producer only. Events wait in the stream; a
consumer-group worker and durable order database are the next steps. A `201`
response confirms a reservation, not a completed order or payment. The stream
is intentionally untrimmed until we implement safe retention after processing.

The prototype targets a single Redis server and trusts caller-supplied user IDs.
It has no authentication or crash-durability guarantee yet. Losing Redis state
can lose both claims and events and cause stock to be reseeded. Lua prevents
interleaving requests but does not provide rollback for runtime errors; the
script validates stock and queue type before writing. See
[Redis scripting documentation](https://redis.io/docs/latest/develop/programmability/eval-intro/).

## Run with Docker

Docker Compose starts the API and Redis together. The API uses the shared Redis
server for stock, claims, replay responses, and queued reservation events.

```bash
docker compose up --build
```

Stop the services with:

```bash
docker compose down
```
