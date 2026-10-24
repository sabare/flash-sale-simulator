# Flash Sale Simulator

A backend project exploring how to reserve limited stock under concurrent
purchase attempts, then create orders asynchronously. Built with **FastAPI,
Redis Lua/Streams, and PostgreSQL**.

The MVP includes atomic stock checks, one reservation per user/product,
idempotent retries, rate limiting, persistent Redis state, a recovering order
worker, order lookup, Prometheus metrics, and reproducible load tests.

## Architecture

```text
Client -> FastAPI -> rate limit -> Redis Lua reservation
                                  | stock + claim + replay record + stream event
                                  v
                            Redis Streams
                                  |
                       Worker consumer group
                                  |
                       PostgreSQL order commit
                                  |
                         Redis acknowledgment

Client -> GET /orders/{reservation_id} -> PostgreSQL
Prometheus -> GET /metrics -> API counters + Redis queue gauges
```

The reservation path does not write to PostgreSQL. Lua serializes the inventory
check, claim and enqueue operation on one Redis server. The worker commits an
order before acknowledging its event. If it crashes between those steps,
redelivery finds the same order via its reservation UUID and does not create
another row. This is at-least-once processing with idempotent order writes,
not exactly-once delivery.

## Quick start with Docker

Requires Docker Engine/Desktop with Compose v2.

```bash
docker compose up -d --build --wait --wait-timeout 120
curl --fail http://127.0.0.1:8000/ready
```

Compose starts the API, Redis, PostgreSQL and worker. The worker initializes the
orders table. API readiness checks Redis, PostgreSQL and that table. The database
credentials in Compose are local demo credentials.

Reserve a product:

```bash
curl -i -X POST http://127.0.0.1:8000/sales/limited-headphones/reservations \
  -H 'Content-Type: application/json' \
  -H 'Idempotency-Key: alice-purchase-1' \
  -d '{"user_id":"alice"}'
```

Copy the returned reservation UUID into this request:

```bash
curl http://127.0.0.1:8000/orders/REPLACE_WITH_RESERVATION_UUID
curl http://127.0.0.1:8000/metrics
docker compose logs --tail=50 worker
```

Repeat the purchase with the same user and key to replay the original response.
A different key for the same user/product returns a duplicate-claim conflict.
The API documentation is at [localhost:8000/docs](http://localhost:8000/docs).

| Endpoint / response | Meaning |
| --- | --- |
| `GET /health` | API process is alive |
| `GET /ready` | Dependencies and order schema are ready; does not prove a worker is alive |
| `GET /products/limited-headphones` | Current stock, initially 100 |
| `POST /sales/{product_id}/reservations` → 201 | Reserved or replayed; not a paid order |
| Reservation → 409 | Sold out or user already reserved with another key |
| Reservation → 429 | Rate limited; observe `Retry-After` |
| `GET /orders/{reservation_id}` → 200 | Saved order, status and creation time |
| Order lookup → 404 | Unknown ID or worker has not saved it yet; retry after an accepted reservation |
| 422 / 503 | Invalid input / required dependency unavailable |
| `GET /metrics` | Prometheus text metrics |

## Correctness and recovery

- Stock and claims are shared across API instances; Redis Lua prevents
  interleaving requests. Scripts do not roll back runtime errors, so the script
  validates stock and queue type before writing.
- Replay records last 24 hours and are scoped to product/user/key. Claims do not
  expire. After replay expiry, the existing claim still prevents another purchase.
- Reservation attempts for known products are limited to **10 per user ID per
  60-second fixed window**, shared across API instances. Replays, duplicate claims
  and sold-out attempts consume budget. Rejected attempts do not extend the
  window or touch inventory. Retry with the same key after `Retry-After`.
- Every worker loop reclaims up to 10 idle pending events, then reads new work.
  `RECLAIM_IDLE_MS` defaults to 60000. Choose a value above normal batch time:
  a slow live worker can overlap with recovery. PostgreSQL constraints make
  duplicate order writes safe.
- Permanently failing or malformed events remain pending and retry. Retry limits
  and a dead-letter queue are future work.
- Orders start as `pending_payment`. No payment provider or payment transition
  endpoint is implemented. The schema supports future paid/cancelled statuses.

## Persistence and shutdown

Redis uses `redis.conf`: AOF enabled, fsync every second, and no eviction.
Stock, claims, replay records, streams and pending consumer state survive a
normal restart. Expiring keys still expire on schedule. Redis data lives in
`redis_data`; orders live in `postgres_data`.

```bash
docker compose restart redis
docker compose down
```

Normal shutdown retains both named volumes. Keep the same Compose project name
to reuse them. **`docker compose down -v` deletes their data.** A pre-persistence
container's data is not automatically migrated into the new volume.

Every-second fsync can lose roughly one second of writes in an unplanned crash.
Disk loss, Redis state loss and database/Redis reconciliation remain unresolved;
this is not a zero-loss or high-availability design.

## Tests

For host development, use Python 3.11+:

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
python -m pytest -q
```

Unit/API tests use fakeredis with Lua support. Real-service tests skip when
their dependencies are not configured. On macOS/Linux, run all checks with a
Redis 7.4+ executable and a disposable PostgreSQL database:

```bash
TEST_REDIS_SERVER=/absolute/path/to/redis-server \
TEST_DATABASE_URL='postgresql://user:password@localhost/test_database' \
  python -m pytest -q
```

Redis tests start private processes on Unix sockets and temporary directories;
they never flush development Redis. PostgreSQL tests create and remove isolated
random schemas. Coverage includes concurrency, replay, worker failures, AOF
restart, forced Redis exit after confirmed fsync, order lookup and metrics.
The fsynced crash test does not promise durability for unsynced writes.

**Verified locally: 40 tests passed with Redis 7.4.11 and PostgreSQL 18.3.**
Docker/Compose startup has not been run locally because no container runtime is
installed. The GitHub Actions workflow will build/start the Compose stack
(PostgreSQL 17), wait for readiness, and run a 1,000-request smoke benchmark on
push/PR. Its result is pending until that workflow executes.

For host-run services, set `REDIS_URL` and `DATABASE_URL`, then run
`uvicorn app.main:app` and `python -m app.worker` in separate terminals.
Use a unique `WORKER_NAME` per worker if overriding its generated name.

## Load testing

Use a fresh isolated sale, since the script deliberately never resets stock.
Do not run it against a shared or production endpoint.

Against a fresh running Compose stack:

```bash
python -m scripts.load_test --requests 1000 --concurrency 50 \
  --output benchmarks/my-run.json
```

Or let the local harness launch isolated Redis/API/worker processes and a
temporary database schema:

```bash
TEST_REDIS_SERVER=/absolute/path/to/redis-server \
TEST_DATABASE_URL='postgresql://user:password@localhost/test_database' \
  python -m scripts.benchmark_local --requests 5000 --concurrency 100 \
  --output benchmarks/my-isolated-run.json
```

The harness consumes its own demo stock and removes only its temporary
processes/schema. Choose a new report filename for each run. The load driver
uses bounded concurrency, unique users and unique keys, measures HTTP response
latency, polls all accepted orders, checks queue drain, and exits nonzero if
correctness checks fail.

See [measured results and methodology](benchmarks/README.md). These are laptop
baselines, not a capacity claim for a million concurrent buyers.

## Metrics

`GET /metrics` exposes request counts/latency, reservation outcomes, and queue
lag/pending gauges. Route labels use templates, not user IDs or UUIDs.
Counters are per API process; scrape each instance. Queue gauges represent
shared state and must not be summed across instances. They are omitted on
Redis failure instead of falsely reporting zero backlog.

See [metric definitions and PromQL queries](docs/metrics.md). The root
`prometheus.yml` is an optional scrape config; no Prometheus server is started
automatically.

## Scope and remaining limitations

This single-product, single-sale demo trusts caller-supplied user IDs and does
not authenticate order lookups. Changing user IDs bypasses per-user throttling;
the limiter is not an edge/DDoS protection layer. Fixed windows permit boundary
bursts, and reads are not rate limited.

Other future work: reservation expiry/compensation, payment simulation,
dead-letter handling, schema migrations, connection pooling, Redis Cluster key
layout, replication/backups, inventory reconciliation, authenticated identity,
dependency refresh, and multi-host load testing. A million requests over time
and a million simultaneous requests are different workloads; neither capacity
has been demonstrated here.

References: [Redis Lua](https://redis.io/docs/latest/develop/programmability/eval-intro/),
[consumer recovery](https://redis.io/docs/latest/commands/xautoclaim/),
[persistence tradeoffs](https://redis.io/docs/latest/operate/oss_and_stack/management/persistence/),
[Psycopg transactions](https://www.psycopg.org/psycopg3/docs/basic/usage.html).
