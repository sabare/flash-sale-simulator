# Measured local baseline

Measured on October 24, 2026. Each row is a separate fresh sale with 100 units,
one API process, one worker, Redis 7.4.11 and PostgreSQL 18.3. The Python 3.14.3
load generator and all services shared a macOS 26.3 x86_64 host with 12 logical
CPUs and 16 GiB RAM. Redis used AOF with every-second fsync. API access logging
was disabled; metrics instrumentation remained enabled.

| Requests | Concurrent clients | Requests/s | HTTP p50 | HTTP p95 | HTTP p99 | Accepted / sold out | Stored orders |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 5,000 | 20 | 263.63 | 36.22 ms | 240.40 ms | 400.99 ms | 100 / 4,900 | 100 |
| 5,000 | 100 | 201.34 | 335.86 ms | 1,447.03 ms | 2,394.39 ms | 100 / 4,900 | 100 |

Both runs ended with stock at zero, 100 unique reservation IDs, exactly 100
database rows, zero undelivered events, zero pending acknowledgments, and no
unexpected HTTP statuses or request errors. All automated correctness checks
passed. The retained stream still contained 100 events, which is not backlog.

Raw reports:
- [20 clients](local-5000-c20.json)
- [100 clients](local-5000-c100.json)

## Method and limits

These are closed-loop runs: each concurrent client sends its next request after
the previous response. Every attempt has a unique user/key, so per-user rate
limiting does not dominate this workload. Each case is one measured run with
no warm-up; it is not a repeated statistical estimate. HTTP latency percentiles
use the nearest-rank method and include client/network time. They do not include
waiting for an available client slot and are not end-to-end order latency.

Only 100 of 5,000 attempts are accepted, so aggregate throughput is dominated
by sold-out rejection throughput. Reports include separate accepted/rejected
latencies. After the HTTP phase, the script polls accepted IDs and verifies
queue drain. `order_poll_seconds` measures that verification phase, not the
time from purchase to durable order creation.

The higher-concurrency run has worse throughput and tail latency. This is a
reason to profile the shared-host setup and API/driver overhead, not evidence
that Redis itself is the bottleneck. No maximum capacity, production SLO,
distributed arrival rate, or million-request concurrency claim follows from
these measurements. Container runs may differ (Python 3.13/PostgreSQL 17).

## Reproduce

From the repository after installing `requirements-dev.txt`, provide a Redis
7.4+ executable and an existing disposable PostgreSQL database:

```bash
TEST_REDIS_SERVER=/absolute/path/to/redis-server \
TEST_DATABASE_URL='postgresql://user:password@localhost/test_database' \
  python -m scripts.benchmark_local --requests 5000 --concurrency 20 \
  --output benchmarks/my-c20-run.json
```

Repeat with `--concurrency 100` and a different output path. Each invocation
creates its own Redis data directory and PostgreSQL schema, launches real API
and worker subprocesses, checks readiness, runs the workload, and cleans up only
its own resources. It never resets the development sale. The harness additionally
checks the database row count, beyond HTTP order polling.

For a fresh Compose sale, use `python -m scripts.load_test --requests 1000
--concurrency 50 --output benchmarks/my-compose-run.json` on one line. The
driver refuses to run when current stock differs from `--stock` (default 100),
and never resets data. A stock check is not a substitute for isolating the target
from other traffic.

An honest resume bullet for this version:

> Built a flash-sale backend using atomic Redis Lua reservations, Redis Streams
> recovery and idempotent PostgreSQL order writes; verified two 5,000-request
> load scenarios with exactly 100 orders from 100 units and no observed overselling.
