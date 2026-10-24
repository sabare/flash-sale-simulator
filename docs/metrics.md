# Metrics

`GET /metrics` serves Prometheus text. `GET /health` is process liveness;
`GET /ready` checks Redis, PostgreSQL and the orders table, not whether a worker
is actively processing messages. Health/readiness/metrics requests are excluded
from HTTP traffic counters.

| Metric | Meaning |
| --- | --- |
| `flash_sale_http_requests_total{method,route,status}` | Completed requests, including HTTP errors |
| `flash_sale_http_request_duration_seconds` | Request latency histogram in seconds |
| `flash_sale_reservation_attempts_total{outcome}` | Reserved, replayed, sold-out, duplicate, throttled or unavailable attempts |
| `flash_sale_queue_pending` | Delivered events still waiting for acknowledgment |
| `flash_sale_queue_lag` | Events not yet delivered to the order consumer group |
| `flash_sale_queue_events` | All retained stream events, including processed ones; **not backlog** |
| `flash_sale_queue_redis_up` | 1 if the shared queue can be inspected, otherwise 0 |
| `flash_sale_queue_group_exists` | Whether the order consumer group has been created |

Reservation counters begin after request validation and product lookup. Invalid
requests are represented in HTTP counters. Replays are counted separately from
new reservations. Queue gauges are read on scrape, not per purchase; they are
omitted when Redis is unavailable, rather than showing a false zero backlog.
If Redis cannot compute consumer-group lag, that gauge is omitted as unknown.

Route labels use templates such as `/orders/{reservation_id}`; arbitrary paths
share `unmatched`. User IDs, reservation UUIDs and product values are not labels.
Counters reset on process restart and are local to one API process. Run one
Uvicorn process per container and scrape each instance. Queue gauges describe
shared state: use `max`, not `sum`, when scraping multiple API instances.

## PromQL examples

```promql
# Total API request rate (requests/second)
sum(rate(flash_sale_http_requests_total[1m]))

# Reservation endpoint p95 latency (seconds)
histogram_quantile(0.95, sum by (le) (
  rate(flash_sale_http_request_duration_seconds_bucket{route="/sales/{product_id}/reservations"}[5m])
))

# Server-side errors, excluding expected 409/429 responses
sum(rate(flash_sale_http_requests_total{status=~"5.."}[1m]))

# Newly accepted reservations, excluding idempotent replays
sum(rate(flash_sale_reservation_attempts_total{outcome="reserved"}[1m]))

# Undelivered plus delivered-but-unacknowledged events
max(flash_sale_queue_lag) + max(flash_sale_queue_pending)
```

The root `prometheus.yml` is an optional scrape configuration for a Prometheus
server attached to the Compose network; its target is `api:8000`. Prometheus is
not a required service and is not started automatically. For a host-run
Prometheus, replace that target with `127.0.0.1:8000`.

This is operational instrumentation, not a durable audit log. No authentication
protects the demo metrics endpoint; restrict its network access when deploying.
Implementation reference: [Prometheus Python instrumentation](https://prometheus.github.io/client_python/instrumenting/).
