"""Bounded-concurrency HTTP benchmark against a fresh, isolated sale."""

import argparse
import asyncio
from collections import Counter
from datetime import datetime, timezone
import json
import math
from pathlib import Path
import platform
from time import perf_counter
from uuid import uuid4

import httpx
from prometheus_client.parser import text_string_to_metric_families


def percentile(values, percent):
    if not values:
        return None
    ordered = sorted(values)
    return round(ordered[max(0, math.ceil(len(ordered) * percent / 100) - 1)], 3)


async def benchmark(base_url, requests=1000, concurrency=50, stock=100, order_timeout=30):
    if requests < stock or concurrency < 1 or stock < 1 or order_timeout <= 0:
        raise ValueError("Use requests >= stock > 0, concurrency > 0, and order_timeout > 0")
    run_id = uuid4().hex
    product = "limited-headphones"
    limits = httpx.Limits(max_connections=concurrency, max_keepalive_connections=concurrency)
    async with httpx.AsyncClient(base_url=base_url, timeout=15, limits=limits) as client:
        ready = await client.get("/ready")
        ready.raise_for_status()
        initial = await client.get(f"/products/{product}")
        initial.raise_for_status()
        if initial.json()["available_stock"] != stock:
            raise ValueError("Sale is not fresh; use isolated services. This script never resets stock.")
        indices = iter(range(requests))
        outcomes, latencies, accepted_latencies, sold_out_latencies = Counter(), [], [], []
        accepted = []

        async def buyer():
            for index in indices:
                user_id = f"load-{run_id}-{index}"
                started = perf_counter()
                try:
                    response = await client.post(
                        f"/sales/{product}/reservations", json={"user_id": user_id},
                        headers={"Idempotency-Key": f"{run_id}-{index}"},
                    )
                    elapsed = (perf_counter() - started) * 1000
                    latencies.append(elapsed)
                    if response.status_code == 201:
                        data = response.json()
                        if data.get("user_id") != user_id or data.get("product_id") != product:
                            outcomes["invalid_success"] += 1
                            continue
                        outcomes["201"] += 1
                        accepted.append((data["reservation_id"], user_id))
                        accepted_latencies.append(elapsed)
                    elif response.status_code == 409 and response.json().get("detail") == "Product is sold out":
                        outcomes["409_sold_out"] += 1
                        sold_out_latencies.append(elapsed)
                    else:
                        outcomes[f"unexpected_{response.status_code}"] += 1
                except (httpx.HTTPError, ValueError, KeyError):
                    outcomes["request_or_response_error"] += 1

        started = perf_counter()
        await asyncio.gather(*(buyer() for _ in range(min(concurrency, requests))))
        duration = perf_counter() - started
        remaining = await client.get(f"/products/{product}")
        remaining.raise_for_status()
        # Poll each accepted reservation, checking identity as well as status.
        awaiting = dict(accepted)
        persisted = set()
        deadline = perf_counter() + order_timeout
        while awaiting and perf_counter() < deadline:
            for offset in range(0, len(awaiting), 20):
                batch = list(awaiting.items())[offset:offset + 20]
                responses = await asyncio.gather(
                    *(client.get(f"/orders/{reservation_id}") for reservation_id, _ in batch),
                    return_exceptions=True,
                )
                for (reservation_id, user_id), response in zip(batch, responses):
                    if isinstance(response, httpx.Response) and response.status_code == 200:
                        data = response.json()
                        if (data.get("reservation_id") == reservation_id and data.get("user_id") == user_id
                                and data.get("product_id") == product and data.get("status") == "pending_payment"):
                            persisted.add(reservation_id)
            awaiting = {key: value for key, value in awaiting.items() if key not in persisted}
            if awaiting:
                await asyncio.sleep(0.2)
        order_wait = perf_counter() - (deadline - order_timeout)
        # A database commit may precede XACK briefly; allow the queue to settle.
        queue = {}
        while perf_counter() < deadline:
            scrape = await client.get("/metrics")
            scrape.raise_for_status()
            queue = {
                sample.name: sample.value
                for family in text_string_to_metric_families(scrape.text)
                for sample in family.samples if sample.name.startswith("flash_sale_queue_")
            }
            if queue.get("flash_sale_queue_pending") == queue.get("flash_sale_queue_lag") == 0:
                break
            await asyncio.sleep(0.2)
        checks = {
            "exactly_stock_accepted": outcomes["201"] == stock,
            "only_expected_responses": outcomes["201"] + outcomes["409_sold_out"] == requests,
            "unique_reservations": len({key for key, _ in accepted}) == len(accepted),
            "stock_is_zero": remaining.json()["available_stock"] == 0,
            "all_orders_persisted": len(persisted) == stock,
            "queue_drained": queue.get("flash_sale_queue_pending") == queue.get("flash_sale_queue_lag") == 0,
            "queue_scrape_healthy": queue.get("flash_sale_queue_redis_up") == queue.get("flash_sale_queue_group_exists") == 1,
        }
        return {
            "timestamp_utc": datetime.now(timezone.utc).isoformat(),
            "run_id": run_id, "python": platform.python_version(), "platform": platform.platform(),
            "workload": {"requests": requests, "concurrency": concurrency, "initial_stock": stock,
                         "pattern": "closed-loop, unique user and idempotency key per request"},
            "duration_seconds": round(duration, 3), "requests_per_second": round(requests / duration, 2),
            "outcomes": dict(outcomes),
            "latency_ms": {"p50": percentile(latencies, 50), "p95": percentile(latencies, 95), "p99": percentile(latencies, 99)},
            "accepted_latency_ms": {"p50": percentile(accepted_latencies, 50), "p95": percentile(accepted_latencies, 95)},
            "sold_out_latency_ms": {"p50": percentile(sold_out_latencies, 50), "p95": percentile(sold_out_latencies, 95)},
            "persisted_orders": len(persisted), "order_poll_seconds": round(order_wait, 3),
            "queue": queue, "checks": checks, "passed": all(checks.values()),
        }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-url", default="http://127.0.0.1:8000")
    parser.add_argument("--requests", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=50)
    parser.add_argument("--stock", type=int, default=100)
    parser.add_argument("--order-timeout", type=float, default=30)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = asyncio.run(benchmark(args.base_url, args.requests, args.concurrency, args.stock, args.order_timeout))
    rendered = json.dumps(report, indent=2)
    print(rendered)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        with args.output.open("x") as output:
            output.write(rendered + "\n")
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
