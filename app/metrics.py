"""Process-local counters plus shared Redis queue gauges at scrape time."""

from time import perf_counter

from prometheus_client import CollectorRegistry, Counter, Histogram
from prometheus_client.core import GaugeMetricFamily
from redis.exceptions import RedisError

from app.inventory import RESERVATION_STREAM, get_redis_client


REGISTRY = CollectorRegistry()
REQUESTS = Counter("flash_sale_http_requests", "Completed HTTP requests", ["method", "route", "status"], registry=REGISTRY)
LATENCY = Histogram("flash_sale_http_request_duration_seconds", "HTTP request duration", ["method", "route"], registry=REGISTRY)
RESERVATIONS = Counter("flash_sale_reservation_attempts", "Reservation outcomes", ["outcome"], registry=REGISTRY)


class QueueCollector:
    def collect(self):
        # Do not initialize inventory or create a consumer group during scrapes.
        up, group_exists = 0, 0
        length = pending = lag = None
        try:
            client = get_redis_client()
            length = client.xlen(RESERVATION_STREAM)
            groups = client.xinfo_groups(RESERVATION_STREAM) if length or client.exists(RESERVATION_STREAM) else []
            pending, lag = 0, length
            for group in groups:
                if group["name"] == "order-workers":
                    group_exists = 1
                    pending, lag = group["pending"], group.get("lag")
                    break
            up = 1
        except RedisError:
            # Omit queue gauges on failure rather than exporting a misleading 0.
            length = pending = lag = None
        yield GaugeMetricFamily("flash_sale_queue_redis_up", "Queue scrape could reach Redis", value=up)
        if up:
            yield GaugeMetricFamily("flash_sale_queue_group_exists", "Order consumer group exists", value=group_exists)
        for name, help_text, value in (
            ("events", "Retained stream entries, including acknowledged events", length),
            ("pending", "Delivered but unacknowledged events", pending),
            ("lag", "Events not yet delivered to the order group", lag),
        ):
            if value is not None:
                yield GaugeMetricFamily(f"flash_sale_queue_{name}", help_text, value=value)


REGISTRY.register(QueueCollector())


class MetricsMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] in {"/metrics", "/health", "/ready"}:
            return await self.app(scope, receive, send)
        started, status = perf_counter(), 500

        async def track_send(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, track_send)
        finally:
            # Route templates avoid one metric series per user/product/UUID.
            route = getattr(scope.get("route"), "path", "unmatched")
            method = scope["method"] if scope["method"] in {"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"} else "OTHER"
            REQUESTS.labels(method, route, str(status)).inc()
            LATENCY.labels(method, route).observe(perf_counter() - started)
