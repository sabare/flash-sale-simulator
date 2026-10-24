import fakeredis
import pytest
from fastapi.testclient import TestClient
from prometheus_client.parser import text_string_to_metric_families

from app import main, metrics
from app.inventory import RedisInventoryGateway, RESERVATION_STREAM, get_inventory_gateway
from app.rate_limit import RedisRateLimiter, get_rate_limiter
from app.worker import ORDER_GROUP


def samples(response):
    assert response.status_code == 200
    return [sample for family in text_string_to_metric_families(response.text) for sample in family.samples]


@pytest.fixture
def monitored(monkeypatch):
    server = fakeredis.FakeServer()
    with fakeredis.FakeRedis(server=server, decode_responses=True) as redis:
        monkeypatch.setattr(metrics, "get_redis_client", lambda: redis)
        monkeypatch.setattr(main, "get_redis_client", lambda: redis)
        main.app.dependency_overrides[get_inventory_gateway] = lambda: RedisInventoryGateway(redis)
        main.app.dependency_overrides[get_rate_limiter] = lambda: RedisRateLimiter(redis)
        try:
            with TestClient(main.app) as client:
                yield client, redis, server
        finally:
            main.app.dependency_overrides.clear()


def test_scrape_distinguishes_lag_pending_and_retained_events(monitored):
    client, redis, _ = monitored
    def gauges():
        return {s.name: s.value for s in samples(client.get("/metrics")) if s.name.startswith("flash_sale_queue_")}
    assert gauges()["flash_sale_queue_lag"] == 0
    redis.xadd(RESERVATION_STREAM, {"payload": "test"})
    assert gauges()["flash_sale_queue_lag"] == 1
    redis.xgroup_create(RESERVATION_STREAM, ORDER_GROUP, id="0")
    batches = redis.xreadgroup(ORDER_GROUP, "test", {RESERVATION_STREAM: ">"})
    assert gauges()["flash_sale_queue_pending"] == 1
    assert gauges()["flash_sale_queue_lag"] == 0
    redis.xack(RESERVATION_STREAM, ORDER_GROUP, batches[0][1][0][0])
    assert gauges()["flash_sale_queue_pending"] == 0
    assert gauges()["flash_sale_queue_events"] == 1


def test_failed_queue_scrape_does_not_report_false_zero_backlog(monitored):
    client, _, server = monitored
    server.connected = False
    gauges = {s.name: s.value for s in samples(client.get("/metrics"))}
    assert gauges["flash_sale_queue_redis_up"] == 0
    assert "flash_sale_queue_pending" not in gauges
    assert "flash_sale_queue_lag" not in gauges


def test_http_and_reservation_metrics_use_bounded_labels(monitored):
    client, _, _ = monitored
    def count(outcome):
        return metrics.REGISTRY.get_sample_value("flash_sale_reservation_attempts_total", {"outcome": outcome}) or 0
    before_reserved, before_replay = count("reserved"), count("idempotent_replay")
    request = {"json": {"user_id": "metrics-user"}, "headers": {"Idempotency-Key": "metrics-buy"}}
    for _ in range(2):
        assert client.post("/sales/limited-headphones/reservations", **request).status_code == 201
    client.get("/random-unmatched-path")
    assert count("reserved") - before_reserved == 1
    assert count("idempotent_replay") - before_replay == 1
    http_samples = [s for s in samples(client.get("/metrics")) if s.name == "flash_sale_http_requests_total"]
    assert any(s.labels["route"] == "/sales/{product_id}/reservations" and s.labels["status"] == "201" for s in http_samples)
    assert any(s.labels["route"] == "unmatched" and s.labels["status"] == "404" for s in http_samples)
    assert all("limited-headphones" not in str(s.labels) and "metrics-user" not in str(s.labels) for s in http_samples)


def test_readiness_fails_when_database_configuration_missing(monitored, monkeypatch):
    client, _, _ = monitored
    monkeypatch.delenv("DATABASE_URL", raising=False)
    assert client.get("/health").status_code == 200
    assert client.get("/ready").status_code == 503
