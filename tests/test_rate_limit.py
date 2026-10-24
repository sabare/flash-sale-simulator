from concurrent.futures import ThreadPoolExecutor

import fakeredis
import pytest
from fastapi.testclient import TestClient

from app.inventory import RESERVATION_STREAM, RedisInventoryGateway, get_inventory_gateway
from app.main import app
from app.rate_limit import RedisRateLimiter, get_rate_limiter


@pytest.fixture
def redis():
    with fakeredis.FakeRedis(server=fakeredis.FakeServer(), decode_responses=True) as client:
        yield client


def test_limit_is_shared_across_instances_under_concurrency(redis):
    limiters = [RedisRateLimiter(redis) for _ in range(4)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda i: limiters[i % 4].check("alice"), range(40)))
    assert results.count(0) == 10
    assert all(1 <= result <= 60 for result in results if result)
    assert limiters[0].check("bob") == 0


def test_expiring_window_allows_new_attempt_without_extending_on_rejection(redis):
    limiter = RedisRateLimiter(redis, limit=1)
    assert limiter.check("alice") == 0
    key = redis.keys("flash-sale:rate-limit:*")[0]
    redis.pexpire(key, 1500)
    assert 1 <= limiter.check("alice") <= 2
    assert redis.pttl(key) <= 1500
    redis.pexpire(key, -1)  # Expire the test window without waiting a minute.
    assert limiter.check("alice") == 0
    assert redis.pttl(key) > 0


@pytest.fixture
def api(redis):
    app.dependency_overrides[get_inventory_gateway] = lambda: RedisInventoryGateway(redis)
    app.dependency_overrides[get_rate_limiter] = lambda: RedisRateLimiter(redis, limit=2)
    try:
        with TestClient(app) as client:
            yield client
    finally:
        app.dependency_overrides.clear()


def test_throttled_retry_preserves_stock_and_queue(api, redis):
    url = "/sales/limited-headphones/reservations"
    request = {"json": {"user_id": "alice"}, "headers": {"Idempotency-Key": "buy"}}
    first = api.post(url, **request)
    replay = api.post(url, **request)
    rejected = api.post(url, **request)
    assert first.status_code == replay.status_code == 201
    assert first.json() == replay.json()
    assert rejected.status_code == 429
    assert 1 <= int(rejected.headers["Retry-After"]) <= 60
    assert api.get("/products/limited-headphones").json()["available_stock"] == 99
    assert redis.xlen(RESERVATION_STREAM) == 1
    assert api.get("/health").status_code == 200


def test_rate_limit_outage_fails_closed(api, redis):
    redis.connection_pool.connection_kwargs["server"].connected = False
    response = api.post(
        "/sales/limited-headphones/reservations",
        json={"user_id": "alice"}, headers={"Idempotency-Key": "buy"},
    )
    assert response.status_code == 503
    assert response.json() == {"detail": "Rate limit service unavailable"}
    redis.connection_pool.connection_kwargs["server"].connected = True
    assert redis.xlen(RESERVATION_STREAM) == 0
