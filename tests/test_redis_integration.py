"""Real Redis Lua, persistence and worker-to-PostgreSQL integration checks."""

from concurrent.futures import ThreadPoolExecutor
from uuid import UUID, uuid4

from fastapi.testclient import TestClient

from app.inventory import RESERVATION_STREAM, RedisInventoryGateway, ReservationOutcome, get_inventory_gateway
from app.main import app, get_order_store
from app.orders import ReservationEvent
from app.rate_limit import RedisRateLimiter, get_rate_limiter
from app.worker import ORDER_GROUP, RECLAIM_IDLE_MS, OrderWorker


def test_real_lua_concurrent_reservations_and_replay(real_redis):
    inventory = RedisInventoryGateway(real_redis.client)
    def reserve(index):
        return inventory.reserve("headphones", 10, f"user-{index}", "buy", str(uuid4()))
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(reserve, range(40)))
    assert sum(r.outcome is ReservationOutcome.RESERVED for r in results) == 10
    assert sum(r.outcome is ReservationOutcome.SOLD_OUT for r in results) == 30
    assert inventory.available_stock("headphones", 10) == 0
    events = real_redis.client.xrange(RESERVATION_STREAM)
    assert len(events) == 10
    winner = events[0][1]
    replay = inventory.reserve("headphones", 10, winner["user_id"], "buy", "unused")
    assert replay.outcome is ReservationOutcome.IDEMPOTENT_REPLAY
    assert replay.reservation_id == winner["reservation_id"]
    assert real_redis.client.xlen(RESERVATION_STREAM) == 10


def test_real_rate_limit_is_shared_under_concurrency(real_redis):
    limiters = [RedisRateLimiter(real_redis.client) for _ in range(4)]
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda i: limiters[i % 4].check("alice"), range(40)))
    assert results.count(0) == 10
    assert all(1 <= result <= 60 for result in results if result)


def test_aof_restart_preserves_inventory_claims_replay_and_pending_events(real_redis):
    client = real_redis.client
    inventory = RedisInventoryGateway(client)
    original = inventory.reserve("headphones", 2, "alice", "buy", str(uuid4()))
    limiter = RedisRateLimiter(client, limit=1)
    assert limiter.check("alice") == 0
    client.xgroup_create(RESERVATION_STREAM, ORDER_GROUP, id="0")
    client.xreadgroup(ORDER_GROUP, "crashed-worker", {RESERVATION_STREAM: ">"})
    real_redis.restart()
    assert inventory.available_stock("headphones", 2) == 1
    assert inventory.reserve("headphones", 2, "alice", "buy", "unused").reservation_id == original.reservation_id
    assert inventory.reserve("headphones", 2, "alice", "other", "unused").outcome is ReservationOutcome.ALREADY_RESERVED
    assert limiter.check("alice") > 0
    assert client.xlen(RESERVATION_STREAM) == 1
    assert client.xpending(RESERVATION_STREAM, ORDER_GROUP)["pending"] == 1
    assert client.config_get("appendonly") == {"appendonly": "yes"}


def test_fsynced_reservation_survives_abrupt_process_exit(real_redis):
    inventory = RedisInventoryGateway(real_redis.client)
    original = inventory.reserve("headphones", 2, "alice", "buy", str(uuid4()))
    # Explicitly wait for this connection's writes to be durable before SIGKILL.
    # This does NOT claim everysec persistence has a zero-loss crash window.
    assert real_redis.client.execute_command("WAITAOF", 1, 0, 5000)[0] == 1
    real_redis.restart(abrupt=True)
    assert inventory.available_stock("headphones", 2) == 1
    assert inventory.reserve("headphones", 2, "alice", "buy", "unused").reservation_id == original.reservation_id
    assert real_redis.client.xlen(RESERVATION_STREAM) == 1


def test_api_to_real_redis_and_postgres_recovers_after_restart(real_redis, store):
    redis = real_redis.client
    app.dependency_overrides[get_inventory_gateway] = lambda: RedisInventoryGateway(redis)
    app.dependency_overrides[get_rate_limiter] = lambda: RedisRateLimiter(redis)
    app.dependency_overrides[get_order_store] = lambda: store
    try:
        with TestClient(app) as api:
            response = api.post(
                "/sales/limited-headphones/reservations", json={"user_id": "alice"},
                headers={"Idempotency-Key": "buy"},
            )
            assert response.status_code == 201
            reservation_id = response.json()["reservation_id"]
            worker = OrderWorker(redis, store, "crashed-worker")
            worker.initialize()
            batches = redis.xreadgroup(ORDER_GROUP, "crashed-worker", {RESERVATION_STREAM: ">"})
            event_id, fields = batches[0][1][0]
            store.save(ReservationEvent.from_fields(fields))  # Crash before XACK.
            created_at = store.get(UUID(reservation_id)).created_at
            real_redis.restart()
            redis.xclaim(RESERVATION_STREAM, ORDER_GROUP, "crashed-worker", 0, [event_id], idle=RECLAIM_IDLE_MS + 1000)
            replacement = OrderWorker(redis, store, "replacement")
            replacement.initialize()
            assert replacement.run_once(block_ms=None) == 1
            assert redis.xpending(RESERVATION_STREAM, ORDER_GROUP)["pending"] == 0
            assert store.get(UUID(reservation_id)).created_at == created_at
            assert api.get(f"/orders/{reservation_id}").json()["status"] == "pending_payment"
    finally:
        app.dependency_overrides.clear()
