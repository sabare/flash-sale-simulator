"""Opt-in real PostgreSQL tests; each test owns a disposable isolated schema."""

from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import fakeredis
import psycopg
import pytest
from fastapi.testclient import TestClient

from app.inventory import RESERVATION_STREAM, RedisInventoryGateway
from app.orders import ReservationEvent
from app.main import app, get_order_store
from app.worker import ORDER_GROUP, RECLAIM_IDLE_MS, OrderWorker


def test_concurrent_redelivery_creates_one_durable_order(store):
    event = ReservationEvent(uuid4(), "headphones", "alice")
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(store.save, [event] * 8))
    with psycopg.connect(store.database_url) as connection:
        rows = connection.execute(
            "SELECT reservation_id, product_id, user_id, status, created_at FROM orders"
        ).fetchall()
    assert len(rows) == 1
    assert rows[0][:4] == (event.reservation_id, "headphones", "alice", "pending_payment")
    assert rows[0][4] is not None


def test_conflicting_reservation_is_rejected_without_changing_order(store):
    event = ReservationEvent(uuid4(), "headphones", "alice")
    store.save(event)
    with pytest.raises(ValueError):
        store.save(ReservationEvent(event.reservation_id, "headphones", "bob"))
    with pytest.raises(ValueError):
        store.save(ReservationEvent(uuid4(), "headphones", "alice"))
    with psycopg.connect(store.database_url) as connection:
        assert connection.execute("SELECT user_id FROM orders").fetchall() == [("alice",)]


def test_order_endpoint_reads_worker_saved_order(store):
    reservation_id = uuid4()
    app.dependency_overrides[get_order_store] = lambda: store
    try:
        with fakeredis.FakeRedis(decode_responses=True) as redis, TestClient(app) as client:
            assert client.get(f"/orders/{reservation_id}").status_code == 404
            RedisInventoryGateway(redis).reserve("headphones", 1, "alice", "buy", str(reservation_id))
            worker = OrderWorker(redis, store, "test-worker")
            worker.initialize()
            assert worker.run_once(block_ms=None) == 1
            response = client.get(f"/orders/{reservation_id}")
            assert response.status_code == 200
            assert response.json()["reservation_id"] == str(reservation_id)
            assert response.json()["status"] == "pending_payment"
            assert response.json()["user_id"] == "alice"
            assert response.json()["created_at"]
            with psycopg.connect(store.database_url) as connection:
                connection.execute("UPDATE orders SET status = 'paid' WHERE reservation_id = %s", (reservation_id,))
            assert client.get(f"/orders/{reservation_id}").json()["status"] == "paid"
    finally:
        app.dependency_overrides.pop(get_order_store, None)


def test_committed_order_can_be_redelivered_then_acknowledged(store):
    with fakeredis.FakeRedis(decode_responses=True) as redis:
        reservation_id = uuid4()
        RedisInventoryGateway(redis).reserve("headphones", 1, "alice", "buy", str(reservation_id))
        worker = OrderWorker(redis, store, "test-worker")
        worker.initialize()
        batches = redis.xreadgroup(ORDER_GROUP, "test-worker", {RESERVATION_STREAM: ">"})
        event_id, fields = batches[0][1][0]
        # Simulate a worker crash after committing but before XACK.
        store.save(ReservationEvent.from_fields(fields))
        assert redis.xpending(RESERVATION_STREAM, ORDER_GROUP)["pending"] == 1
        redis.xclaim(
            RESERVATION_STREAM, ORDER_GROUP, "test-worker", 0, [event_id],
            idle=RECLAIM_IDLE_MS + 1000,
        )
        replacement = OrderWorker(redis, store, "replacement-worker")
        replacement.initialize()
        assert replacement.run_once(block_ms=None) == 1
        assert redis.xpending(RESERVATION_STREAM, ORDER_GROUP)["pending"] == 0
        with psycopg.connect(store.database_url) as connection:
            assert connection.execute("SELECT reservation_id FROM orders").fetchall() == [(reservation_id,)]
