"""Opt-in real PostgreSQL tests; each test owns a disposable isolated schema."""

import os
from concurrent.futures import ThreadPoolExecutor
from uuid import uuid4

import fakeredis
import psycopg
from psycopg import sql
from psycopg.conninfo import make_conninfo
import pytest

from app.inventory import RESERVATION_STREAM, RedisInventoryGateway
from app.orders import PostgresOrderStore, ReservationEvent
from app.worker import ORDER_GROUP, RECLAIM_IDLE_MS, OrderWorker


@pytest.fixture
def store():
    database_url = os.getenv("TEST_DATABASE_URL")
    if not database_url:
        pytest.skip("Set TEST_DATABASE_URL to run real PostgreSQL tests")
    schema = f"test_orders_{uuid4().hex}"
    with psycopg.connect(database_url, autocommit=True) as connection:
        connection.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        try:
            store = PostgresOrderStore(make_conninfo(database_url, options=f"-c search_path={schema}"))
            store.initialize()
            yield store
        finally:
            connection.execute(sql.SQL("DROP SCHEMA {} CASCADE").format(sql.Identifier(schema)))


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
