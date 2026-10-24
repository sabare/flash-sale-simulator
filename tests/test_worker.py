from uuid import uuid4

import fakeredis
import psycopg
import pytest
from redis.exceptions import ConnectionError

from app.inventory import RESERVATION_STREAM, RedisInventoryGateway
from app.worker import ORDER_GROUP, OrderWorker


@pytest.fixture
def redis():
    with fakeredis.FakeRedis(decode_responses=True) as client:
        yield client


class RecordingStore:
    def __init__(self):
        self.events = []

    def save(self, event):
        self.events.append(event)


def enqueue(redis):
    reservation_id = str(uuid4())
    RedisInventoryGateway(redis).reserve("headphones", 100, "alice", "buy", reservation_id)
    return reservation_id


def test_worker_consumes_preexisting_reservation_and_acknowledges(redis):
    reservation_id = enqueue(redis)
    store = RecordingStore()
    worker = OrderWorker(redis, store, "test-worker")
    worker.initialize()
    worker.initialize()  # Existing group must not reset delivery position.

    assert worker.run_once(block_ms=None) == 1
    assert str(store.events[0].reservation_id) == reservation_id
    assert store.events[0].user_id == "alice"
    assert redis.xpending(RESERVATION_STREAM, ORDER_GROUP)["pending"] == 0
    assert worker.run_once(block_ms=None) == 0


def test_database_failure_leaves_event_pending(redis):
    class FailingStore:
        def save(self, event):
            raise psycopg.OperationalError("Database unavailable")

    enqueue(redis)
    worker = OrderWorker(redis, FailingStore(), "test-worker")
    worker.initialize()
    assert worker.run_once(block_ms=None) == 0
    assert redis.xpending(RESERVATION_STREAM, ORDER_GROUP)["pending"] == 1


def test_ack_failure_occurs_after_save_and_leaves_event_pending(redis, monkeypatch):
    enqueue(redis)
    store = RecordingStore()
    worker = OrderWorker(redis, store, "test-worker")
    worker.initialize()

    def fail_ack(*args):
        assert len(store.events) == 1
        raise ConnectionError("Ack connection lost")

    monkeypatch.setattr(redis, "xack", fail_ack)
    assert worker.run_once(block_ms=None) == 0
    assert len(store.events) == 1
    assert redis.xpending(RESERVATION_STREAM, ORDER_GROUP)["pending"] == 1


def test_invalid_event_stays_pending_without_blocking_valid_event(redis):
    redis.xadd(RESERVATION_STREAM, {"event_type": "unsupported"})
    enqueue(redis)
    store = RecordingStore()
    worker = OrderWorker(redis, store, "test-worker")
    worker.initialize()
    assert worker.run_once(block_ms=None) == 1
    assert len(store.events) == 1
    assert redis.xpending(RESERVATION_STREAM, ORDER_GROUP)["pending"] == 1
