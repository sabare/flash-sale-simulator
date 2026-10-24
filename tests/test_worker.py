from uuid import uuid4

import fakeredis
import psycopg
import pytest
from redis.exceptions import ConnectionError

from app.inventory import RESERVATION_STREAM, RedisInventoryGateway
from app.worker import ORDER_GROUP, RECLAIM_IDLE_MS, OrderWorker


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


def age_pending(redis, consumer, ids):
    # Deterministically simulate idle time without sleeping or mocking recovery.
    redis.xclaim(RESERVATION_STREAM, ORDER_GROUP, consumer, 0, ids, idle=RECLAIM_IDLE_MS + 1000)


def test_other_worker_recovers_crashed_consumers_event(redis):
    reservation_id = enqueue(redis)
    store = RecordingStore()
    worker = OrderWorker(redis, store, "replacement")
    worker.initialize()
    batches = redis.xreadgroup(ORDER_GROUP, "crashed", {RESERVATION_STREAM: ">"})
    event_id = batches[0][1][0][0]

    # Fresh in-flight work should remain owned by its original consumer.
    assert worker.run_once(block_ms=None) == 0
    assert redis.xpending_range(RESERVATION_STREAM, ORDER_GROUP, "-", "+", 1)[0]["consumer"] == "crashed"
    age_pending(redis, "crashed", [event_id])
    assert worker.run_once(block_ms=None) == 1
    assert str(store.events[0].reservation_id) == reservation_id
    assert redis.xpending(RESERVATION_STREAM, ORDER_GROUP)["pending"] == 0


def test_database_failure_is_retried_after_idle_period(redis):
    class TransientStore(RecordingStore):
        fail = True

        def save(self, event):
            if self.fail:
                raise psycopg.OperationalError("Temporary database outage")
            super().save(event)

    enqueue(redis)
    store = TransientStore()
    worker = OrderWorker(redis, store, "worker")
    worker.initialize()
    assert worker.run_once(block_ms=None) == 0
    store.fail = False
    assert worker.run_once(block_ms=None) == 0
    event_id = redis.xrange(RESERVATION_STREAM)[0][0]
    age_pending(redis, "worker", [event_id])
    assert worker.run_once(block_ms=None) == 1
    assert len(store.events) == 1
    assert redis.xpending(RESERVATION_STREAM, ORDER_GROUP)["pending"] == 0


def test_recovery_scans_multiple_pages_and_keeps_processing_new_work(redis):
    store = RecordingStore()
    worker = OrderWorker(redis, store, "replacement")
    worker.initialize()
    ids = [redis.xadd(RESERVATION_STREAM, {"event_type": "unsupported"}) for _ in range(25)]
    redis.xreadgroup(ORDER_GROUP, "crashed", {RESERVATION_STREAM: ">"}, count=25)
    age_pending(redis, "crashed", ids)
    enqueue(redis)

    # Bad pending events don't starve the new valid reservation.
    assert worker.run_once(block_ms=None) == 1
    worker.run_once(block_ms=None)
    worker.run_once(block_ms=None)
    pending = redis.xpending_range(RESERVATION_STREAM, ORDER_GROUP, "-", "+", 30)
    assert len(pending) == 25
    assert all(item["consumer"] == "replacement" for item in pending)
    attempts = {item["message_id"]: item["times_delivered"] for item in pending}
    worker.run_once(block_ms=None)
    assert {
        item["message_id"]: item["times_delivered"]
        for item in redis.xpending_range(RESERVATION_STREAM, ORDER_GROUP, "-", "+", 30)
    } == attempts  # Claimed failures must wait again before another attempt.
