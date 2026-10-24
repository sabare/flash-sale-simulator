"""Execute the production Lua script against an in-memory Redis emulator."""

from concurrent.futures import ThreadPoolExecutor

import fakeredis
import pytest

from app.inventory import (
    InventoryUnavailableError,
    RedisInventoryGateway,
    RESERVATION_STREAM,
    ReservationOutcome,
)


@pytest.fixture
def inventory():
    with fakeredis.FakeRedis(decode_responses=True) as client:
        yield RedisInventoryGateway(client)


def test_reservation_queues_event_and_replay_does_not_duplicate_it(inventory):
    first = inventory.reserve("headphones", 2, "alice", "request-1", "reservation-1")
    replay = inventory.reserve("headphones", 2, "alice", "request-1", "unused-id")

    assert first.outcome is ReservationOutcome.RESERVED
    assert replay.outcome is ReservationOutcome.IDEMPOTENT_REPLAY
    assert replay.reservation_id == first.reservation_id == "reservation-1"
    assert replay.remaining_stock == first.remaining_stock == 1
    events = inventory.client.xrange(RESERVATION_STREAM)
    assert len(events) == 1
    assert events[0][1] == {
        "event_type": "reservation.created",
        "schema_version": "1",
        "reservation_id": "reservation-1",
        "product_id": "headphones",
        "user_id": "alice",
    }
    assert inventory.available_stock("headphones", 2) == 1


def test_duplicate_and_sold_out_attempts_do_not_enqueue(inventory):
    inventory.reserve("headphones", 1, "alice", "request-1", "reservation-1")
    duplicate = inventory.reserve("headphones", 1, "alice", "request-2", "unused-1")
    sold_out = inventory.reserve("headphones", 1, "bob", "request-3", "unused-2")

    assert duplicate.outcome is ReservationOutcome.ALREADY_RESERVED
    assert sold_out.outcome is ReservationOutcome.SOLD_OUT
    assert inventory.client.xlen(RESERVATION_STREAM) == 1
    assert inventory.available_stock("headphones", 1) == 0


def test_invalid_queue_does_not_consume_stock_or_claim_user(inventory):
    inventory.client.set(RESERVATION_STREAM, "wrong-type")
    with pytest.raises(InventoryUnavailableError):
        inventory.reserve("headphones", 1, "alice", "request-1", "reservation-1")
    assert inventory.available_stock("headphones", 1) == 1

    inventory.client.delete(RESERVATION_STREAM)
    result = inventory.reserve("headphones", 1, "alice", "request-1", "reservation-1")
    assert result.outcome is ReservationOutcome.RESERVED
    assert inventory.client.xlen(RESERVATION_STREAM) == 1


def test_concurrent_attempts_queue_exactly_available_stock(inventory):
    def reserve(index):
        return inventory.reserve("headphones", 10, f"user-{index}", "buy", f"id-{index}")

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(reserve, range(40)))

    assert sum(r.outcome is ReservationOutcome.RESERVED for r in results) == 10
    assert sum(r.outcome is ReservationOutcome.SOLD_OUT for r in results) == 30
    assert inventory.available_stock("headphones", 10) == 0
    assert inventory.client.xlen(RESERVATION_STREAM) == 10
