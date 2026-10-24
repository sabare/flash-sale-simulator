"""Run with python -m app.worker. Retry idle pending events before new work."""

import logging
import os
import signal
from threading import Event
from typing import Protocol
from uuid import uuid4

import psycopg
from redis import Redis
from redis.exceptions import RedisError, ResponseError

from app.inventory import RESERVATION_STREAM
from app.orders import PostgresOrderStore, ReservationEvent


ORDER_GROUP = "order-workers"
RECLAIM_IDLE_MS = 60_000
logger = logging.getLogger(__name__)


class OrderStore(Protocol):
    def save(self, event: ReservationEvent) -> None: ...


class OrderWorker:
    def __init__(
        self, redis: Redis, store: OrderStore, consumer: str,
        reclaim_idle_ms: int = RECLAIM_IDLE_MS,
    ) -> None:
        if reclaim_idle_ms <= 0:
            raise ValueError("reclaim_idle_ms must be positive")
        self.redis = redis
        self.store = store
        self.consumer = consumer
        self.reclaim_idle_ms = reclaim_idle_ms
        self.reclaim_cursor = "0-0"

    def initialize(self) -> None:
        try:
            # Start at 0 so reservations queued before worker startup are read.
            self.redis.xgroup_create(RESERVATION_STREAM, ORDER_GROUP, id="0", mkstream=True)
        except ResponseError as error:
            if not str(error).startswith("BUSYGROUP"):
                raise

    def process(self, event_id: str, fields: dict[str, str]) -> None:
        event = ReservationEvent.from_fields(fields)
        self.store.save(event)
        self.redis.xack(RESERVATION_STREAM, ORDER_GROUP, event_id)
        logger.info("Order saved for reservation %s (event %s)", event.reservation_id, event_id)

    def _process_messages(self, messages: list) -> int:
        processed = 0
        for event_id, fields in messages:
            try:
                self.process(event_id, fields)
            except (psycopg.Error, RedisError, ValueError):
                logger.exception("Event %s failed; left pending for recovery", event_id)
            else:
                processed += 1
        return processed

    def recover_once(self) -> int:
        # Redis resets idle time on claim, spacing out repeated failures. Keep
        # the scan cursor even when no messages qualify, so later entries aren't
        # starved by a large pending list. Redis 7+ also reports deleted entries.
        cursor, messages, deleted = self.redis.xautoclaim(
            RESERVATION_STREAM, ORDER_GROUP, self.consumer,
            min_idle_time=self.reclaim_idle_ms, start_id=self.reclaim_cursor, count=10,
        )
        self.reclaim_cursor = cursor
        if deleted:
            logger.error("Pending events are missing from the stream: %s", deleted)
        if messages:
            logger.info("Worker %s reclaimed %d events", self.consumer, len(messages))
        return self._process_messages(messages)

    def run_once(self, block_ms: int | None = 1000) -> int:
        # Bound recovery to one page, then read new work even if retries fail.
        processed = self.recover_once()
        batches = self.redis.xreadgroup(
            ORDER_GROUP, self.consumer, {RESERVATION_STREAM: ">"}, count=10, block=block_ms
        )
        for _, messages in batches:
            processed += self._process_messages(messages)
        return processed


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        raise SystemExit("DATABASE_URL is required")
    stop = Event()
    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, lambda *_: stop.set())
    with Redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True, socket_connect_timeout=5, socket_timeout=5,
    ) as redis:
        store = PostgresOrderStore(database_url)
        store.initialize()
        worker = OrderWorker(
            redis, store, os.getenv("WORKER_NAME", f"worker-{uuid4().hex}"),
            reclaim_idle_ms=int(os.getenv("RECLAIM_IDLE_MS", str(RECLAIM_IDLE_MS))),
        )
        worker.initialize()
        logger.info("Order worker %s started", worker.consumer)
        while not stop.is_set():
            try:
                worker.run_once()
            except RedisError:
                logger.exception("Redis read failed; retrying in 2 seconds")
                stop.wait(2)


if __name__ == "__main__":
    main()
