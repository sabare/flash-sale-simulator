"""Run with python -m app.worker. Failed events remain pending for recovery."""

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
logger = logging.getLogger(__name__)


class OrderStore(Protocol):
    def save(self, event: ReservationEvent) -> None: ...


class OrderWorker:
    def __init__(self, redis: Redis, store: OrderStore, consumer: str) -> None:
        self.redis = redis
        self.store = store
        self.consumer = consumer

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

    def run_once(self, block_ms: int | None = 1000) -> int:
        batches = self.redis.xreadgroup(
            ORDER_GROUP, self.consumer, {RESERVATION_STREAM: ">"}, count=10, block=block_ms
        )
        processed = 0
        for _, messages in batches:
            for event_id, fields in messages:
                try:
                    self.process(event_id, fields)
                except (psycopg.Error, RedisError, ValueError):
                    logger.exception("Event %s failed; left pending for recovery", event_id)
                else:
                    processed += 1
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
        worker = OrderWorker(redis, store, os.getenv("WORKER_NAME", f"worker-{uuid4().hex}"))
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
