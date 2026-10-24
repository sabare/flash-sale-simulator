"""Inventory gateways for the flash-sale reservation flow."""

import json
import os
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from typing import Protocol

from redis import Redis
from redis.exceptions import RedisError


# The script checks the idempotency record, prevents duplicate user claims, and
# enqueues work and decrements stock in one Redis operation.
# Codes 0 and 1 include a JSON payload
# with the original reservation id and remaining stock.
RESERVE_STOCK_SCRIPT = """
local existing_reservation = redis.call('GET', KEYS[3])
if existing_reservation then
    return {1, existing_reservation}
end
if redis.call('EXISTS', KEYS[2]) == 1 then
    return {-3, ''}
end
local stock = redis.call('GET', KEYS[1])
if not stock then
    return {-2, ''}
end
-- Require a non-negative integer within Lua's exact integer range before XADD.
local count = tonumber(stock)
if not string.match(stock, '^%d+$') or not count or count > 9007199254740991 then
    return {-2, ''}
end
if count == 0 then
    return {-1, ''}
end
-- Validate before writing: Redis scripts do not roll back on runtime errors.
local queue_type = redis.call('TYPE', KEYS[4]).ok
if queue_type ~= 'none' and queue_type ~= 'stream' then
    return {-2, ''}
end
-- Append first so an XADD failure cannot consume inventory without an event.
redis.call('XADD', KEYS[4], '*',
    'event_type', 'reservation.created',
    'schema_version', '1',
    'reservation_id', ARGV[1],
    'product_id', ARGV[3],
    'user_id', ARGV[4])
local remaining_stock = redis.call('DECR', KEYS[1])
redis.call('SET', KEYS[2], 'claimed')
local reservation = cjson.encode({
    reservation_id = ARGV[1],
    remaining_stock = remaining_stock
})
redis.call('SET', KEYS[3], reservation, 'EX', ARGV[2])
return {0, reservation}
"""

IDEMPOTENCY_TTL_SECONDS = 86_400
RESERVATION_STREAM = "flash-sale:reservations"


class InventoryUnavailableError(Exception):
    """Raised when the shared inventory store cannot be used."""


class ReservationOutcome(str, Enum):
    RESERVED = "reserved"
    IDEMPOTENT_REPLAY = "idempotent_replay"
    SOLD_OUT = "sold_out"
    ALREADY_RESERVED = "already_reserved"


@dataclass(frozen=True)
class ReservationResult:
    outcome: ReservationOutcome
    reservation_id: str | None = None
    remaining_stock: int | None = None


class InventoryGateway(Protocol):
    def available_stock(self, product_id: str, initial_stock: int) -> int: ...

    def reserve(
        self,
        product_id: str,
        initial_stock: int,
        user_id: str,
        idempotency_key: str,
        reservation_id: str,
    ) -> ReservationResult: ...


class RedisInventoryGateway:
    """Redis-backed inventory gateway shared by every API instance."""

    def __init__(self, client: Redis) -> None:
        self.client = client

    @staticmethod
    def _stock_key(product_id: str) -> str:
        return f"flash-sale:product:{product_id}:stock"

    @staticmethod
    def _claim_key(product_id: str, user_id: str) -> str:
        return f"flash-sale:product:{product_id}:claim:{user_id}"

    @staticmethod
    def _idempotency_key(product_id: str, user_id: str, idempotency_key: str) -> str:
        return (
            f"flash-sale:product:{product_id}:user:{user_id}:"
            f"idempotency:{idempotency_key}"
        )

    def _ensure_stock_counter(self, product_id: str, initial_stock: int) -> str:
        key = self._stock_key(product_id)
        try:
            # SETNX prevents restarts or extra API instances from resetting stock.
            self.client.setnx(key, initial_stock)
        except RedisError as error:
            raise InventoryUnavailableError from error
        return key

    def available_stock(self, product_id: str, initial_stock: int) -> int:
        key = self._ensure_stock_counter(product_id, initial_stock)
        try:
            value = self.client.get(key)
        except RedisError as error:
            raise InventoryUnavailableError from error

        if value is None:
            raise InventoryUnavailableError
        return int(value)

    def reserve(
        self,
        product_id: str,
        initial_stock: int,
        user_id: str,
        idempotency_key: str,
        reservation_id: str,
    ) -> ReservationResult:
        stock_key = self._ensure_stock_counter(product_id, initial_stock)
        claim_key = self._claim_key(product_id, user_id)
        idempotency_record_key = self._idempotency_key(
            product_id, user_id, idempotency_key
        )
        try:
            result_code, reservation_json = self.client.eval(
                RESERVE_STOCK_SCRIPT,
                4,
                stock_key,
                claim_key,
                idempotency_record_key,
                RESERVATION_STREAM,
                reservation_id,
                IDEMPOTENCY_TTL_SECONDS,
                product_id,
                user_id,
            )
        except RedisError as error:
            raise InventoryUnavailableError from error

        result_code = int(result_code)
        if result_code == -1:
            return ReservationResult(ReservationOutcome.SOLD_OUT)
        if result_code == -2:
            raise InventoryUnavailableError
        if result_code == -3:
            return ReservationResult(ReservationOutcome.ALREADY_RESERVED)
        if result_code not in {0, 1}:
            raise InventoryUnavailableError

        payload = json.loads(reservation_json)
        outcome = (
            ReservationOutcome.RESERVED
            if result_code == 0
            else ReservationOutcome.IDEMPOTENT_REPLAY
        )
        return ReservationResult(
            outcome=outcome,
            reservation_id=payload["reservation_id"],
            remaining_stock=payload["remaining_stock"],
        )


@lru_cache
def get_redis_client() -> Redis:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return Redis.from_url(redis_url, decode_responses=True, socket_connect_timeout=2, socket_timeout=2)


def get_inventory_gateway() -> InventoryGateway:
    return RedisInventoryGateway(get_redis_client())
