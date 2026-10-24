"""Inventory gateways for the flash-sale reservation flow."""

import os
from enum import Enum
from functools import lru_cache
from typing import Protocol

from redis import Redis
from redis.exceptions import RedisError


# The script checks a per-user claim and decrements stock in one Redis
# operation. Any non-negative result is the remaining stock after reservation.
RESERVE_STOCK_SCRIPT = """
if redis.call('EXISTS', KEYS[2]) == 1 then
    return -3
end
local stock = redis.call('GET', KEYS[1])
if not stock then
    return -2
end
if tonumber(stock) <= 0 then
    return -1
end
local remaining_stock = redis.call('DECR', KEYS[1])
redis.call('SET', KEYS[2], 'claimed')
return remaining_stock
"""


class InventoryUnavailableError(Exception):
    """Raised when the shared inventory store cannot be used."""


class ReservationOutcome(str, Enum):
    SOLD_OUT = "sold_out"
    ALREADY_RESERVED = "already_reserved"


class InventoryGateway(Protocol):
    def available_stock(self, product_id: str, initial_stock: int) -> int: ...

    def reserve(
        self, product_id: str, initial_stock: int, user_id: str
    ) -> int | ReservationOutcome: ...


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
        self, product_id: str, initial_stock: int, user_id: str
    ) -> int | ReservationOutcome:
        stock_key = self._ensure_stock_counter(product_id, initial_stock)
        claim_key = self._claim_key(product_id, user_id)
        try:
            remaining_stock = int(
                self.client.eval(RESERVE_STOCK_SCRIPT, 2, stock_key, claim_key)
            )
        except RedisError as error:
            raise InventoryUnavailableError from error

        if remaining_stock == -1:
            return ReservationOutcome.SOLD_OUT
        if remaining_stock == -2:
            raise InventoryUnavailableError
        if remaining_stock == -3:
            return ReservationOutcome.ALREADY_RESERVED
        return remaining_stock


@lru_cache
def get_redis_client() -> Redis:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return Redis.from_url(redis_url, decode_responses=True)


def get_inventory_gateway() -> InventoryGateway:
    return RedisInventoryGateway(get_redis_client())
