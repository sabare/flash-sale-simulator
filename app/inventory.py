"""Inventory gateways for the flash-sale reservation flow."""

import os
from functools import lru_cache
from typing import Protocol

from redis import Redis
from redis.exceptions import RedisError


# The script reads and decrements in one Redis operation. Returning -1 means
# the counter was already zero; any non-negative result is remaining stock.
RESERVE_STOCK_SCRIPT = """
local stock = redis.call('GET', KEYS[1])
if not stock then
    return -2
end
if tonumber(stock) <= 0 then
    return -1
end
return redis.call('DECR', KEYS[1])
"""


class InventoryUnavailableError(Exception):
    """Raised when the shared inventory store cannot be used."""


class InventoryGateway(Protocol):
    def available_stock(self, product_id: str, initial_stock: int) -> int: ...

    def reserve(self, product_id: str, initial_stock: int) -> int | None: ...


class RedisInventoryGateway:
    """Redis-backed inventory gateway shared by every API instance."""

    def __init__(self, client: Redis) -> None:
        self.client = client

    @staticmethod
    def _stock_key(product_id: str) -> str:
        return f"flash-sale:product:{product_id}:stock"

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

    def reserve(self, product_id: str, initial_stock: int) -> int | None:
        key = self._ensure_stock_counter(product_id, initial_stock)
        try:
            remaining_stock = int(self.client.eval(RESERVE_STOCK_SCRIPT, 1, key))
        except RedisError as error:
            raise InventoryUnavailableError from error

        if remaining_stock == -1:
            return None
        if remaining_stock == -2:
            raise InventoryUnavailableError
        return remaining_stock


@lru_cache
def get_redis_client() -> Redis:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return Redis.from_url(redis_url, decode_responses=True)


def get_inventory_gateway() -> InventoryGateway:
    return RedisInventoryGateway(get_redis_client())
