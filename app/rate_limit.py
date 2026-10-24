"""Shared fixed-window limit for reservation attempts by caller-supplied user ID."""

from hashlib import sha256
from math import ceil

from redis import Redis
from redis.exceptions import RedisError

from app.inventory import get_redis_client


RATE_LIMIT_SCRIPT = """
local count = tonumber(redis.call('GET', KEYS[1]) or '0')
local ttl = redis.call('PTTL', KEYS[1])
if ttl <= 0 then
    redis.call('SET', KEYS[1], '1', 'PX', ARGV[2])
    return 0
end
if count >= tonumber(ARGV[1]) then
    return ttl
end
redis.call('INCR', KEYS[1])
return 0
"""


class RateLimitUnavailableError(Exception):
    """The shared request budget cannot be checked."""


class RedisRateLimiter:
    def __init__(self, client: Redis, limit: int = 10, window_seconds: int = 60) -> None:
        if limit <= 0 or window_seconds <= 0:
            raise ValueError("Rate limit and window must be positive")
        self.client = client
        self.limit = limit
        self.window_ms = window_seconds * 1000

    def check(self, user_id: str) -> int:
        """Consume an attempt; return zero if allowed, otherwise retry seconds."""
        key = "flash-sale:rate-limit:reservations:" + sha256(user_id.encode()).hexdigest()
        try:
            retry_ms = int(self.client.eval(RATE_LIMIT_SCRIPT, 1, key, self.limit, self.window_ms))
        except RedisError as error:
            raise RateLimitUnavailableError from error
        return ceil(retry_ms / 1000)


def get_rate_limiter() -> RedisRateLimiter:
    return RedisRateLimiter(get_redis_client())
