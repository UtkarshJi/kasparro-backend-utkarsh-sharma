"""Rate limiter service with exponential backoff."""

import asyncio
import time
from collections import defaultdict
from typing import Any, Callable, TypeVar

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from core.config import get_settings
from core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

T = TypeVar("T")


class RateLimitError(Exception):
    """Raised when rate limit is exceeded."""

    def __init__(self, source: str, retry_after: float):
        self.source = source
        self.retry_after = retry_after
        super().__init__(f"Rate limit exceeded for {source}. Retry after {retry_after:.1f}s")


class TokenBucket:
    """Token bucket rate limiter."""

    def __init__(self, rate: float, capacity: int):
        """
        Initialize token bucket.

        Args:
            rate: Tokens added per second
            capacity: Maximum tokens (burst capacity)
        """
        self.rate = rate
        self.capacity = capacity
        self.tokens = capacity
        self.last_update = time.time()
        self._lock = asyncio.Lock()

    async def acquire(self, tokens: int = 1) -> bool:
        """
        Try to acquire tokens.

        Returns True if successful, False otherwise.
        """
        async with self._lock:
            now = time.time()
            elapsed = now - self.last_update

            # Add tokens based on elapsed time
            self.tokens = min(
                self.capacity,
                self.tokens + elapsed * self.rate,
            )
            self.last_update = now

            if self.tokens >= tokens:
                self.tokens -= tokens
                return True
            return False

    async def wait_for_token(self, tokens: int = 1) -> float:
        """
        Wait until token is available.

        Returns wait time in seconds.
        """
        while True:
            if await self.acquire(tokens):
                return 0.0

            # Calculate wait time
            async with self._lock:
                tokens_needed = tokens - self.tokens
                wait_time = tokens_needed / self.rate

            if wait_time > 0:
                await asyncio.sleep(wait_time)
            else:
                return wait_time


class RateLimiter:
    """Per-source rate limiter with backoff."""

    def __init__(self):
        self.buckets: dict[str, TokenBucket] = {}
        self.request_counts: dict[str, int] = defaultdict(int)
        self.error_counts: dict[str, int] = defaultdict(int)
        self.logger = get_logger("rate_limiter")

    def get_bucket(self, source: str) -> TokenBucket:
        """Get or create bucket for a source."""
        if source not in self.buckets:
            # Convert per-minute rate to per-second
            rate = settings.rate_limit_requests_per_minute / 60
            self.buckets[source] = TokenBucket(
                rate=rate,
                capacity=settings.rate_limit_burst,
            )
        return self.buckets[source]

    async def acquire(self, source: str) -> bool:
        """Try to acquire permission to make a request."""
        bucket = self.get_bucket(source)
        acquired = await bucket.acquire()

        if acquired:
            self.request_counts[source] += 1
            self.logger.debug(
                "rate_limit_acquired",
                source=source,
                total_requests=self.request_counts[source],
            )
        else:
            self.logger.warning(
                "rate_limit_exceeded",
                source=source,
                tokens_available=bucket.tokens,
            )

        return acquired

    async def wait_and_acquire(self, source: str) -> float:
        """Wait until rate limit allows request."""
        bucket = self.get_bucket(source)
        wait_time = await bucket.wait_for_token()

        self.request_counts[source] += 1
        self.logger.debug(
            "rate_limit_acquired_after_wait",
            source=source,
            wait_time=wait_time,
        )

        return wait_time

    def record_error(self, source: str) -> None:
        """Record an error for backoff calculation."""
        self.error_counts[source] += 1
        self.logger.warning(
            "rate_limiter_error_recorded",
            source=source,
            error_count=self.error_counts[source],
        )

    def reset_errors(self, source: str) -> None:
        """Reset error count after successful request."""
        if source in self.error_counts:
            self.error_counts[source] = 0

    def get_backoff_seconds(self, source: str) -> float:
        """Calculate exponential backoff based on error count."""
        errors = self.error_counts.get(source, 0)
        if errors == 0:
            return 0.0
        # Exponential backoff: 2^errors seconds, max 300 seconds
        return min(2 ** errors, 300)

    def get_stats(self) -> dict[str, Any]:
        """Get rate limiter statistics."""
        return {
            "request_counts": dict(self.request_counts),
            "error_counts": dict(self.error_counts),
            "bucket_tokens": {
                source: bucket.tokens
                for source, bucket in self.buckets.items()
            },
        }


# Global rate limiter instance
_rate_limiter: RateLimiter | None = None


def get_rate_limiter() -> RateLimiter:
    """Get or create global rate limiter."""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RateLimiter()
    return _rate_limiter


def with_rate_limit(source: str):
    """Decorator for rate-limited async functions."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @retry(
            stop=stop_after_attempt(3),
            wait=wait_exponential(multiplier=1, min=1, max=30),
            retry=retry_if_exception_type(RateLimitError),
        )
        async def wrapper(*args, **kwargs) -> T:
            limiter = get_rate_limiter()

            # Check rate limit
            if not await limiter.acquire(source):
                backoff = limiter.get_backoff_seconds(source)
                raise RateLimitError(source, backoff)

            try:
                result = await func(*args, **kwargs)
                limiter.reset_errors(source)
                return result
            except Exception as e:
                limiter.record_error(source)
                raise

        return wrapper

    return decorator
