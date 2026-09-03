"""Retry policy: idempotency rules, bounded attempts, exponential backoff with jitter."""

from __future__ import annotations

import enum
import random
from collections.abc import Mapping
from dataclasses import dataclass

IDEMPOTENT_METHODS = frozenset({"GET", "HEAD", "PUT", "DELETE", "OPTIONS", "TRACE"})
IDEMPOTENCY_KEY_HEADER = "idempotency-key"


class FailureKind(enum.Enum):
    """Where an attempt failed. Decides whether a retry is safe."""

    CONNECT = "connect"  # no bytes reached the upstream; always safe to retry
    TIMEOUT = "timeout"  # request may have been processed
    READ = "read"  # connection dropped mid-response; request may have been processed
    STATUS = "status"  # upstream answered with a retryable status


def is_idempotent(
    method: str,
    headers: Mapping[str, str] | None = None,
    *,
    idempotent_post: bool = False,
) -> bool:
    """POST/PATCH are only idempotent when the route says so or the client sends
    an Idempotency-Key header (RFC draft-ietf-httpapi-idempotency-key-header)."""
    m = method.upper()
    if m in IDEMPOTENT_METHODS:
        return True
    if m in ("POST", "PATCH"):
        if idempotent_post:
            return True
        if headers is not None:
            for k in headers:
                if k.lower() == IDEMPOTENCY_KEY_HEADER and headers[k]:
                    return True
    return False


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int = 3
    base_delay: float = 0.02  # seconds
    max_delay: float = 0.25  # seconds
    retry_on_status: frozenset[int] = frozenset({500, 502, 503, 504})

    def __post_init__(self) -> None:
        if self.max_attempts < 1:
            raise ValueError("max_attempts must be >= 1")
        if self.base_delay < 0 or self.max_delay < self.base_delay:
            raise ValueError("delays must satisfy 0 <= base_delay <= max_delay")

    def should_retry(self, attempt: int, kind: FailureKind, idempotent: bool) -> bool:
        """`attempt` is 1-based: the number of attempts already made."""
        if attempt >= self.max_attempts:
            return False
        if kind is FailureKind.CONNECT:
            return True
        return idempotent

    def retryable_status(self, status: int) -> bool:
        return status in self.retry_on_status

    def backoff(self, attempt: int, rng: random.Random | None = None) -> float:
        """Full-jitter exponential backoff: uniform(0, min(max, base * 2**(attempt-1)))."""
        if attempt < 1:
            raise ValueError("attempt must be >= 1")
        ceiling = min(self.max_delay, self.base_delay * (2 ** (attempt - 1)))
        r = rng if rng is not None else random
        return r.uniform(0.0, ceiling)
