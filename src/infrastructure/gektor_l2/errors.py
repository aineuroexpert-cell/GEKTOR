"""Typed errors for L2 / REST boundaries."""


class BybitRestRateLimited(Exception):
    """HTTP 429 or exchange-side rate limit — caller must back off (global gate handles this)."""

    def __init__(self, message: str = "rate_limited") -> None:
        super().__init__(message)


class SnapshotIsolationError(Exception):
    """
    Cross-asset state cannot be synchronized within Intent TTL budget.

    Raised by `CrossAssetSnapshot.take_with_retry` when all retry attempts
    fail to produce an `all_ready` snapshot. This means the market is in
    a state of absolute chaos (fragmented liquidity, multiple books blind/stale).

    Signal Engine MUST catch this and suppress the signal.
    A missed trade costs zero. A desynchronized trade costs -X.
    """

    def __init__(self, attempts: int, ttl_sec: float, worst_readiness: str) -> None:
        super().__init__(
            f"Failed to acquire consistent CrossAssetSnapshot "
            f"after {attempts} retries within {ttl_sec:.1f}s TTL. "
            f"Worst readiness: {worst_readiness}"
        )
        self.attempts = attempts
        self.ttl_sec = ttl_sec
        self.worst_readiness = worst_readiness
