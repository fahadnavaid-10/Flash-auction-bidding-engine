"""
Overload Protection & Traffic Shaping.
1. Per-user token-bucket rate limiter.
2. Gateway connection ceiling.
3. System load shedding / backpressure.
"""

import time
from typing import Dict, Tuple

class OverloadProtector:
    def __init__(
        self,
        max_connections: int = 10000,
        rate_limit_per_sec: int = 5,
        rate_limit_burst: int = 10
    ):
        self.max_connections = max_connections
        self.rate_limit_per_sec = rate_limit_per_sec
        self.rate_limit_burst = rate_limit_burst
        # In-memory token bucket tracking: user_id -> (tokens, last_update_time)
        self._user_buckets: Dict[str, Tuple[float, float]] = {}
        self.current_connection_count = 0

    def check_connection_allowed(self) -> bool:
        """Enforces hard gateway connection ceiling."""
        return self.current_connection_count < self.max_connections

    def track_connect(self):
        self.current_connection_count += 1

    def track_disconnect(self):
        if self.current_connection_count > 0:
            self.current_connection_count -= 1

    def check_rate_limit(self, user_id: str) -> Tuple[bool, str]:
        """
        Token Bucket Algorithm per User.
        Allows bursts up to rate_limit_burst, refills at rate_limit_per_sec.
        Rejects abusive bot spam before it reaches Redis or database.
        """
        now = time.monotonic()
        tokens, last_update = self._user_buckets.get(user_id, (self.rate_limit_burst, now))

        # Calculate refilled tokens based on elapsed time
        elapsed = now - last_update
        tokens = min(self.rate_limit_burst, tokens + elapsed * self.rate_limit_per_sec)

        if tokens >= 1.0:
            # Consume 1 token
            self._user_buckets[user_id] = (tokens - 1.0, now)
            return True, "OK"
        else:
            self._user_buckets[user_id] = (tokens, now)
            return False, f"RATE_LIMITED: Max {self.rate_limit_per_sec} bids/sec exceeded"
