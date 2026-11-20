import time
from sentry.lease_guard import DistributedLeaseGuard


def test_lease_guard_lifecycle():
    """Verify local lease acquisition, remaining time calculation, and expiration."""
    guard = DistributedLeaseGuard(cooldown_seconds=2)
    key = "prod:payment-service:pod-a"

    # Initially not in cooldown
    in_cool, remaining = guard.is_in_cooldown(key)
    assert in_cool is False
    assert remaining == 0.0

    # Record execution
    guard.record_execution(key)

    # Immediately in cooldown
    in_cool, remaining = guard.is_in_cooldown(key)
    assert in_cool is True
    assert 0.0 < remaining <= 2.0

    # Sleep past duration
    time.sleep(2.1)
    in_cool_after, rem_after = guard.is_in_cooldown(key)
    assert in_cool_after is False
    assert rem_after == 0.0


def test_sanitize_lease_name_rfc1123():
    """Verify conversion of multi-segment target keys to RFC 1123 compliant lease names."""
    guard = DistributedLeaseGuard()
    raw_key = "Production_US_West:core-payment/service_v2:pod_7f8d_99"
    sanitized = guard._sanitize_lease_name(raw_key)

    assert "_" not in sanitized
    assert ":" not in sanitized
    assert "/" not in sanitized
    assert sanitized.startswith("sentry-cooldown-")
    assert len(sanitized) <= 63
