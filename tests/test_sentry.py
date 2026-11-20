import pytest
import asyncio
from sentry.schemas import FailureContext, RemediationAction, RemediationStatus, AlertmanagerWebhook, AlertItem
from sentry.engine import MockJevEngine
from sentry.safety import SafetyEnvelope, BlastRadiusGuard
from sentry.executor import CompositeRemediationExecutor
from sentry.server import SentryCore


@pytest.mark.asyncio
async def test_jev_mock_engine_lock_timeout():
    """Verify that DB lock / connection starvation prioritizes FORCE_KILL_LONG_TRANSACTION."""
    engine = MockJevEngine()
    ctx = FailureContext(
        service_name="order-service",
        namespace="prod",
        pod_name="order-service-7f8d-a1b2",
        cpu_usage_pct=98.5,
        memory_usage_pct=45.0,
        active_connections=100,
        max_connections=100,
        error_logs="HikariPool-1 - Connection is not available, request timed out after 30000ms. Lock wait timeout exceeded",
        trigger_rule="HikariCPConnectionStarvation",
    )
    result = await engine.evaluate(ctx)

    assert result.selected_action == RemediationAction.FORCE_KILL_LONG_TRANSACTION
    assert result.confidence >= 0.80
    assert result.distribution[RemediationAction.FORCE_KILL_LONG_TRANSACTION.value] >= 0.80
    # Pod restart probability must be suppressed to avoid cascading thundering herd
    assert result.distribution[RemediationAction.RESTART_POD.value] < 0.10


@pytest.mark.asyncio
async def test_jev_mock_engine_oom():
    """Verify that OOM error prioritizes RESTART_POD."""
    engine = MockJevEngine()
    ctx = FailureContext(
        service_name="payment-service",
        namespace="prod",
        pod_name="payment-service-x91k",
        memory_usage_pct=98.0,
        error_logs="java.lang.OutOfMemoryError: Java heap space",
        trigger_rule="PodMemoryLimitExceeded",
    )
    result = await engine.evaluate(ctx)

    assert result.selected_action == RemediationAction.RESTART_POD
    assert result.confidence >= 0.80


@pytest.mark.asyncio
async def test_safety_envelope_blocks_dangerous_restart_on_deadlock():
    """Safety envelope must intercept and block RESTART_POD when deadlock/pool starvation occurs."""
    guard = BlastRadiusGuard()
    safety = SafetyEnvelope(guard=guard)

    ctx = FailureContext(
        service_name="order-service",
        namespace="prod",
        pod_name="order-pod-1",
        error_logs="Lock wait timeout exceeded; try restarting transaction, HikariPool-1 full",
    )

    # Artificially create an arbitration proposing RESTART_POD
    from sentry.schemas import ArbitrationResult
    dangerous_arbitration = ArbitrationResult(
        selected_action=RemediationAction.RESTART_POD,
        confidence=0.90,
        margin=0.50,
        distribution={"RESTART_POD": 0.90},
        latency_ms=20.0,
        engine_name="TestEngine",
        reason="Erroneous prediction test",
    )

    status, action, verdict, requires_approval = safety.verify(ctx, dangerous_arbitration)

    assert status == RemediationStatus.BLOCKED_BY_SAFETY
    assert action == RemediationAction.ESCALATE_HUMAN
    assert requires_approval is True
    assert "strictly forbidden" in verdict


@pytest.mark.asyncio
async def test_blast_radius_cooldown_flapping():
    """Subsequent failures within cooldown window must be suppressed to prevent flapping."""
    guard = BlastRadiusGuard(cooldown_seconds=60)
    safety = SafetyEnvelope(guard=guard)

    ctx = FailureContext(
        service_name="inventory-service",
        namespace="prod",
        pod_name="inv-pod-1",
        cpu_usage_pct=95.0,
        error_logs="oomkilled heap full",
    )

    from sentry.schemas import ArbitrationResult
    valid_arbitration = ArbitrationResult(
        selected_action=RemediationAction.RESTART_POD,
        confidence=0.85,
        margin=0.40,
        distribution={"RESTART_POD": 0.85},
        latency_ms=21.0,
        engine_name="TestEngine",
        reason="Valid OOM",
    )

    # First attempt: Pass
    status, action, _, _ = safety.verify(ctx, valid_arbitration)
    assert status == RemediationStatus.EXECUTED

    # Record execution
    guard.record_execution("prod:inventory-service:inv-pod-1")

    # Immediate second attempt: Cooled down
    status2, action2, verdict2, req2 = safety.verify(ctx, valid_arbitration)
    assert status2 == RemediationStatus.COOLED_DOWN
    assert action2 == RemediationAction.ESCALATE_HUMAN
    assert req2 is True


@pytest.mark.asyncio
async def test_end_to_end_pipeline():
    """End-to-end execution of SentryCore on lock timeout alert."""
    core = SentryCore()
    ctx = FailureContext(
        service_name="account-service",
        namespace="default",
        pod_name="account-pod-88",
        cpu_usage_pct=99.0,
        memory_usage_pct=40.0,
        active_connections=98,
        error_logs="Lock wait timeout exceeded, HikariPool connection starvation",
        trigger_rule="DBDeadlockAlert",
    )
    res = await core.process_context(ctx)

    assert res.executed is True
    assert res.status == RemediationStatus.EXECUTED
    assert res.action == RemediationAction.FORCE_KILL_LONG_TRANSACTION
    assert res.requires_human_approval is False
    assert res.arbitration.confidence >= 0.80
