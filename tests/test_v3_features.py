import asyncio
import pytest
from sentry.restorer import ServiceRestorer
from sentry.postmortem import PostMortemEngine
from sentry.shadow_mode import ShadowModeManager
from sentry.schemas import (
    FailureContext,
    RemediationAction,
    ArbitrationResult,
    RemediationStatus,
    RestoreRequest,
    GroundTruthSubmission,
)
from sentry.server import core, restore_service, submit_human_ground_truth, get_shadow_scoreboard


@pytest.mark.asyncio
async def test_service_restorer_traffic_and_scale():
    """Verify traffic un-draining and replica scale-in to baseline."""
    restorer = ServiceRestorer(dry_run=True)

    # 1. Restore Traffic
    success, msg = await restorer.restore_traffic(namespace="prod", pod_name="order-pod-1")
    assert success is True
    assert "Traffic restored" in msg

    # 2. Scale in to baseline
    success_scale, msg_scale = await restorer.scale_in_to_baseline(namespace="prod", service_name="order-service", baseline_replicas=2)
    assert success_scale is True
    assert "baseline (2 replicas)" in msg_scale


@pytest.mark.asyncio
async def test_restore_endpoint_flow():
    """Verify POST /v1/sentry/restore endpoint."""
    req = RestoreRequest(
        namespace="prod",
        pod_name="payment-pod-9",
        service_name="payment-service",
        baseline_replicas=3,
    )
    resp = await restore_service(req)
    assert resp.success is True
    assert "payment-pod-9" in resp.target or "payment-service" in resp.target


def test_postmortem_engine_markdown_generation():
    """Verify PostMortemEngine compiles structured retrospective with timeline and metrics."""
    engine = PostMortemEngine()
    ctx = FailureContext(
        service_name="auth-service",
        namespace="prod",
        pod_name="auth-pod-1",
        trigger_rule="DeadlockContention",
        error_logs="Lock wait timeout exceeded; try restarting transaction",
    )
    arbitration = ArbitrationResult(
        selected_action=RemediationAction.FORCE_KILL_LONG_TRANSACTION,
        confidence=0.85,
        margin=0.77,
        distribution={"FORCE_KILL_LONG_TRANSACTION": 0.85, "DRAIN_TRAFFIC": 0.08},
        latency_ms=18.5,
        engine_name="MockEngine",
        reason="Lock contention root cause",
    )

    rec = engine.record_incident(
        incident_id="inc-test-auth-001",
        ctx=ctx,
        arbitration=arbitration,
        action=RemediationAction.FORCE_KILL_LONG_TRANSACTION,
        execution_message="Killed thread 1024",
        started_at=1000.0,
        resolved_at=1000.065,
        root_blocker_id=1024,
        blocking_query="SELECT * FROM auth_tokens FOR UPDATE",
    )

    assert rec.mttr_seconds == 0.065
    md = engine.export_markdown("inc-test-auth-001")

    assert "# SRE Incident Post-Mortem: auth-service" in md
    assert "65.0 ms" in md
    assert "SELECT * FROM auth_tokens FOR UPDATE" in md
    assert "timeline" in md
    assert "FORCE_KILL_LONG_TRANSACTION" in md


def test_shadow_mode_dry_run_and_scoreboard():
    """Verify shadow mode suppresses actual execution and accurately evaluates hit rate."""
    shadow = ShadowModeManager()
    shadow.set_service_shadow_mode("billing-service", enabled=True)

    assert shadow.is_shadow_mode_active("billing-service") is True
    assert shadow.is_shadow_mode_active("other-service") is False

    ctx = FailureContext(
        service_name="billing-service",
        namespace="prod",
        trigger_rule="OOM",
        memory_usage_pct=98.0,
        error_logs="java.lang.OutOfMemoryError",
    )
    arbitration = ArbitrationResult(
        selected_action=RemediationAction.RESTART_POD,
        confidence=0.88,
        margin=0.70,
        distribution={"RESTART_POD": 0.88},
        latency_ms=17.0,
        engine_name="MockEngine",
        reason="Heap exhaustion",
    )

    # 1. Record shadow prediction
    rec = shadow.record_prediction("inc-shadow-001", ctx, arbitration)
    assert rec.suggested_action == RemediationAction.RESTART_POD

    # 2. Record human ground truth (Human operator agrees: RESTART_POD)
    updated = shadow.record_ground_truth("inc-shadow-001", RemediationAction.RESTART_POD)
    assert updated.is_match is True

    # 3. Scoreboard evaluation
    board = shadow.get_scoreboard()
    assert board.total_shadow_predictions == 1
    assert board.evaluated_with_ground_truth == 1
    assert board.matched_predictions == 1
    assert board.accuracy_rate_pct == 100.0
