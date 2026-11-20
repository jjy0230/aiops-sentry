import asyncio
import pytest
from sentry.storm_guard import AlertFingerprintDeduplicator
from sentry.closed_loop import (
    PostRemediationProbeManager,
    EscalationLadder,
    VerificationState,
)
from sentry.telemetry import SentryTelemetryRegistry
from sentry.schemas import (
    FailureContext,
    RemediationAction,
    AlertmanagerWebhook,
    AlertItem,
)
from sentry.server import core, alertmanager_webhook, prometheus_metrics


def test_storm_guard_sliding_window_deduplication():
    """Verify that multiple alerts for the same service within 3s are aggregated."""
    dedup = AlertFingerprintDeduplicator(window_seconds=1.5)

    ctx1 = FailureContext(
        service_name="payment-service",
        namespace="prod",
        pod_name="payment-pod-1",
        trigger_rule="HighCPU",
        cpu_usage_pct=92.0,
    )
    ctx2 = FailureContext(
        service_name="payment-service",
        namespace="prod",
        pod_name="payment-pod-2",
        trigger_rule="HighCPU",
        cpu_usage_pct=95.0,
    )

    # First pod alert triggers new arbitration
    is_new1, group1 = dedup.process_incoming(ctx1)
    assert is_new1 is True
    assert group1.aggregated_count == 1
    assert "payment-pod-1" in group1.impacted_pods

    # Second pod alert within window is aggregated
    is_new2, group2 = dedup.process_incoming(ctx2)
    assert is_new2 is False
    assert group2.aggregated_count == 2
    assert "payment-pod-2" in group2.impacted_pods
    assert group2.primary_context.cpu_usage_pct == 95.0  # Peak tracked


def test_escalation_ladder_mapping():
    """Verify deterministic escalation ladder transitions."""
    assert (
        EscalationLadder.get_next_action(RemediationAction.FORCE_KILL_LONG_TRANSACTION)
        == RemediationAction.DRAIN_TRAFFIC
    )
    assert (
        EscalationLadder.get_next_action(RemediationAction.DRAIN_TRAFFIC)
        == RemediationAction.RESTART_POD
    )
    assert (
        EscalationLadder.get_next_action(RemediationAction.RESTART_POD)
        == RemediationAction.ESCALATE_HUMAN
    )


@pytest.mark.asyncio
async def test_closed_loop_verification_recovery_success():
    """Verify that a successful post-remediation probe marks job as VERIFIED_HEALTHY."""
    dispatched = []

    async def mock_dispatch(action, ctx):
        dispatched.append(action)

    mgr = PostRemediationProbeManager(
        executor_dispatch_fn=mock_dispatch,
        verification_delay_seconds=0.1,  # Fast test delay
    )

    ctx = FailureContext(service_name="order-service", namespace="prod")
    job = mgr.schedule_verification(
        job_id="test-job-healthy",
        ctx=ctx,
        action=RemediationAction.FORCE_KILL_LONG_TRANSACTION,
        probe_evaluator=lambda: True,  # Evaluates to healthy
    )

    # Wait for probe to complete
    await asyncio.sleep(0.15)
    assert job.state == VerificationState.VERIFIED_HEALTHY
    assert len(dispatched) == 0  # No escalation needed


@pytest.mark.asyncio
async def test_closed_loop_verification_unresolved_escalation():
    """Verify that an unresolved incident automatically escalates to next tier."""
    dispatched = []

    async def mock_dispatch(action, ctx):
        dispatched.append(action)

    mgr = PostRemediationProbeManager(
        executor_dispatch_fn=mock_dispatch,
        verification_delay_seconds=0.1,
    )

    ctx = FailureContext(service_name="order-service", namespace="prod")
    job = mgr.schedule_verification(
        job_id="test-job-escalate",
        ctx=ctx,
        action=RemediationAction.FORCE_KILL_LONG_TRANSACTION,
        probe_evaluator=lambda: False,  # Metrics still abnormal
    )

    await asyncio.sleep(0.15)
    assert job.state == VerificationState.ESCALATED_NEXT_TIER
    assert job.executed_action == RemediationAction.DRAIN_TRAFFIC
    assert len(dispatched) == 1
    assert dispatched[0] == RemediationAction.DRAIN_TRAFFIC


def test_telemetry_prometheus_exporter():
    """Verify Prometheus text format exporter contains registered metrics."""
    reg = SentryTelemetryRegistry()
    reg.record_arbitration("cart-service", "DRAIN_TRAFFIC", "EXECUTED", 22.5)
    reg.record_safety_block("BLOCKED_BY_SAFETY")
    reg.record_deduplication("cart-service", 5)

    text = reg.export_prometheus_text()
    assert "sentry_arbitrations_total{" in text
    assert 'service="cart-service"' in text
    assert "sentry_safety_blocks_total{" in text
    assert "sentry_deduplicated_alerts_total{" in text


@pytest.mark.asyncio
async def test_alertmanager_webhook_storm_deduplication_e2e():
    """Verify Alertmanager webhook receives a batch of 10 alerts and suppresses duplicates."""
    core.deduplicator.reset()
    alerts = []

    # 8 alerts from order-service (same rule, different pods)
    for i in range(8):
        alerts.append(
            AlertItem(
                status="firing",
                labels={"service": "order-service", "namespace": "prod", "alertname": "HikariPoolStarvation"},
                annotations={"pod": f"order-pod-{i}", "cpu_usage_pct": "95.0", "sentry.aiops/stacktrace": "Lock wait timeout exceeded"},
            )
        )

    # 2 alerts from payment-service (different rule)
    for j in range(2):
        alerts.append(
            AlertItem(
                status="firing",
                labels={"service": "payment-service", "namespace": "prod", "alertname": "PodOOM"},
                annotations={"pod": f"payment-pod-{j}", "memory_usage_pct": "98.0", "sentry.aiops/stacktrace": "java.lang.OutOfMemoryError"},
            )
        )

    webhook_payload = AlertmanagerWebhook(
        alerts=alerts,
        commonLabels={"severity": "P0"},
    )

    resp = await alertmanager_webhook(webhook_payload)

    assert resp["received_alerts"] == 10
    # Should only arbitrate 2 distinct service/rule groups (order-service and payment-service)
    assert resp["arbitrated_count"] == 2
    assert resp["deduplicated_suppressed"] == 8
