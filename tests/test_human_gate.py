import time
import pytest
from sentry.human_gate import ApprovalTokenManager, InteractiveCardBuilder
from sentry.schemas import RemediationAction, FailureContext, ArbitrationResult, ApprovalRequest
from sentry.server import core, handle_human_approval


def test_token_creation_and_successful_verification():
    """Verify HMAC token generation and valid parsing."""
    mgr = ApprovalTokenManager(secret="test-secret-salt", ttl_seconds=60)
    ctx = FailureContext(
        service_name="payment-service",
        namespace="prod",
        pod_name="payment-pod-1",
    )
    token = mgr.create_token(
        action=RemediationAction.SCALE_OUT,
        ctx=ctx,
        reason="High load",
    )

    valid, payload, err = mgr.verify_and_consume(token)
    assert valid is True
    assert payload is not None
    assert payload.action == RemediationAction.SCALE_OUT
    assert payload.service_name == "payment-service"
    assert payload.pod_name == "payment-pod-1"


def test_token_tampering_rejected():
    """Verify cryptographic signature check detects tampered token."""
    mgr = ApprovalTokenManager(secret="test-secret-salt", ttl_seconds=60)
    ctx = FailureContext(service_name="order-service", namespace="prod")
    token = mgr.create_token(action=RemediationAction.RESTART_POD, ctx=ctx, reason="OOM")

    # Tamper with the base64 payload part
    parts = token.split(".")
    tampered_token = f"dGFtcGVyZWRfcGF5bG9hZA==.{parts[1]}"

    valid, payload, err = mgr.verify_and_consume(tampered_token)
    assert valid is False
    assert payload is None
    assert "signature" in err.lower() or "failed" in err.lower()


def test_token_replay_protection():
    """Verify single-use guarantee: second attempt to consume the same token is rejected."""
    mgr = ApprovalTokenManager(secret="test-secret-salt", ttl_seconds=60)
    ctx = FailureContext(service_name="auth-service", namespace="prod")
    token = mgr.create_token(action=RemediationAction.DRAIN_TRAFFIC, ctx=ctx, reason="Cascading 500")

    # First consumption: Success
    valid1, payload1, _ = mgr.verify_and_consume(token)
    assert valid1 is True

    # Replay attempt: Rejected
    valid2, payload2, err2 = mgr.verify_and_consume(token)
    assert valid2 is False
    assert "replay" in err2.lower()


def test_feishu_and_dingtalk_card_structure():
    """Verify generated ChatOps cards contain expected fields and actions."""
    ctx = FailureContext(
        service_name="order-service",
        namespace="prod",
        pod_name="order-pod-99",
        trigger_rule="HikariCPPoolStarvation",
        error_logs="HikariPool-1 - Connection not available",
    )
    arbitration = ArbitrationResult(
        selected_action=RemediationAction.FORCE_KILL_LONG_TRANSACTION,
        confidence=0.85,
        margin=0.77,
        distribution={"FORCE_KILL_LONG_TRANSACTION": 0.85},
        latency_ms=19.2,
        engine_name="MockEngine",
        reason="Lock timeout detected",
    )
    token = "mock-token.xyz"
    verdict = "Passed safety envelopes"

    feishu_card = InteractiveCardBuilder.build_feishu_card(ctx, arbitration, token, verdict)
    assert "header" in feishu_card
    assert feishu_card["header"]["title"]["content"].startswith("🚨 [AIOps-Sentry]")
    assert len(feishu_card["elements"]) >= 3

    dingtalk_card = InteractiveCardBuilder.build_dingtalk_card(ctx, arbitration, token)
    assert dingtalk_card["msgtype"] == "actionCard"
    assert "order-service" in dingtalk_card["actionCard"]["text"]


@pytest.mark.asyncio
async def test_human_approval_api_endpoint_flow():
    """Verify /v1/sentry/approval endpoint executes approved action upon valid token."""
    ctx = FailureContext(
        service_name="cart-service",
        namespace="prod",
        pod_name="cart-pod-42",
    )
    token = core.token_manager.create_token(
        action=RemediationAction.DRAIN_TRAFFIC,
        ctx=ctx,
        reason="Manual approval required",
    )

    req = ApprovalRequest(
        approval_token=token,
        approved=True,
        operator_id="sre-alice",
    )

    resp = await handle_human_approval(req)
    assert resp.success is True
    assert resp.executed_action == RemediationAction.DRAIN_TRAFFIC
    assert "sre-alice approved" in resp.message
