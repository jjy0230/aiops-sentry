import time
from typing import Dict, Tuple, Optional
from sentry.config import settings
from sentry.schemas import FailureContext, RemediationAction, ArbitrationResult, RemediationStatus
from sentry.lease_guard import DistributedLeaseGuard


class BlastRadiusGuard:
    """
    Guards against cascading failures, flapping, and uncontrolled blast radius.
    Delegates to DistributedLeaseGuard for Kubernetes Lease / distributed coordination.
    """

    def __init__(self, cooldown_seconds: int = settings.cooldown_seconds, lease_guard: Optional[DistributedLeaseGuard] = None):
        self.cooldown_seconds = cooldown_seconds
        self.lease_guard = lease_guard or DistributedLeaseGuard(cooldown_seconds=cooldown_seconds)

    def is_in_cooldown(self, target_key: str) -> Tuple[bool, float]:
        return self.lease_guard.is_in_cooldown(target_key)

    def record_execution(self, target_key: str):
        self.lease_guard.record_execution(target_key)

    def reset(self):
        self.lease_guard.reset()


class SafetyEnvelope:
    """
    Enforces invariant safety constraints (Safety Envelopes) on Jev decisions.
    Even if the model suggests an action with high probability, physical metrics
    and invariant domain constraints must pass before triggering remediation.
    """

    def __init__(
        self,
        guard: Optional[BlastRadiusGuard] = None,
        min_confidence: float = settings.min_confidence_threshold,
        min_margin: float = settings.min_margin_threshold,
    ):
        self.guard = guard or BlastRadiusGuard()
        self.min_confidence = min_confidence
        self.min_margin = min_margin

    def verify(
        self, ctx: FailureContext, arbitration: ArbitrationResult
    ) -> Tuple[RemediationStatus, RemediationAction, str, bool]:
        """
        Validates whether the arbitrated action is safe to execute.
        Returns: (status, final_action, verdict_reason, requires_human_approval)
        """
        action = arbitration.selected_action
        target_key = f"{ctx.namespace}:{ctx.service_name}:{ctx.pod_name or 'any'}"

        # 1. Check confidence and probability margin
        if arbitration.confidence < self.min_confidence or arbitration.margin < self.min_margin:
            return (
                RemediationStatus.ESCALATED,
                RemediationAction.ESCALATE_HUMAN,
                f"Confidence ({arbitration.confidence:.2f}) or margin ({arbitration.margin:.2f}) below safety envelope threshold.",
                True,
            )

        # 2. Check Cooldown / Flapping protection via Distributed Lease
        in_cooldown, remaining = self.guard.is_in_cooldown(target_key)
        if in_cooldown:
            return (
                RemediationStatus.COOLED_DOWN,
                RemediationAction.ESCALATE_HUMAN,
                f"Target '{target_key}' is in cooldown window ({remaining:.1f}s remaining). Suppressing automated loop to avoid flapping.",
                True,
            )

        # 3. Domain invariant checks (Physical pre-flight gates)
        logs_lower = ctx.error_logs.lower()

        # Invariant A: Prohibit RESTART_POD when root cause is DB deadlock or pool exhaustion
        if action == RemediationAction.RESTART_POD:
            if "lock wait timeout" in logs_lower or "hikaripool" in logs_lower or "deadlock" in logs_lower:
                return (
                    RemediationStatus.BLOCKED_BY_SAFETY,
                    RemediationAction.ESCALATE_HUMAN,
                    "Safety Envelope Intercept: RESTART_POD is strictly forbidden on lock/pool starvation (prevents thundering herd on DB).",
                    True,
                )

        # Invariant B: FORCE_KILL_LONG_TRANSACTION requires evidence of active connections
        if action == RemediationAction.FORCE_KILL_LONG_TRANSACTION:
            if ctx.active_connections < 10 and "lock" not in logs_lower and "connection" not in logs_lower:
                return (
                    RemediationStatus.BLOCKED_BY_SAFETY,
                    RemediationAction.ESCALATE_HUMAN,
                    "Safety Envelope Intercept: FORCE_KILL requires verified active connection saturation or lock logs.",
                    True,
                )

        # Invariant C: SCALE_OUT is useless if CPU is low and no queue buildup
        if action == RemediationAction.SCALE_OUT:
            if ctx.cpu_usage_pct < 50.0 and ctx.p99_latency_ms < 500:
                return (
                    RemediationStatus.BLOCKED_BY_SAFETY,
                    RemediationAction.ESCALATE_HUMAN,
                    "Safety Envelope Intercept: SCALE_OUT rejected because CPU usage is moderate and latency is healthy.",
                    True,
                )

        # Invariant D: Explicit human escalation
        if action == RemediationAction.ESCALATE_HUMAN:
            return (
                RemediationStatus.ESCALATED,
                RemediationAction.ESCALATE_HUMAN,
                "Arbitrator directly requested human escalation.",
                True,
            )

        # All safety envelopes passed
        return (
            RemediationStatus.EXECUTED,
            action,
            f"Passed all safety envelopes (Confidence: {arbitration.confidence:.2f}, Margin: {arbitration.margin:.2f}).",
            False,
        )
