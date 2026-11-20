import abc
import time
import httpx
from typing import Dict, Tuple
from sentry.config import settings
from sentry.schemas import FailureContext, RemediationAction, ArbitrationResult


class BaseJevEngine(abc.ABC):
    """Abstract base class for Jev decision engines."""

    @abc.abstractmethod
    async def evaluate(self, ctx: FailureContext) -> ArbitrationResult:
        """Perform single forward evaluation and return calibrated probability distribution."""
        pass


class MockJevEngine(BaseJevEngine):
    """
    High-fidelity mock engine simulating Jev single-pass prefill logit computation.
    Produces calibrated discrete action probabilities for zero-dependency CI and local dev.
    """

    async def evaluate(self, ctx: FailureContext) -> ArbitrationResult:
        start_time = time.perf_counter()

        logs_lower = ctx.error_logs.lower()
        rule_lower = ctx.trigger_rule.lower()

        # Pattern 1: Database lock timeout or connection pool exhaustion
        if (
            "lock wait timeout" in logs_lower
            or "hikaripool" in logs_lower
            or "connection is not available" in logs_lower
            or "deadlock found" in logs_lower
            or "max_connections" in rule_lower
        ):
            # Key diagnostic insight: Root cause is DB lock / connection exhaustion; Pod restart causes thundering herd!
            dist = {
                RemediationAction.FORCE_KILL_LONG_TRANSACTION.value: 0.85,
                RemediationAction.DRAIN_TRAFFIC.value: 0.08,
                RemediationAction.ESCALATE_HUMAN.value: 0.04,
                RemediationAction.RESTART_POD.value: 0.02,
                RemediationAction.SCALE_OUT.value: 0.01,
            }
            reason = "Detected DB lock contention or pool starvation. Restarting pod would worsen cascading failure."

        # Pattern 2: Memory leak / Out of Memory
        elif (
            "outofmemoryerror" in logs_lower
            or "oomkilled" in logs_lower
            or ctx.memory_usage_pct >= 95.0
        ):
            dist = {
                RemediationAction.RESTART_POD.value: 0.82,
                RemediationAction.DRAIN_TRAFFIC.value: 0.12,
                RemediationAction.SCALE_OUT.value: 0.04,
                RemediationAction.ESCALATE_HUMAN.value: 0.02,
                RemediationAction.FORCE_KILL_LONG_TRANSACTION.value: 0.00,
            }
            reason = "Heap space exhaustion or Pod OOM. Immediate restart cleans rogue heap state."

        # Pattern 3: Pure Traffic surge / High CPU without fatal errors
        elif ctx.cpu_usage_pct >= 90.0 and "error" not in logs_lower and "timeout" not in logs_lower:
            dist = {
                RemediationAction.SCALE_OUT.value: 0.86,
                RemediationAction.DRAIN_TRAFFIC.value: 0.08,
                RemediationAction.ESCALATE_HUMAN.value: 0.04,
                RemediationAction.RESTART_POD.value: 0.02,
                RemediationAction.FORCE_KILL_LONG_TRANSACTION.value: 0.00,
            }
            reason = "Pure compute capacity saturation without fatal exception stacktrace. Horizontal scaling recommended."

        # Pattern 4: Severe cascading error / network fault requiring traffic isolation
        elif "connection refused" in logs_lower or "upstream connect error" in logs_lower:
            dist = {
                RemediationAction.DRAIN_TRAFFIC.value: 0.78,
                RemediationAction.ESCALATE_HUMAN.value: 0.15,
                RemediationAction.RESTART_POD.value: 0.05,
                RemediationAction.SCALE_OUT.value: 0.02,
                RemediationAction.FORCE_KILL_LONG_TRANSACTION.value: 0.00,
            }
            reason = "Cascading network or upstream dependency failure. Traffic must be drained to prevent error amplification."

        # Default: Ambiguous or unknown failure -> Escalate to human
        else:
            dist = {
                RemediationAction.ESCALATE_HUMAN.value: 0.45,
                RemediationAction.DRAIN_TRAFFIC.value: 0.20,
                RemediationAction.RESTART_POD.value: 0.15,
                RemediationAction.FORCE_KILL_LONG_TRANSACTION.value: 0.10,
                RemediationAction.SCALE_OUT.value: 0.10,
            }
            reason = "Ambiguous failure symptom. Confidence distribution falls below safety margin."

        # Sort distribution
        sorted_actions = sorted(dist.items(), key=lambda x: x[1], reverse=True)
        top_action, top_prob = sorted_actions[0]
        second_prob = sorted_actions[1][1] if len(sorted_actions) > 1 else 0.0
        margin = top_prob - second_prob

        # Simulating ~20ms model prefill latency
        elapsed_ms = (time.perf_counter() - start_time) * 1000.0 + 18.5

        return ArbitrationResult(
            selected_action=RemediationAction(top_action),
            confidence=top_prob,
            margin=round(margin, 4),
            distribution=dist,
            latency_ms=round(elapsed_ms, 2),
            engine_name="MockJevEngine",
            reason=reason,
        )


class HttpJevClient(BaseJevEngine):
    """
    Production HTTP client interfacing with AnyJev / llm2jev / vLLM-jev serving endpoint.
    Calls POST /v1/systemone with raw context and retrieves normalized logits.
    """

    def __init__(self, endpoint: str = settings.jev_endpoint, timeout_ms: int = settings.request_timeout_ms):
        self.endpoint = endpoint
        self.timeout = timeout_ms / 1000.0
        self.fallback_engine = MockJevEngine()

    def _build_prompt(self, ctx: FailureContext) -> str:
        return (
            f"<|system|>\nYou are an ultra-low latency SRE decision arbitrator.\n"
            f"<|telemetry|>\n"
            f"Service: {ctx.service_name}, Pod: {ctx.pod_name or 'N/A'}\n"
            f"CPU: {ctx.cpu_usage_pct}%, Memory: {ctx.memory_usage_pct}%\n"
            f"Connections: {ctx.active_connections}/{ctx.max_connections}\n"
            f"Rule: {ctx.trigger_rule}\n"
            f"<|logs|>\n{ctx.error_logs[:1000]}\n"
            f"<|question|>\nChoose the optimal SOP action from: "
            f"RESTART_POD, DRAIN_TRAFFIC, FORCE_KILL_LONG_TRANSACTION, SCALE_OUT, ESCALATE_HUMAN."
        )

    async def evaluate(self, ctx: FailureContext) -> ArbitrationResult:
        start_time = time.perf_counter()
        prompt = self._build_prompt(ctx)
        payload = {
            "model": settings.jev_model_name,
            "prompt": prompt,
            "choices": [a.value for a in RemediationAction],
            "temperature": 0.0,
        }

        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.post(self.endpoint, json=payload)
                resp.raise_for_status()
                data = resp.json()

                dist: Dict[str, float] = data.get("probabilities", {})
                if not dist:
                    raise ValueError("Empty probability distribution returned from Jev service.")

                sorted_actions = sorted(dist.items(), key=lambda x: x[1], reverse=True)
                top_action, top_prob = sorted_actions[0]
                second_prob = sorted_actions[1][1] if len(sorted_actions) > 1 else 0.0
                margin = top_prob - second_prob
                elapsed_ms = (time.perf_counter() - start_time) * 1000.0

                return ArbitrationResult(
                    selected_action=RemediationAction(top_action),
                    confidence=top_prob,
                    margin=round(margin, 4),
                    distribution=dist,
                    latency_ms=round(elapsed_ms, 2),
                    engine_name="RemoteHttpJev",
                    reason="Model calibrated logits prediction from Jev service.",
                )
        except Exception as err:
            # Fallback to local mock engine to maintain SRE availability
            res = await self.fallback_engine.evaluate(ctx)
            res.engine_name = f"HttpJevFallback (Error: {type(err).__name__})"
            return res
