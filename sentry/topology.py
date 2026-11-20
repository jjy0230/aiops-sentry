"""
Service Dependency Topology & Cascading Failure Suppressor:
Constructs microservice call-graph DAGs to differentiate between root cause failures
and innocent upstream victims experiencing cascading timeouts.
Prevents destructive remediation (e.g. misdirected pod restarts or horizontal scaling) on upstream services.
"""

import time
import logging
from typing import Dict, Set, List, Optional, Tuple
from pydantic import BaseModel, Field
from sentry.schemas import FailureContext, RemediationAction

logger = logging.getLogger("AIOps-Sentry.Topology")


class ServiceDependencyEdge(BaseModel):
    upstream_service: str
    downstream_service: str


class CascadeSuppressionVerdict(BaseModel):
    should_suppress: bool
    root_cause_service: Optional[str] = None
    verdict_message: str = ""


class ActiveRootFailure(BaseModel):
    service_name: str
    action: RemediationAction
    recorded_at: float
    expires_at: float
    trigger_rule: str


class TopologyGraph:
    """
    Directed Acyclic Graph representing microservice call dependencies.
    Edge A -> B means Service A calls Service B.
    """

    def __init__(self):
        self._downstreams: Dict[str, Set[str]] = {}
        self._upstreams: Dict[str, Set[str]] = {}
        self._init_default_topology()

    def _init_default_topology(self):
        # Default enterprise microservice call-chain
        defaults = [
            ("api-gateway", "order-service"),
            ("api-gateway", "auth-service"),
            ("order-service", "payment-service"),
            ("order-service", "inventory-service"),
            ("payment-service", "payment-mysql"),
            ("inventory-service", "inventory-redis"),
        ]
        for up, down in defaults:
            self.add_edge(up, down)

    def add_edge(self, upstream: str, downstream: str):
        self._downstreams.setdefault(upstream, set()).add(downstream)
        self._upstreams.setdefault(downstream, set()).add(upstream)

    def get_direct_downstreams(self, service: str) -> Set[str]:
        return self._downstreams.get(service, set())

    def get_all_downstreams(self, service: str) -> Set[str]:
        """Transitive BFS traversal of all downstream dependencies."""
        visited: Set[str] = set()
        queue = list(self._downstreams.get(service, set()))
        while queue:
            curr = queue.pop(0)
            if curr not in visited:
                visited.add(curr)
                queue.extend(self._downstreams.get(curr, set()))
        return visited

    def to_dict(self) -> Dict[str, List[str]]:
        return {k: sorted(list(v)) for k, v in self._downstreams.items()}


class CascadeSuppressor:
    """
    Tracks in-flight root cause outages and intercepts alerts from victim upstream services.
    """

    def __init__(self, topology: Optional[TopologyGraph] = None):
        self.topology = topology or TopologyGraph()
        self.active_root_failures: Dict[str, ActiveRootFailure] = {}
        self.suppression_history: List[Dict[str, Any]] = []

    def register_root_failure(
        self,
        service_name: str,
        action: RemediationAction,
        ctx: FailureContext,
        ttl_seconds: float = 30.0,
    ):
        """Registers a verified root failure on a service."""
        now = time.time()
        self.active_root_failures[service_name] = ActiveRootFailure(
            service_name=service_name,
            action=action,
            recorded_at=now,
            expires_at=now + ttl_seconds,
            trigger_rule=ctx.trigger_rule,
        )
        logger.info("Registered active root failure on '%s' (Action: %s, TTL: %.0fs)", service_name, action.value, ttl_seconds)

    def evaluate_cascade(self, ctx: FailureContext) -> CascadeSuppressionVerdict:
        """
        Evaluates whether the incoming failure context is merely an upstream victim.
        """
        now = time.time()
        self._cleanup_expired(now)

        # Get all downstream dependencies of current service
        downstreams = self.topology.get_all_downstreams(ctx.service_name)
        if not downstreams:
            return CascadeSuppressionVerdict(should_suppress=False)

        # Check if any active root failure exists in downstreams
        logs_lower = ctx.error_logs.lower()
        is_cascading_symptom = (
            "timeout" in logs_lower
            or "connection refused" in logs_lower
            or "504" in logs_lower
            or "502" in logs_lower
            or "upstream connect error" in logs_lower
            or "circuitbreaker" in logs_lower
        )

        for down_svc in downstreams:
            active_failure = self.active_root_failures.get(down_svc)
            if active_failure is not None:
                # Upstream is a victim of an active downstream outage!
                msg = (
                    f"Cascade Suppression Activated: '{ctx.service_name}' is an upstream caller of '{down_svc}' "
                    f"which is currently undergoing root outage remediation ({active_failure.action.value} for {active_failure.trigger_rule}). "
                    f"Suppressed destructive pod restarts or scaling on victim service."
                )
                logger.warning(msg)
                
                record = {
                    "victim_service": ctx.service_name,
                    "root_cause_service": down_svc,
                    "downstream_action": active_failure.action.value,
                    "timestamp": now,
                    "reason": msg,
                }
                self.suppression_history.append(record)
                if len(self.suppression_history) > 500:
                    self.suppression_history.pop(0)

                return CascadeSuppressionVerdict(
                    should_suppress=True,
                    root_cause_service=down_svc,
                    verdict_message=msg,
                )

        return CascadeSuppressionVerdict(should_suppress=False)

    def _cleanup_expired(self, now: float):
        expired = [k for k, v in self.active_root_failures.items() if now > v.expires_at]
        for k in expired:
            del self.active_root_failures[k]

    def reset(self):
        self.active_root_failures.clear()
        self.suppression_history.clear()


cascade_suppressor = CascadeSuppressor()
