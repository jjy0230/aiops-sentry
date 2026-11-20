from enum import Enum
from typing import Dict, List, Optional
from pydantic import BaseModel, Field


class RemediationAction(str, Enum):
    RESTART_POD = "RESTART_POD"
    DRAIN_TRAFFIC = "DRAIN_TRAFFIC"
    FORCE_KILL_LONG_TRANSACTION = "FORCE_KILL_LONG_TRANSACTION"
    SCALE_OUT = "SCALE_OUT"
    ESCALATE_HUMAN = "ESCALATE_HUMAN"


class FailureContext(BaseModel):
    service_name: str
    namespace: str = "default"
    pod_name: Optional[str] = None
    node_name: Optional[str] = None
    
    # Telemetry metrics
    cpu_usage_pct: float = 0.0
    memory_usage_pct: float = 0.0
    active_connections: int = 0
    max_connections: int = 100
    p99_latency_ms: float = 0.0
    
    # Log and stacktrace snippet
    error_logs: str = ""
    trigger_rule: str = ""
    extra_labels: Dict[str, str] = Field(default_factory=dict)


class AlertItem(BaseModel):
    status: str = "firing"
    labels: Dict[str, str] = Field(default_factory=dict)
    annotations: Dict[str, str] = Field(default_factory=dict)
    startsAt: Optional[str] = None


class AlertmanagerWebhook(BaseModel):
    version: str = "4"
    groupKey: Optional[str] = None
    status: str = "firing"
    receiver: Optional[str] = None
    alerts: List[AlertItem] = Field(default_factory=list)
    commonLabels: Dict[str, str] = Field(default_factory=dict)
    commonAnnotations: Dict[str, str] = Field(default_factory=dict)


class ActionProbability(BaseModel):
    action: RemediationAction
    probability: float


class ArbitrationResult(BaseModel):
    selected_action: RemediationAction
    confidence: float
    margin: float
    distribution: Dict[str, float]
    latency_ms: float
    engine_name: str
    reason: str


class RemediationStatus(str, Enum):
    EXECUTED = "EXECUTED"
    ESCALATED = "ESCALATED"
    BLOCKED_BY_SAFETY = "BLOCKED_BY_SAFETY"
    COOLED_DOWN = "COOLED_DOWN"
    DRY_RUN = "DRY_RUN"


class RemediationExecutionResult(BaseModel):
    action: RemediationAction
    status: RemediationStatus
    executed: bool
    requires_human_approval: bool
    safety_verdict: str
    execution_message: str
    target_service: str
    target_pod: Optional[str] = None
    arbitration: ArbitrationResult
