"""
Shadow Mode & Decision Accuracy Scoreboard:
Enables risk-free pre-production observation and evaluates Jev decision accuracy
against human SRE ground truth without executing disruptive physical actions.
"""

import time
import os
from typing import Dict, List, Optional
from pydantic import BaseModel, Field
from sentry.schemas import RemediationAction, FailureContext, ArbitrationResult


class ShadowPredictionRecord(BaseModel):
    incident_id: str
    service_name: str
    namespace: str
    trigger_rule: str
    suggested_action: RemediationAction
    confidence: float
    human_ground_truth: Optional[RemediationAction] = None
    is_match: Optional[bool] = None
    timestamp: float


class AccuracyScoreboard(BaseModel):
    total_shadow_predictions: int
    evaluated_with_ground_truth: int
    matched_predictions: int
    accuracy_rate_pct: float
    action_distribution: Dict[str, int] = Field(default_factory=dict)
    matches_by_action: Dict[str, int] = Field(default_factory=dict)


class ShadowModeManager:
    """
    Manages non-disruptive shadow evaluation and accuracy metrics tracking.
    """

    def __init__(self, global_shadow_mode: bool = False):
        self.global_shadow_mode = global_shadow_mode or os.getenv("SENTRY_SHADOW_MODE", "false").lower() in ("true", "1")
        self.service_overrides: Dict[str, bool] = {}
        self.records: Dict[str, ShadowPredictionRecord] = {}

    def is_shadow_mode_active(self, service_name: str) -> bool:
        if service_name in self.service_overrides:
            return self.service_overrides[service_name]
        return self.global_shadow_mode

    def set_service_shadow_mode(self, service_name: str, enabled: bool):
        self.service_overrides[service_name] = enabled

    def record_prediction(
        self,
        incident_id: str,
        ctx: FailureContext,
        arbitration: ArbitrationResult,
    ) -> ShadowPredictionRecord:
        rec = ShadowPredictionRecord(
            incident_id=incident_id,
            service_name=ctx.service_name,
            namespace=ctx.namespace,
            trigger_rule=ctx.trigger_rule,
            suggested_action=arbitration.selected_action,
            confidence=arbitration.confidence,
            timestamp=time.time(),
        )
        self.records[incident_id] = rec
        return rec

    def record_ground_truth(self, incident_id: str, human_action: RemediationAction) -> Optional[ShadowPredictionRecord]:
        rec = self.records.get(incident_id)
        if not rec:
            return None

        rec.human_ground_truth = human_action
        rec.is_match = (rec.suggested_action == human_action)
        return rec

    def get_scoreboard(self) -> AccuracyScoreboard:
        total = len(self.records)
        evaluated = [r for r in self.records.values() if r.human_ground_truth is not None]
        eval_count = len(evaluated)
        matches = [r for r in evaluated if r.is_match]
        match_count = len(matches)

        rate = (match_count / eval_count * 100.0) if eval_count > 0 else 0.0

        act_dist: Dict[str, int] = {}
        act_match: Dict[str, int] = {}

        for r in self.records.values():
            act_name = r.suggested_action.value
            act_dist[act_name] = act_dist.get(act_name, 0) + 1

        for r in matches:
            act_name = r.suggested_action.value
            act_match[act_name] = act_match.get(act_name, 0) + 1

        return AccuracyScoreboard(
            total_shadow_predictions=total,
            evaluated_with_ground_truth=eval_count,
            matched_predictions=match_count,
            accuracy_rate_pct=round(rate, 2),
            action_distribution=act_dist,
            matches_by_action=act_match,
        )


shadow_manager = ShadowModeManager()
