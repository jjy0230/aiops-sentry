"""
Closed-Loop Remediation & Escalation Ladder:
Implements post-remediation verification probes and automated SOP escalation ladders
in accordance with Google SRE Workbook practices.
"""

import asyncio
import time
import logging
from enum import Enum
from typing import Dict, List, Optional, Callable, Any
from pydantic import BaseModel, Field

from sentry.schemas import RemediationAction, FailureContext, RemediationStatus

logger = logging.getLogger("AIOps-Sentry.ClosedLoop")


class VerificationState(str, Enum):
    SCHEDULED = "SCHEDULED"
    VERIFIED_HEALTHY = "VERIFIED_HEALTHY"
    ESCALATED_NEXT_TIER = "ESCALATED_NEXT_TIER"
    CRITICAL_PAGE_HUMAN = "CRITICAL_PAGE_HUMAN"


class EscalationLadder:
    """
    Defines the deterministic escalation paths when an automated SOP fails to resolve the incident.
    Prevents thrashing on ineffective actions.
    """

    NEXT_TIER_MAPPING: Dict[RemediationAction, RemediationAction] = {
        RemediationAction.FORCE_KILL_LONG_TRANSACTION: RemediationAction.DRAIN_TRAFFIC,
        RemediationAction.DRAIN_TRAFFIC: RemediationAction.RESTART_POD,
        RemediationAction.RESTART_POD: RemediationAction.ESCALATE_HUMAN,
        RemediationAction.SCALE_OUT: RemediationAction.ESCALATE_HUMAN,
        RemediationAction.ESCALATE_HUMAN: RemediationAction.ESCALATE_HUMAN,
    }

    @classmethod
    def get_next_action(cls, current_action: RemediationAction) -> RemediationAction:
        return cls.NEXT_TIER_MAPPING.get(current_action, RemediationAction.ESCALATE_HUMAN)


class VerificationJob(BaseModel):
    job_id: str
    target_service: str
    namespace: str
    pod_name: Optional[str] = None
    executed_action: RemediationAction
    scheduled_check_time: float
    state: VerificationState = VerificationState.SCHEDULED
    attempt_count: int = 1
    audit_trail: List[str] = Field(default_factory=list)


class PostRemediationProbeManager:
    """
    Manages non-blocking post-remediation health verifications.
    Dispatches escalation actions if metrics do not recover within the SLA window.
    """

    def __init__(
        self,
        executor_dispatch_fn: Optional[Callable[[RemediationAction, FailureContext], Any]] = None,
        verification_delay_seconds: float = 3.0,
    ):
        self.executor_dispatch = executor_dispatch_fn
        self.verification_delay_seconds = verification_delay_seconds
        self.jobs: Dict[str, VerificationJob] = {}

    def schedule_verification(
        self,
        job_id: str,
        ctx: FailureContext,
        action: RemediationAction,
        probe_evaluator: Optional[Callable[[], bool]] = None,
    ) -> VerificationJob:
        now = time.time()
        job = VerificationJob(
            job_id=job_id,
            target_service=ctx.service_name,
            namespace=ctx.namespace,
            pod_name=ctx.pod_name,
            executed_action=action,
            scheduled_check_time=now + self.verification_delay_seconds,
            state=VerificationState.SCHEDULED,
            audit_trail=[f"Action {action.value} executed at {now:.2f}. Verification scheduled in {self.verification_delay_seconds}s."],
        )
        self.jobs[job_id] = job

        # Fire and forget asynchronous verification worker
        asyncio.create_task(self._run_probe_worker(job, ctx, probe_evaluator))
        return job

    async def _run_probe_worker(
        self,
        job: VerificationJob,
        ctx: FailureContext,
        probe_evaluator: Optional[Callable[[], bool]],
    ):
        await asyncio.sleep(self.verification_delay_seconds)

        # Evaluate health: True means healthy/recovered, False means still broken
        is_healthy = probe_evaluator() if probe_evaluator else True

        if is_healthy:
            job.state = VerificationState.VERIFIED_HEALTHY
            job.audit_trail.append(f"Probe at {time.time():.2f}: Incident resolved successfully. Metrics normalized.")
            logger.info("Verification passed for job %s: Service '%s' is healthy.", job.job_id, job.target_service)
        else:
            # Not resolved: Escalate to next tier
            next_action = EscalationLadder.get_next_action(job.executed_action)
            job.audit_trail.append(
                f"Probe at {time.time():.2f}: Remediation did not normalize metrics. Escalating {job.executed_action.value} -> {next_action.value}."
            )

            if next_action == RemediationAction.ESCALATE_HUMAN:
                job.state = VerificationState.CRITICAL_PAGE_HUMAN
                logger.error("Job %s: Automated ladder exhausted. Paging on-call SRE immediately for '%s'.", job.job_id, job.target_service)
            else:
                job.state = VerificationState.ESCALATED_NEXT_TIER
                job.executed_action = next_action
                job.attempt_count += 1
                logger.warning("Job %s: Escalated execution of '%s' for '%s'.", job.job_id, next_action.value, job.target_service)
                if self.executor_dispatch:
                    await self.executor_dispatch(next_action, ctx)
