import time
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware

from sentry.config import settings
from sentry.schemas import (
    FailureContext,
    AlertmanagerWebhook,
    ArbitrationResult,
    RemediationExecutionResult,
    RemediationStatus,
    RemediationAction,
    InteractiveCardDetails,
    ApprovalRequest,
    ApprovalResponse,
)
from sentry.engine import BaseJevEngine, MockJevEngine, HttpJevClient
from sentry.safety import SafetyEnvelope, BlastRadiusGuard
from sentry.executor import CompositeRemediationExecutor
from sentry.human_gate import ApprovalTokenManager, InteractiveCardBuilder

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("AIOps-Sentry")


class SentryCore:
    def __init__(self):
        # Choose engine
        if settings.use_mock_engine:
            self.engine: BaseJevEngine = MockJevEngine()
        else:
            self.engine = HttpJevClient(
                endpoint=settings.jev_endpoint,
                timeout_ms=settings.request_timeout_ms,
            )

        self.guard = BlastRadiusGuard(cooldown_seconds=settings.cooldown_seconds)
        self.safety = SafetyEnvelope(
            guard=self.guard,
            min_confidence=settings.min_confidence_threshold,
            min_margin=settings.min_margin_threshold,
        )
        self.executor = CompositeRemediationExecutor()
        self.token_manager = ApprovalTokenManager()

    async def process_context(self, ctx: FailureContext) -> RemediationExecutionResult:
        # Step 1: Fast single-pass Jev forward arbitration (~20ms)
        arbitration = await self.engine.evaluate(ctx)

        # Step 2: Invariant safety envelope check (~2ms)
        status, final_action, verdict, requires_approval = self.safety.verify(ctx, arbitration)

        # Step 3: Execution or Human Approval Escalation
        executed = False
        exec_message = ""
        approval_details = None

        if status == RemediationStatus.EXECUTED:
            # Action is safe to auto-execute
            success, msg = await self.executor.execute(final_action, ctx)
            executed = success
            exec_message = msg
            target_key = f"{ctx.namespace}:{ctx.service_name}:{ctx.pod_name or 'any'}"
            self.guard.record_execution(target_key)
        else:
            # Escalated, Cooled down, or Blocked by Safety Envelopes -> Human in the loop
            exec_message = f"Remediation blocked/escalated. Reason: {verdict}"
            
            # Generate cryptographic approval token and ChatOps cards
            token_str = self.token_manager.create_token(
                action=arbitration.selected_action,
                ctx=ctx,
                reason=verdict,
            )
            feishu_card = InteractiveCardBuilder.build_feishu_card(
                ctx=ctx,
                arbitration=arbitration,
                approval_token=token_str,
                verdict=verdict,
            )
            dingtalk_card = InteractiveCardBuilder.build_dingtalk_card(
                ctx=ctx,
                arbitration=arbitration,
                approval_token=token_str,
            )
            approval_details = InteractiveCardDetails(
                approval_token=token_str,
                feishu_card=feishu_card,
                dingtalk_card=dingtalk_card,
            )

        return RemediationExecutionResult(
            action=final_action,
            status=status,
            executed=executed,
            requires_human_approval=requires_approval,
            safety_verdict=verdict,
            execution_message=exec_message,
            target_service=ctx.service_name,
            target_pod=ctx.pod_name,
            arbitration=arbitration,
            approval_details=approval_details,
        )


core = SentryCore()


@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Initializing AIOps-Sentry Engine (%s)...", "Mock" if settings.use_mock_engine else "RemoteJev")
    yield
    logger.info("AIOps-Sentry shutdown complete.")


app = FastAPI(
    title="AIOps-Sentry",
    description="Sub-100ms Autonomous Remediation Arbitrator for Kubernetes & Microservices powered by Jev",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/v1/sentry/health")
async def health():
    return {
        "status": "healthy",
        "engine": "MockJevEngine" if settings.use_mock_engine else "RemoteHttpJev",
        "cooldown_seconds": settings.cooldown_seconds,
        "min_confidence": settings.min_confidence_threshold,
    }


@app.post("/v1/sentry/arbitrate", response_model=RemediationExecutionResult)
async def arbitrate(ctx: FailureContext):
    """Direct contextual arbitration endpoint for integration, CLI testing, or automated probes."""
    return await core.process_context(ctx)


@app.post("/v1/sentry/approval", response_model=ApprovalResponse)
async def handle_human_approval(req: ApprovalRequest):
    """
    Human-in-the-loop authorization callback endpoint.
    Validates token signature and executes approved SOP action.
    """
    valid, payload, err_msg = core.token_manager.verify_and_consume(req.approval_token)
    if not valid or not payload:
        raise HTTPException(status_code=400, detail=f"Approval authorization failed: {err_msg}")

    if not req.approved:
        logger.info("SOP execution rejected by operator '%s' for service '%s'", req.operator_id, payload.service_name)
        return ApprovalResponse(
            success=False,
            message=f"Action '{payload.action.value}' was rejected by operator {req.operator_id}.",
            executed_action=payload.action,
            target_service=payload.service_name,
            operator_id=req.operator_id,
        )

    # Reconstruct execution context from validated payload
    ctx = FailureContext(
        service_name=payload.service_name,
        namespace=payload.namespace,
        pod_name=payload.pod_name,
        error_logs=f"Approved by operator {req.operator_id}",
    )

    success, msg = await core.executor.execute(payload.action, ctx)
    target_key = f"{payload.namespace}:{payload.service_name}:{payload.pod_name or 'any'}"
    core.guard.record_execution(target_key)

    logger.info("Operator '%s' authorized action '%s' for '%s': %s", req.operator_id, payload.action.value, payload.service_name, msg)
    return ApprovalResponse(
        success=success,
        message=f"Operator {req.operator_id} approved: {msg}",
        executed_action=payload.action,
        target_service=payload.service_name,
        operator_id=req.operator_id,
    )


@app.post("/v1/sentry/webhook/alertmanager")
async def alertmanager_webhook(payload: AlertmanagerWebhook):
    """
    RFC-compliant Prometheus Alertmanager webhook receiver.
    Parses firing alerts, extracts telemetry & stacktraces, and triggers instant arbitration.
    """
    results = []
    for alert in payload.alerts:
        if alert.status != "firing":
            continue

        labels = {**payload.commonLabels, **alert.labels}
        annotations = {**payload.commonAnnotations, **alert.annotations}

        service_name = labels.get("service") or labels.get("app") or labels.get("job", "unknown-service")
        namespace = labels.get("namespace", "default")
        pod_name = labels.get("pod") or labels.get("instance")

        try:
            cpu_val = float(annotations.get("cpu_usage_pct", labels.get("cpu", "0.0")))
        except ValueError:
            cpu_val = 0.0

        try:
            mem_val = float(annotations.get("memory_usage_pct", labels.get("memory", "0.0")))
        except ValueError:
            mem_val = 0.0

        try:
            active_conn = int(annotations.get("active_connections", "0"))
        except ValueError:
            active_conn = 0

        logs = annotations.get("sentry.aiops/stacktrace") or annotations.get("error_logs") or annotations.get("description") or annotations.get("summary", "")
        rule = labels.get("alertname", "UnknownAlert")

        ctx = FailureContext(
            service_name=service_name,
            namespace=namespace,
            pod_name=pod_name,
            cpu_usage_pct=cpu_val,
            memory_usage_pct=mem_val,
            active_connections=active_conn,
            error_logs=logs,
            trigger_rule=rule,
            extra_labels=labels,
        )

        res = await core.process_context(ctx)
        results.append(res)

    return {"processed_alerts": len(results), "decisions": results}
