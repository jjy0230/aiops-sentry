import time
import logging
import asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Response
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
from sentry.storm_guard import AlertFingerprintDeduplicator, StormConcurrencyPool
from sentry.closed_loop import PostRemediationProbeManager
from sentry.telemetry import telemetry

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("AIOps-Sentry")


class SentryCore:
    def __init__(self):
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
        self.deduplicator = AlertFingerprintDeduplicator(window_seconds=3.0)
        self.concurrency_pool = StormConcurrencyPool(max_concurrency=10)
        self.probe_manager = PostRemediationProbeManager(
            executor_dispatch_fn=self.executor.execute,
            verification_delay_seconds=2.0,
        )

    async def process_context(self, ctx: FailureContext) -> RemediationExecutionResult:
        start_time = time.perf_counter()

        # Step 1: Fast single-pass Jev forward arbitration (~20ms)
        arbitration = await self.engine.evaluate(ctx)

        # Step 2: Invariant safety envelope check (~2ms)
        status, final_action, verdict, requires_approval = self.safety.verify(ctx, arbitration)

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

            # Schedule post-remediation verification probe
            job_id = f"job-{ctx.service_name}-{int(time.time()*1000)}"
            self.probe_manager.schedule_verification(
                job_id=job_id,
                ctx=ctx,
                action=final_action,
            )
        else:
            # Escalated, Cooled down, or Blocked by Safety Envelopes -> Human in the loop
            exec_message = f"Remediation blocked/escalated. Reason: {verdict}"
            telemetry.record_safety_block(status.value)

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

        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        telemetry.record_arbitration(ctx.service_name, final_action.value, status.value, elapsed_ms)

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
    version="2.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/metrics")
async def prometheus_metrics():
    """Prometheus self-observability metrics endpoint."""
    return Response(content=telemetry.export_prometheus_text(), media_type="text/plain; version=0.0.4")


@app.get("/v1/sentry/health")
async def health():
    return {
        "status": "healthy",
        "engine": "MockJevEngine" if settings.use_mock_engine else "RemoteHttpJev",
        "cooldown_seconds": settings.cooldown_seconds,
        "min_confidence": settings.min_confidence_threshold,
        "storm_deduplicated_count": core.deduplicator.total_deduplicated_count,
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
    High-throughput Prometheus Alertmanager webhook receiver.
    Applies 3s sliding-window fingerprint deduplication and bounded-concurrency execution.
    """
    parsed_contexts = []

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
        parsed_contexts.append(ctx)

    # Apply 3s Sliding Window Fingerprint Deduplication
    contexts_to_arbitrate = []
    for ctx in parsed_contexts:
        is_new, group = core.deduplicator.process_incoming(ctx)
        if is_new:
            contexts_to_arbitrate.append(ctx)
        else:
            telemetry.record_deduplication(ctx.service_name)

    # Process in bounded concurrency pool
    tasks = [
        core.concurrency_pool.execute_bounded(core.process_context(ctx))
        for ctx in contexts_to_arbitrate
    ]
    results = await asyncio.gather(*tasks)

    return {
        "received_alerts": len(parsed_contexts),
        "arbitrated_count": len(results),
        "deduplicated_suppressed": len(parsed_contexts) - len(results),
        "decisions": results,
    }
