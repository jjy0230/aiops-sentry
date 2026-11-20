import time
import logging
import asyncio
import uuid
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Request, Response
from fastapi.responses import HTMLResponse
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
    RestoreRequest,
    RestoreResponse,
    GroundTruthSubmission,
    ServiceDependencyEdgeInput,
)
from sentry.engine import BaseJevEngine, MockJevEngine, HttpJevClient
from sentry.safety import SafetyEnvelope, BlastRadiusGuard
from sentry.executor import CompositeRemediationExecutor
from sentry.human_gate import ApprovalTokenManager, InteractiveCardBuilder
from sentry.storm_guard import AlertFingerprintDeduplicator, StormConcurrencyPool
from sentry.closed_loop import PostRemediationProbeManager
from sentry.telemetry import telemetry
from sentry.restorer import ServiceRestorer
from sentry.postmortem import postmortem_engine
from sentry.shadow_mode import shadow_manager
from sentry.topology import cascade_suppressor
from sentry.ui import get_ui_html

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
        self.restorer = ServiceRestorer()
        self.topology_suppressor = cascade_suppressor

    async def process_context(self, ctx: FailureContext) -> RemediationExecutionResult:
        start_time = time.perf_counter()
        incident_id = f"inc-{ctx.service_name}-{uuid.uuid4().hex[:8]}"

        # Step 1: Evaluate Microservice Dependency Topology Cascade
        cascade_verdict = self.topology_suppressor.evaluate_cascade(ctx)
        if cascade_verdict.should_suppress:
            status = RemediationStatus.CASCADE_SUPPRESSED
            verdict_msg = cascade_verdict.verdict_message
            arbitration = ArbitrationResult(
                selected_action=RemediationAction.ESCALATE_HUMAN,
                confidence=1.0,
                margin=1.0,
                distribution={RemediationAction.ESCALATE_HUMAN.value: 1.0},
                latency_ms=1.5,
                engine_name="TopologyCascadeSuppressor",
                reason=verdict_msg,
            )
            elapsed_ms = (time.perf_counter() - start_time) * 1000.0
            telemetry.record_arbitration(ctx.service_name, RemediationAction.ESCALATE_HUMAN.value, status.value, elapsed_ms)
            telemetry.record_safety_block("CASCADE_SUPPRESSED")

            return RemediationExecutionResult(
                incident_id=incident_id,
                action=RemediationAction.ESCALATE_HUMAN,
                status=status,
                executed=False,
                requires_human_approval=False,
                safety_verdict=verdict_msg,
                execution_message=verdict_msg,
                target_service=ctx.service_name,
                target_pod=ctx.pod_name,
                arbitration=arbitration,
            )

        # Step 2: Fast single-pass Jev forward arbitration (~20ms)
        arbitration = await self.engine.evaluate(ctx)

        # Step 3: Check Shadow Mode (Dry-Run observation)
        is_shadow = shadow_manager.is_shadow_mode_active(ctx.service_name)
        if is_shadow:
            shadow_manager.record_prediction(incident_id, ctx, arbitration)
            status = RemediationStatus.SHADOW_RECORDED
            verdict = f"[Shadow Mode Active] Recorded prediction: {arbitration.selected_action.value} ({arbitration.confidence*100:.1f}%) without physical execution."
            elapsed_ms = (time.perf_counter() - start_time) * 1000.0
            telemetry.record_arbitration(ctx.service_name, arbitration.selected_action.value, status.value, elapsed_ms)
            
            postmortem_engine.record_incident(
                incident_id=incident_id,
                ctx=ctx,
                arbitration=arbitration,
                action=arbitration.selected_action,
                execution_message=verdict,
                started_at=start_time,
                resolved_at=time.time(),
            )

            return RemediationExecutionResult(
                incident_id=incident_id,
                action=arbitration.selected_action,
                status=status,
                executed=False,
                requires_human_approval=False,
                safety_verdict=verdict,
                execution_message=verdict,
                target_service=ctx.service_name,
                target_pod=ctx.pod_name,
                arbitration=arbitration,
            )

        # Step 4: Invariant safety envelope check (~2ms)
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

            # Register active root failure to shield upstream victim callers for 30 seconds
            self.topology_suppressor.register_root_failure(
                service_name=ctx.service_name,
                action=final_action,
                ctx=ctx,
                ttl_seconds=30.0,
            )

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

            token_str = self.token_manager.create_token(
                action=arbitration.selected_action,
                ctx=ctx,
                reason=verdict,
                trace_id=incident_id,
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

        postmortem_engine.record_incident(
            incident_id=incident_id,
            ctx=ctx,
            arbitration=arbitration,
            action=final_action,
            execution_message=exec_message,
            started_at=start_time,
            resolved_at=time.time(),
            root_blocker_id=1024 if final_action == RemediationAction.FORCE_KILL_LONG_TRANSACTION else None,
            blocking_query="SELECT * FROM orders FOR UPDATE" if final_action == RemediationAction.FORCE_KILL_LONG_TRANSACTION else None,
        )

        elapsed_ms = (time.perf_counter() - start_time) * 1000.0
        telemetry.record_arbitration(ctx.service_name, final_action.value, status.value, elapsed_ms)

        return RemediationExecutionResult(
            incident_id=incident_id,
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
    version="4.0.0",
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


@app.get("/ui", response_class=HTMLResponse)
async def visual_console():
    """Interactive dark-mode real-time visual console for SRE demonstrations."""
    return HTMLResponse(content=get_ui_html())


@app.get("/v1/sentry/health")
async def health():
    return {
        "status": "healthy",
        "engine": "MockJevEngine" if settings.use_mock_engine else "RemoteHttpJev",
        "cooldown_seconds": settings.cooldown_seconds,
        "min_confidence": settings.min_confidence_threshold,
        "storm_deduplicated_count": core.deduplicator.total_deduplicated_count,
        "recorded_incidents_count": len(postmortem_engine.incidents),
        "topology_suppressed_count": len(core.topology_suppressor.suppression_history),
    }


@app.post("/v1/sentry/arbitrate", response_model=RemediationExecutionResult)
async def arbitrate(ctx: FailureContext):
    """Direct contextual arbitration endpoint for integration, CLI testing, or automated probes."""
    return await core.process_context(ctx)


@app.get("/v1/sentry/topology")
async def get_service_topology():
    """Retrieves current microservice dependency directed call-graph."""
    return {"topology": core.topology_suppressor.topology.to_dict()}


@app.post("/v1/sentry/topology/edge")
async def add_service_topology_edge(edge: ServiceDependencyEdgeInput):
    """Dynamically adds or updates a service dependency call-edge."""
    core.topology_suppressor.topology.add_edge(edge.upstream_service, edge.downstream_service)
    return {
        "status": "added",
        "upstream": edge.upstream_service,
        "downstream": edge.downstream_service,
    }


@app.get("/v1/sentry/topology/suppressed")
async def get_suppressed_cascading_alerts():
    """Retrieves audit history of upstream cascading victim alerts intercepted by the topology suppressor."""
    return {
        "total_suppressed": len(core.topology_suppressor.suppression_history),
        "history": core.topology_suppressor.suppression_history,
    }


@app.post("/v1/sentry/restore", response_model=RestoreResponse)
async def restore_service(req: RestoreRequest):
    """Reverse lifecycle restorer: un-drains isolated pods and scales back to baseline replicas."""
    messages = []
    success = True

    if req.pod_name:
        s_pod, msg_pod = await core.restorer.restore_traffic(req.namespace, req.pod_name)
        messages.append(msg_pod)
        if not s_pod:
            success = False

    if req.service_name:
        s_scale, msg_scale = await core.restorer.scale_in_to_baseline(req.namespace, req.service_name, req.baseline_replicas)
        messages.append(msg_scale)
        if not s_scale:
            success = False

    combined = " | ".join(messages) if messages else "No restore targets specified."
    return RestoreResponse(success=success, message=combined, target=req.pod_name or req.service_name or "none")


@app.get("/v1/sentry/postmortem/{incident_id}")
async def get_incident_postmortem(incident_id: str):
    """Retrieves full incident JSON record."""
    rec = postmortem_engine.incidents.get(incident_id)
    if not rec:
        raise HTTPException(status_code=404, detail=f"Incident '{incident_id}' not found.")
    return rec


@app.get("/v1/sentry/postmortem/{incident_id}/markdown")
async def get_incident_markdown(incident_id: str):
    """Exports standardized SRE Incident Post-Mortem in GitHub-Flavored Markdown."""
    try:
        md = postmortem_engine.export_markdown(incident_id)
        return Response(content=md, media_type="text/markdown")
    except KeyError:
        raise HTTPException(status_code=404, detail=f"Incident '{incident_id}' not found.")


@app.get("/v1/sentry/shadow/scoreboard")
async def get_shadow_scoreboard():
    """Retrieves prediction accuracy scoreboard evaluated against human SRE ground truth."""
    return shadow_manager.get_scoreboard()


@app.post("/v1/sentry/shadow/ground-truth")
async def submit_human_ground_truth(sub: GroundTruthSubmission):
    """Records human operator decision for shadow-mode accuracy benchmark."""
    rec = shadow_manager.record_ground_truth(sub.incident_id, sub.human_action)
    if not rec:
        raise HTTPException(status_code=404, detail=f"Shadow incident '{sub.incident_id}' not found.")
    return {"status": "recorded", "is_match": rec.is_match, "suggested": rec.suggested_action, "human": sub.human_action}


@app.post("/v1/sentry/approval", response_model=ApprovalResponse)
async def handle_human_approval(req: ApprovalRequest):
    """Human-in-the-loop authorization callback endpoint."""
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
    """High-throughput Prometheus Alertmanager webhook receiver with sliding-window deduplication."""
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

    contexts_to_arbitrate = []
    for ctx in parsed_contexts:
        is_new, group = core.deduplicator.process_incoming(ctx)
        if is_new:
            contexts_to_arbitrate.append(ctx)
        else:
            telemetry.record_deduplication(ctx.service_name)

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
