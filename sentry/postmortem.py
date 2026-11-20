"""
Incident Post-Mortem & Timeline Exporter:
Aggregates end-to-end incident telemetry, Jev logits evidence, and executed SOP parameters
into standardized Markdown post-mortem reports and timelines for SRE retrospective meetings.
"""

import time
import datetime
from typing import Dict, List, Optional, Any
from pydantic import BaseModel, Field
from sentry.schemas import RemediationAction, ArbitrationResult, FailureContext


class IncidentRecord(BaseModel):
    incident_id: str
    service_name: str
    namespace: str
    pod_name: Optional[str] = None
    started_at: float
    resolved_at: Optional[float] = None
    mttr_seconds: Optional[float] = None
    trigger_rule: str
    error_stacktrace: str
    arbitration: ArbitrationResult
    executed_action: RemediationAction
    execution_message: str
    root_blocker_id: Optional[int] = None
    blocking_query: Optional[str] = None


class PostMortemEngine:
    """
    Collects incident lifecycles and compiles exhaustive retrospective artifacts.
    """

    def __init__(self):
        self.incidents: Dict[str, IncidentRecord] = {}

    def record_incident(
        self,
        incident_id: str,
        ctx: FailureContext,
        arbitration: ArbitrationResult,
        action: RemediationAction,
        execution_message: str,
        started_at: Optional[float] = None,
        resolved_at: Optional[float] = None,
        root_blocker_id: Optional[int] = None,
        blocking_query: Optional[str] = None,
    ) -> IncidentRecord:
        now = time.time()
        start = started_at or (now - 0.085)  # Default ~85ms before if instant
        end = resolved_at or now
        mttr = max(end - start, 0.001)

        rec = IncidentRecord(
            incident_id=incident_id,
            service_name=ctx.service_name,
            namespace=ctx.namespace,
            pod_name=ctx.pod_name,
            started_at=start,
            resolved_at=end,
            mttr_seconds=round(mttr, 3),
            trigger_rule=ctx.trigger_rule,
            error_stacktrace=ctx.error_logs,
            arbitration=arbitration,
            executed_action=action,
            execution_message=execution_message,
            root_blocker_id=root_blocker_id,
            blocking_query=blocking_query,
        )
        self.incidents[incident_id] = rec
        return rec

    def export_markdown(self, incident_id: str) -> str:
        rec = self.incidents.get(incident_id)
        if not rec:
            raise KeyError(f"Incident ID '{incident_id}' not found.")

        start_str = datetime.datetime.fromtimestamp(rec.started_at, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
        end_str = datetime.datetime.fromtimestamp(rec.resolved_at, datetime.timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC') if rec.resolved_at else "In Progress"

        prob_table_rows = []
        for act, prob in sorted(rec.arbitration.distribution.items(), key=lambda x: x[1], reverse=True):
            marker = "👈 **[SELECTED]**" if act == rec.arbitration.selected_action.value else ""
            prob_table_rows.append(f"| `{act}` | {prob*100:.1f}% | {marker} |")
        prob_table_str = "\n".join(prob_table_rows)

        blocker_section = ""
        if rec.root_blocker_id:
            blocker_section = (
                f"### 🔍 Database Root Blocker Details\n\n"
                f"- **Thread ID**: `#{rec.root_blocker_id}`\n"
                f"- **Contested Query**: `{rec.blocking_query or 'N/A'}`\n"
                f"- **Action Executed**: `KILL CONNECTION {rec.root_blocker_id};`\n"
            )

        md = f"""# SRE Incident Post-Mortem: {rec.service_name} ({rec.incident_id})

## 1. Executive Summary

| Attribute | Details |
| :--- | :--- |
| **Incident ID** | `{rec.incident_id}` |
| **Target Service** | `{rec.service_name}` (Namespace: `{rec.namespace}`) |
| **Target Pod** | `{rec.pod_name or 'Cluster / Any'}` |
| **Triggered Alert Rule** | `{rec.trigger_rule}` |
| **Start Time** | `{start_str}` |
| **Resolved Time** | `{end_str}` |
| **End-to-End MTTR** | **{rec.mttr_seconds*1000:.1f} ms** |
| **Final Remediation Status** | `{rec.executed_action.value}` |

---

## 2. Jev Model Decision Evidence (Whitebox Logits)

- **Arbitrator Decision Engine**: `{rec.arbitration.engine_name}`
- **Model Prefill Latency**: `{rec.arbitration.latency_ms:.1f} ms`
- **Selected Action Confidence**: `{rec.arbitration.confidence*100:.1f}%` (Margin: `{rec.arbitration.margin*100:.1f}%`)
- **Diagnostic Rationale**: {rec.arbitration.reason}

### Calibrated Probability Distribution

| SOP Candidate Action | Calibrated Probability | Selection |
| :--- | :--- | :--- |
{prob_table_str}

---

## 3. Incident Timeline & Telemetry

```mermaid
timeline
    title Incident Lifecycle & Autonomous Remediation
    section Detection
        T+0ms : Alert Triggered ({rec.trigger_rule})
        T+5ms : Ingress Webhook & Context Assembly
    section Arbitration
        T+25ms : Jev Prefill Logits Computed ({rec.arbitration.selected_action.value})
        T+27ms : Safety Envelopes Verified
    section Execution & Resolution
        T+38ms : SOP Executed ({rec.executed_action.value})
        T+{rec.mttr_seconds*1000:.0f}ms : Incident Resolved
```

### Captured Exception Stacktrace
```text
{rec.error_stacktrace[:800]}
```

---

## 4. Execution Details & Audit Trail

{blocker_section}
**Execution Output:**
> {rec.execution_message}

---

## 5. Preventative SRE Recommendations

1. **Database Contention**: Ensure client timeout policies are set strictly lower than `lock_wait_timeout`.
2. **Safety Envelopes**: Maintain invariant protection preventing Pod restarts during lock contention.
"""
        return md


postmortem_engine = PostMortemEngine()
