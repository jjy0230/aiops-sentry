# AIOps-Sentry

AIOps-Sentry is an ultra-low latency remediation arbitrator for Kubernetes microservices using structured decision models (Jev).

Instead of relying on slow generative LLMs that output verbose text (taking 3-10 seconds), AIOps-Sentry leverages single-pass prefill logits evaluation (~20ms) over a discrete action space combined with strict safety envelope guardrails.

## Features

- **Sub-100ms End-to-End Remediation**: ~20ms model prefill decision + safety verification + fast execution dispatch.
- **Calibrated SOP Action Space**:
  - `FORCE_KILL_LONG_TRANSACTION`: Targeted Root Blocker thread kill via LockGraphResolver DAG.
  - `DRAIN_TRAFFIC`: Millisecond Pod label isolation without destroying instance state.
  - `RESTART_POD`: Graceful Pod eviction and reconstruction for memory exhaustion.
  - `SCALE_OUT`: Controlled horizontal scaling under strict replica quota.
  - `ESCALATE_HUMAN`: Automated escalation to on-call engineers.
- **Safety Envelopes & Blast Radius Guard**:
  - Confidence & Margin thresholds.
  - Physical invariant checks (e.g., preventing Pod restarts during DB lock contention to avoid cascading thundering herds).
  - Distributed Lease Guard: Uses Kubernetes-native `coordination.k8s.io/v1` `Lease` for multi-replica anti-flapping coordination without Redis.
- **Two-Tier Remediation**:
  - Tier-1: Kubernetes resource controller (Pod drain, restart, scale).
  - Tier-2: Database lock contention resolver (MySQL/PostgreSQL lock-wait DAG traversal to isolate Root Blocker).
- **Human-in-the-Loop ChatOps**:
  - Feishu & DingTalk interactive card dispatch.
  - HMAC-signed, single-use, replay-protected approval tokens.
  - One-click authorization callback endpoint (`POST /v1/sentry/approval`).
- **Zero-Dependency Quickstart**: Built-in mock decision engine for local development and CI testing.

## Quickstart

```bash
git clone https://github.com/jjy0230/aiops-sentry.git
cd aiops-sentry

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
python tests/run_tests.py
```

## Running the Server

```bash
uvicorn sentry.server:app --host 0.0.0.0 --port 8080 --reload
```

Interactive API documentation is available at `http://localhost:8080/docs`.

## Alertmanager Integration

Configure Prometheus Alertmanager webhook receiver:

```yaml
receivers:
  - name: 'aiops-sentry'
    webhook_configs:
      - url: 'http://aiops-sentry:8080/v1/sentry/webhook/alertmanager'
        send_resolved: false
```
