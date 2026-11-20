# AIOps-Sentry

[![Live Demo](https://img.shields.io/badge/Live_Demo-GitHub_Pages-blue.svg)](https://jjy0230.github.io/aiops-sentry/)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Python 3.10+](https://img.shields.io/badge/Python-3.10%2B-brightgreen.svg)](https://www.python.org/)

> 🚀 **Live Interactive Web Console**: Try the zero-latency autonomous remediation simulator directly in your browser: [https://jjy0230.github.io/aiops-sentry/](https://jjy0230.github.io/aiops-sentry/)

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
- **Dark-Mode Visual Console (`/ui`)**:
  - Built-in responsive single-page web dashboard at `http://localhost:8080/ui`.
  - Displays real-time microservice DAG topology with breathing health indicators.
  - Live Jev logits distribution probability bars, audit logs, and interactive chaos buttons.
- **Interactive Chaos Simulator CLI (`sentry.cli`)**:
  - One-click terminal simulation: `python -m sentry.cli simulate --scenario deadlock|cascade|oom`.
- **Microservice Dependency Topology & Cascade Suppressor**:
  - Maintains directed call-graph DAGs. Automatically identifies and shields innocent upstream caller services (victims) from destructive restarts or scaling when downstreams experience root outages.
- **Reverse Lifecycle & Service Restorer**:
  - Un-drains isolated Pods and scales deployments back to baseline replicas via `POST /v1/sentry/restore`.
- **Incident Post-Mortem & Timeline Exporter**:
  - Generates structured Markdown post-mortem reports and Mermaid incident timelines via `GET /v1/sentry/postmortem/{id}/markdown`.
- **Shadow Mode & Accuracy Scoreboard**:
  - Non-disruptive pre-production observation; records predictions and compares against human SRE ground truth (`GET /v1/sentry/shadow/scoreboard`).
- **Storm Guard (Alert Flood Suppression)**:
  - 3s sliding-window fingerprint deduplication and bounded-concurrency execution pool.
- **Closed-Loop Verification & Escalation Ladder**:
  - Post-remediation health probes; automatically escalates to next-tier SOPs if metrics fail to recover.
- **Safety Envelopes & Blast Radius Guard**:
  - Confidence & Margin thresholds.
  - Physical invariant checks (e.g., preventing Pod restarts during DB lock contention).
  - Distributed Lease Guard: Uses Kubernetes-native `coordination.k8s.io/v1` `Lease` for multi-replica coordination without Redis.
- **Self-Observability**:
  - Prometheus `/metrics` exposition and pre-configured Grafana dashboard.

## Quickstart

```bash
git clone https://github.com/jjy0230/aiops-sentry.git
cd aiops-sentry

python -m venv .venv
source .venv/bin/activate

pip install -r requirements.txt
python tests/run_tests.py
```

## Running the Server & Visual Console

```bash
uvicorn sentry.server:app --host 0.0.0.0 --port 8080 --reload
```

- **Interactive Visual Console**: Open [http://localhost:8080/ui](http://localhost:8080/ui) in your browser.
- **Swagger OpenAPI Docs**: Open [http://localhost:8080/docs](http://localhost:8080/docs).

## One-Click Chaos Simulation CLI

```bash
# Simulate database row-lock contention and pool exhaustion
python -m sentry.cli simulate --scenario deadlock

# Simulate multi-hop microservice cascading failure and victim shielding
python -m sentry.cli simulate --scenario cascade

# Simulate Pod JVM Heap OutOfMemoryError
python -m sentry.cli simulate --scenario oom
```
