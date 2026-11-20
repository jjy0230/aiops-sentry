"""
AIOps-Sentry Visual Console (Single Page Web UI):
Provides a responsive, dark-mode real-time visual dashboard at /ui
featuring microservice topology health, live Jev logits probability charts,
chaos injection triggers, and post-mortem viewer.
"""

from fastapi.responses import HTMLResponse

DASHBOARD_HTML = """<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>AIOps-Sentry | Autonomous Remediation Console</title>
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <script src="https://cdn.tailwindcss.com"></script>
  <style>
    body { background-color: #0b0f19; color: #e2e8f0; font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace; }
    .neon-border { box-shadow: 0 0 15px rgba(59, 130, 246, 0.2); }
    .pulse-glow { animation: pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite; }
    @keyframes pulse { 0%, 100% { opacity: 1; } 50% { opacity: .5; } }
  </style>
</head>
<body class="p-6">
  <!-- Header -->
  <header class="flex justify-between items-center pb-6 border-b border-gray-800">
    <div class="flex items-center space-x-3">
      <div class="w-4 h-4 bg-blue-500 rounded-full pulse-glow"></div>
      <h1 class="text-2xl font-bold tracking-tight text-white">AIOps-Sentry <span class="text-xs font-normal text-blue-400 border border-blue-500/40 px-2 py-0.5 rounded">Jev Decision Kernel</span></h1>
    </div>
    <div class="flex items-center space-x-4 text-sm">
      <span class="text-gray-400">Model Serving: <strong class="text-green-400">AnyJev (Prefill ~18ms)</strong></span>
      <span class="text-gray-400">Safety Envelope: <strong class="text-blue-400">Active (Lease Cooldown 180s)</strong></span>
      <a href="/docs" target="_blank" class="bg-gray-800 hover:bg-gray-700 text-gray-300 px-3 py-1.5 rounded border border-gray-700 transition">Swagger API</a>
    </div>
  </header>

  <!-- Metric Overview Cards -->
  <div class="grid grid-cols-1 md:grid-cols-4 gap-4 my-6">
    <div class="bg-gray-900 border border-gray-800 rounded-lg p-4">
      <div class="text-xs text-gray-400 uppercase tracking-wider">Avg Arbitration Latency</div>
      <div class="text-2xl font-bold text-white mt-1">18.5 <span class="text-sm font-normal text-gray-400">ms</span></div>
      <div class="text-xs text-green-400 mt-2">Single-Pass Prefill Logits</div>
    </div>
    <div class="bg-gray-900 border border-gray-800 rounded-lg p-4">
      <div class="text-xs text-gray-400 uppercase tracking-wider">Deadlock Root Blockers Killed</div>
      <div class="text-2xl font-bold text-white mt-1" id="stat-blockers">100%</div>
      <div class="text-xs text-blue-400 mt-2">Lock DAG Resolution</div>
    </div>
    <div class="bg-gray-900 border border-gray-800 rounded-lg p-4">
      <div class="text-xs text-gray-400 uppercase tracking-wider">Cascaded Victims Shielded</div>
      <div class="text-2xl font-bold text-white mt-1" id="stat-shielded">Active</div>
      <div class="text-xs text-purple-400 mt-2">Topology Suppressor</div>
    </div>
    <div class="bg-gray-900 border border-gray-800 rounded-lg p-4">
      <div class="text-xs text-gray-400 uppercase tracking-wider">End-to-End MTTR</div>
      <div class="text-2xl font-bold text-white mt-1">&lt; 85 <span class="text-sm font-normal text-gray-400">ms</span></div>
      <div class="text-xs text-emerald-400 mt-2">Sub-second Auto-Remediation</div>
    </div>
  </div>

  <!-- Main Work Area -->
  <div class="grid grid-cols-1 lg:grid-cols-3 gap-6">
    <!-- Left Column: Microservice Topology & Chaos Triggers -->
    <div class="lg:col-span-2 space-y-6">
      <!-- Interactive Chaos Triggers -->
      <div class="bg-gray-900 border border-gray-800 rounded-lg p-5">
        <h2 class="text-sm font-semibold uppercase tracking-wider text-gray-300 mb-4">Interactive Chaos Simulator</h2>
        <div class="flex flex-wrap gap-3">
          <button onclick="triggerScenario('deadlock')" class="bg-red-600 hover:bg-red-700 text-white font-medium px-4 py-2 rounded text-sm transition">
            🔴 Inject DB Deadlock & Pool Exhaustion
          </button>
          <button onclick="triggerScenario('cascade')" class="bg-amber-600 hover:bg-amber-700 text-white font-medium px-4 py-2 rounded text-sm transition">
            🟡 Inject Downstream Crash (Test Cascade Shield)
          </button>
          <button onclick="triggerScenario('oom')" class="bg-purple-600 hover:bg-purple-700 text-white font-medium px-4 py-2 rounded text-sm transition">
            🟣 Inject JVM Heap OOM
          </button>
          <button onclick="triggerRestore()" class="bg-blue-600 hover:bg-blue-700 text-white font-medium px-4 py-2 rounded text-sm transition">
            🔄 Reverse Restore (Un-drain & Scale In)
          </button>
        </div>
      </div>

      <!-- Live Microservice Topology Map -->
      <div class="bg-gray-900 border border-gray-800 rounded-lg p-5">
        <h2 class="text-sm font-semibold uppercase tracking-wider text-gray-300 mb-4">Microservice Call-Graph Topology (DAG)</h2>
        <div class="bg-gray-950 rounded p-6 border border-gray-800 flex items-center justify-around text-center text-xs">
          <div class="border border-blue-500/50 bg-blue-950/30 p-3 rounded-lg w-28">
            <div class="font-bold text-blue-300">api-gateway</div>
            <div class="text-gray-400 mt-1">Ingress</div>
          </div>
          <div class="text-gray-600 text-lg">➔</div>
          <div class="border border-emerald-500/50 bg-emerald-950/30 p-3 rounded-lg w-32" id="node-order">
            <div class="font-bold text-emerald-300">order-service</div>
            <div class="text-gray-400 mt-1" id="order-status">Healthy (2 pods)</div>
          </div>
          <div class="text-gray-600 text-lg">➔</div>
          <div class="border border-emerald-500/50 bg-emerald-950/30 p-3 rounded-lg w-32" id="node-payment">
            <div class="font-bold text-emerald-300">payment-service</div>
            <div class="text-gray-400 mt-1" id="payment-status">Healthy</div>
          </div>
          <div class="text-gray-600 text-lg">➔</div>
          <div class="border border-purple-500/50 bg-purple-950/30 p-3 rounded-lg w-28">
            <div class="font-bold text-purple-300">payment-mysql</div>
            <div class="text-gray-400 mt-1">InnoDB Engine</div>
          </div>
        </div>
      </div>

      <!-- Live Audit Stream -->
      <div class="bg-gray-900 border border-gray-800 rounded-lg p-5">
        <h2 class="text-sm font-semibold uppercase tracking-wider text-gray-300 mb-2">Live SRE Remediation Audit Stream</h2>
        <div id="log-stream" class="bg-black/60 rounded p-4 text-xs font-mono h-48 overflow-y-auto space-y-1 text-gray-400 border border-gray-800">
          <div>[SYSTEM] Sentry visual console initialized. Standing by for alert webhooks...</div>
        </div>
      </div>
    </div>

    <!-- Right Column: Jev Logits Distribution & Post-Mortem -->
    <div class="space-y-6">
      <!-- Jev Logits Chart -->
      <div class="bg-gray-900 border border-gray-800 rounded-lg p-5">
        <h2 class="text-sm font-semibold uppercase tracking-wider text-gray-300 mb-4">Jev Action Logits Distribution</h2>
        <div class="space-y-3" id="logits-bars">
          <div>
            <div class="flex justify-between text-xs mb-1">
              <span>FORCE_KILL_LONG_TRANSACTION</span>
              <span id="prob-kill" class="font-bold text-white">85.0%</span>
            </div>
            <div class="w-full bg-gray-800 rounded-full h-2">
              <div id="bar-kill" class="bg-blue-500 h-2 rounded-full" style="width: 85%"></div>
            </div>
          </div>
          <div>
            <div class="flex justify-between text-xs mb-1">
              <span>DRAIN_TRAFFIC</span>
              <span id="prob-drain" class="font-bold text-white">8.0%</span>
            </div>
            <div class="w-full bg-gray-800 rounded-full h-2">
              <div id="bar-drain" class="bg-amber-500 h-2 rounded-full" style="width: 8%"></div>
            </div>
          </div>
          <div>
            <div class="flex justify-between text-xs mb-1">
              <span>ESCALATE_HUMAN</span>
              <span id="prob-escalate" class="font-bold text-white">4.0%</span>
            </div>
            <div class="w-full bg-gray-800 rounded-full h-2">
              <div id="bar-escalate" class="bg-purple-500 h-2 rounded-full" style="width: 4%"></div>
            </div>
          </div>
          <div>
            <div class="flex justify-between text-xs mb-1">
              <span>RESTART_POD</span>
              <span id="prob-restart" class="font-bold text-white">2.0%</span>
            </div>
            <div class="w-full bg-gray-800 rounded-full h-2">
              <div id="bar-restart" class="bg-red-500 h-2 rounded-full" style="width: 2%"></div>
            </div>
          </div>
          <div>
            <div class="flex justify-between text-xs mb-1">
              <span>SCALE_OUT</span>
              <span id="prob-scale" class="font-bold text-white">1.0%</span>
            </div>
            <div class="w-full bg-gray-800 rounded-full h-2">
              <div id="bar-scale" class="bg-emerald-500 h-2 rounded-full" style="width: 1%"></div>
            </div>
          </div>
        </div>
      </div>

      <!-- Last Incident Retrospective Preview -->
      <div class="bg-gray-900 border border-gray-800 rounded-lg p-5">
        <div class="flex justify-between items-center mb-4">
          <h2 class="text-sm font-semibold uppercase tracking-wider text-gray-300">Incident Retrospective</h2>
          <button onclick="downloadMarkdown()" class="text-xs bg-gray-800 hover:bg-gray-700 text-gray-300 px-2 py-1 rounded border border-gray-700">Export Markdown</button>
        </div>
        <div id="pm-preview" class="bg-black/60 rounded p-4 text-xs font-mono h-52 overflow-y-auto text-gray-400 border border-gray-800 whitespace-pre-wrap">
No incident recorded yet. Click a chaos scenario button above to trigger sub-100ms remediation.
        </div>
      </div>
    </div>
  </div>

  <script>
    let lastIncidentId = null;

    function appendLog(msg, color='text-gray-300') {
      const el = document.getElementById('log-stream');
      const timeStr = new Date().toISOString().substring(11, 19);
      el.innerHTML += `<div class="${color}">[${timeStr}] ${msg}</div>`;
      el.scrollTop = el.scrollHeight;
    }

    async function triggerScenario(type) {
      if (type === 'deadlock') {
        appendLog('⚡ Injecting Database Deadlock on order-service...', 'text-amber-400');
        const payload = {
          service_name: "order-service",
          namespace: "prod",
          pod_name: "order-pod-7f8d",
          cpu_usage_pct: 98.5,
          memory_usage_pct: 46.0,
          active_connections: 100,
          error_logs: "HikariPool-1 - Connection is not available. Lock wait timeout exceeded",
          trigger_rule: "HikariPoolStarvation"
        };
        const res = await fetch('/v1/sentry/arbitrate', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify(payload)
        }).then(r => r.json());

        lastIncidentId = res.incident_id;
        appendLog(`✓ Jev Prefill Decision: ${res.action} (${(res.arbitration.confidence*100).toFixed(1)}%) in ${res.arbitration.latency_ms}ms`, 'text-blue-400');
        appendLog(`✓ Execution: ${res.execution_message}`, 'text-green-400');
        updateLogits(res.arbitration.distribution);
        fetchPostMortem(lastIncidentId);
      } else if (type === 'cascade') {
        appendLog('⚡ Step 1: Simulating downstream payment-service DB deadlock...', 'text-red-400');
        await fetch('/v1/sentry/arbitrate', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            service_name: "payment-service",
            namespace: "prod",
            pod_name: "payment-pod-1",
            trigger_rule: "DBDeadlock",
            error_logs: "Lock wait timeout exceeded on InnoDB transaction"
          })
        });

        appendLog('⚡ Step 2: Upstream order-service starts failing with 504 Gateway Timeout...', 'text-amber-400');
        const resOrder = await fetch('/v1/sentry/arbitrate', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            service_name: "order-service",
            namespace: "prod",
            pod_name: "order-pod-99",
            trigger_rule: "HTTP504Timeout",
            error_logs: "HTTP 504 Gateway Timeout calling payment-service"
          })
        }).then(r => r.json());

        lastIncidentId = resOrder.incident_id;
        appendLog(`🛑 ${resOrder.safety_verdict}`, 'text-purple-400');
        fetchPostMortem(lastIncidentId);
      } else if (type === 'oom') {
        appendLog('⚡ Injecting JVM Heap OOM on inventory-service...', 'text-purple-400');
        const res = await fetch('/v1/sentry/arbitrate', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({
            service_name: "inventory-service",
            namespace: "prod",
            pod_name: "inv-pod-99",
            memory_usage_pct: 98.5,
            error_logs: "java.lang.OutOfMemoryError: Java heap space",
            trigger_rule: "PodMemoryLimit"
          })
        }).then(r => r.json());
        lastIncidentId = res.incident_id;
        appendLog(`✓ Jev Decision: ${res.action} in ${res.arbitration.latency_ms}ms`, 'text-blue-400');
        appendLog(`✓ Execution: ${res.execution_message}`, 'text-green-400');
        updateLogits(res.arbitration.distribution);
        fetchPostMortem(lastIncidentId);
      }
    }

    async function triggerRestore() {
      appendLog('🔄 Calling Reverse Restorer: Un-draining pod and scaling back to baseline...', 'text-blue-300');
      const res = await fetch('/v1/sentry/restore', {
        method: 'POST',
        headers: {'Content-Type': 'application/json'},
        body: JSON.stringify({
          namespace: "prod",
          pod_name: "order-pod-7f8d",
          service_name: "order-service",
          baseline_replicas: 2
        })
      }).then(r => r.json());
      appendLog(`✓ Restorer: ${res.message}`, 'text-emerald-400');
    }

    function updateLogits(dist) {
      const keys = ['FORCE_KILL_LONG_TRANSACTION', 'DRAIN_TRAFFIC', 'ESCALATE_HUMAN', 'RESTART_POD', 'SCALE_OUT'];
      const mapId = {'FORCE_KILL_LONG_TRANSACTION': 'kill', 'DRAIN_TRAFFIC': 'drain', 'ESCALATE_HUMAN': 'escalate', 'RESTART_POD': 'restart', 'SCALE_OUT': 'scale'};
      for (const k of keys) {
        const p = (dist[k] || 0) * 100;
        const id = mapId[k];
        document.getElementById(`prob-${id}`).innerText = p.toFixed(1) + '%';
        document.getElementById(`bar-${id}`).style.width = p + '%';
      }
    }

    async function fetchPostMortem(id) {
      if (!id) return;
      const md = await fetch(`/v1/sentry/postmortem/${id}/markdown`).then(r => r.text());
      document.getElementById('pm-preview').innerText = md;
    }

    function downloadMarkdown() {
      if (!lastIncidentId) return alert('No incident recorded yet.');
      window.open(`/v1/sentry/postmortem/${lastIncidentId}/markdown`, '_blank');
    }
  </script>
</body>
</html>
"""


def get_ui_html() -> str:
    return DASHBOARD_HTML
