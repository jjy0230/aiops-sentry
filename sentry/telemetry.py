"""
Telemetry & Metrics Exporter:
Lightweight, zero-dependency Prometheus metrics collector for Sentry self-observability.
Exposes RFC-compliant Prometheus exposition format on /metrics.
"""

from typing import Dict, List, Tuple
from collections import defaultdict
import time


class SentryTelemetryRegistry:
    def __init__(self):
        self.arbitrations_count: Dict[Tuple[str, str, str], int] = defaultdict(int)
        self.safety_blocks_count: Dict[str, int] = defaultdict(int)
        self.deduplicated_count: Dict[str, int] = defaultdict(int)
        self.verification_count: Dict[Tuple[str, str], int] = defaultdict(int)
        self.latency_samples: List[float] = []

    def record_arbitration(self, service: str, action: str, status: str, latency_ms: float):
        self.arbitrations_count[(service, action, status)] += 1
        self.latency_samples.append(latency_ms)
        # Keep recent 1000 samples
        if len(self.latency_samples) > 1000:
            self.latency_samples.pop(0)

    def record_safety_block(self, reason_tag: str):
        self.safety_blocks_count[reason_tag] += 1

    def record_deduplication(self, service: str, count: int = 1):
        self.deduplicated_count[service] += count

    def record_verification(self, service: str, state: str):
        self.verification_count[(service, state)] += 1

    def export_prometheus_text(self) -> str:
        lines = []

        # Metric 1: Arbitrations total
        lines.append("# HELP sentry_arbitrations_total Total count of Sentry remediation arbitrations")
        lines.append("# TYPE sentry_arbitrations_total counter")
        for (svc, act, st), cnt in self.arbitrations_count.items():
            lines.append(f'sentry_arbitrations_total{{service="{svc}",action="{act}",status="{st}"}} {cnt}')

        # Metric 2: Safety blocks total
        lines.append("# HELP sentry_safety_blocks_total Total actions intercepted by Safety Envelopes")
        lines.append("# TYPE sentry_safety_blocks_total counter")
        for reason, cnt in self.safety_blocks_count.items():
            lines.append(f'sentry_safety_blocks_total{{reason="{reason}"}} {cnt}')

        # Metric 3: Deduplicated alerts
        lines.append("# HELP sentry_deduplicated_alerts_total Total alerts suppressed by 3s sliding window")
        lines.append("# TYPE sentry_deduplicated_alerts_total counter")
        for svc, cnt in self.deduplicated_count.items():
            lines.append(f'sentry_deduplicated_alerts_total{{service="{svc}"}} {cnt}')

        # Metric 4: Latency summary
        lines.append("# HELP sentry_arbitration_duration_ms_avg Average arbitration duration in milliseconds")
        lines.append("# TYPE sentry_arbitration_duration_ms_avg gauge")
        avg_lat = sum(self.latency_samples) / len(self.latency_samples) if self.latency_samples else 0.0
        lines.append(f"sentry_arbitration_duration_ms_avg {avg_lat:.2f}")

        return "\n".join(lines) + "\n"


telemetry = SentryTelemetryRegistry()
