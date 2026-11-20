import asyncio
import sys
import os

# Add root project directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Sprint 1 tests
from tests.test_sentry import (
    test_jev_mock_engine_lock_timeout,
    test_jev_mock_engine_oom,
    test_safety_envelope_blocks_dangerous_restart_on_deadlock,
    test_blast_radius_cooldown_flapping,
    test_end_to_end_pipeline,
)

# Sprint 2 tests
from tests.test_lock_resolver import (
    test_single_blocker_multiple_waiters,
    test_cascading_lock_chain,
    test_multiple_competing_blockers_ranking,
    test_deadlock_cycle_detection,
    test_format_kill_command_security,
    test_db_executor_integration,
)

# Sprint 3 tests
from tests.test_lease_guard import (
    test_lease_guard_lifecycle,
    test_sanitize_lease_name_rfc1123,
)

# Sprint 4 tests
from tests.test_human_gate import (
    test_token_creation_and_successful_verification,
    test_token_tampering_rejected,
    test_token_replay_protection,
    test_feishu_and_dingtalk_card_structure,
    test_human_approval_api_endpoint_flow,
)

# Sprint 5 & 6 (V2 features) tests
from tests.test_v2_features import (
    test_storm_guard_sliding_window_deduplication,
    test_escalation_ladder_mapping,
    test_closed_loop_verification_recovery_success,
    test_closed_loop_verification_unresolved_escalation,
    test_telemetry_prometheus_exporter,
    test_alertmanager_webhook_storm_deduplication_e2e,
)

# Sprint 7 & 8 (V3 features: Restorer, PostMortem, Shadow Mode) tests
from tests.test_v3_features import (
    test_service_restorer_traffic_and_scale,
    test_restore_endpoint_flow,
    test_postmortem_engine_markdown_generation,
    test_shadow_mode_dry_run_and_scoreboard,
)

# Sprint 9 (V4 features: Topology Graph & Cascade Suppressor) tests
from tests.test_v4_topology import (
    test_topology_graph_multi_hop_traversal,
    test_cascade_suppression_victim_shielding,
    test_end_to_end_topology_cascade_pipeline,
    test_topology_api_endpoints,
)


async def main():
    sync_tests = [
        # Phase 2: Lock DAG
        ("Phase 2 - Lock DAG: Single Blocker Multiple Waiters", test_single_blocker_multiple_waiters),
        ("Phase 2 - Lock DAG: Cascading Lock Chain Resolution", test_cascading_lock_chain),
        ("Phase 2 - Lock DAG: Multiple Blocker Priority Ranking", test_multiple_competing_blockers_ranking),
        ("Phase 2 - Lock DAG: Deadlock Cycle Detection", test_deadlock_cycle_detection),
        ("Phase 2 - Security: KILL Command Thread ID Sanitization", test_format_kill_command_security),
        # Phase 3: Lease Guard
        ("Phase 3 - Lease Guard: Lifecycle & Expiration", test_lease_guard_lifecycle),
        ("Phase 3 - Lease Guard: RFC 1123 Sanitization", test_sanitize_lease_name_rfc1123),
        # Phase 4: Human Gate
        ("Phase 4 - Human Gate: Token Creation & Verification", test_token_creation_and_successful_verification),
        ("Phase 4 - Human Gate: Tampered Token Rejection", test_token_tampering_rejected),
        ("Phase 4 - Human Gate: Replay Protection", test_token_replay_protection),
        ("Phase 4 - Human Gate: Feishu & DingTalk Cards", test_feishu_and_dingtalk_card_structure),
        # Phase 5: Storm Guard & V2 Features
        ("Phase 5 - Storm Guard: 3s Sliding Window Fingerprint Deduplication", test_storm_guard_sliding_window_deduplication),
        ("Phase 6 - Closed Loop: Escalation Ladder Action Mapping", test_escalation_ladder_mapping),
        ("Phase 7 - Telemetry: Prometheus Exposition Format", test_telemetry_prometheus_exporter),
        # Phase 8: V3 Features (PostMortem & Shadow Mode)
        ("Phase 8 - Post-Mortem: Markdown & Timeline Generation", test_postmortem_engine_markdown_generation),
        ("Phase 8 - Shadow Mode: Scoreboard Evaluation", test_shadow_mode_dry_run_and_scoreboard),
        # Phase 9: V4 Topology Graph & Cascade Suppressor
        ("Phase 9 - Topology: Multi-hop Directed Traversal", test_topology_graph_multi_hop_traversal),
        ("Phase 9 - Cascade: Upstream Victim Shielding Check", test_cascade_suppression_victim_shielding),
    ]

    async_tests = [
        # Phase 1: Core Engine & Safety Envelope
        ("Phase 1 - Jev Mock: Lock Timeout / DB Contention", test_jev_mock_engine_lock_timeout),
        ("Phase 1 - Jev Mock: OOM / Heap Exhaustion", test_jev_mock_engine_oom),
        ("Phase 1 - Safety Envelope: Dangerous Restart Blocked", test_safety_envelope_blocks_dangerous_restart_on_deadlock),
        ("Phase 1 - Blast Radius: Cooldown Flapping Protection", test_blast_radius_cooldown_flapping),
        ("Phase 1 - End-to-End Pipeline: Lock Timeout Remediation", test_end_to_end_pipeline),
        # Phase 2: DB Executor
        ("Phase 2 - DB Executor: Blocker Targeted Kill", test_db_executor_integration),
        # Phase 4: Human Approval Flow
        ("Phase 4 - Approval API: Operator Authorization Flow", test_human_approval_api_endpoint_flow),
        # Phase 6: Closed Loop Probes
        ("Phase 6 - Closed Loop: Verification Recovery Success", test_closed_loop_verification_recovery_success),
        ("Phase 6 - Closed Loop: Unresolved Incident Escalates to Next Tier", test_closed_loop_verification_unresolved_escalation),
        ("Phase 5 - E2E Webhook: Alert Storm Batch Deduplication", test_alertmanager_webhook_storm_deduplication_e2e),
        # Phase 8: V3 Restorer
        ("Phase 8 - Restorer: Traffic & Scale-in Reversal", test_service_restorer_traffic_and_scale),
        ("Phase 8 - Restorer API: POST /v1/sentry/restore Flow", test_restore_endpoint_flow),
        # Phase 9: V4 Cascade Pipeline & API
        ("Phase 9 - Pipeline: E2E Cascading Failure Suppression", test_end_to_end_topology_cascade_pipeline),
        ("Phase 9 - Topology API: Endpoints & Edge Addition", test_topology_api_endpoints),
    ]

    passed = 0
    failed = 0

    print("=" * 80)
    print("AIOps-Sentry Full Production Test Suite (Phase 1 -> Phase 9 / V4)")
    print("=" * 80)

    for name, func in sync_tests:
        try:
            func()
            print(f"[PASS] {name}")
            passed += 1
        except Exception as e:
            print(f"[FAIL] {name}: {e}")
            failed += 1

    for name, func in async_tests:
        try:
            await func()
            print(f"[PASS] {name}")
            passed += 1
        except Exception as e:
            print(f"[FAIL] {name}: {e}")
            failed += 1

    print("=" * 80)
    print(f"Summary: {passed} passed, {failed} failed.")
    print("=" * 80)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
