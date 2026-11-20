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
    ]

    passed = 0
    failed = 0

    print("=" * 70)
    print("AIOps-Sentry Full Production Test Suite (Phase 1 -> Phase 4)")
    print("=" * 70)

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

    print("=" * 70)
    print(f"Summary: {passed} passed, {failed} failed.")
    print("=" * 70)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
