import asyncio
import sys
import os

# Add root project directory to sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from tests.test_sentry import (
    test_jev_mock_engine_lock_timeout,
    test_jev_mock_engine_oom,
    test_safety_envelope_blocks_dangerous_restart_on_deadlock,
    test_blast_radius_cooldown_flapping,
    test_end_to_end_pipeline,
)
from tests.test_lock_resolver import (
    test_single_blocker_multiple_waiters,
    test_cascading_lock_chain,
    test_multiple_competing_blockers_ranking,
    test_deadlock_cycle_detection,
    test_format_kill_command_security,
    test_db_executor_integration,
)


async def main():
    async_tests = [
        ("Jev Mock: Lock Timeout / DB Contention", test_jev_mock_engine_lock_timeout),
        ("Jev Mock: OOM / Heap Exhaustion", test_jev_mock_engine_oom),
        ("Safety Envelope: Dangerous Restart Blocked", test_safety_envelope_blocks_dangerous_restart_on_deadlock),
        ("Blast Radius: Cooldown Flapping Protection", test_blast_radius_cooldown_flapping),
        ("End-to-End Pipeline: Lock Timeout Remediation", test_end_to_end_pipeline),
        ("DB Executor: Blocker Targeted Kill", test_db_executor_integration),
    ]

    sync_tests = [
        ("Lock DAG: Single Blocker Multiple Waiters", test_single_blocker_multiple_waiters),
        ("Lock DAG: Cascading Lock Chain Resolution", test_cascading_lock_chain),
        ("Lock DAG: Multiple Blocker Priority Ranking", test_multiple_competing_blockers_ranking),
        ("Lock DAG: Deadlock Cycle Detection", test_deadlock_cycle_detection),
        ("Security: KILL Command Thread ID Sanitization", test_format_kill_command_security),
    ]

    passed = 0
    failed = 0

    print("=" * 65)
    print("AIOps-Sentry Zero-Dependency Test Suite (Sprint 1 + Sprint 2)")
    print("=" * 65)

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

    print("=" * 65)
    print(f"Summary: {passed} passed, {failed} failed.")
    print("=" * 65)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
