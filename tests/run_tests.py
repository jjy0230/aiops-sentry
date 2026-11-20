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


async def main():
    test_cases = [
        ("Jev Mock: Lock Timeout / DB Contention", test_jev_mock_engine_lock_timeout),
        ("Jev Mock: OOM / Heap Exhaustion", test_jev_mock_engine_oom),
        ("Safety Envelope: Dangerous Restart Blocked", test_safety_envelope_blocks_dangerous_restart_on_deadlock),
        ("Blast Radius: Cooldown Flapping Protection", test_blast_radius_cooldown_flapping),
        ("End-to-End Pipeline: Lock Timeout Remediation", test_end_to_end_pipeline),
    ]

    passed = 0
    failed = 0

    print("=" * 60)
    print("AIOps-Sentry Zero-Dependency Test Suite")
    print("=" * 60)

    for name, func in test_cases:
        try:
            await func()
            print(f"[PASS] {name}")
            passed += 1
        except Exception as e:
            print(f"[FAIL] {name}: {e}")
            failed += 1

    print("=" * 60)
    print(f"Summary: {passed} passed, {failed} failed.")
    print("=" * 60)

    if failed > 0:
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
