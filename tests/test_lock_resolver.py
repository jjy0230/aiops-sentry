import pytest
from sentry.lock_resolver import LockGraphResolver, LockWaitEdge, RootBlockerAnalysis
from sentry.executor import DatabaseActionExecutor
from sentry.schemas import RemediationAction, FailureContext


def test_single_blocker_multiple_waiters():
    """Verify single blocker thread with multiple direct waiters."""
    edges = [
        LockWaitEdge(requesting_thread_id=101, blocking_thread_id=999, duration_sec=12.0),
        LockWaitEdge(requesting_thread_id=102, blocking_thread_id=999, duration_sec=15.5),
        LockWaitEdge(requesting_thread_id=103, blocking_thread_id=999, duration_sec=8.2),
    ]
    results = LockGraphResolver.resolve_root_blockers(edges)

    assert len(results) == 1
    root = results[0]
    assert root.root_blocker_id == 999
    assert sorted(root.direct_waiter_ids) == [101, 102, 103]
    assert root.total_impacted_waiters == 3
    assert root.max_wait_duration_sec == 15.5
    assert root.cycle_detected is False


def test_cascading_lock_chain():
    """Verify transitive cascading chain: 300 -> 200 -> 100 (100 is root blocker)."""
    edges = [
        LockWaitEdge(requesting_thread_id=200, blocking_thread_id=100, duration_sec=40.0),
        LockWaitEdge(requesting_thread_id=300, blocking_thread_id=200, duration_sec=25.0),
    ]
    results = LockGraphResolver.resolve_root_blockers(edges)

    assert len(results) == 1
    root = results[0]
    # 100 is waiting for no one, while 200 is waiting for 100
    assert root.root_blocker_id == 100
    assert root.direct_waiter_ids == [200]
    assert root.total_impacted_waiters == 2  # 200 and 300


def test_multiple_competing_blockers_ranking():
    """When multiple independent root blockers exist, rank by impacted waiters and duration."""
    edges = [
        # Group A: Blocker 500 blocks 1 waiter for 50s
        LockWaitEdge(requesting_thread_id=501, blocking_thread_id=500, duration_sec=50.0),
        # Group B: Blocker 800 blocks 3 waiters for 20s
        LockWaitEdge(requesting_thread_id=801, blocking_thread_id=800, duration_sec=20.0),
        LockWaitEdge(requesting_thread_id=802, blocking_thread_id=800, duration_sec=18.0),
        LockWaitEdge(requesting_thread_id=803, blocking_thread_id=800, duration_sec=15.0),
    ]
    results = LockGraphResolver.resolve_root_blockers(edges)

    assert len(results) == 2
    # Blocker 800 affects 3 waiters -> should be ranked first
    assert results[0].root_blocker_id == 800
    assert results[0].total_impacted_waiters == 3

    assert results[1].root_blocker_id == 500
    assert results[1].total_impacted_waiters == 1


def test_deadlock_cycle_detection():
    """Verify that circular waits (A blocks B, B blocks A) are detected as cycle."""
    edges = [
        LockWaitEdge(requesting_thread_id=1, blocking_thread_id=2, duration_sec=10.0),
        LockWaitEdge(requesting_thread_id=2, blocking_thread_id=1, duration_sec=12.0),
    ]
    results = LockGraphResolver.resolve_root_blockers(edges)

    assert len(results) >= 1
    assert results[0].cycle_detected is True
    assert "[Deadlock Cycle]" in results[0].recommendation_reason


def test_format_kill_command_security():
    """Verify injection prevention on thread ID formatting."""
    cmd = LockGraphResolver.format_kill_command(1024)
    assert cmd == "KILL CONNECTION 1024;"

    query_cmd = LockGraphResolver.format_kill_command(1024, query_only=True)
    assert query_cmd == "KILL QUERY 1024;"

    with pytest.raises(ValueError):
        LockGraphResolver.format_kill_command(-5)


@pytest.mark.asyncio
async def test_db_executor_integration():
    """Verify DatabaseActionExecutor successfully solves blocker and generates audit log."""
    executor = DatabaseActionExecutor()
    ctx = FailureContext(
        service_name="payment-service",
        namespace="prod",
        error_logs="Lock wait timeout exceeded on transaction",
    )
    success, msg = await executor.execute(RemediationAction.FORCE_KILL_LONG_TRANSACTION, ctx)

    assert success is True
    assert "Root Blocker Thread #1024" in msg
    assert "KILL CONNECTION 1024;" in msg
