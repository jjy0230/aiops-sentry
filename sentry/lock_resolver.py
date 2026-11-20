"""
LockGraphResolver: Database lock contention graph construction and root blocker isolation.
Analyzes lock-wait directed acyclic graphs (DAGs) to identify root blocker thread IDs
and prevent misdirected termination of waiting sessions.
"""

from typing import List, Dict, Set, Optional
from pydantic import BaseModel, Field


class LockWaitEdge(BaseModel):
    """Represents a single lock wait dependency: requesting_thread_id is waiting for blocking_thread_id."""
    requesting_thread_id: int
    blocking_thread_id: int
    duration_sec: float = 0.0
    blocking_query: Optional[str] = None
    requesting_query: Optional[str] = None


class RootBlockerAnalysis(BaseModel):
    """Result of lock dependency graph analysis."""
    root_blocker_id: int
    direct_waiter_ids: List[int] = Field(default_factory=list)
    total_impacted_waiters: int = 0
    max_wait_duration_sec: float = 0.0
    blocking_query: Optional[str] = None
    cycle_detected: bool = False
    recommendation_reason: str = ""


# Standard read-only lock probe queries for production databases
MYSQL_8_LOCK_WAITS_SQL = """
SELECT 
    r.trx_mysql_thread_id AS requesting_thread_id,
    b.trx_mysql_thread_id AS blocking_thread_id,
    TIMESTAMPDIFF(SECOND, b.trx_started, NOW()) AS duration_sec,
    LEFT(b.trx_query, 200) AS blocking_query
FROM performance_schema.data_lock_waits w
JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_engine_transaction_id
JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_engine_transaction_id;
"""

MYSQL_57_LOCK_WAITS_SQL = """
SELECT 
    r.trx_mysql_thread_id AS requesting_thread_id,
    b.trx_mysql_thread_id AS blocking_thread_id,
    TIMESTAMPDIFF(SECOND, b.trx_started, NOW()) AS duration_sec,
    LEFT(b.trx_query, 200) AS blocking_query
FROM information_schema.innodb_lock_waits w
JOIN information_schema.innodb_trx b ON b.trx_id = w.blocking_trx_id
JOIN information_schema.innodb_trx r ON r.trx_id = w.requesting_trx_id;
"""

POSTGRES_LOCK_WAITS_SQL = """
SELECT 
    blocked_locks.pid     AS requesting_thread_id,
    blocking_locks.pid    AS blocking_thread_id,
    EXTRACT(EPOCH FROM (now() - blocking_activity.query_start))::float AS duration_sec,
    LEFT(blocking_activity.query, 200) AS blocking_query
FROM  pg_catalog.pg_locks         blocked_locks
JOIN pg_catalog.pg_stat_activity blocked_activity ON blocked_activity.pid = blocked_locks.pid
JOIN pg_catalog.pg_locks         blocking_locks 
    ON blocking_locks.locktype = blocked_locks.locktype
    AND blocking_locks.database IS NOT DISTINCT FROM blocked_locks.database
    AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
    AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
    AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
    AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
    AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
    AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
    AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
    AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
    AND blocking_locks.pid != blocked_locks.pid
JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
WHERE NOT blocked_locks.granted;
"""


class LockGraphResolver:
    """
    Constructs and traverses the lock-wait DAG to identify root blocker thread IDs.
    Ensures safe, targeted termination without impacting innocent waiter transactions.
    """

    @staticmethod
    def format_kill_command(thread_id: int, query_only: bool = False) -> str:
        """Generates sanitized, injection-safe KILL command."""
        if not isinstance(thread_id, int) or thread_id <= 0:
            raise ValueError(f"Invalid thread ID: {thread_id}")
        if query_only:
            return f"KILL QUERY {thread_id};"
        return f"KILL CONNECTION {thread_id};"

    @classmethod
    def resolve_root_blockers(cls, edges: List[LockWaitEdge]) -> List[RootBlockerAnalysis]:
        if not edges:
            return []

        # Graph representations:
        # blocker_to_waiters: blocker -> set of waiters directly blocked by it
        # waiter_to_blockers: waiter -> set of blockers it is directly waiting for
        blocker_to_waiters: Dict[int, Set[int]] = {}
        waiter_to_blockers: Dict[int, Set[int]] = {}
        thread_metadata: Dict[int, Dict[str, any]] = {}

        all_threads: Set[int] = set()

        for edge in edges:
            req = edge.requesting_thread_id
            blk = edge.blocking_thread_id

            all_threads.add(req)
            all_threads.add(blk)

            blocker_to_waiters.setdefault(blk, set()).add(req)
            waiter_to_blockers.setdefault(req, set()).add(blk)

            # Record longest duration and query snippet
            if blk not in thread_metadata or edge.duration_sec > thread_metadata[blk].get("duration_sec", 0.0):
                thread_metadata[blk] = {
                    "duration_sec": edge.duration_sec,
                    "blocking_query": edge.blocking_query,
                }

        # Step 1: Detect Root Blockers (in-degree in waiter_to_blockers is 0, out-degree > 0)
        # Root blockers are holding locks and waiting for NO ONE.
        candidate_root_ids = [
            tid for tid in blocker_to_waiters
            if tid not in waiter_to_blockers or len(waiter_to_blockers[tid]) == 0
        ]

        cycle_detected = False
        if not candidate_root_ids and edges:
            # All active nodes are waiting on each other -> Deadlock cycle exists!
            cycle_detected = True
            # In a deadlock cycle, break the cycle by choosing the thread with the highest out-degree (blocking the most)
            candidate_root_ids = sorted(
                blocker_to_waiters.keys(),
                key=lambda tid: (len(blocker_to_waiters[tid]), thread_metadata.get(tid, {}).get("duration_sec", 0.0)),
                reverse=True,
            )[:1]

        results: List[RootBlockerAnalysis] = []

        for root_id in candidate_root_ids:
            # Traverse downstream impact (BFS/DFS)
            visited_waiters: Set[int] = set()
            queue = list(blocker_to_waiters.get(root_id, []))

            while queue:
                curr = queue.pop(0)
                if curr not in visited_waiters and curr != root_id:
                    visited_waiters.add(curr)
                    # Add transitive waiters
                    for next_waiter in blocker_to_waiters.get(curr, []):
                        if next_waiter not in visited_waiters:
                            queue.append(next_waiter)

            direct_waiters = sorted(list(blocker_to_waiters.get(root_id, set())))
            meta = thread_metadata.get(root_id, {})
            duration = meta.get("duration_sec", 0.0)
            query = meta.get("blocking_query")

            reason = (
                f"Root Blocker Thread #{root_id} is holding lock for {duration:.1f}s, "
                f"directly blocking {len(direct_waiters)} sessions and transitively cascading to {len(visited_waiters)} sessions."
            )
            if cycle_detected:
                reason = f"[Deadlock Cycle] " + reason

            analysis = RootBlockerAnalysis(
                root_blocker_id=root_id,
                direct_waiter_ids=direct_waiters,
                total_impacted_waiters=len(visited_waiters),
                max_wait_duration_sec=duration,
                blocking_query=query,
                cycle_detected=cycle_detected,
                recommendation_reason=reason,
            )
            results.append(analysis)

        # Sort candidate root blockers by total impacted waiters descending, then duration descending
        results.sort(key=lambda r: (r.total_impacted_waiters, r.max_wait_duration_sec), reverse=True)
        return results
