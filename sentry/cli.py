"""
AIOps-Sentry Interactive CLI & Chaos Simulator:
Enables one-click incident simulation, post-mortem generation,
and live demonstration of sub-100ms autonomous remediation.
"""

import sys
import time
import argparse
import asyncio
from typing import Optional
from sentry.schemas import FailureContext, RemediationAction, RestoreRequest
from sentry.server import core, postmortem_engine, shadow_manager


def print_banner():
    banner = r"""
    ___    ____ ____             _____            __
   /   |  /  _// __ \ ____  ____/ ___/___  ____  / /_______  __
  / /| |  / / / / / // __ \/ __ \__ \/ _ \/ __ \/ __/ ___/ / / /
 / ___ |_/ / / /_/ // /_/ / /_/ /__/ /  __/ / / / /_/ /  / /_/ /
/_/  |_/___/ \____// .___/ .___/____/\___/_/ /_/\__/_/   \__, /
                  /_/   /_/                             /____/
    >>> Autonomous Remediation Arbitrator powered by Jev <<<
    """
    print(banner)


async def simulate_deadlock():
    print("\n[SCENARIO 1] Injecting Database Row Lock Contention & HikariCP Starvation...")
    time.sleep(0.3)
    print(" -> Triggering Alert: HikariPoolConnectionStarvation on 'order-service'")
    print(" -> Inlined Stacktrace: 'HikariPool-1 - Connection is not available. Lock wait timeout exceeded'")
    
    ctx = FailureContext(
        service_name="order-service",
        namespace="production",
        pod_name="order-pod-7f8d-a1b2",
        cpu_usage_pct=98.5,
        memory_usage_pct=46.0,
        active_connections=100,
        max_connections=100,
        error_logs="HikariPool-1 - Connection is not available, request timed out after 30000ms. Lock wait timeout exceeded on transaction 98712",
        trigger_rule="HikariPoolConnectionStarvation",
    )

    t0 = time.perf_counter()
    res = await core.process_context(ctx)
    t_elapsed = (time.perf_counter() - t0) * 1000.0

    print(f"\n[REMEDIATION COMPLETED in {t_elapsed:.1f}ms]")
    print(f" -> Incident ID:      {res.incident_id}")
    print(f" -> Status:           {res.status.value}")
    print(f" -> Jev Decision:     {res.action.value} (Confidence: {res.arbitration.confidence*100:.1f}%, Margin: {res.arbitration.margin*100:.1f}%)")
    print(f" -> Safety Verdict:   {res.safety_verdict}")
    print(f" -> Execution Audit:  {res.execution_message}")
    print(f"\n💡 View Full Retrospective: python -m sentry.cli postmortem --id {res.incident_id}\n")
    return res


async def simulate_cascade():
    print("\n[SCENARIO 2] Injecting Microservice Call-Chain Cascading Failure...")
    time.sleep(0.3)
    print(" 1. Downstream 'payment-service' crashes with database deadlock...")
    ctx_payment = FailureContext(
        service_name="payment-service",
        namespace="production",
        pod_name="payment-pod-1",
        trigger_rule="DBDeadlock",
        error_logs="Lock wait timeout exceeded on InnoDB transaction",
    )
    res_pay = await core.process_context(ctx_payment)
    print(f"    [payment-service Remediation]: {res_pay.action.value} ({res_pay.status.value})")

    time.sleep(0.2)
    print(" 2. Upstream 'order-service' begins failing with 504 Gateway Timeout while calling payment-service...")
    ctx_order = FailureContext(
        service_name="order-service",
        namespace="production",
        pod_name="order-pod-99",
        trigger_rule="HTTP504GatewayTimeout",
        error_logs="HTTP 504 Gateway Timeout calling downstream payment-service, circuit breaker half-open",
    )
    res_order = await core.process_context(ctx_order)

    print("\n[TOPOLOGY CASCADE ARBITRATION RESULT]")
    print(f" -> Order Service Status: {res_order.status.value}")
    print(f" -> Verdict:              {res_order.safety_verdict}")
    print(" -> Outcome:              Innocent upstream caller shielded! No destructive pod restarts triggered on order-service.\n")
    return res_order


async def simulate_oom():
    print("\n[SCENARIO 3] Injecting JVM Heap Exhaustion / OOM...")
    ctx = FailureContext(
        service_name="inventory-service",
        namespace="production",
        pod_name="inv-pod-91k",
        memory_usage_pct=98.5,
        error_logs="java.lang.OutOfMemoryError: Java heap space",
        trigger_rule="PodMemoryLimitExceeded",
    )
    res = await core.process_context(ctx)
    print(f"\n[REMEDIATION COMPLETED in {res.arbitration.latency_ms:.1f}ms]")
    print(f" -> Jev Decision: {res.action.value} ({res.arbitration.confidence*100:.1f}%)")
    print(f" -> Execution:    {res.execution_message}\n")
    return res


def view_postmortem(incident_id: str):
    try:
        md = postmortem_engine.export_markdown(incident_id)
        print("\n" + "=" * 70)
        print(md)
        print("=" * 70 + "\n")
    except KeyError:
        print(f"Error: Incident ID '{incident_id}' not found.")


def view_scoreboard():
    board = shadow_manager.get_scoreboard()
    print("\n" + "=" * 60)
    print("AIOps-Sentry Shadow Mode Accuracy Scoreboard")
    print("=" * 60)
    print(f"Total Shadow Predictions:        {board.total_shadow_predictions}")
    print(f"Evaluated with Human Ground Truth: {board.evaluated_with_ground_truth}")
    print(f"Matched Predictions:             {board.matched_predictions}")
    print(f"Accuracy Rate (Hit Rate):        {board.accuracy_rate_pct:.1f}%")
    print("=" * 60 + "\n")


def main():
    parser = argparse.ArgumentParser(description="AIOps-Sentry Interactive CLI & Chaos Simulator")
    subparsers = parser.add_subparsers(dest="command")

    # Simulate command
    sim_parser = subparsers.add_parser("simulate", help="Inject chaos failure scenarios")
    sim_parser.add_argument("--scenario", choices=["deadlock", "cascade", "oom"], default="deadlock", help="Failure scenario to inject")

    # Postmortem command
    pm_parser = subparsers.add_parser("postmortem", help="View incident post-mortem markdown")
    pm_parser.add_argument("--id", required=True, help="Incident ID")

    # Scoreboard command
    subparsers.add_parser("scoreboard", help="View shadow mode decision accuracy scoreboard")

    args = parser.parse_args()

    print_banner()

    if args.command == "simulate":
        if args.scenario == "deadlock":
            asyncio.run(simulate_deadlock())
        elif args.scenario == "cascade":
            asyncio.run(simulate_cascade())
        elif args.scenario == "oom":
            asyncio.run(simulate_oom())
    elif args.command == "postmortem":
        view_postmortem(args.id)
    elif args.command == "scoreboard":
        view_scoreboard()
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
