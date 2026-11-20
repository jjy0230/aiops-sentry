import asyncio
import pytest
from sentry.topology import TopologyGraph, CascadeSuppressor, ServiceDependencyEdge
from sentry.schemas import FailureContext, RemediationAction, RemediationStatus, ServiceDependencyEdgeInput
from sentry.server import core, get_service_topology, add_service_topology_edge, get_suppressed_cascading_alerts


def test_topology_graph_multi_hop_traversal():
    """Verify directed acyclic call-graph traversal across multiple microservice hops."""
    graph = TopologyGraph()
    # Path: api-gateway -> order-service -> payment-service -> payment-mysql
    downstreams_gw = graph.get_all_downstreams("api-gateway")
    assert "order-service" in downstreams_gw
    assert "payment-service" in downstreams_gw
    assert "payment-mysql" in downstreams_gw

    downstreams_order = graph.get_all_downstreams("order-service")
    assert "payment-service" in downstreams_order
    assert "api-gateway" not in downstreams_order  # Directed only

    # Dynamic edge addition
    graph.add_edge("recommendation-service", "user-profile-db")
    assert "user-profile-db" in graph.get_all_downstreams("recommendation-service")


def test_cascade_suppression_victim_shielding():
    """Verify that an upstream service experiencing timeout is shielded when downstream has root outage."""
    suppressor = CascadeSuppressor()
    suppressor.reset()

    # Step 1: Downstream payment-service suffers from root deadlock
    ctx_payment = FailureContext(
        service_name="payment-service",
        namespace="prod",
        trigger_rule="DBDeadlock",
        error_logs="Lock wait timeout exceeded",
    )
    suppressor.register_root_failure(
        service_name="payment-service",
        action=RemediationAction.FORCE_KILL_LONG_TRANSACTION,
        ctx=ctx_payment,
        ttl_seconds=10.0,
    )

    # Step 2: Upstream order-service starts reporting 504 timeouts
    ctx_order = FailureContext(
        service_name="order-service",
        namespace="prod",
        trigger_rule="HTTP504UpstreamTimeout",
        error_logs="HTTP 504 Gateway Timeout while calling downstream payment-service",
    )
    verdict_order = suppressor.evaluate_cascade(ctx_order)

    # Must be intercepted as a victim
    assert verdict_order.should_suppress is True
    assert verdict_order.root_cause_service == "payment-service"
    assert "upstream caller" in verdict_order.verdict_message.lower()

    # Step 3: Unrelated auth-service with genuine OOM should NOT be suppressed
    ctx_auth = FailureContext(
        service_name="auth-service",
        namespace="prod",
        trigger_rule="PodOOM",
        error_logs="java.lang.OutOfMemoryError",
    )
    verdict_auth = suppressor.evaluate_cascade(ctx_auth)
    assert verdict_auth.should_suppress is False


@pytest.mark.asyncio
async def test_end_to_end_topology_cascade_pipeline():
    """Verify full server pipeline suppresses destructive restart on upstream order-service."""
    core.topology_suppressor.reset()

    # 1. Simulate root outage on payment-service
    ctx_payment = FailureContext(
        service_name="payment-service",
        namespace="prod",
        pod_name="payment-pod-1",
        trigger_rule="DBDeadlock",
        error_logs="Lock wait timeout exceeded; HikariPool-1 connection not available",
    )
    res_payment = await core.process_context(ctx_payment)
    assert res_payment.status == RemediationStatus.EXECUTED
    assert res_payment.action == RemediationAction.FORCE_KILL_LONG_TRANSACTION

    # 2. Simulate subsequent cascaded timeout alert from upstream order-service
    ctx_order = FailureContext(
        service_name="order-service",
        namespace="prod",
        pod_name="order-pod-88",
        trigger_rule="UpstreamTimeout504",
        error_logs="Connection refused or 504 Gateway Timeout calling payment-service",
    )
    res_order = await core.process_context(ctx_order)

    # Order service must NOT be restarted or scaled!
    assert res_order.status == RemediationStatus.CASCADE_SUPPRESSED
    assert res_order.executed is False
    assert "Cascade Suppression" in res_order.safety_verdict


@pytest.mark.asyncio
async def test_topology_api_endpoints():
    """Verify GET and POST endpoints for microservice topology inspection and edge addition."""
    # 1. Get topology
    topo = await get_service_topology()
    assert "order-service" in topo["topology"]

    # 2. Add edge
    edge_in = ServiceDependencyEdgeInput(
        upstream_service="cart-service",
        downstream_service="redis-cluster",
    )
    add_resp = await add_service_topology_edge(edge_in)
    assert add_resp["status"] == "added"

    # 3. Get suppressed history
    supp_resp = await get_suppressed_cascading_alerts()
    assert "total_suppressed" in supp_resp
