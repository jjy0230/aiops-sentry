import asyncio
import pytest
from sentry.ui import get_ui_html
from sentry.server import visual_console, core
from sentry.cli import simulate_deadlock, simulate_cascade, simulate_oom, view_scoreboard, view_postmortem
from sentry.schemas import RemediationStatus, RemediationAction


def test_ui_html_content_structure():
    """Verify visual console HTML contains dashboard elements."""
    html = get_ui_html()
    assert "<title>AIOps-Sentry" in html
    assert "Jev Decision Kernel" in html
    assert "Microservice Call-Graph Topology" in html
    assert "Interactive Chaos Simulator" in html


@pytest.mark.asyncio
async def test_ui_endpoint_response():
    """Verify GET /ui endpoint returns valid HTMLResponse."""
    resp = await visual_console()
    assert resp.status_code == 200
    assert "AIOps-Sentry" in resp.body.decode("utf-8")


@pytest.mark.asyncio
async def test_cli_chaos_deadlock_simulation():
    """Verify CLI simulation of database deadlock contention."""
    core.topology_suppressor.reset()
    res = await simulate_deadlock()
    assert res.status == RemediationStatus.EXECUTED
    assert res.action == RemediationAction.FORCE_KILL_LONG_TRANSACTION
    assert res.arbitration.confidence >= 0.80


@pytest.mark.asyncio
async def test_cli_chaos_cascade_simulation():
    """Verify CLI simulation of multi-hop cascading failure."""
    res_order = await simulate_cascade()
    assert res_order.status == RemediationStatus.CASCADE_SUPPRESSED
    assert "Cascade Suppression" in res_order.safety_verdict


@pytest.mark.asyncio
async def test_cli_chaos_oom_simulation():
    """Verify CLI simulation of Pod memory exhaustion."""
    res = await simulate_oom()
    assert res.action == RemediationAction.RESTART_POD


def test_cli_view_postmortem_and_scoreboard():
    """Verify CLI utility view helpers run without exceptions."""
    # Should not raise exception
    view_scoreboard()
    view_postmortem("non-existent-id-fallback")
