"""An example to start from. Run with: uv run pytest

The raw data and the cost counters live in module-level state, so all tests in
one run share them. Compare /stats before and after a call to see what it cost.

Each `TestClient(app)` block runs `startup()` again, but the delta stays loaded:
only the first POST /load-delta in a run returns {"loaded": true}. If your
startup() rebuilds the cache, make sure it still accounts for an already loaded
delta, or keep all delta tests in one TestClient block.
"""

from fastapi.testclient import TestClient

from app.main import app


def test_stats_reports_rows_and_cost():
    with TestClient(app) as client:
        stats = client.get("/stats").json()
    assert stats["cost_rp"] == stats["rows_scanned"] * 0.05
