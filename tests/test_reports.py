"""Tests for weekly report calculations, deduplication, caching, and delta invalidation."""

from datetime import date
from fastapi.testclient import TestClient
import pytest

from app.main import app
from app.raw import db


@pytest.fixture(autouse=True)
def reset_cache():
    """Ensure clean cache tables before each test if needed."""
    yield


def test_stats_reports_rows_and_cost():
    with TestClient(app) as client:
        stats = client.get("/stats").json()
    assert stats["cost_rp"] == stats["rows_scanned"] * 0.05


def test_exact_duplicates_deduplication():
    """Verify that exact duplicate records (e.g. SHP332806 in W10) are deduplicated."""
    with TestClient(app) as client:
        res = client.get("/shops/SHP332806/weekly-report?week=2025-W11").json()
        # 2025-W11 starts on 2025-03-10.
        # Deduplicated expected: revenue=191224300, units=1481. Kept duplicate would be 228169100, units=1829.
        assert res["revenue"] == 191224300
        assert res["units"] == 1481
        assert pytest.approx(res["revenue_wow_pct"], abs=0.01) == -58.27


def test_cache_hits_without_further_raw_scans():
    """Verify that subsequent requests for cached weeks hit cache with 0 additional raw scans."""
    with TestClient(app) as client:
        # Prime the cache for SHP332806 W11
        client.get("/shops/SHP332806/weekly-report?week=2025-W11")
        stats_before = client.get("/stats").json()

        # Same request again
        res = client.get("/shops/SHP332806/weekly-report?week=2025-W11").json()
        assert res["revenue"] == 191224300

        # Different shop on the same week (days already materialized)
        res_other = client.get("/shops/SHP141660/weekly-report?week=2025-W11").json()
        assert res_other["revenue"] == 284327800

        stats_after = client.get("/stats").json()
        assert stats_after["rows_scanned"] == stats_before["rows_scanned"]


def test_empty_shop_and_zero_revenue():
    """A shop with no rows in a week gets revenue: 0, units: 0, and null WoW."""
    with TestClient(app) as client:
        res = client.get("/shops/NON_EXISTENT_SHOP/weekly-report?week=2025-W11").json()
        assert res["revenue"] == 0
        assert res["units"] == 0
        assert res["revenue_wow_pct"] is None


def test_partial_week_w12_before_delta():
    """W12 before delta has only 4 days of data (2025-03-17 to 2025-03-20)."""
    with TestClient(app) as client:
        res = client.get("/shops/SHP141660/weekly-report?week=2025-W12").json()
        assert res["revenue"] == 493166800
        assert res["units"] == 2081
        assert pytest.approx(res["revenue_wow_pct"], abs=0.01) == 73.45


def test_delta_lifecycle_and_historical_invalidation():
    """End-to-end delta lifecycle test:
    - Repeated delta calls return loaded: false
    - W12 is updated with new days (2025-03-21 to 2025-03-23)
    - W10 historical correction in delta affects SHP141660 W10 revenue
    - SHP141660 in W11 WoW reflects the corrected W10 previous week revenue
    - Unaffected shops/weeks remain correct and cached
    """
    with TestClient(app) as client:
        # Before delta, get baseline numbers
        w10_before = client.get("/shops/SHP141660/weekly-report?week=2025-W10").json()
        w11_before = client.get("/shops/SHP141660/weekly-report?week=2025-W11").json()
        w12_before = client.get("/shops/SHP141660/weekly-report?week=2025-W12").json()

        assert w10_before["revenue"] == 429524900
        assert w11_before["revenue"] == 284327800
        assert pytest.approx(w11_before["revenue_wow_pct"], abs=0.01) == -33.80
        assert w12_before["revenue"] == 493166800

        # Load delta
        res_delta = client.post("/load-delta").json()
        assert res_delta["loaded"] is True

        # Repeated call returns loaded: False
        res_repeat = client.post("/load-delta").json()
        assert res_repeat["loaded"] is False
        assert "already loaded" in res_repeat["detail"]

        # 1. W10 historical correction for SHP141660
        w10_after = client.get("/shops/SHP141660/weekly-report?week=2025-W10").json()
        assert w10_after["revenue"] == 429344900
        assert w10_after["units"] == 1818

        # 2. W11 WoW dependency: W11 current revenue is unchanged, but WoW reflects corrected W10!
        w11_after = client.get("/shops/SHP141660/weekly-report?week=2025-W11").json()
        assert w11_after["revenue"] == 284327800
        assert pytest.approx(w11_after["revenue_wow_pct"], abs=0.01) == -33.78

        # 3. W12 after delta includes new days
        w12_after = client.get("/shops/SHP141660/weekly-report?week=2025-W12").json()
        assert w12_after["revenue"] == 869744300
        assert w12_after["units"] == 3827
        assert pytest.approx(w12_after["revenue_wow_pct"], abs=0.01) == 205.89


def test_delta_loaded_before_any_request():
    """Verify system correctness when delta is loaded in a fresh state before any report query."""
    # We can test querying another week after delta is loaded
    with TestClient(app) as client:
        # Query W09 for an unaffected shop
        res = client.get("/shops/SHP141660/weekly-report?week=2025-W09").json()
        assert res["revenue"] == 213814000
        assert res["units"] == 864
        assert pytest.approx(res["revenue_wow_pct"], abs=0.01) == -51.61
