"""Tests for weekly report calculations, deduplication, caching, rollback, and delta invalidation."""

from datetime import date
import subprocess
import sys

import duckdb
from fastapi.testclient import TestClient
import pytest

from app import raw, reports
from app.main import app


@pytest.fixture(autouse=True)
def reset_cache_and_delta_state():
    """Ensure every test starts and ends with a clean cache and pre-delta state."""
    def _clean():
        with raw.lock:
            raw.db.execute("DROP TABLE IF EXISTS cache_daily")
            raw.db.execute("DROP TABLE IF EXISTS cache_dates")
            raw.db.execute("DROP TABLE IF EXISTS raw_delta")
            raw.db.execute("DROP TABLE IF EXISTS new_daily_aggregates")
            raw._landed.discard("delta")
            raw._rows_scanned = 0

    _clean()
    yield
    _clean()


def test_stats_reports_rows_and_cost():
    with TestClient(app) as client:
        stats = client.get("/stats").json()
    assert stats["rows_scanned"] == 0
    assert stats["cost_rp"] == stats["rows_scanned"] * 0.05


def test_exact_duplicates_deduplication():
    """Verify that exact duplicate records (e.g. SHP332806 in W11) are deduplicated."""
    with TestClient(app) as client:
        res = client.get("/shops/SHP332806/weekly-report?week=2025-W11").json()
        # 2025-W11 starts on 2025-03-10.
        # Deduplicated expected: revenue=191224300, units=1481. Kept duplicate would be 228169100, units=1829.
        assert res["revenue"] == 191224300
        assert res["units"] == 1481
        assert pytest.approx(res["revenue_wow_pct"], abs=0.01) == -58.27


def test_cache_hits_and_overlapping_week_scans():
    """Verify cache misses scan only unmaterialized dates and hits incur 0 raw scans."""
    with TestClient(app) as client:
        assert client.get("/stats").json()["rows_scanned"] == 0

        # 1. Cold miss for SHP332806 W11 materializes 14 days (W10 + W11: 2025-03-03..2025-03-16)
        res_initial = client.get("/shops/SHP332806/weekly-report?week=2025-W11").json()
        assert res_initial["revenue"] == 191224300
        stats_after_w11 = client.get("/stats").json()
        assert stats_after_w11["rows_scanned"] == 81314

        # 2. Repeated request for the same shop and week incurs 0 additional scans
        res_repeat = client.get("/shops/SHP332806/weekly-report?week=2025-W11").json()
        assert res_repeat["revenue"] == 191224300

        # 3. Different shop on the same week also incurs 0 additional scans
        res_other = client.get("/shops/SHP141660/weekly-report?week=2025-W11").json()
        assert res_other["revenue"] == 284327800
        assert client.get("/stats").json()["rows_scanned"] == stats_after_w11["rows_scanned"]

        # 4. Overlapping week W12 (needs W11 + W12) scans only the 7 uncached dates of W12 (23,256 rows)
        res_w12 = client.get("/shops/SHP141660/weekly-report?week=2025-W12").json()
        assert res_w12["revenue"] == 493166800
        stats_after_w12 = client.get("/stats").json()
        assert stats_after_w12["rows_scanned"] - stats_after_w11["rows_scanned"] == 23256


def test_empty_shop_and_zero_revenue():
    """A shop with no rows in a week gets revenue: 0, units: 0, and null WoW."""
    with TestClient(app) as client:
        res = client.get("/shops/NON_EXISTENT_SHOP/weekly-report?week=2025-W11").json()
        assert res["revenue"] == 0
        assert res["units"] == 0
        assert res["revenue_wow_pct"] is None


def test_iso_year_boundary_and_invalid_week():
    """Verify ISO year boundary week (2025-W01 starting 2024-12-30) and 422 on invalid weeks."""
    with TestClient(app) as client:
        res = client.get("/shops/SHP141660/weekly-report?week=2025-W01").json()
        assert res["revenue"] == 267107100
        assert res["units"] == 1313
        assert pytest.approx(res["revenue_wow_pct"], abs=0.01) == 31.80

        invalid_iso = client.get("/shops/SHP141660/weekly-report?week=2025-W54")
        assert invalid_iso.status_code == 422

        malformed = client.get("/shops/SHP141660/weekly-report?week=2025-11")
        assert malformed.status_code == 422


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
    - Unaffected shops/weeks (e.g. W09) remain correct and cached with 0 additional scans
    """
    with TestClient(app) as client:
        # Before delta, prime an unaffected week (W09) and affected weeks (W10, W11, W12)
        w09_before = client.get("/shops/SHP141660/weekly-report?week=2025-W09").json()
        w10_before = client.get("/shops/SHP141660/weekly-report?week=2025-W10").json()
        w11_before = client.get("/shops/SHP141660/weekly-report?week=2025-W11").json()
        w12_before = client.get("/shops/SHP141660/weekly-report?week=2025-W12").json()

        assert w09_before["revenue"] == 213814000
        assert w09_before["units"] == 864
        assert pytest.approx(w09_before["revenue_wow_pct"], abs=0.01) == -51.61
        assert w10_before["revenue"] == 429524900
        assert w11_before["revenue"] == 284327800
        assert pytest.approx(w11_before["revenue_wow_pct"], abs=0.01) == -33.80
        assert w12_before["revenue"] == 493166800

        # Load delta
        res_delta = client.post("/load-delta").json()
        assert res_delta["loaded"] is True
        stats_after_delta = client.get("/stats").json()

        # Repeated call returns loaded: False and performs 0 scans
        res_repeat = client.post("/load-delta").json()
        assert res_repeat["loaded"] is False
        assert "already loaded" in res_repeat["detail"]
        assert client.get("/stats").json()["rows_scanned"] == stats_after_delta["rows_scanned"]

        # 1. Unaffected week W09 remains cached with identical values and 0 new scans
        w09_after = client.get("/shops/SHP141660/weekly-report?week=2025-W09").json()
        assert w09_after == w09_before

        # 2. W10 historical correction for SHP141660
        w10_after = client.get("/shops/SHP141660/weekly-report?week=2025-W10").json()
        assert w10_after["revenue"] == 429344900
        assert w10_after["units"] == 1818

        # 3. W11 WoW dependency: W11 current revenue is unchanged, but WoW reflects corrected W10!
        w11_after = client.get("/shops/SHP141660/weekly-report?week=2025-W11").json()
        assert w11_after["revenue"] == 284327800
        assert pytest.approx(w11_after["revenue_wow_pct"], abs=0.01) == -33.78

        # 4. W12 after delta includes new days
        w12_after = client.get("/shops/SHP141660/weekly-report?week=2025-W12").json()
        assert w12_after["revenue"] == 869744300
        assert w12_after["units"] == 3827
        assert pytest.approx(w12_after["revenue_wow_pct"], abs=0.01) == 205.89

        # All post-delta report queries were served from cache with 0 additional raw scans
        assert client.get("/stats").json()["rows_scanned"] == stats_after_delta["rows_scanned"]


def test_transaction_rollback_on_materialization_failure(monkeypatch):
    """Verify that if cache_dates insertion fails in _ensure_dates_materialized, cache_daily is rolled back."""
    reports.startup()
    real_db = reports.db

    class FailingDBProxy:
        def execute(self, query: str, *args, **kwargs):
            if "INSERT INTO cache_dates" in query:
                raise duckdb.Error("Simulated failure inserting into cache_dates")
            return real_db.execute(query, *args, **kwargs)

        def __getattr__(self, name: str):
            return getattr(real_db, name)

    monkeypatch.setattr(reports, "db", FailingDBProxy())

    with pytest.raises(duckdb.Error, match="Simulated failure"):
        reports._ensure_dates_materialized([date(2025, 2, 24)])

    # Neither cache_daily nor cache_dates may contain partial state
    assert real_db.execute("SELECT count(*) FROM cache_daily").fetchone()[0] == 0
    assert real_db.execute("SELECT count(*) FROM cache_dates").fetchone()[0] == 0

    # Restoring normal DB connection allows clean materialization on retry
    monkeypatch.setattr(reports, "db", real_db)
    reports._ensure_dates_materialized([date(2025, 2, 24)])
    assert real_db.execute("SELECT count(*) FROM cache_daily").fetchone()[0] == 499
    assert real_db.execute("SELECT count(*) FROM cache_dates").fetchone()[0] == 1


def test_transaction_rollback_on_delta_failure(monkeypatch):
    """Verify that a mid-transaction failure in on_delta_loaded rolls back DELETE on cache_daily and drops temp table."""
    with TestClient(app) as client:
        w10_before = client.get("/shops/SHP141660/weekly-report?week=2025-W10").json()
        assert w10_before["revenue"] == 429524900

        real_db = reports.db

        class FailingDeltaDBProxy:
            def execute(self, query: str, *args, **kwargs):
                if "INSERT INTO cache_daily SELECT * FROM new_daily_aggregates" in query:
                    raise duckdb.Error("Simulated failure after DELETE FROM cache_daily")
                return real_db.execute(query, *args, **kwargs)

            def __getattr__(self, name: str):
                return getattr(real_db, name)

        raw.land_delta()
        monkeypatch.setattr(reports, "db", FailingDeltaDBProxy())

        with pytest.raises(duckdb.Error, match="Simulated failure after DELETE"):
            reports.on_delta_loaded()

        monkeypatch.setattr(reports, "db", real_db)

        # 1. Pre-delta cached aggregates for W10 must still be intact (DELETE rolled back)
        w10_after_rollback = client.get("/shops/SHP141660/weekly-report?week=2025-W10").json()
        assert w10_after_rollback["revenue"] == 429524900

        # 2. Unmaterialized delta dates (2025-03-21..23) were not added to cache_dates
        assert real_db.execute(
            "SELECT count(*) FROM cache_dates WHERE date >= DATE '2025-03-21'"
        ).fetchone()[0] == 0

        # 3. Temp table new_daily_aggregates was cleaned up by finally block
        assert real_db.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = 'new_daily_aggregates'"
        ).fetchone()[0] == 0


def test_delta_loaded_before_any_request():
    """Genuinely isolated test: delta arrives before the very first weekly report request.

    Runs in a separate Python process to ensure fresh, unpolluted module-level
    state (raw._landed, db tables, and scan counters).
    """
    code = """
import pytest
from fastapi.testclient import TestClient
from app.main import app

with TestClient(app) as client:
    # 1. Load delta immediately before querying any weekly reports
    load_res = client.post('/load-delta').json()
    assert load_res['loaded'] is True

    # 2. Query W10 for SHP141660 - should immediately reflect delta correction
    w10 = client.get('/shops/SHP141660/weekly-report?week=2025-W10').json()
    assert w10['revenue'] == 429344900
    assert w10['units'] == 1818

    # 3. Query W12 - should include full week with delta dates
    w12 = client.get('/shops/SHP141660/weekly-report?week=2025-W12').json()
    assert w12['revenue'] == 869744300
    assert w12['units'] == 3827
    assert pytest.approx(w12['revenue_wow_pct'], abs=0.01) == 205.89

    # 4. Repeated delta load returns loaded: False
    repeat = client.post('/load-delta').json()
    assert repeat['loaded'] is False
"""
    result = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True)
    assert result.returncode == 0, f"Subprocess failed:\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}"
