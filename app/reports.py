"""Weekly reports and daily aggregation cache with precise delta invalidation.

Rules:
- Read raw rows only through scan_raw().
- Keep cache in DuckDB tables on `db`, holding aggregated results (no raw product rows).
"""

from datetime import date, timedelta

from app.raw import db, scan_raw


def startup() -> None:
    """Initialize aggregate cache tables and materialized-date tracking."""
    # cache_daily: daily aggregates per shop and date
    db.execute("""
        CREATE TABLE IF NOT EXISTS cache_daily (
            shop_id VARCHAR,
            date DATE,
            revenue BIGINT,
            units BIGINT,
            PRIMARY KEY (shop_id, date)
        )
    """)
    # cache_dates: dates that have been materialized
    db.execute("""
        CREATE TABLE IF NOT EXISTS cache_dates (
            date DATE PRIMARY KEY
        )
    """)


def _ensure_dates_materialized(dates: list[date]) -> None:
    """Scan and materialize daily aggregates for any dates not yet in cache_dates."""
    if not dates:
        return

    # Check which dates are missing from cache_dates
    placeholders = ", ".join(["?"] * len(dates))
    cached = set(
        row[0]
        for row in db.execute(
            f"SELECT date FROM cache_dates WHERE date IN ({placeholders})", dates
        ).fetchall()
    )
    missing_dates = sorted(set(dates) - cached)
    if not missing_dates:
        return

    # Scan raw base for missing dates
    with scan_raw("base", missing_dates) as scan:
        # Atomic transaction: commit daily aggregates and materialization markers together
        db.begin()
        try:
            # Deduplicate exact records before aggregation, aggregate per shop and date
            db.execute(f"""
                INSERT INTO cache_daily
                SELECT shop_id, date, sum(revenue)::BIGINT AS revenue, sum(units_sold)::BIGINT AS units
                FROM (SELECT DISTINCT * FROM {scan})
                GROUP BY shop_id, date
            """)
            # Mark all missing dates as materialized (even if some dates had zero rows)
            db.execute(
                f"INSERT INTO cache_dates VALUES {', '.join(['(?)'] * len(missing_dates))}",
                missing_dates,
            )
            db.commit()
        except Exception:
            db.rollback()
            raise


def weekly_report(shop_id: str, week_start: date) -> dict:
    """Return the report for one shop and one ISO week.

    `week_start` is the Monday of the requested week.
    Returns `revenue`, `units`, and `revenue_wow_pct`.
    """
    prev_week_start = week_start - timedelta(days=7)

    # Dates to ensure materialized: 7 days of previous week + 7 days of current week
    all_needed_dates = [prev_week_start + timedelta(days=i) for i in range(14)]
    _ensure_dates_materialized(all_needed_dates)

    # Query aggregates for both weeks from cache_daily
    curr_end = week_start + timedelta(days=6)
    prev_end = prev_week_start + timedelta(days=6)

    curr_row = db.execute(
        """
        SELECT coalesce(sum(revenue), 0)::BIGINT, coalesce(sum(units), 0)::BIGINT
        FROM cache_daily
        WHERE shop_id = ? AND date BETWEEN ? AND ?
    """,
        [shop_id, week_start, curr_end],
    ).fetchone()

    prev_row = db.execute(
        """
        SELECT coalesce(sum(revenue), 0)::BIGINT, coalesce(sum(units), 0)::BIGINT
        FROM cache_daily
        WHERE shop_id = ? AND date BETWEEN ? AND ?
    """,
        [shop_id, prev_week_start, prev_end],
    ).fetchone()

    curr_rev, curr_units = int(curr_row[0]), int(curr_row[1])
    prev_rev = int(prev_row[0])

    if prev_rev == 0:
        wow_pct = None
    else:
        wow_pct = 100.0 * (curr_rev - prev_rev) / prev_rev

    return {
        "revenue": curr_rev,
        "units": curr_units,
        "revenue_wow_pct": wow_pct,
    }


def on_delta_loaded() -> None:
    """Handle delta arrival: scan delta, re-scan affected base dates, update cache."""
    # 1. Scan delta to inspect distinct dates
    with scan_raw("delta") as scan_d:
        delta_dates = [
            row[0]
            for row in db.execute(
                f"SELECT DISTINCT date FROM {scan_d} ORDER BY date"
            ).fetchall()
        ]

        if not delta_dates:
            return

        # 2. Read base for those dates
        with scan_raw("base", delta_dates) as scan_b:
            # 3. Apply replacement semantics by (shop_id, product_id, date)
            # Deduplicate exact records, aggregate per shop and date into a temp table
            db.execute(f"""
                CREATE TEMP TABLE new_daily_aggregates AS
                WITH effective AS (
                    SELECT b.* FROM {scan_b} b ANTI JOIN {scan_d} d USING (shop_id, product_id, date)
                    UNION ALL
                    SELECT * FROM {scan_d}
                )
                SELECT shop_id, date, sum(revenue)::BIGINT AS revenue, sum(units_sold)::BIGINT AS units
                FROM (SELECT DISTINCT * FROM effective)
                GROUP BY shop_id, date
            """)

    try:
        # Atomic transaction: delete existing aggregates, insert new ones, and update markers
        db.begin()
        try:
            placeholders = ", ".join(["?"] * len(delta_dates))
            db.execute(f"DELETE FROM cache_daily WHERE date IN ({placeholders})", delta_dates)
            db.execute("INSERT INTO cache_daily SELECT * FROM new_daily_aggregates")
            db.execute(f"""
                INSERT OR IGNORE INTO cache_dates
                VALUES {', '.join(['(?)'] * len(delta_dates))}
            """, delta_dates)
            db.commit()
        except Exception:
            db.rollback()
            raise
    finally:
        db.execute("DROP TABLE IF EXISTS new_daily_aggregates")
