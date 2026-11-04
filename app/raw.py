"""Raw data access. This file is done: you should not need to change it.

The raw tables behave like BigQuery tables partitioned by date. A scan pays for
every row of every date it touches, for all shops, even if you only need one
shop. `scan_raw()` is the only way to read them. Do not query the `raw_*`
tables anywhere else.

Example:

    with scan_raw("base", [date(2025, 3, 10), date(2025, 3, 11)]) as scan:
        db.execute(f"SELECT shop_id, sum(revenue) FROM {scan} GROUP BY shop_id")

`scan` is the name of a temporary table holding the scanned rows. It is
dropped when the `with` block ends. Keep your cache in your own DuckDB tables
on `db`, holding aggregated results, not copies of raw rows.
"""

from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from datetime import date
import itertools
import logging
from pathlib import Path
import threading

import duckdb

COST_PER_ROW_RP = 0.05
DATA = Path(__file__).resolve().parent.parent / "data"
FILES = {"base": "shops_90d.csv.gz", "delta": "delta.csv.gz"}
COLUMNS = {
    "shop_id": "VARCHAR", "shop_name": "VARCHAR", "platform": "VARCHAR",
    "date": "DATE", "product_id": "VARCHAR", "product_name": "VARCHAR",
    "category": "VARCHAR", "price": "BIGINT", "units_sold": "BIGINT",
    "orders": "BIGINT", "revenue": "BIGINT", "ad_spend": "BIGINT",
    "views": "BIGINT", "clicks": "BIGINT",
}

log = logging.getLogger("scan_raw")

# One shared connection for raw tables and your cache tables. DuckDB connections
# are not thread-safe, so app/main.py holds `lock` around every call into
# app/reports.py. You do not need to lock anything yourself.
db = duckdb.connect()
lock = threading.RLock()

_landed: set[str] = set()
_rows_scanned = 0
_scan_ids = itertools.count()


def _land(source: str) -> None:
    """Make a source available, like data arriving in the warehouse. Free."""
    path = DATA / FILES[source]
    db.execute(f"CREATE TABLE raw_{source} AS SELECT * FROM read_csv(?, header = true, columns = ?)",
               [str(path), COLUMNS])
    _landed.add(source)


def land_delta() -> bool:
    """Called by POST /load-delta. Return False if the delta was already loaded."""
    with lock:
        if "delta" in _landed:
            return False
        _land("delta")
        return True


@contextmanager
def scan_raw(source: str, dates: Iterable[date] | None = None) -> Iterator[str]:
    """Scan raw rows of `source` ("base" or "delta") on `dates` (None = every date).

    Yields the name of a temporary table holding the rows. Costs every row on
    those dates, for all shops, at Rp 0.05 per row.
    """
    global _rows_scanned
    if source not in _landed:
        raise ValueError(f"raw source {source!r} is not loaded")
    name = f"scan_{next(_scan_ids)}"
    with lock:
        if dates is None:
            scope = "all dates"
            db.execute(f"CREATE TEMP TABLE {name} AS SELECT * FROM raw_{source}")
        else:
            dates = sorted(set(dates))
            scope = f"{len(dates)} dates"
            db.execute(f"CREATE TEMP TABLE {name} AS SELECT * FROM raw_{source} "
                       "WHERE date IN (SELECT unnest(?::DATE[]))", [dates])
        rows = db.execute(f"SELECT count(*) FROM {name}").fetchone()[0]
        _rows_scanned += rows
        log.info("%s, %s: %s rows, Rp %.2f (total Rp %.2f)", source, scope, f"{rows:,}",
                 rows * COST_PER_ROW_RP, _rows_scanned * COST_PER_ROW_RP)
    try:
        yield name
    finally:
        with lock:
            db.execute(f"DROP TABLE IF EXISTS {name}")


def stats() -> dict:
    with lock:
        return {"rows_scanned": _rows_scanned, "cost_rp": _rows_scanned * COST_PER_ROW_RP}


_land("base")
