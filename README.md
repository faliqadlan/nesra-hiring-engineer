# Nesra Scale — Cost-Aware Weekly Reporting & Invalidation

This repository implements the Nesra weekly reporting service with date-partitioned lazy daily aggregates, exact deduplication, and atomic delta invalidation.

---

## 1. Cache Design & Architecture

### Core Design Philosophy
Reading raw rows via `scan_raw()` simulates partitioned analytical warehouse scans (like BigQuery), incurring Rp 0.05 per row for all shops on touched dates. To minimize cost and avoid scanning unnecessary dates:
- **No full-dataset precompute at startup:** `startup()` only defines lightweight table schemas on DuckDB.
- **Lazy daily aggregates per shop:** Instead of caching reports at the weekly level, raw product sales are aggregated daily per shop (`cache_daily(shop_id, date, revenue, units)`). A weekly report merely sums the cached daily totals for the requested week (and previous week for WoW).
- **Date-level materialization tracking:** `cache_dates(date)` tracks materialized calendar dates. Unmaterialized dates are scanned on demand in a single consolidated scan.
- **Strictly aggregate storage:** Raw product-level details (`product_id`, `category`, `ad_spend`, etc.) are never retained in persistent cache tables.

### Cache Keys & Schemas
- `cache_daily`: `PRIMARY KEY (shop_id, date)` storing `revenue BIGINT` and `units BIGINT`.
- `cache_dates`: `PRIMARY KEY (date)` tracking dates whose raw data has already been read and aggregated into `cache_daily`.

### Invalidation Algorithm (`on_delta_loaded`)
When `POST /load-delta` is invoked:
1. **Delta Date Discovery:** Delta is scanned via `scan_raw("delta")` to identify distinct dates present in the batch (`delta_dates`).
2. **Partitioned Base Read:** The exact same subset of dates is retrieved from the base dataset using `scan_raw("base", delta_dates)`.
3. **Atomic Replacement Semantics:**
   - Base records are anti-joined with delta on `(shop_id, product_id, date)` to discard superseded base records.
   - The remaining base rows and delta rows are combined via `UNION ALL`.
   - Exact duplicate records are eliminated using `DISTINCT *`.
   - Daily aggregates are computed per `(shop_id, date)` into a temporary table `new_daily_aggregates`.
4. **Transactional Cache Update:**
   - Within an explicit DuckDB transaction (`db.begin()` ... `db.commit()` with `db.rollback()` on error), existing aggregates for `delta_dates` are deleted from `cache_daily`, `new_daily_aggregates` are inserted, and all `delta_dates` are committed into `cache_dates` simultaneously.
   - A matching transaction pattern is applied in `_ensure_dates_materialized()`, ensuring aggregate insertion and date-marker registration are committed atomically without partial materialization.

---

## 2. Benchmark Scorecard & Cost Analysis

### Official Benchmark (`measure.py`) Results
The service passes all correctness checks and operates well within the Rp 20,000 budget:

```
MEASURE PASS | fresh service required
Cost Rp | before run 0.00 | phase1 11,308.15 | phase2 3,197.80 | total 14,505.95 | budget <20,000 PASS
Before PASS | requests 7500 | correct 100.00% (7422/7422) | schema errors 0 | numeric failures 0 | stale keys 0 | decision-dependent 26 | diagnostic mismatches 0 | p50/p95 84.45/118.49 ms
After PASS  | requests 6000 | correct 100.00% (5943/5943) | schema errors 0 | numeric failures 0 | stale keys 0 | decision-dependent 19 | diagnostic mismatches 0 | p50/p95 98.36/220.30 ms
Efficiency  | within scan budget
```

- **Phase 1 Scans:** 226,163 rows scanned across scored dates (W07 baseline through W12 partial) = **Rp 11,308.15**.
- **Phase 2 Scans:** 17,458 delta rows + 46,498 touched base rows = 63,956 rows scanned = **Rp 3,197.80**.
- **Lifetime Scan Cost:** **Rp 14,505.95** (Budget: < Rp 20,000).

### Comparison against Naive Architecture
In a naive setup without caching:
- Every report request scans 14 days of partitioned raw data (~81,000 rows, or ~Rp 4,050 per request).
- **Benchmark workload (`measure.py` — 13,500 requests):**
  - **Naive cost:** `13,500 × Rp 4,050` = **~Rp 54,675,000**.
  - **Cached cost:** **Rp 14,505.95** (**99.97% reduction**).
- **Simulated 1-month production load (500 shops × 30 days × 20 requests/day = 300,000 requests):**
  - **Naive cost:** `300,000 × Rp 4,050` = **~Rp 1,215,000,000** (~Rp 1.22 billion/month).
  - **Cached cost:** Scanning 30 daily partitions (~172,000 rows) + delta updates costs **< Rp 15,000/month** (**99.999% reduction**).

---

## 3. Data Issues & Decisions

During data audit and verification of `shops_90d.csv.gz` and `delta.csv.gz`:
1. **Exact Duplicate Rows:**
   - The base dataset contains 69 exact duplicate records (516,213 physical rows vs. 516,144 distinct rows) across 4 shop-days (`SHP285014` on `2025-02-14`: 37 extra rows; `SHP866221` on `2025-02-17`: 7; `SHP422010` on `2025-02-28`: 12; `SHP332806` on `2025-03-10`: 13).
   - *Resolution:* All scans perform `SELECT DISTINCT *` prior to grouping and summing. This deduplicates exact identical records without dropping legitimate distinct products on the same date.
2. **Effective Merged Row Count:**
   - Base physical rows total 516,213, while delta contains 17,458 physical rows (all distinct).
   - 12 base rows match delta on `(shop_id, product_id, date)` and are replaced by the anti-join (`516,213 - 12 = 516,201` surviving base rows).
   - Combining surviving base rows and delta via `UNION ALL` yields `516,201 + 17,458 = 533,659` physical merged rows.
   - Base contains 69 exact duplicate records (none of which coincide with the 12 replaced keys). Deduplicating exact identical records across the merged dataset (`SELECT DISTINCT *`) eliminates these 69 duplicate rows, resulting in an effective distinct merged count of **533,590** rows (`533,659 - 69 = 533,590`).
3. **Partial Calendar Weeks:**
   - `2025-W12` contains only 4 days of data in base (March 17–20, 2025). The remaining 3 days (March 21–23, 2025; 17,446 rows) arrive in delta.
   - *Resolution:* Days are materialized dynamically; partial weeks report sums of available days without padding artificial zeroes or rejecting requests.
4. **Historical Late Corrections & Cross-Week Dependencies:**
   - Delta contains 12 historical rows across 8 dates in `2025-W10` (`2025-03-07`, `2025-03-08`), `2025-W11` (`2025-03-10`, `2025-03-11`, `2025-03-13`, `2025-03-14`), and `2025-W12` (`2025-03-17`, `2025-03-19`). Nine of these rows revise `revenue` and `units_sold` (e.g. `SHP141660` on `2025-03-07`), while 3 rows in `2025-W10` re-deliver unchanged values.
   - *Resolution:* Storing daily aggregates and computing weekly reports on demand ensures that updating `delta_dates` in `cache_daily` automatically refreshes both the affected week's totals and the subsequent week's WoW baseline without re-scanning unaffected dates.
5. **Negative Price & Revenue Records:**
   - 3 records in base exhibit negative prices and revenues (`SHP909583` on `2025-01-14`, `SHP410771` on `2025-02-12`, and `SHP468596` on `2025-03-18`).
   - *Resolution:* Preserved as delivered (consistent with adjustments/refunds and the benchmark reference policy).
6. **Zero-Price Promotional / Sample Units & Nullable `ad_spend`:**
   - 3 records in base (`SHP625423` on `2025-01-19`, `SHP135948` on `2025-01-24`, `SHP980903` on `2025-02-22`) have `price = 0` and `revenue = 0` with positive `units_sold` (6, 9, and 6 units).
   - 3,599 records in base (`SHP042488`, `SHP483681`, `SHP860524`) and 119 records in delta have `ad_spend IS NULL`.
   - *Resolution:* Aggregations do not filter out zero-price rows or rows with NULL non-key attributes, preventing silent undercounting of units or revenue.

---

## 4. Trade-Offs & Limitations

### Trade-Offs
- **Daily vs. Weekly Aggregation:** Caching daily aggregates requires DuckDB to sum up to 7 rows per week at request time (< 1 ms in-memory). In exchange, invalidation is exact at the date-partition grain: updating a date automatically corrects both its containing week and the following week's WoW baseline without re-scanning untouched days of either week.
- **Lazy vs. Full Startup Precomputation:** Scanning all 90 days of base data at startup would scan 516,213 rows (Rp 25,810.65) upfront—exceeding the Rp 20,000 benchmark budget before serving a single request. Lazy date materialization ensures scans only occur for queried date partitions.

### Known Starter / Scope Limitations
- **In-Memory Ephemerality:** `app/raw.py` initializes an in-memory DuckDB connection (`duckdb.connect()`). Cache tables reset on process restart, which is expected for `measure.py` (where lifetime scan counters also reset to zero on a fresh service).
- **Single-Connection Lock Serialization:** Because DuckDB connections are not thread-safe across concurrent writes, `app/main.py` serializes requests with `raw.lock` (`threading.RLock`).
- **One-Shot Delta Landing:** `raw.land_delta()` in the starter marks `"delta"` as landed before `reports.on_delta_loaded()` runs and only supports a single delta batch per process lifetime.

---

## 5. AI Assistance Disclosure

- **AI Tools Used:**
  - **ChatGPT:** High-level solution planning, architecture review, and handoff structuring.
  - **Antigravity:** Local workspace execution, DuckDB query implementation, automated test suite & rollback fault-injection verification, and benchmark execution.
- **Verification Performed:**
  - Verified DuckDB schema constraints, replacement `ANTI JOIN` semantics, and explicit transaction rollback behavior (`db.begin()` / `db.commit()` / `db.rollback()`).
  - Validated physical vs. distinct row counts (`533,590` effective merged rows) and data anomalies (`exact_duplicate`, negative price/revenue, zero-price units, `ad_spend IS NULL`) against the raw gzip source files.
  - Executed `pytest` (including reverse-order isolation and fault-injection tests) and `measure.py` against a fresh service instance.
