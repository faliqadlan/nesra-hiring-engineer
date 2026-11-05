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
- Every report request scans 14 days of partitioned raw data (~81,000 rows, or Rp 4,050 per request).
- For the simulated benchmark workload (13,500 total requests):
  - **Naive cost:** 13,500 * Rp 4,050 = **~Rp 54,675,000**.
  - **Nesra cached cost:** **Rp 14,505.95**.
  - **Cost reduction:** **99.97% reduction** in raw data scan spend.

---

## 3. Data Issues & Decisions

During data audit and verification of `shops_90d.csv.gz` and `delta.csv.gz`:
1. **Exact Duplicate Rows:**
   - The base dataset contains 69 exact duplicate records (516,213 physical rows vs. 516,144 distinct rows).
   - *Resolution:* All scans perform `SELECT DISTINCT *` prior to grouping and summing. This deduplicates exact identical records without dropping legitimate distinct products on the same date.
2. **Effective Merged Row Count:**
   - Base physical rows total 516,213, while delta contains 17,458 physical rows.
   - 12 base rows match delta on `(shop_id, product_id, date)` and are replaced by the anti-join (`516,213 - 12 = 516,201` surviving base rows).
   - Combining surviving base rows and delta via `UNION ALL` yields `516,201 + 17,458 = 533,659` physical merged rows.
   - Base contains 69 exact duplicate records (none of which coincide with the 12 replaced keys). Deduplicating exact identical records across the merged dataset (`SELECT DISTINCT *`) eliminates these 69 duplicate rows, resulting in an effective distinct merged count of exactly **533,590** rows (`533,659 - 69 = 533,590`).
3. **Partial Calendar Weeks:**
   - `2025-W12` contains only 4 days of data in base (March 17–20, 2025). The remaining days (March 21–23, 2025) arrive in delta.
   - *Resolution:* Days are materialized dynamically; partial weeks report sums of available days without padding artificial zeroes or rejecting requests.
4. **Historical Late Corrections & Cross-Week Dependencies:**
   - Delta provides late corrections for W10 dates (March 7–8, 2025), impacting shops like `SHP141660`.
   - *Resolution:* By storing daily aggregates and computing weekly reports on demand, updating W10 daily aggregates automatically updates W10 totals and the previous-week baseline for W11 WoW calculations without requiring separate weekly cache invalidation.
5. **Negative Revenue & Price Anomalies:**
   - 3 records in base exhibit negative prices/revenues (e.g. `SHP909583`, `SHP410771`, `SHP468596`).
   - *Resolution:* In line with the specification and benchmark reference policy, these were preserved as delivered without arbitrary truncation.

---

## 4. Trade-Offs

- **Daily vs. Weekly Aggregation:** Caching daily aggregates requires DuckDB to sum 7 rows per week at request time. However, this dramatically simplifies invalidation (updating a single date automatically corrects both its containing week and the subsequent week's WoW), completely preventing stale dependent metrics.
- **Lazy vs. Full Startup Precomputation:** Scanning all 90 days of base data at startup would cost Rp 25,810.65 upfront—exceeding the entire benchmark budget before answering a single query. Lazy date materialization ensures scans only occur for actively queried date partitions.

---

## 5. AI Assistance Disclosure

- **AI Tools Used:**
  - **ChatGPT:** High-level solution planning, architecture review, and handoff structuring.
  - **Antigravity:** Local workspace execution, DuckDB query implementation, test writing, and benchmark execution.
- **Human Review & Verification:**
  - Inspected and verified DuckDB schema constraints and replacement anti-join semantics.
  - Validated physical vs. distinct row counts (`533,590` effective merged rows) against raw gzip source data.
  - Reviewed test assertions in `tests/test_reports.py` and benchmark scorecard outputs from `measure.py`.
