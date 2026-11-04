# /// script
# requires-python = ">=3.11"
# dependencies = ["duckdb>=1.4,<2"]
# ///
"""Run against a fresh service. Write measure_result.json in the current directory.

Reference policy: exact deduplication, delta replacement by (shop, product, date),
other rows kept as delivered, and available calendar weeks including partial W12.
WoW is 100 * (current - previous) / previous, or null for missing/zero previous
revenue. Integers match exactly; percentages use absolute tolerance 0.01.
Anomalies and their next-week dependencies receive numeric diagnostics only.
Duplicate diagnostics compare removed/kept values with other reference choices fixed.
HTTP counters are self-reported; scan routing and aggregate-only storage need review.
Only W08-W12 reports (and their W07 baseline) are scored. Data issues on earlier
dates do not affect the score; reviewers read the candidate's README for those.
Cost is the service's lifetime counter, so startup precompute counts; the scorecard
shows what was already spent before the first request of this run.
"""

import argparse
import hashlib
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
import http.client
import json
import math
from pathlib import Path
import random
import statistics
import threading
import time
from types import MappingProxyType
from typing import Mapping
from urllib.parse import quote, urlsplit

import duckdb

Key = tuple[str, date]
Report = tuple[int, int, float | None]
Expected = Mapping[Key, Report]
Ambiguous = Mapping[Key, tuple[str, ...]]
FIELDS = ("revenue", "units", "revenue_wow_pct")
BUDGET_RP = Decimal(20000)
DATA_SHA256 = {
    "shops_90d.csv.gz": "2524b6c4ec36821fc1c97757a3ccbac9639cc7bbb7af5d0b74166552ccba766d",
    "delta.csv.gz": "ca04709b1cbfbee44893f93e3b75beddb3691197ca17a964ea5f1130d9e3741c",
}
# Shop-weeks whose numbers depend on how a candidate handles anomalies other than
# exact duplicates. Precomputed from the files pinned above, so the script does not
# describe the anomalies. Shared by both phases; each phase uses its own weeks.
DATA_ISSUE_WEEKS = {
    "base": (
        "SHP042488/2025-W08", "SHP042488/2025-W09", "SHP042488/2025-W10", "SHP042488/2025-W11",
        "SHP042488/2025-W12", "SHP410771/2025-W08", "SHP468596/2025-W12", "SHP483681/2025-W08",
        "SHP483681/2025-W09", "SHP483681/2025-W10", "SHP483681/2025-W11", "SHP483681/2025-W12",
        "SHP860524/2025-W08", "SHP860524/2025-W09", "SHP860524/2025-W10", "SHP860524/2025-W11",
        "SHP860524/2025-W12", "SHP980903/2025-W08", "SHP980903/2025-W09",
    ),
    "merged": (
        "SHP042488/2025-W09", "SHP042488/2025-W10", "SHP042488/2025-W11", "SHP042488/2025-W12",
        "SHP468596/2025-W12", "SHP483681/2025-W09", "SHP483681/2025-W10", "SHP483681/2025-W11",
        "SHP483681/2025-W12", "SHP860524/2025-W09", "SHP860524/2025-W10", "SHP860524/2025-W11",
        "SHP860524/2025-W12", "SHP980903/2025-W09",
    ),
}
SCHEMA = {
    "shop_id": "VARCHAR", "shop_name": "VARCHAR", "platform": "VARCHAR",
    "date": "DATE", "product_id": "VARCHAR", "product_name": "VARCHAR",
    "category": "VARCHAR", "price": "BIGINT", "units_sold": "BIGINT",
    "orders": "BIGINT", "revenue": "BIGINT", "ad_spend": "BIGINT",
    "views": "BIGINT", "clicks": "BIGINT",
}


@dataclass(frozen=True)
class Oracle:
    before: Expected
    after: Expected
    before_kept: Expected
    after_kept: Expected
    before_ambiguous: Ambiguous
    after_ambiguous: Ambiguous
    evidence: dict


@dataclass(frozen=True)
class Stats:
    rows: int
    cost: Decimal


@dataclass(frozen=True)
class Observation:
    key: Key
    latency_ms: float
    report: Report | None
    error: str | None


def check_data(data: Path):
    for filename, digest in DATA_SHA256.items():
        if hashlib.sha256((data / filename).read_bytes()).hexdigest() != digest:
            raise ValueError(f"{data / filename} differs from the published data file")


def build_oracle(data: Path) -> Oracle:
    check_data(data)
    with duckdb.connect() as db:
        for table, filename in (("base", "shops_90d.csv.gz"), ("delta", "delta.csv.gz")):
            db.read_csv(str(data / filename), header=True, columns=SCHEMA).create(table)
        db.execute("""
            CREATE TABLE merged AS
            SELECT b.* FROM base b ANTI JOIN delta d USING (shop_id, product_id, date)
            UNION ALL SELECT * FROM delta;
            CREATE TABLE wanted AS
            SELECT DISTINCT shop_id, make_date(2025, 2, 17) + (i * 7)::INTEGER AS week
            FROM base CROSS JOIN range(5) r(i);
        """)
        shops = db.execute("SELECT count(DISTINCT shop_id) FROM base").fetchone()[0]
        if shops != 500:
            raise ValueError(f"expected 500 shops, found {shops}")
        maps, kept_maps, reasons = [], [], []
        for table, first_week in (("base", "2025-02-17"), ("merged", "2025-02-24")):
            db.execute(f"""
                CREATE OR REPLACE TEMP TABLE duplicates AS
                SELECT *, count(*) AS copies FROM {table} GROUP BY ALL HAVING count(*) > 1;
            """)
            for distinct in ("DISTINCT", ""):
                rows = db.execute(f"""
                    WITH weekly AS (
                        SELECT shop_id, date_trunc('week', date)::DATE AS week,
                               sum(revenue) AS revenue, sum(units_sold) AS units
                        FROM (SELECT {distinct} * FROM {table}) GROUP BY ALL
                    )
                    SELECT w.shop_id, w.week, coalesce(c.revenue, 0), coalesce(c.units, 0),
                           CASE WHEN p.revenue IS NULL OR p.revenue = 0 THEN NULL
                                ELSE 100.0 * (coalesce(c.revenue, 0) - p.revenue) / p.revenue END
                    FROM wanted w LEFT JOIN weekly c USING (shop_id, week)
                    LEFT JOIN weekly p ON p.shop_id = w.shop_id AND p.week = w.week - 7
                    WHERE w.week >= DATE '{first_week}' ORDER BY w.shop_id, w.week
                """).fetchall()
                report_map = {(s, w): (r, u, p) for s, w, r, u, p in rows}
                if distinct:
                    maps.append(MappingProxyType(report_map))
                else:
                    kept_maps.append(report_map)
            rows = db.execute(f"""
                WITH expanded AS (
                    SELECT shop_id, date_trunc('week', date)::DATE + offset_days::INTEGER AS week, reason
                    FROM (SELECT shop_id, date, 'exact_duplicate' AS reason FROM duplicates)
                CROSS JOIN (VALUES (0), (7)) n(offset_days)
                )
                SELECT shop_id, week, list(DISTINCT reason ORDER BY reason)
                FROM expanded JOIN wanted USING (shop_id, week)
                WHERE week >= DATE '{first_week}' GROUP BY ALL ORDER BY shop_id, week
            """).fetchall()
            found = {(s, w): set(r) for s, w, r in rows}
            for key in report_map:
                if name(key) in DATA_ISSUE_WEEKS[table]:
                    found.setdefault(key, set()).add("data_issue")
            reasons.append(MappingProxyType({k: tuple(sorted(found[k])) for k in sorted(found)}))
            kept_maps[-1] = MappingProxyType({
                k: v for k, v in kept_maps[-1].items()
                if "exact_duplicate" in reasons[-1].get(k, ())})
        evidence = dict(zip(("base_physical_rows", "delta_physical_rows", "touched_base_rows"),
            db.execute("""SELECT (SELECT count(*) FROM base), (SELECT count(*) FROM delta),
                (SELECT count(*) FROM base WHERE date IN (SELECT date FROM delta))""").fetchone()))
        evidence.update(shops=shops)
    return Oracle(*maps, *kept_maps, *reasons, evidence)


class API:
    def __init__(self, url: str, timeout: float):
        self.url = urlsplit(url)
        if self.url.scheme not in ("http", "https") or not self.url.hostname:
            raise ValueError("--url must be an http or https URL")
        if self.url.query or self.url.fragment or self.url.username or self.url.password:
            raise ValueError("--url cannot contain credentials, a query, or a fragment")
        self.timeout = timeout
        self.local = threading.local()

    def request(self, method: str, path: str) -> object:
        try:
            connection = getattr(self.local, "connection", None)
            if connection and (path == "/stats" or method == "POST"):
                connection.close()
                self.local.connection = None
            if not getattr(self.local, "connection", None):
                cls = http.client.HTTPSConnection if self.url.scheme == "https" else http.client.HTTPConnection
                self.local.connection = cls(self.url.hostname, self.url.port, timeout=self.timeout)
            conn = self.local.connection
            conn.request(method, self.url.path.rstrip("/") + path, headers={"Accept": "application/json"})
            response = conn.getresponse()
            body = response.read()
            if not 200 <= response.status < 300:
                raise ValueError(f"HTTP {response.status}: {body[:160].decode('utf-8', errors='replace')}")
            if method == "POST":
                return None
            return json.loads(body, parse_constant=reject_constant, parse_float=finite_float)
        except (OSError, http.client.HTTPException, ValueError, OverflowError) as exc:
            if getattr(self.local, "connection", None):
                self.local.connection.close()
                self.local.connection = None
            raise ValueError(f"{method} {path}: {exc}") from None


def reject_constant(value: str):
    raise ValueError(f"invalid JSON number {value}")


def finite_float(value: str) -> float:
    number = float(value)
    if not math.isfinite(number):
        raise ValueError(f"non-finite JSON number {value}")
    return number


def parse_report(payload: object) -> Report:
    if not isinstance(payload, dict):
        raise ValueError("report must be a JSON object")
    for field in FIELDS:
        if field not in payload:
            raise ValueError(f"missing field {field}")
    for field in FIELDS[:2]:
        if type(payload[field]) is not int:
            raise ValueError(f"{field} must be an integer, excluding booleans")
    pct = payload[FIELDS[2]]
    if pct is not None and (type(pct) not in (int, float) or not math.isfinite(pct)):
        raise ValueError("revenue_wow_pct must be a finite number or null")
    return payload["revenue"], payload["units"], pct


def parse_stats(payload: object) -> Stats:
    if not isinstance(payload, dict) or type(payload.get("rows_scanned")) is not int:
        raise ValueError("stats.rows_scanned must be a nonnegative integer")
    rows, cost = payload["rows_scanned"], payload.get("cost_rp")
    if rows < 0 or type(cost) not in (int, float):
        raise ValueError("stats counters must be nonnegative finite numbers, excluding booleans")
    amount = Decimal(str(cost))
    if not amount.is_finite() or amount < 0 or abs(amount - Decimal(rows) * Decimal(".05")) > Decimal(".000001"):
        raise ValueError(f"stats cost_rp {cost} disagrees with {rows} rows at Rp 0.05")
    return Stats(rows, Decimal(rows) * Decimal(".05"))


def equal_field(index: int, left, right) -> bool:
    if left is None or right is None:
        return left is right
    return abs(left - right) <= 0.01 if index == 2 else left == right


def compare_fields(actual: Report, expected: Report) -> tuple[str, ...]:
    return tuple(field for i, field in enumerate(FIELDS) if not equal_field(i, actual[i], expected[i]))


def stale_fields(actual: Report, old: Report, new: Report) -> tuple[str, ...]:
    return tuple(field for i, field in enumerate(FIELDS)
                 if old[i] != new[i] and equal_field(i, actual[i], old[i])
                 and not equal_field(i, actual[i], new[i]))


def name(key: Key) -> str:
    return f"{key[0]}/{key[1].strftime('%G-W%V')}"


def run_phase(api: API, expected: Expected, ambiguous: Ambiguous, *, seed: int,
              workers: int, kept: Expected, previous: Expected | None = None) -> dict:
    keys = sorted(expected) * 3
    random.Random(seed).shuffle(keys)
    required = 7500 if previous is None else 6000
    if len(keys) != required:
        raise ValueError(f"workload must contain {required} requests, found {len(keys)}")

    def observe(key: Key) -> Observation:
        start = time.perf_counter()
        try:
            payload = api.request("GET", f"/shops/{quote(key[0], safe='')}/weekly-report?week={key[1].strftime('%G-W%V')}")
            return Observation(key, (time.perf_counter() - start) * 1000, parse_report(payload), None)
        except (ValueError, OverflowError) as exc:
            return Observation(key, (time.perf_counter() - start) * 1000, None, str(exc))

    result = dict(requests=len(keys), seed=seed, schema_errors=[], mismatches=[],
                  decision_diagnostics=[], stale={}, decision_stale={}, decision_dependent={
                      name(k): list(v) for k, v in sorted(ambiguous.items())})
    labels = ("removed", "kept", "both", "neither", "invalid")
    duplicate_reports = {
        name(k): dict(removed=expected[k], kept=v, matches=dict.fromkeys(labels, 0),
                      classifications=[], actuals=[], other_reasons=[
                          r for r in ambiguous[k] if r != "exact_duplicate"])
        for k, v in sorted(kept.items())}
    latency = []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for observation in pool.map(observe, keys):
            key, actual = observation.key, observation.report
            latency.append(observation.latency_ms)
            if key in kept:
                entry = duplicate_reports[name(key)]
                classification = "invalid"
                if actual is not None:
                    removed_match = not compare_fields(actual, expected[key])
                    kept_match = not compare_fields(actual, kept[key])
                    classification = {(True, False): "removed", (False, True): "kept",
                                      (True, True): "both", (False, False): "neither"}[
                                          removed_match, kept_match]
                entry["matches"][classification] += 1
                entry["classifications"].append(classification)
                entry["actuals"].append(actual)
            if observation.error:
                result["schema_errors"].append(dict(key=name(key), error=observation.error))
                continue
            fields = compare_fields(actual, expected[key])
            if fields:
                target = "decision_diagnostics" if key in ambiguous else "mismatches"
                result[target].append(dict(key=name(key), fields=fields, actual=actual, expected=expected[key]))
            if previous is not None:
                fields = stale_fields(actual, previous[key], expected[key])
                if fields:
                    target = "decision_stale" if key in ambiguous else "stale"
                    result[target].setdefault(name(key), set()).update(fields)
    classification_counts = dict.fromkeys((*labels, "inconsistent"), 0)
    for entry in duplicate_reports.values():
        observed = set(entry["classifications"])
        entry["classification"] = next(iter(observed)) if len(observed) == 1 else "inconsistent"
        classification_counts[entry["classification"]] += 1
    result["duplicate_policy"] = dict(count=len(duplicate_reports),
                                      classification_counts=classification_counts,
                                      shop_weeks=duplicate_reports)
    for target in ("stale", "decision_stale"):
        result[target] = {key: sorted(value) for key, value in sorted(result[target].items())}
    latency.sort()
    result["latency_ms"] = dict(p50=round(statistics.median(latency), 3),
                                p95=round(latency[math.ceil(len(latency) * .95) - 1], 3))
    checked = len(keys) - 3 * len(ambiguous)
    schema_failures = sum(item["key"] not in result["decision_dependent"]
                          for item in result["schema_errors"])
    result["checked"] = checked
    result["correct_count"] = checked - len(result["mismatches"]) - schema_failures
    result["correctness_pct"] = 100 * result["correct_count"] / checked
    result["correct"] = not result["schema_errors"] and not result["mismatches"] and not result["stale"]
    return result


def print_scorecard(result: dict):
    print(f"MEASURE {'PASS' if result['pass'] else 'FAIL'} | fresh service required")
    if "cost" in result:
        c = result["cost"]
        print(f"Cost Rp | before run {c['before_run_rp']:,.2f} | phase1 {c['phase1_rp']:,.2f} "
              f"| phase2 {c['phase2_rp']:,.2f} | total {c['total_rp']:,.2f} "
              f"| budget <{BUDGET_RP:,} {'PASS' if c['budget_pass'] else 'FAIL'}")
    for label in ("before", "after"):
        if label not in result:
            continue
        p = result[label]
        print(f"{label.capitalize()} {'PASS' if p['correct'] else 'FAIL'} | requests {p['requests']} "
              f"| correct {p['correctness_pct']:.2f}% ({p['correct_count']}/{p['checked']}) "
              f"| schema errors {len(p['schema_errors'])} | numeric failures {len(p['mismatches'])} "
              f"| stale keys {len(p['stale'])} | decision-dependent {len(p['decision_dependent'])} "
              f"| diagnostic mismatches {len(p['decision_diagnostics'])} "
              f"| p50/p95 {p['latency_ms']['p50']:.2f}/{p['latency_ms']['p95']:.2f} ms")
        duplicates = p["duplicate_policy"]
        print(f"{label} duplicate-affected {duplicates['count']} | value matches; other reference choices fixed "
              + "| " + ", ".join(f"{k} {v}" for k, v in duplicates["classification_counts"].items()))
        for key, entry in duplicates["shop_weeks"].items():
            print(f"{label} duplicate {key} | {entry['classification']} | "
                  + ", ".join(f"{k} {v}" for k, v in entry["matches"].items()))
    print(f"Efficiency | {result.get('efficiency', 'incomplete measurement')}")
    for error in result["errors"]:
        print(f"Error | {error}")
    for label in ("before", "after"):
        if label not in result:
            continue
        p = result[label]
        for group in ("decision_dependent", "stale", "decision_stale"):
            if p[group]:
                print(f"{label} {group} names | " + "; ".join(f"{k} [{', '.join(v)}]" for k, v in sorted(p[group].items())))
        errors = sorted({f"{item['key']} {item['error']}" for item in p["schema_errors"]})
        if errors:
            print(f"{label} response errors | " + "; ".join(errors[:5]))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--url", required=True)
    parser.add_argument("--data", type=Path, default=Path("data"))
    parser.add_argument("--workers", type=int, default=8)
    parser.add_argument("--timeout", type=float, default=30)
    args = parser.parse_args()
    result = {"pass": False, "errors": [], "policy": __doc__,
              "workers": args.workers, "timeout": args.timeout}
    try:
        if args.workers < 1 or not math.isfinite(args.timeout) or args.timeout <= 0:
            raise ValueError("workers and timeout must be positive and finite")
        oracle = build_oracle(args.data)
        result["oracle"] = oracle.evidence
        result["expectations"] = {
            label: [[name(k), *v] for k, v in sorted(expected.items())]
            for label, expected in (("before", oracle.before), ("after", oracle.after))}
        api = API(args.url, args.timeout)
        snapshots = []
        result["stats"] = []
        for label, expected, ambiguous, seed in (
            ("before", oracle.before, oracle.before_ambiguous, 42),
            ("after", oracle.after, oracle.after_ambiguous, 43),
        ):
            if label == "before":
                initial = parse_stats(api.request("GET", "/stats"))
                snapshots.append(initial)
                result["stats"].append(dict(rows=initial.rows, cost_rp=str(initial.cost)))
            else:
                api.request("POST", "/load-delta")
            result[label] = run_phase(api, expected, ambiguous, seed=seed, workers=args.workers,
                                      kept=oracle.before_kept if label == "before" else oracle.after_kept,
                                      previous=oracle.before if label == "after" else None)
            snapshot = parse_stats(api.request("GET", "/stats"))
            if snapshot.rows < snapshots[-1].rows or snapshot.cost < snapshots[-1].cost:
                raise ValueError("stats counters decreased; use cumulative lifetime counters")
            snapshots.append(snapshot)
            result["stats"].append(dict(rows=snapshot.rows, cost_rp=str(snapshot.cost)))
        initial, phase1, total = snapshots[0].cost, snapshots[1].cost, snapshots[2].cost
        result["cost"] = dict(before_run_rp=float(initial), phase1_rp=float(phase1 - initial),
                              phase2_rp=float(total - phase1), total_rp=float(total),
                              budget_pass=total < BUDGET_RP)
        result["pass"] = result["cost"]["budget_pass"] and result["before"]["correct"] and result["after"]["correct"]
        result["efficiency"] = "within scan budget" if result["cost"]["budget_pass"] else "raw reads exceed scan budget"
    except (ValueError, OSError, duckdb.Error, OverflowError) as exc:
        result["errors"].append(str(exc))
    result_written = True
    try:
        Path("measure_result.json").write_text(json.dumps(result, indent=2, allow_nan=False) + "\n")
    except OSError as exc:
        result_written = False
        result["pass"] = False
        result["errors"].append(f"cannot write measure_result.json: {exc}")
    print_scorecard(result)
    if result_written:
        print("Details | measure_result.json")
    return 0 if result["pass"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
