# Nesra Scale - Software Engineer Take-Home Task

*Nesra, Surabaya. Please read the whole document before you start.*

## 1. Welcome

Thank you for your interest in joining Nesra. This task is a small, realistic version of a problem we solve every day. We want to see how you think, not how many hours you can spend.

- **Timebox:** about 2 to 3 hours of work. We give you a working starter, so your time goes into the interesting part: caching and invalidation.
- **Suggested split:** 30 minutes looking at the data, 90 minutes on the cache and invalidation, 30 minutes on your README.
- **Unfinished is OK.** If you run out of time, write down what is missing and how you would finish it.

## 2. About Nesra Scale

Nesra Scale is a marketing-data product. It collects shop, ads and sales data from marketplaces such as TikTok Shop and Shopee, and shows brands simple insights about their business.

In production we use **BigQuery + dbt** for data, **Prefect** for pipelines, and **Cloudflare Workers** for serving. BigQuery charges by how much data a query scans, so every unnecessary read costs real money. That is why this task is about one thing:

**Cost-aware caching: serve fast, correct reports while reading as little raw data as possible.**

## 3. The data

The data is in [`data/`](data/). One row is one product in one shop on one day. The column names, types and meanings are in [`data/data_dictionary.md`](data/data_dictionary.md).

- **`shops_90d.csv.gz`** - 500 shops x 90 days of daily product sales, 21 December 2024 to 20 March 2025. It is based on real data, but masked (names, IDs and amounts are changed).
- **`delta.csv.gz`** - new data that arrives after the first load. It contains new days, and also some **late corrections to older days**.

**The data is real-world messy.** Look at it carefully before you trust it. Handle any problems in a sensible way, and write down what you found and what you decided.

## 4. The starter

The repository is a working Python service. You need [uv](https://docs.astral.sh/uv/) installed.

```
./run.sh                 # starts the service on http://localhost:8000
uv run pytest            # runs the tests
```

| File | What it is |
| :---- | :---- |
| `app/raw.py` | **Done.** Loads the raw data, provides `scan_raw()`, and counts the scan cost. |
| `app/main.py` | **Done.** The HTTP routes. |
| `app/reports.py` | **Your work.** Three empty functions: `startup()`, `weekly_report()` and `on_delta_loaded()`. |
| `tests/` | One example test to start from. |

You can add files and helpers, and change the starter if you have a good reason. Say why in your README.

## 5. What to build

The service has this endpoint:

```
GET /shops/{shop_id}/weekly-report?week=YYYY-Www
```

`week` is an ISO week, for example `2025-W11`. The JSON response must include:

| Field | Type | Meaning |
| :---- | :---- | :---- |
| `revenue` | integer | Total revenue of the shop in that week |
| `units` | integer | Total units sold in that week |
| `revenue_wow_pct` | number or `null` | Week-over-week change in revenue, in percent |

Nothing else is required. Do not spend time on extra fields.

These rules are fixed, because `measure.py` checks them:

- **Weeks** are ISO weeks, Monday to Sunday.
- **Partial weeks count.** `shops_90d.csv.gz` ends on Thursday 20 March 2025, so `2025-W12` has only 4 days of data before the delta arrives. Return the totals of the days that exist. Do not return `null` or an error for an incomplete week.
- **A shop with no rows in a week** gets `revenue: 0` and `units: 0`, not an error.
- **`revenue_wow_pct`** is `100 * (this week - previous week) / previous week`, using revenue. It is `null` when there is no previous week **or** when the previous week's revenue is 0.

## 6. Pretend the database is expensive

In this task, reading raw data is "expensive", like BigQuery. `app/raw.py` already handles this for you:

- **`scan_raw(source, dates)` is the only way to read raw rows.** Do not read the raw tables or the CSV files anywhere else.
- Like a BigQuery table partitioned by date, a scan pays for **every row on each date it reads, for all shops**, even if you only need one shop. The rate is Rp 0.05 per row.
- Every scan is logged, and `GET /stats` returns the running totals.

Reads from your own cache tables are free. Your cache must be **DuckDB tables holding aggregated results** (for example per shop and day, or per shop and week), not a copy of the raw rows.

## 7. Budget goal

Imagine this production load: a weekly report for **all 500 shops, every day, for one month**, each requested **20 times a day** (users refresh, share, open on mobile, and so on).

`measure.py` simulates a small version of this. It requests weekly reports for every shop, calls `POST /load-delta`, requests them again, checks every answer and reads `GET /stats`. Run it against a **freshly started** service, because the cost counters start from zero:

```
./run.sh                                      # in one terminal
uv run measure.py --url http://localhost:8000 # in another
```

It prints a scorecard and writes the details to `measure_result.json`.

**Target: the run passes, with a total under Rp 20,000.** For scale, scanning raw data on every request costs tens of millions. Cheaper is better.

## 8. Cache invalidation (the important part)

New data arrives after reports are already cached. When `POST /load-delta` is called, the starter makes the delta available as source `"delta"` and calls your `on_delta_loaded()`. You must:

1. Return only when your reports are up to date.
2. Make **affected reports refresh** with the new correct numbers. This includes reports for **older weeks** that changed because of late corrections.
3. Keep **unaffected reports cached**, so they cost **nothing** to serve again.

A short test or log output that proves this is very welcome.

## 9. Your README

Replace this file with your own README. Keep it short; one page is enough. It must include:

- **Cache design** - what you cache, the cache keys, precompute or on-demand, and what happens step by step when the delta arrives.
- **Cost** - the `measure.py` scorecard, and a rough estimate of what a naive version (no cache) would cost, so we can compare.
- **Data issues** - what problems you found in the data and how you handled them.
- **Notes** - how you used AI tools (see below), and anything unfinished.

## 10. AI tools

AI tools (Codex, Claude Code, Cursor and others) are **allowed and expected**. We use them every day.

- In your README, say **how** you used them: which tools, for which parts, and what you changed or fixed yourself.
- You must be able to **explain and modify any line** of your code live in the interview.

## 11. How to submit

- Start from this repository: click **Use this template** on GitHub to create your own copy.
- Push your work to a **public GitHub repository**.
- Send the repository link to the email or WhatsApp this task came with.

## 12. How we evaluate

| Area | Weight | What we look for |
| :---- | :---- | :---- |
| Cache design & invalidation correctness | 40% | Sensible keys, precise invalidation (including late corrections), no stale reports, unaffected reports stay cached |
| Cost | 25% | A passing `measure.py` run, smart choices to reduce scans, clear comparison with a naive version |
| Code quality & correctness | 15% | Readable code, correct revenue, units and WoW, a test that shows invalidation works |
| Data-issue handling | 10% | You noticed real problems, handled them sensibly, and documented your decisions |
| README communication | 10% | Clear, honest and short, including trade-offs and what is unfinished |

We value **clear thinking and honest trade-offs** more than lots of features.

## 13. What happens next

If your submission is a good fit, we will invite you to a **60-minute interview**:

- You walk us through your solution.
- We discuss how you work with AI tools.

## 14. Questions

If something is unclear, make a reasonable assumption, write it in your README, and continue. You can also reply to the email with questions.

Good luck, and have fun!
