# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Real-time Korean ETF investment score system. Collects data from Toss Securities (supply/demand) and Naver Finance (stock prices, pre-market), aggregates it into ETF-level scores (YG Score, Naver Score, Premarket Score), and stores results in Supabase.

## Running Scripts

Scripts accept session arguments and run in a loop until a stop time is reached:

```bash
# Naver real-time stock data (loops 08:50–12:00 or 12:00–15:20)
python naver/naver_realtime.py morning
python naver/naver_realtime.py afternoon
python naver/naver_realtime.py --once   # single run

# ETF price data (same pattern)
python naver/naver_etf_price.py morning
python naver/naver_etf_price.py --once

# Pre-market data (one-time run at 08:51)
python naver/naver_premarket.py

# Toss supply/demand crawler (requires Chrome)
python toss_crawling/toss_yg_score_stk.py --session morning
python toss_crawling/toss_yg_score_stk.py --session afternoon

# Market trend snapshots (runs once, captures preset snapshot times)
python naver/naver_trend.py
```

## Installing Dependencies

```bash
pip install -r requirements.txt
```

Requires Chrome installed for the Selenium-based Toss crawler.

## Environment Variables

Copy `.env` with:
- `Project_URL` — Supabase project URL
- `Secret_keys` — Supabase service role key (bypasses RLS)

## Architecture

### Data Flow

```
Raw source tables (_stk)  →  Server-side RPC  →  Aggregated ETF tables (_etf)
```

- `naver_realtime_stk` → `calculate_naver_etf_score()` → `naver_realtime_etf`
- `naver_premarket_stk` → `calculate_naver_premarket_score()` → `naver_premarket_etf`
- `toss_yg_score_stk` → `calculate_yg_score_server(target_time)` → `toss_yg_score_etf`

ETF rankings are served from `_ranking_table` views built on top of the `_etf` tables. The history tables (`naver_realtime_etf_history`, `naver_premarket_etf_history`) are deprecated — do not use them.

### Market Open Validation

All real-time crawlers check at 08:58 whether `naver_premarket_stk` contains today's data. If no pre-market data exists → holiday or non-trading day → process exits immediately **without deleting existing DB data**, preserving the last trading day's records.

### Morning Protection (Toss Crawler)

Between 09:00–10:00, institutional (기관) investor amounts are zeroed out to avoid carryover from the previous trading day.

### Shared DB Utilities

`toss_crawling/supabase_client.py` contains all shared Supabase helpers: `get_kst_now()`, `load_toss_data_from_supabase()`, `save_score_to_supabase()`, `delete_old_scores()`.

## GitHub Actions Automation

All workflows are `workflow_dispatch` (manual trigger). Each workflow runs two jobs — morning (08:50–12:00) and afternoon (12:00–15:20) — with `if: always()` so the afternoon job runs even if morning fails.

Legacy files (Kiwoom API, old Nextrade integration) are in `backup/` and are not active.

## Conventions

- All scores are rounded to 2 decimal places.
- Database table naming: `*_stk` (raw source), `*_etf` (calculated result), `*_ranking_table` (view).
- KST (UTC+9) is used for all timestamps; use `get_kst_now()` from `supabase_client.py`.
- Data collection answers are in Korean; code comments may be in English or Korean.
