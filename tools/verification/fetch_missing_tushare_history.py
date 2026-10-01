"""Read-only bounded Tushare daily fetcher for the missing 120-session prefix."""

from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys


TARGET_DATES = (
    "2025-12-24", "2025-12-25", "2025-12-26", "2025-12-29", "2025-12-30", "2025-12-31",
    "2026-01-05", "2026-01-06", "2026-01-07", "2026-01-08", "2026-01-09",
    "2026-01-12", "2026-01-13", "2026-01-14", "2026-01-15", "2026-01-16",
    "2026-01-19", "2026-01-20", "2026-01-21", "2026-01-22", "2026-01-23",
    "2026-01-26", "2026-01-27", "2026-01-28", "2026-01-29", "2026-01-30",
    "2026-02-02", "2026-02-03", "2026-02-04", "2026-02-05", "2026-02-06",
    "2026-02-09", "2026-02-10", "2026-02-11", "2026-02-12", "2026-02-13",
    "2026-02-24", "2026-02-25", "2026-02-26", "2026-02-27",
    "2026-03-02", "2026-03-03", "2026-03-04", "2026-03-05", "2026-03-06",
    "2026-03-09", "2026-03-10", "2026-03-11", "2026-03-12", "2026-03-13",
    "2026-03-16", "2026-03-17", "2026-03-18", "2026-03-19", "2026-03-20",
    "2026-03-23", "2026-03-24", "2026-03-25", "2026-03-26", "2026-03-27",
    "2026-03-30", "2026-03-31", "2026-04-01", "2026-04-02",
)
REQUIRED_FIELDS = ("trade_date", "code", "ts_code", "open", "high", "low", "close", "pre_close", "pct_change", "volume", "amount")


def _repo_imports() -> None:
    repo_parent = Path(__file__).resolve().parents[3]
    if str(repo_parent) not in sys.path:
        sys.path.insert(0, str(repo_parent))


def _digest(value) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _redact(value: object, secret: str) -> str:
    text = str(value or "")
    return text.replace(secret, "<redacted>") if secret else text


def _validate_rows(rows: list[dict], expected_date: str) -> dict:
    codes = [str(row.get("code") or "") for row in rows]
    duplicate_codes = sorted({code for code in codes if codes.count(code) > 1 and code})
    invalid_rows = []
    market_counts: dict[str, int] = {}
    for row in rows:
        if any(field not in row for field in REQUIRED_FIELDS):
            invalid_rows.append("missing_field")
            continue
        if row["trade_date"] != expected_date or row["source"] != "tushare" or row["basis"] != "unadjusted":
            invalid_rows.append("provenance")
            continue
        try:
            open_price = float(row["open"])
            high = float(row["high"])
            low = float(row["low"])
            close = float(row["close"])
            pre_close = float(row["pre_close"])
            pct_change = float(row["pct_change"])
            expected_pct = (close / pre_close - 1) * 100
            valid_prices = high >= max(open_price, close) and low <= min(open_price, close) and high >= low > 0
            valid_pct = abs(pct_change - expected_pct) <= 0.35
        except (TypeError, ValueError, ZeroDivisionError):
            valid_prices = False
            valid_pct = False
        if not valid_prices:
            invalid_rows.append("ohlc")
        if not valid_pct:
            invalid_rows.append("pct_change")
        ts_code = str(row.get("ts_code") or "")
        market = ts_code.rsplit(".", 1)[-1] if "." in ts_code else "unknown"
        market_counts[market] = market_counts.get(market, 0) + 1
    return {
        "row_count": len(rows),
        "unique_code_count": len(set(codes)),
        "duplicate_codes": duplicate_codes,
        "invalid_row_checks": invalid_rows,
        "market_counts": market_counts,
        "daily_omitted_suspended_unknown": True,
        "historical_universe_status": "unknown_without_bak_basic",
    }


async def fetch_one(provider, trade_date: str, *, page_size: int, min_row_count: int, token: str) -> dict:
    observed_at = datetime.now(timezone.utc).isoformat()
    rows: list[dict] = []
    pages = []
    offset = 0
    async with provider.http.slot() as client:
        while True:
            page = await provider.fetch_daily_page(trade_date, offset=offset, page_size=page_size, client=client)
            pages.append({
                "offset": offset,
                "row_count": len(page),
                "page_digest": _digest(page),
                "terminal": not page,
            })
            if not page:
                break
            rows.extend(page)
            offset += page_size
    checks = _validate_rows(rows, trade_date)
    checks.update({
        "min_row_count": min_row_count,
        "row_threshold_passed": checks["row_count"] >= min_row_count,
        "full_day_bulk_sample": bool(rows) and not checks["duplicate_codes"] and not checks["invalid_row_checks"],
    })
    return {
        "status": "verified" if checks["full_day_bulk_sample"] and checks["row_threshold_passed"] else "invalid",
        "source": "tushare.daily",
        "basis": "unadjusted",
        "trade_date": trade_date,
        "observed_at": observed_at,
        "pagination": {"page_size": page_size, "pages": pages, "request_count": len(pages)},
        "checks": checks,
        "row_digest": _digest(rows),
        "token_exposed": False,
    }


async def run(args) -> dict:
    token = os.environ.get(args.token_env, "").strip()
    target_dates = list(TARGET_DATES)
    start = max(0, min(args.start_index, len(target_dates)))
    selected = target_dates[start:start + max(0, args.max_days)]
    report = {
        "mode": "read_only_isolated_tushare_daily",
        "observed_at": datetime.now(timezone.utc).isoformat(),
        "status": "blocked" if not token else "started",
        "source": "tushare.daily",
        "basis": "unadjusted",
        "requested_window": {"as_of": "2026-06-26", "missing_sessions": len(target_dates), "first": target_dates[0], "last": target_dates[-1]},
        "selection": {"start_index": start, "max_days": args.max_days, "selected_dates": selected},
        "credentials": {"token_env": args.token_env, "present": bool(token), "value_printed": False},
        "days": [],
        "missing_day_tracking": {"target": target_dates, "fetched": [], "failed": [], "unattempted": target_dates},
        "production_mutation": False,
        "pi_contacted": False,
        "config_modified": False,
    }
    if not token:
        report["reason"] = "configured_tushare_access_not_available_in_safe_local_environment"
        return report
    _repo_imports()
    from astrbot_stock_watch.providers import HttpRuntime, TushareBulkDailyProvider

    provider = TushareBulkDailyProvider(token, url=args.url, http_runtime=HttpRuntime(args.timeout, 1), storage=None, page_size=args.page_size, retry_attempts=args.retry_attempts)
    try:
        report["status"] = "partial"
        for trade_date in selected:
            try:
                day = await fetch_one(provider, trade_date, page_size=args.page_size, min_row_count=args.min_row_count, token=token)
                report["days"].append(day)
                if day["status"] == "verified":
                    report["missing_day_tracking"]["fetched"].append(trade_date)
                else:
                    report["missing_day_tracking"]["failed"].append(trade_date)
            except Exception as exc:
                report["days"].append({"status": "failed", "source": "tushare.daily", "trade_date": trade_date, "observed_at": datetime.now(timezone.utc).isoformat(), "error": _redact(exc, token), "token_exposed": False})
                report["missing_day_tracking"]["failed"].append(trade_date)
        report["missing_day_tracking"]["unattempted"] = [day for day in target_dates if day not in report["missing_day_tracking"]["fetched"] and day not in report["missing_day_tracking"]["failed"]]
        if len(report["missing_day_tracking"]["fetched"]) == len(target_dates):
            report["status"] = "complete"
    finally:
        await provider.http.close()
    return report


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token-env", default="TUSHARE_TOKEN")
    parser.add_argument("--url", default="https://api.tushare.pro")
    parser.add_argument("--timeout", type=float, default=30)
    parser.add_argument("--retry-attempts", type=int, default=3)
    parser.add_argument("--page-size", type=int, default=6000)
    parser.add_argument("--min-row-count", type=int, default=4000)
    parser.add_argument("--start-index", type=int, default=0)
    parser.add_argument("--max-days", type=int, default=1)
    args = parser.parse_args(argv)
    if not 1 <= args.page_size <= 6000 or args.max_days < 0 or args.min_row_count < 1:
        parser.error("invalid bounded fetch arguments")
    report = asyncio.run(run(args))
    print(json.dumps(report, ensure_ascii=False, sort_keys=True, allow_nan=False))
    return 3 if report["status"] == "blocked" else 0


if __name__ == "__main__":
    raise SystemExit(main())
