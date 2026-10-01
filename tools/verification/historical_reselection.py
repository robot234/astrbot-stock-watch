"""Read-only retrospective technical-score simulation, never a live recommendation."""

from __future__ import annotations

import argparse
from collections import defaultdict
from contextlib import contextmanager
from datetime import date
import hashlib
import importlib.util
import json
from pathlib import Path
import sqlite3
import sys


def load_core(path):
    spec = importlib.util.spec_from_file_location("stock_watch_offline_core", path)
    if not spec or not spec.loader:
        raise ValueError("core.py not found")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@contextmanager
def connect_readonly(path):
    if not path.is_file():
        raise ValueError(f"database absent: {path}")
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        yield connection
    finally:
        connection.close()


def observed_return_summary(outcomes):
    observed = [item["target_close"] / item["as_of_close"] - 1
                for item in outcomes if item["outcome"] == "observed_unadjusted_close"]
    return {
        "label": "non_executable_retrospective_unadjusted_close_to_close",
        "denominator_observed_only": len(observed),
        "mean_raw_return_pct": round(sum(observed) / len(observed) * 100, 4) if observed else None,
        "positive": sum(value > 0 for value in observed),
        "negative": sum(value < 0 for value in observed),
        "zero": sum(value == 0 for value in observed),
    }


def run(db, core, *, batch_id, as_of, through, horizon=5, limit=10, deep_limit=300,
        min_market_rows=4000):
    if date.fromisoformat(as_of) >= date.fromisoformat(through):
        raise ValueError("as-of must precede through")
    if not 1 <= horizon <= 60 or not 1 <= limit <= 100 or not 1 <= deep_limit <= 1000 or min_market_rows < 1:
        raise ValueError("selection bounds invalid")
    batch = db.execute(
        "SELECT b.status,b.quality,b.source,b.basis,b.published_at,d.dataset_key "
        "FROM batches b JOIN datasets d ON d.dataset_id=b.dataset_id WHERE b.batch_id=?",
        (batch_id,),
    ).fetchone()
    if not batch or (batch["status"], batch["quality"], batch["basis"], batch["dataset_key"]) != (
        "published", "good", "unadjusted", "tushare_daily"
    ) or not batch["source"] or batch["source"].lower() in {"unknown", "none"}:
        raise ValueError("select a published good unadjusted tushare_daily batch")
    partitions = db.execute(
        "SELECT bd.trade_date,bd.partition_id,bd.row_count,dp.row_count AS actual_count,"
        "dp.validation_status,dp.source,dp.basis FROM batch_days bd "
        "JOIN day_partitions dp ON dp.partition_id=bd.partition_id "
        "WHERE bd.batch_id=? ORDER BY bd.trade_date", (batch_id,),
    ).fetchall()
    by_day = defaultdict(list)
    for part in partitions:
        by_day[part["trade_date"]].append(part)
        if (part["validation_status"] != "validated" or part["basis"] != "unadjusted"
                or part["source"] != batch["source"] or part["row_count"] != part["actual_count"]):
            raise ValueError("inconsistent batch partition provenance")
    dates = sorted(by_day)
    if as_of not in by_day or through not in by_day or any(len(parts) != 1 for parts in by_day.values()):
        raise ValueError("endpoints must exist and every batch day needs one partition")
    prior = sum(day <= as_of for day in dates)
    report = {
        "mode": "retrospective_only_not_point_in_time", "batch_id": batch_id,
        "as_of": as_of, "through": through, "batch_published_at": batch["published_at"],
        "source": batch["source"], "basis": batch["basis"],
        "earliest_bar_date": dates[0], "history_dates_at_start": prior,
        "configured_live_raw_session_count": 120,
        "strategy": "core.apply_daily_indicators + core.score_quote, technical only; top amount deep screen",
        "selection": {"deep_limit": deep_limit, "min_score": 10, "limit": limit,
                      "min_market_rows": min_market_rows},
        "horizon_trading_days": horizon,
        "warnings": [
            "Retrospective only: publication and historical universe/status availability at each as-of date are unproven.",
            "No as-of ST/suspension/limit, factors or announcement evidence; risk is unknown, never eligible.",
            "Unadjusted raw close-to-close comparison only, not executable returns; no costs, dividends or corporate actions.",
        ], "days": [],
    }
    if prior < 120:
        report["warnings"].append(f"Only {prior} sessions at start, not the configured 120-session raw lookback.")
    if not batch["published_at"] or batch["published_at"][:10] > as_of:
        report["warnings"].append("Selected batch was not published by the first as-of date; strict point-in-time backtest is impossible.")
    selected_dates = [day for day in dates if as_of <= day <= through]
    for day_index, day in enumerate(selected_dates):
        current = [dict(row) for row in db.execute(
            "SELECT pb.* FROM batch_days bd JOIN partition_bars pb ON pb.partition_id=bd.partition_id "
            "WHERE bd.batch_id=? AND bd.trade_date=? ORDER BY pb.code", (batch_id, day),
        )]
        if len(current) != by_day[day][0]["row_count"] or len({row["code"] for row in current}) != len(current):
            raise ValueError(f"incomplete cross section: {day}")
        if len(current) < min_market_rows:
            raise ValueError(f"not full-market by minimum row count: {day}, {len(current)} < {min_market_rows}")
        if any(row["trade_date"] != day or row["basis"] != "unadjusted" or row["source"] != batch["source"] for row in current):
            raise ValueError(f"cross section provenance mismatch: {day}")
        quotes = [core.Quote(row["code"], row["name"], row["close"],
                             prev_close=row["pre_close"], amount=row["amount"],
                             pct_change=row["pct_change"], volume=row["volume"],
                             source=row["source"]) for row in current]
        tradable = [quote for quote in quotes if core.is_tradable(quote)]
        tradable.sort(key=lambda quote: (float(quote.amount or 0), quote.code), reverse=True)
        targets = tradable[:deep_limit]
        history = defaultdict(list)
        for offset in range(0, len(targets), 400):
            codes = [quote.code for quote in targets[offset:offset + 400]]
            marks = ",".join("?" for _ in codes)
            for raw in db.execute(
                "SELECT pb.* FROM batch_days bd JOIN partition_bars pb ON pb.partition_id=bd.partition_id "
                f"WHERE bd.batch_id=? AND bd.trade_date<=? AND pb.code IN ({marks}) "
                "ORDER BY pb.trade_date", (batch_id, day, *codes),
            ):
                row = dict(raw)
                if row["trade_date"] > day or row["basis"] != "unadjusted" or row["source"] != batch["source"]:
                    raise ValueError("history leakage or mixed provenance")
                row["price_basis"] = row["basis"]
                history[row["code"]].append(row)
        usable = [quote for quote in targets if core.apply_daily_indicators(quote, history[quote.code])
                  and quote.indicator_last_date == day and quote.indicator_last_close == quote.price]
        coverage = len(usable) / len(targets) if targets else 0
        candidates = []
        if coverage >= 0.8:
            candidates = [core.score_quote(quote) for quote in usable]
            candidates = [item for item in candidates if item.base_score >= 10 and item.risk_level != "blocked"]
            candidates.sort(key=lambda item: (item.score, item.quote.amount, item.quote.code), reverse=True)
            candidates = candidates[:limit]
        day_report = {"date": day, "universe_rows": len(current), "tradable_price_only": len(tradable),
                      "deep_targets": len(targets), "history_usable": len(usable),
                      "history_coverage": round(coverage, 4),
                      "status": "selected" if coverage >= 0.8 else "unknown_history_coverage",
                      "candidates": []}
        target_date = selected_dates[day_index + horizon] if day_index + horizon < len(selected_dates) else None
        # Future prices are queried only after the day's candidate list is frozen.
        for item in candidates:
            outcome = "pending" if target_date is None else "unknown"
            close = None
            if target_date:
                future = db.execute(
                    "SELECT pb.close,pb.basis,pb.source FROM batch_days bd "
                    "JOIN partition_bars pb ON pb.partition_id=bd.partition_id "
                    "WHERE bd.batch_id=? AND bd.trade_date=? AND pb.code=?",
                    (batch_id, target_date, item.quote.code),
                ).fetchone()
                if future and future["basis"] == "unadjusted" and future["source"] == batch["source"] and future["close"] > 0:
                    outcome, close = "observed_unadjusted_close", future["close"]
            day_report["candidates"].append({
                "code": item.quote.code, "score": item.base_score, "risk": item.risk_level,
                "as_of_close": item.quote.price, "history_days": item.quote.history_days,
                "target_date": target_date, "outcome": outcome, "target_close": close,
                "raw_return_pct": round((close / item.quote.price - 1) * 100, 4) if close is not None else None,
            })
        report["days"].append(day_report)
    outcomes = [item for day in report["days"] for item in day["candidates"]]
    report["summary"] = {"trading_dates": len(selected_dates), "candidate_events": len(outcomes),
                         "observed_raw_close": sum(item["outcome"] == "observed_unadjusted_close" for item in outcomes),
                         "pending": sum(item["outcome"] == "pending" for item in outcomes),
                         "unknown": sum(item["outcome"] == "unknown" for item in outcomes),
                         "unusable_dates": sum(day["status"] != "selected" for day in report["days"]),
                         "observed_returns": observed_return_summary(outcomes)}
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--core", required=True, type=Path)
    parser.add_argument("--as-of", default="2026-06-26")
    parser.add_argument("--through", default="2026-09-24")
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--deep-limit", type=int, default=300)
    parser.add_argument("--min-market-rows", type=int, default=4000)
    args = parser.parse_args(argv)
    try:
        core = load_core(args.core)
        with connect_readonly(args.db) as db:
            result = run(db, core, batch_id=args.batch_id, as_of=args.as_of,
                         through=args.through, horizon=args.horizon,
                         limit=args.limit, deep_limit=args.deep_limit,
                         min_market_rows=args.min_market_rows)
        result["core_sha256"] = hashlib.sha256(args.core.read_bytes()).hexdigest()
        print(json.dumps(result, ensure_ascii=False, sort_keys=True, allow_nan=False))
        return 0
    except (OSError, ValueError, sqlite3.Error) as exc:
        print(f"historical reselection not executed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
