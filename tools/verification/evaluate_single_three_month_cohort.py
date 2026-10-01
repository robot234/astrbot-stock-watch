"""Evaluate one pre-frozen selection after 90 calendar days, never reselect daily."""

import argparse
from collections import defaultdict
from contextlib import contextmanager
from datetime import date, timedelta
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import sys


@contextmanager
def connect_readonly(path):
    if not path.is_file():
        raise ValueError("database absent")
    connection = sqlite3.connect(f"{path.resolve().as_uri()}?mode=ro", uri=True)
    try:
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA query_only=ON")
        yield connection
    finally:
        connection.close()


def evaluate(connection, packet, *, batch_id, holding_days=90, min_market_rows=4000):
    if holding_days < 1 or min_market_rows < 1:
        raise ValueError("invalid bounds")
    if packet.get("schema_version") != "historical_reasoning_input.v1":
        raise ValueError("unexpected frozen packet schema")
    as_of = date.fromisoformat(packet["date"])
    target = as_of + timedelta(days=holding_days)
    candidates = packet["candidates"]
    codes = [item["code"] for item in candidates]
    if not codes or len(codes) != len(set(codes)) or len(codes) > 100:
        raise ValueError("missing or duplicate frozen candidates")
    if any(item["reasoning_status"] != "not_run" or item["technical_risk"] != "unknown"
           for item in candidates):
        raise ValueError("this evaluates the unchanged technical cohort only")
    batch = connection.execute(
        "SELECT b.source,b.basis,b.status,b.quality,b.published_at,d.dataset_key "
        "FROM batches b JOIN datasets d ON d.dataset_id=b.dataset_id WHERE b.batch_id=?",
        (batch_id,),
    ).fetchone()
    if not batch or (batch["source"], batch["basis"], batch["status"], batch["quality"],
                     batch["dataset_key"]) != ("tushare", "unadjusted", "published", "good", "tushare_daily"):
        raise ValueError("invalid batch provenance")
    partitions = connection.execute(
        "SELECT bd.trade_date,bd.row_count,dp.row_count AS actual_count,dp.validation_status,"
        "dp.source,dp.basis FROM batch_days bd JOIN day_partitions dp "
        "ON dp.partition_id=bd.partition_id WHERE bd.batch_id=? AND bd.trade_date>=? "
        "AND bd.trade_date<=? ORDER BY bd.trade_date",
        (batch_id, as_of.isoformat(), target.isoformat()),
    ).fetchall()
    dates = [item["trade_date"] for item in partitions]
    if (not dates or dates[0] != as_of.isoformat() or dates[-1] != target.isoformat()
            or len(set(dates)) != len(dates) or len(dates) < 2):
        raise ValueError("fixed 90-day endpoint or session metadata missing")
    if any(item["validation_status"] != "validated" or item["source"] != batch["source"]
           or item["basis"] != batch["basis"] or item["row_count"] != item["actual_count"]
           or item["row_count"] < min_market_rows for item in partitions):
        raise ValueError("unverified market-day provenance")
    marks = ",".join("?" for _ in codes)
    bars = defaultdict(dict)
    for row in connection.execute(
        "SELECT pb.trade_date,pb.code,pb.name,pb.open,pb.close,pb.source,pb.basis "
        "FROM batch_days bd JOIN partition_bars pb ON pb.partition_id=bd.partition_id "
        f"WHERE bd.batch_id=? AND bd.trade_date>=? AND bd.trade_date<=? AND pb.code IN ({marks})",
        (batch_id, as_of.isoformat(), target.isoformat(), *codes),
    ):
        if row["source"] != batch["source"] or row["basis"] != batch["basis"]:
            raise ValueError("mixed bar provenance")
        if row["trade_date"] in bars[row["code"]]:
            raise ValueError("duplicate candidate bar")
        bars[row["code"]][row["trade_date"]] = row
    results = []
    for candidate in candidates:
        code = candidate["code"]
        as_of_bar = bars[code].get(dates[0])
        if (as_of_bar is None or as_of_bar["close"] is None or
                not math.isclose(as_of_bar["close"], candidate["as_of_unadjusted_close"], rel_tol=1e-9)):
            raise ValueError(f"frozen selection price changed: {code}")
        end = bars[code].get(dates[-1])
        start_open = bars[code].get(dates[1])
        valid_end = end is not None and end["close"] is not None and end["close"] > 0
        valid_open = start_open is not None and start_open["open"] is not None and start_open["open"] > 0
        start_close = candidate["as_of_unadjusted_close"]
        end_close = end["close"] if valid_end else None
        results.append({
            "code": code, "name": as_of_bar["name"], "technical_score": candidate["technical_score"],
            "selection_close": start_close, "end_close": end_close,
            "outcome": "observed_raw_close" if valid_end else "unknown_endpoint_bar",
            "raw_90d_close_change_pct": round((end_close / start_close - 1) * 100, 4) if valid_end else None,
            "positive_raw_close_change": end_close > start_close if valid_end else None,
            "next_session": dates[1], "next_open": start_open["open"] if valid_open else None,
            "next_open_to_end_close_pct": round((end_close / start_open["open"] - 1) * 100, 4)
            if valid_end and valid_open else None,
            "observed_intermediate_bars": len(bars[code]), "expected_session_bars": len(dates),
            "missing_intermediate_sessions": len(dates) - len(bars[code]),
            "execution_status": "unknown_no_historical_tradability_or_cost_proof",
        })
    observed = [item for item in results if item["outcome"] == "observed_raw_close"]
    wins = sum(item["positive_raw_close_change"] for item in observed)
    return {
        "mode": "single_frozen_cohort_retrospective_technical_only", "as_of": as_of.isoformat(),
        "target": target.isoformat(), "holding_calendar_days": holding_days,
        "batch_published_at": batch["published_at"], "selection_was_repeated": False,
        "point_in_time_universe_proven": False, "historical_market_status_proven": False,
        "reported_return_kind": "non_executable_unadjusted_price_only_no_corporate_actions_or_costs",
        "trading_sessions_in_window": len(dates), "candidates": results,
        "summary": {
            "selected_once": len(results), "observed": len(observed), "unknown": len(results) - len(observed),
            "positive": wins, "non_positive": len(observed) - wins,
            "observed_positive_pct": round(100 * wins / len(observed), 4) if observed else None,
            "strict_positive_pct_of_all_selected": round(100 * wins / len(results), 4),
            "mean_raw_change_pct": round(sum(item["raw_90d_close_change_pct"] for item in observed) / len(observed), 4)
            if observed else None,
            "completed_news_risk_strategy_decisions": 0,
        },
        "warnings": [
            "A single ten-stock cohort cannot establish generalizable strategy accuracy.",
            "The retrospective batch was published after selection and does not prove historical point-in-time availability.",
            "News/risk judgments, adjustment factors, suspension/limit status, fill price and fees are not verified.",
        ],
    }


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", required=True, type=Path)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--db", required=True, type=Path)
    parser.add_argument("--batch-id", required=True)
    parser.add_argument("--holding-days", type=int, default=90)
    parser.add_argument("--min-market-rows", type=int, default=4000)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        packet_bytes = args.packet.read_bytes()
        manifest = json.loads(args.manifest.read_bytes())
        if manifest["packet_sha256"].get(args.packet.name) != hashlib.sha256(packet_bytes).hexdigest():
            raise ValueError("frozen selection hash mismatch")
        if args.output.exists():
            raise ValueError("output already exists")
        with connect_readonly(args.db) as connection:
            report = evaluate(connection, json.loads(packet_bytes), batch_id=args.batch_id,
                              holding_days=args.holding_days, min_market_rows=args.min_market_rows)
        report["frozen_packet_sha256"] = hashlib.sha256(packet_bytes).hexdigest()
        args.output.write_text(json.dumps(report, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n",
                               encoding="utf-8")
        print(json.dumps({"output": str(args.output), "summary": report["summary"]}, ensure_ascii=False))
        return 0
    except (OSError, ValueError, KeyError, TypeError, sqlite3.Error) as error:
        print(f"single-cohort evaluation not executed: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
