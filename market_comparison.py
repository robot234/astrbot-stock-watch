"""Auditable matched-universe turnover and exact-index comparison, report only."""
from __future__ import annotations

from datetime import date, datetime, timezone
import hashlib
import json
import math
import re
import statistics


def utc_now():
    return datetime.now(timezone.utc).isoformat()


def timestamp(value):
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        raise ValueError("timezone required")
    return parsed.astimezone(timezone.utc)


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()).hexdigest()


def unknown(reason, **identity):
    return {
        **identity, "status": "unknown", "reason": reason,
        "turnover_change_pct": None, "market_return_pct": None,
        "index_return_pct": None, "divergence_pct": None,
    }


def evaluate(packet, *, as_of):
    identity = {key: packet.get(key, "") for key in ("trade_date", "previous_date", "benchmark_code", "universe_ref")}
    try:
        cutoff = timestamp(as_of)
        if timestamp(packet["available_at"]) > cutoff:
            return unknown("not_available_at_cutoff", **identity)
        if packet.get("status") == "unknown":
            return unknown(packet.get("reason") or "evidence_missing", **identity)
        current, previous = identity["trade_date"], identity["previous_date"]
        if date.fromisoformat(previous).isoformat() != previous or date.fromisoformat(current).isoformat() != current or previous >= current:
            return unknown("date_mismatch", **identity)
        if timestamp(packet["available_at"]) < timestamp(current + "T15:00:00+08:00"):
            return unknown("session_not_complete_at_observation", **identity)
        if not re.fullmatch(r"\d{6}\.(SH|SZ|CSI)", identity["benchmark_code"]) or not identity["universe_ref"]:
            return unknown("benchmark_or_universe_unknown", **identity)
        if packet.get("source") != "tushare" or packet.get("amount_unit") != "CNY" or packet.get("amount_source_unit") != "thousand_CNY":
            return unknown("source_or_unit_unknown", **identity)
        if packet.get("scope") != "matched_raw_batch_universe":
            return unknown("universe_scope_unknown", **identity)
        calendar = packet["calendar"]
        if len(calendar) != 2:
            return unknown("calendar_missing", **identity)
        for row, expected in zip(calendar, (previous, current)):
            if row.get("cal_date") != expected or row.get("exchange") != "SSE" or row.get("is_open") != 1:
                return unknown("calendar_mismatch", **identity)
        if calendar[1].get("pretrade_date") != previous:
            return unknown("not_adjacent_sessions", **identity)
        stock_days, factors = packet["stock_days"], packet["factors"]
        codes = set(packet["expected_codes"])
        if not codes or len(codes) != len(packet["expected_codes"]) or digest(sorted(codes)) != packet["universe_digest"]:
            return unknown("universe_mismatch", **identity)
        mapped, factor_maps = {}, {}
        for day in (previous, current):
            rows = stock_days[day]
            mapped[day] = {row["ts_code"]: row for row in rows}
            factor_maps[day] = {row["ts_code"]: row for row in factors[day]}
            if (len(mapped[day]) != len(rows) or set(mapped[day]) != codes
                    or len(factor_maps[day]) != len(factors[day]) or not codes.issubset(factor_maps[day])):
                return unknown("universe_or_factor_coverage", **identity)
            for code in codes:
                row, factor = mapped[day][code], factor_maps[day][code]
                if (row.get("trade_date") != day or row.get("source") != "tushare" or row.get("basis") != "unadjusted"
                        or factor.get("trade_date") != day
                        or factor.get("evidence") != f"tushare:adj_factor:{day}:{code}"):
                    return unknown("stock_or_factor_provenance", **identity)
                values = [float(row[key]) for key in ("close", "pre_close", "volume", "amount")]
                values.append(float(factor["adj_factor"]))
                if any(not math.isfinite(v) or v <= 0 for v in values):
                    return unknown("unusable_stock_or_factor", **identity)
        for code in codes:
            if (not math.isclose(float(factor_maps[previous][code]["adj_factor"]), float(factor_maps[current][code]["adj_factor"]), rel_tol=1e-10)
                    or not math.isclose(float(mapped[previous][code]["close"]), float(mapped[current][code]["pre_close"]), rel_tol=1e-8)):
                return unknown("corporate_action_or_price_discontinuity", **identity)
        index = packet["index_days"]
        if len(index) != 2:
            return unknown("index_missing", **identity)
        for row, expected in zip(index, (previous, current)):
            if (row.get("trade_date") != expected or row.get("ts_code") != identity["benchmark_code"]
                    or row.get("source") != "tushare:index_daily" or row.get("basis") != "index_points"):
                return unknown("index_identity_date_or_basis", **identity)
            if any(not math.isfinite(float(row[k])) or float(row[k]) <= 0 for k in ("close", "pre_close")):
                return unknown("index_price_invalid", **identity)
        if not math.isclose(float(index[0]["close"]), float(index[1]["pre_close"]), rel_tol=1e-8):
            return unknown("index_price_discontinuity", **identity)
        old_amount = math.fsum(float(mapped[previous][c]["amount"]) for c in sorted(codes))
        new_amount = math.fsum(float(mapped[current][c]["amount"]) for c in sorted(codes))
        turnover = (new_amount / old_amount - 1) * 100
        market = statistics.fmean((float(mapped[current][c]["close"]) / float(mapped[previous][c]["close"]) - 1) * 100 for c in sorted(codes))
        benchmark = (float(index[1]["close"]) / float(index[0]["close"]) - 1) * 100
        if not all(math.isfinite(v) for v in (turnover, market, benchmark, market - benchmark)):
            return unknown("nonfinite_metric", **identity)
        return {
            **identity, "status": "available", "reason": "",
            "turnover_change_pct": turnover, "market_return_pct": market,
            "index_return_pct": benchmark, "divergence_pct": market - benchmark,
            "sample_size": len(codes), "amount_unit": "CNY",
            "return_basis": "equal_weight_unadjusted_close_to_close",
            "available_at": packet["available_at"], "universe_digest": packet["universe_digest"],
        }
    except (KeyError, ValueError, TypeError, OverflowError, ZeroDivisionError):
        return unknown("malformed_evidence", **identity)


def render(result):
    if result.get("status") != "available":
        return f"市场量价：unknown（{result.get('reason') or 'evidence_missing'}）"
    return (
        f"市场量价｜{result['previous_date']}→{result['trade_date']}｜同池 {result['sample_size']} 只"
        f"｜成交额变化 {result['turnover_change_pct']:+.2f}%"
        f"｜等权涨跌 {result['market_return_pct']:+.2f}%"
        f"｜指数 {result['benchmark_code']} {result['index_return_pct']:+.2f}%"
        f"｜相对指数差 {result['divergence_pct']:+.2f}个百分点（仅报告，不调分）"
    )


def read_report(db, trade_date, benchmark_code, universe_ref, *, as_of):
    cutoff = timestamp(as_of).isoformat()
    identity = {"trade_date": trade_date, "benchmark_code": benchmark_code, "universe_ref": universe_ref}
    if not db.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='market_comparison_observations'").fetchone():
        return unknown("evidence_table_missing", **identity)
    row = db.execute(
        "SELECT * FROM market_comparison_observations WHERE trade_date=? AND benchmark_code=? AND universe_ref=? "
        "AND available_at<=? AND recorded_at<=? ORDER BY recorded_at DESC,rowid DESC LIMIT 1",
        (trade_date, benchmark_code, universe_ref, cutoff, cutoff),
    ).fetchone()
    if not row:
        return unknown("evidence_missing_at_cutoff", **identity)
    if hashlib.sha256(row["payload_json"].encode()).hexdigest() != row["payload_hash"]:
        return unknown("evidence_integrity_failed", **identity)
    packet = json.loads(row["payload_json"])
    if any(packet.get(key) != value for key, value in identity.items()):
        return unknown("evidence_identity_mismatch", **identity)
    return {**evaluate(packet, as_of=as_of), "observation_id": row["observation_id"]}


def main():
    import argparse
    from pathlib import Path
    import sqlite3
    import time

    parser = argparse.ArgumentParser(description="Read-only, exact-key market comparison audit; no network or migrations.")
    parser.add_argument("--database", required=True)
    parser.add_argument("--trade-date", required=True)
    parser.add_argument("--benchmark-code", required=True)
    parser.add_argument("--universe-ref", required=True)
    parser.add_argument("--as-of", required=True, help="Timezone-aware availability cutoff")
    args = parser.parse_args()
    path = Path(args.database).resolve(strict=True)
    db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    deadline = time.monotonic() + 20
    db.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
    try:
        db.execute("PRAGMA query_only=ON")
        db.execute("BEGIN")
        result = read_report(db, args.trade_date, args.benchmark_code, args.universe_ref, as_of=args.as_of)
        print(json.dumps({**result, "read_only": True}, sort_keys=True, allow_nan=False))
    finally:
        db.rollback()
        db.close()


if __name__ == "__main__":
    main()
