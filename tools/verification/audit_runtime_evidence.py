"""Bounded, read-only SQLite/source audit. Never initialize plugin runtime."""
from __future__ import annotations

import argparse
import ast
from collections import Counter
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import sqlite3
import time


def source_capabilities(root):
    root = Path(root)
    result = {}
    for relative in ("astrbot/core/star/context.py", "astrbot/core/platform/platform.py"):
        path = root / relative
        if not path.is_file():
            result[relative] = {"status": "missing"}
            continue
        content = path.read_bytes()
        tree = ast.parse(content.decode("utf-8"))
        methods = {
            n.name: [a.arg for a in [*n.args.posonlyargs, *n.args.args, *n.args.kwonlyargs]]
            + (["**" + n.args.kwarg.arg] if n.args.kwarg else [])
            for n in ast.walk(tree)
            if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))
            and any(key in n.name.lower() for key in ("send_message", "send_by_session", "receipt", "idempoten", "delivery"))
        }
        result[relative] = {"sha256": hashlib.sha256(content).hexdigest(), "methods": methods}
    return result


def intraday_review_check(row, *, fsm_linked, candidate_plan, quote_max_age_seconds=None, market_max_age_seconds=None):
    failures = []
    def instant(value):
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return parsed.replace(tzinfo=timezone.utc) if parsed.tzinfo is None else parsed.astimezone(timezone.utc)
    if row.get("risk_event"):
        failures.append("risk_only_event")
    if not fsm_linked:
        failures.append("fsm_or_payload_unlinked")
    event_key = "intraday:" + hashlib.sha256(
        f"{row['origin']}\0{row['code']}\0{row['signal']}\0{row['plan_version']}\0{row['event_sequence']}".encode()).hexdigest()
    if row.get("event_key") != event_key:
        failures.append("event_identity_mismatch")
    if not candidate_plan or row.get("plan_version") != f"{row.get('run_id')}:{hashlib.sha256(candidate_plan.encode()).hexdigest()[:16]}":
        failures.append("immutable_plan_binding_missing")
    if row.get("state") != "sent" or not row.get("sent_at"):
        failures.append("confirmed_sender_state_missing")
    try:
        created, sent = instant(row["created_at"]), instant(row["sent_at"])
        if sent < created:
            failures.append("send_order_invalid")
        for field, limit in (("quote_fetched_at", quote_max_age_seconds), ("market_snapshot_at", market_max_age_seconds)):
            if limit is None or not math.isfinite(float(limit)) or float(limit) <= 0:
                failures.append(field + ":explicit_freshness_policy_missing")
                continue
            value = instant(row[field])
            if not 0 <= (created - value).total_seconds() <= float(limit) or not 0 <= (sent - value).total_seconds() <= float(limit):
                failures.append(field + ":stale_or_future")
    except (KeyError, ValueError, TypeError, OverflowError):
        failures.append("freshness_or_delivery_timestamp_missing")
    return {"ready_for_review": not failures, "failures": failures, "downstream_receipt_proven": False}


def audit_database(path, *, as_of, max_seconds=20, quote_max_age_seconds=None, market_max_age_seconds=None):
    cutoff = date.fromisoformat(as_of)
    upper_utc = (datetime.fromisoformat(as_of + "T00:00:00+08:00") + timedelta(days=1)).astimezone(timezone.utc).replace(tzinfo=None).isoformat()
    path = Path(path).resolve(strict=True)
    db = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True, timeout=5)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA query_only=ON")
    deadline = time.monotonic() + max_seconds
    db.set_progress_handler(lambda: int(time.monotonic() > deadline), 10000)
    try:
        db.execute("BEGIN")
        tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        def rows(sql, args=()):
            return [dict(r) for r in db.execute(sql, args)]
        def columns(table):
            return {r[1] for r in db.execute("PRAGMA table_info(" + table + ")")}
        health = db.execute("PRAGMA quick_check(1)").fetchone()[0]
        report = {
            "schema_version": 1, "as_of": as_of, "read_only": True,
            "health": health, "sqlite_user_version": db.execute("PRAGMA user_version").fetchone()[0],
            "ranking": {"status": "insufficient_data", "strategy_quality_established": False},
            "market_inputs": {
                "turnover_change": {"status": "unknown", "reason": "no_audited_same_universe_amount_unit_series"},
                "index_divergence": {"status": "unknown", "reason": "no_persisted_exact_index_baseline_contract"},
                "production_scoring_changed": False,
            },
        }
        if "batches" in tables:
            report["ranking"]["raw_batches"] = rows(
                "SELECT actual_trade_date,status,quality,basis,expected_days,row_count,created_at,published_at "
                "FROM batches WHERE actual_trade_date<=? AND created_at<? ORDER BY created_at DESC LIMIT 5", (as_of, upper_utc))
        if "screen_runs" in tables:
            report["ranking"]["screen_history"] = rows(
                "SELECT MIN(actual_trade_date) first_date,MAX(actual_trade_date) last_date,"
                "COUNT(DISTINCT actual_trade_date) decision_dates,COUNT(*) runs "
                "FROM screen_runs WHERE status='completed' AND actual_trade_date<=?", (as_of,))
        if "daily_bars" in tables:
            report["ranking"]["legacy_bars"] = rows(
                "SELECT MIN(trade_date) first_date,MAX(trade_date) last_date,COUNT(*) rows,"
                "COUNT(DISTINCT trade_date) dates FROM daily_bars WHERE trade_date<=?", (as_of,))
        report["market_inputs"]["persisted_index_tables"] = sorted(
            name for name in tables if "benchmark" in name or name.startswith("index_"))
        if "market_comparison_observations" in tables:
            observations = rows(
                "SELECT observation_id,trade_date,benchmark_code,universe_ref,available_at,recorded_at,payload_hash,payload_json "
                "FROM market_comparison_observations WHERE available_at<? AND recorded_at<? ORDER BY recorded_at DESC LIMIT 5",
                (upper_utc, upper_utc))
            summaries = []
            for observation in observations:
                payload = observation.pop("payload_json")
                valid = hashlib.sha256(payload.encode()).hexdigest() == observation["payload_hash"]
                content = json.loads(payload) if valid else {}
                summaries.append({**observation, "integrity_valid": valid,
                                  "stored_status": content.get("status", "unknown"), "reason": content.get("reason", "integrity_failed")})
            report["market_inputs"]["observations"] = summaries
            reason = "report_revalidation_required" if summaries else "no_observation_at_cutoff"
            report["market_inputs"]["turnover_change"]["reason"] = reason
            report["market_inputs"]["index_divergence"]["reason"] = reason
            report["market_inputs"]["next_check"] = "exact trade_date/benchmark_code/universe_ref report at an explicit availability cutoff"
        calendar = {
            r["trade_date"]: r for r in rows(
                "SELECT trade_date,is_open,status,source FROM trading_calendar WHERE trade_date<=?", (as_of,))
        } if "trading_calendar" in tables else {}
        records = rows(
            "SELECT * FROM recommendation_records WHERE recommended_date>=? AND recommended_date<=? AND created_at<?",
            ("2026-09-09", as_of, upper_utc),
        ) if "recommendation_records" in tables else []
        groups = []
        for recommended in sorted({r["recommended_date"] for r in records}):
            group = [r for r in records if r["recommended_date"] == recommended]
            future, gap = [], False
            cursor = date.fromisoformat(recommended) + timedelta(days=1)
            while cursor <= cutoff:
                day = calendar.get(cursor.isoformat())
                if (not day or not day.get("source") or day["status"] not in ("open", "closed")
                        or bool(day["is_open"]) != (day["status"] == "open")):
                    gap = True
                    break
                if day["is_open"]:
                    future.append(cursor.isoformat())
                cursor += timedelta(days=1)
            horizons = {}
            for horizon in (1, 3, 5, 10):
                saved = rows(
                    "SELECT o.status,COUNT(*) n FROM recommendation_outcomes o "
                    "JOIN recommendation_records r USING(recommendation_id) "
                    "WHERE r.recommended_date=? AND o.horizon=? AND o.evaluated_through<=? "
                    "AND o.updated_at<? AND r.created_at<? GROUP BY o.status",
                    (recommended, horizon, as_of, upper_utc, upper_utc),
                ) if "recommendation_outcomes" in tables else []
                horizons[str(horizon)] = {
                    "maturity": "mature" if len(future) >= horizon else "pending",
                    "known_forward_sessions": min(len(future), horizon),
                    "maturity_reason": "calendar_unverified" if gap and len(future) < horizon else "verified_session_count",
                    "stored_status_counts": {r["status"]: r["n"] for r in saved},
                    "due_date": future[horizon - 1] if len(future) >= horizon else None,
                }
                eligible = rows(
                    "SELECT o.* FROM recommendation_outcomes o JOIN recommendation_records r USING(recommendation_id) "
                    "WHERE r.recommended_date=? AND o.horizon=? AND o.evaluated_through<=? AND o.updated_at<? AND r.created_at<?",
                    (recommended, horizon, as_of, upper_utc, upper_utc),
                ) if "recommendation_outcomes" in tables else []
                def price_complete(row):
                    try:
                        return (row["status"] in ("complete", "unknown_order") and row["session_complete"] == 1
                                and row["price_basis"] == "unadjusted"
                                and all(math.isfinite(float(row[k])) for k in ("return_pct", "max_gain_pct", "max_drawdown_pct")))
                    except (KeyError, ValueError, TypeError, OverflowError):
                        return False
                complete_samples = sum(price_complete(row) for row in eligible)
                def factors_present(record):
                    if "corporate_action_factors" not in tables or "daily_bars" not in tables:
                        return False
                    if not {"observed_at", "response_sha256", "conflicted"} <= columns("corporate_action_factors"):
                        return False
                    if not {"corporate_action_observed_at", "corporate_action_response_sha256", "corporate_action_evidence"} <= columns("recommendation_records"):
                        return False
                    if not {"corporate_action_observed_at", "corporate_action_response_sha256"} <= columns("daily_bars"):
                        return False
                    try:
                        base = float(record.get("corporate_action_factor"))
                        if not math.isfinite(base) or base <= 0:
                            return False
                        end = datetime.fromisoformat(upper_utc).replace(tzinfo=timezone.utc)
                        freeze = datetime.fromisoformat(record["created_at"]).replace(tzinfo=timezone.utc)
                        freeze = min(freeze, datetime.fromisoformat(recommended + "T23:59:59+08:00").astimezone(timezone.utc))
                        for day in [recommended, *future[:horizon]]:
                            factor = db.execute(
                                "SELECT * FROM corporate_action_factors WHERE code=? AND trade_date=?",
                                (record["code"], day)).fetchone()
                            suffix = "SH" if record["code"].startswith("6") else "SZ" if record["code"].startswith(("0", "3")) else "BJ"
                            expected = f"tushare:adj_factor:{day}:{record['code']}.{suffix}"
                            if (not factor or factor["source"] != "tushare_adj_factor" or factor["conflicted"]
                                    or factor["evidence"] != expected
                                    or not math.isclose(float(factor["adj_factor"]), base, rel_tol=1e-10)):
                                return False
                            observed = datetime.fromisoformat(factor["observed_at"])
                            digest = factor["response_sha256"]
                            if (observed.tzinfo is None or observed.astimezone(timezone.utc) >= end
                                    or len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest)):
                                return False
                            owner = record if day == recommended else db.execute(
                                "SELECT * FROM daily_bars WHERE code=? AND trade_date=? AND fetched_at<?",
                                (record["code"], day, upper_utc)).fetchone()
                            if (not owner or owner["corporate_action_evidence"] != expected
                                    or owner["corporate_action_observed_at"] != factor["observed_at"]
                                    or owner["corporate_action_response_sha256"] != digest):
                                return False
                            if day == recommended and observed.astimezone(timezone.utc) > freeze:
                                return False
                            if day != recommended:
                                if (owner["price_basis"] != "unadjusted"
                                        or observed.astimezone(timezone.utc) > datetime.fromisoformat(owner["fetched_at"]).replace(tzinfo=timezone.utc)
                                        or not math.isclose(float(owner["corporate_action_factor"]), base, rel_tol=1e-10)):
                                    return False
                        return True
                    except (KeyError, ValueError, TypeError, OverflowError):
                        return False
                horizon_report = horizons[str(horizon)]
                horizon_report["price_complete_samples"] = complete_samples
                horizon_report["ready_for_review"] = (
                    horizon_report["maturity"] == "mature" and complete_samples == len(group)
                    and all(r.get("comparability_status") == "comparable" for r in group)
                    and all(factors_present(r) for r in group)
                )
            groups.append({
                "recommended_date": recommended, "samples": len(group), "horizons": horizons,
                "comparability_counts": dict(Counter(r.get("comparability_status", "unknown") for r in group)),
                "base_factor_column_present": "corporate_action_factor" in columns("recommendation_records"),
            })
        report["recommendations"] = {
            "status": "pending", "samples": len(records), "groups": groups,
            "returns_recomputed": False, "production_rows_modified": False,
            "factor_table_present": "corporate_action_factors" in tables,
            "ready_for_review": bool(groups) and all(h["ready_for_review"] for g in groups for h in g["horizons"].values()),
            "next_check": "all four horizons mature with price-complete stored evidence; unknown_order is never a provable path outcome",
        }
        if "intraday_event_outbox" in tables:
            outbox = rows("SELECT * FROM intraday_event_outbox WHERE created_at<? AND updated_at<?", (upper_utc, upper_utc))
            correlated, opportunities, review_ready, evidence = 0, 0, 0, []
            for row in outbox:
                state = db.execute(
                    "SELECT trigger_count FROM intraday_signal_states WHERE origin=? AND code=? AND signal=? AND plan_version=?",
                    (row["origin"], row["code"], row["signal"], row["plan_version"]),
                ).fetchone() if "intraday_signal_states" in tables else None
                digest_valid = hashlib.sha256(row["payload"].encode()).hexdigest() == row["payload_hash"]
                linked = bool(state and state[0] >= row["event_sequence"] and digest_valid
                              and row["invocation_id"].startswith("intraday:") and row["origin"] and row["plan_version"])
                correlated += int(linked)
                opportunities += int(not row["risk_event"])
                candidate = db.execute("SELECT price_plan FROM screen_candidates WHERE run_id=? AND code=?",
                                       (row["run_id"], row["code"])).fetchone() if "screen_candidates" in tables else None
                review = intraday_review_check(
                    row, fsm_linked=linked, candidate_plan=candidate[0] if candidate else None,
                    quote_max_age_seconds=quote_max_age_seconds, market_max_age_seconds=market_max_age_seconds)
                review_ready += int(review["ready_for_review"])
                if len(evidence) < 3 or review["ready_for_review"]:
                    evidence.append({
                        "event_key": row["event_key"], "origin_sha256": hashlib.sha256(row["origin"].encode()).hexdigest(),
                        "code": row["code"], "signal": row["signal"], "plan_version": row["plan_version"],
                        "run_id": row["run_id"], "invocation_id": row["invocation_id"],
                        "created_at": row["created_at"], "sent_at": row["sent_at"], "state": row["state"],
                        "risk_event": bool(row["risk_event"]), "fsm_payload_linked": linked,
                        "quote_fetched_at": row["quote_fetched_at"], "market_snapshot_at": row["market_snapshot_at"],
                        "review_check": review,
                    })
            report["intraday"] = {
                "status": "pending", "outbox_count": len(outbox),
                "state_counts": dict(Counter(r["state"] for r in outbox)),
                "signal_counts": dict(Counter(r["signal"] for r in outbox)),
                "correlated_fsm_payload_count": correlated, "opportunity_count": opportunities,
                "freshness_acceptance": "not_established",
                "reason": "risk_only_events" if not opportunities else "freshness_and_downstream_receipt_require_review",
                "evidence": evidence,
                "ready_for_review": review_ready > 0,
                "review_ready_count": review_ready,
                "next_check": "natural non-risk event with exact FSM/plan/payload binding, explicit quote+market freshness policy and sent state",
            }
        else:
            report["intraday"] = {"status": "pending", "reason": "outbox_missing"}
        return report
    finally:
        db.rollback()
        db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--runtime-root")
    parser.add_argument("--max-seconds", type=float, default=20)
    parser.add_argument("--quote-max-age-seconds", type=float)
    parser.add_argument("--market-max-age-seconds", type=float)
    args = parser.parse_args()
    report = audit_database(args.database, as_of=args.as_of, max_seconds=args.max_seconds,
                            quote_max_age_seconds=args.quote_max_age_seconds, market_max_age_seconds=args.market_max_age_seconds)
    if args.runtime_root:
        report["delivery_source_capabilities"] = source_capabilities(args.runtime_root)
    report["audited_at_utc"] = datetime.now(timezone.utc).isoformat()
    print(json.dumps(report, indent=2, sort_keys=True, allow_nan=False))


if __name__ == "__main__":
    main()
