"""Create a NEW synthetic demo database. Never open or migrate plugin data."""
from __future__ import annotations

import argparse
from datetime import date, timedelta
import hashlib
import json
import math
from pathlib import Path
import sqlite3


def create_demo(path, settings=None):
    path = Path(path)
    if path.exists() or (settings is not None and Path(settings).exists()):
        raise FileExistsError("demo outputs must not already exist")
    path.parent.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(path)
    try:
        db.executescript("""
        CREATE TABLE web_demo_metadata(key TEXT PRIMARY KEY,value TEXT);
        CREATE TABLE screen_runs(run_id TEXT PRIMARY KEY,job_name TEXT,requested_date TEXT,actual_trade_date TEXT,source TEXT,started_at TEXT,finished_at TEXT,quote_count INTEGER,candidate_count INTEGER,status TEXT,quality TEXT,coverage REAL,error TEXT);
        CREATE TABLE screen_candidates(run_id TEXT,code TEXT,name TEXT,score INTEGER,score_max INTEGER,risk_level TEXT,price_plan TEXT,factor_payload TEXT);
        CREATE TABLE active_candidate_runs(scope TEXT,run_id TEXT);
        CREATE TABLE daily_quotes(code TEXT,trade_date TEXT,name TEXT,price REAL,provider_ts TEXT,fetched_at TEXT);
        CREATE TABLE daily_bars(code TEXT,trade_date TEXT,open REAL,high REAL,low REAL,close REAL,volume REAL,amount REAL,source TEXT,fetched_at TEXT,price_basis TEXT,corporate_action_factor REAL,corporate_action_evidence TEXT);
        CREATE TABLE market_contexts(as_of TEXT,payload TEXT,source TEXT,quality TEXT);
        CREATE TABLE daily_snapshot_meta(trade_date TEXT,requested_date TEXT,complete INTEGER,quality TEXT);
        CREATE TABLE trading_calendar(trade_date TEXT,is_open INTEGER,status TEXT,source TEXT);
        CREATE TABLE intraday_event_outbox(event_key TEXT,origin TEXT,code TEXT,name TEXT,signal TEXT,plan_version TEXT,run_id TEXT,event_sequence INTEGER,invocation_id TEXT,payload TEXT,payload_hash TEXT,quote_fetched_at TEXT,market_snapshot_at TEXT,risk_event INTEGER,state TEXT,created_at TEXT,sent_at TEXT,last_error TEXT);
        CREATE TABLE automatic_close_deliveries(origin TEXT,state TEXT,created_at TEXT);
        CREATE TABLE provider_health(provider TEXT,last_success_at TEXT,last_error_at TEXT,last_quality TEXT,last_error TEXT);
        CREATE TABLE job_runs(job_key TEXT,job_name TEXT,trade_date TEXT,started_at TEXT,status TEXT,error TEXT);
        CREATE TABLE batches(batch_id TEXT,actual_trade_date TEXT,generation INTEGER,status TEXT,row_count INTEGER,basis TEXT,created_at TEXT,published_at TEXT);
        CREATE TABLE risk_events(code TEXT,state TEXT,risk_level TEXT,event_at TEXT);
        CREATE TABLE recommendation_records(recommendation_id TEXT,code TEXT,name TEXT,recommended_date TEXT,created_at TEXT,visibility TEXT,origin TEXT,plan_version TEXT,strategy_version TEXT,plan_status TEXT,comparability_status TEXT,price_basis TEXT,corporate_action_factor REAL,confirmation_price REAL,candidate_price REAL,target_low REAL,invalidation_price REAL);
        CREATE TABLE recommendation_outcomes(recommendation_id TEXT,horizon INTEGER,status TEXT);
        """)
        db.execute("INSERT INTO web_demo_metadata VALUES('kind','synthetic_demo')")
        db.execute("INSERT INTO active_candidate_runs VALUES('global','demo-close')")
        dates, cursor = [], date(2026, 7, 1)
        while cursor <= date(2026, 9, 12):
            opened = cursor.weekday() < 5
            db.execute("INSERT INTO trading_calendar VALUES(?,?,?,?)", (cursor.isoformat(), int(opened), "open" if opened else "closed", "synthetic_fixture"))
            if opened:
                dates.append(cursor.isoformat())
                db.execute("INSERT INTO daily_snapshot_meta VALUES(?,?,1,'good')", (cursor.isoformat(), cursor.isoformat()))
            cursor += timedelta(days=1)
        names = ["演示材料", "演示科技", "演示制造", "演示能源", "演示电气", "演示医药", "演示消费", "演示通信"]
        industries = ["基础材料", "信息技术", "装备制造", "新能源", "电气设备", "医疗健康", "消费", "通信"]
        states = ["pending", "sent", "unknown_delivery", "sending", "failed", "cancelled"]
        for index in range(8):
            code = f"DEMO{index + 1:02d}"
            prices = {}
            for i, day in enumerate(dates):
                close = round(18 + index * 7 + math.sin(i / 4 + index) * 1.5 + i * (0.10 if index % 3 else -0.025), 2)
                open_ = round(close + math.sin(i + index) * 0.45, 2)
                high, low = round(max(close, open_) + 0.36, 2), round(min(close, open_) - 0.32, 2)
                prices[day] = close
                db.execute("INSERT INTO daily_bars VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           (code, day, open_, high, low, close, 100000 + i * 1700, close * 100000,
                            "synthetic_fixture", day + "T07:05:00", "unadjusted", 1, "synthetic_fixture:no_action"))
            last = prices[dates[-1]]
            plan = {
                "validated": index != 6, "quality": "good", "reference_price": last,
                "attention_low": round(last * .975, 2), "attention_high": round(last * 1.012, 2),
                "sell_low": round(last * 1.06, 2), "sell_high": round(last * 1.10, 2),
                "invalidation": round(last * .94, 2), "confirmation": round(last * 1.025, 2), "atr": 0.7,
                "provenance": {"basis": "unadjusted", "actual_date": dates[-1]},
            }
            plan_json = json.dumps(plan)
            version = "demo-close:" + hashlib.sha256(plan_json.encode()).hexdigest()[:16]
            factors = {"industry_name": industries[index], "industry_score": 3, "fundamental_score": 2,
                       "market_adjustment": 0, "fundamental_coverage": .75 if index < 5 else .25}
            risk = "eligible" if index < 5 else "watch_only" if index < 7 else "blocked"
            db.execute("INSERT INTO screen_candidates VALUES(?,?,?,?,?,?,?,?)", ("demo-close", code, names[index], 42 - index * 3, 50, risk, plan_json, json.dumps(factors)))
            db.execute("INSERT INTO daily_quotes VALUES(?,?,?,?,?,?)", (code, dates[-1], names[index], last, dates[-1] + "T15:00:00+08:00", dates[-1] + "T07:00:00"))
            if index < 6:
                signal = "attention_entry" if index < 3 else "risk_invalidated"
                payload = "Synthetic demo signal"
                key = "intraday:" + hashlib.sha256("\0".join(("demo", code, signal, version, "1")).encode()).hexdigest()
                db.execute("INSERT INTO intraday_event_outbox VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           (key, "demo", code, names[index], signal, version, "demo-close", 1, "intraday:synthetic",
                            payload, hashlib.sha256(payload.encode()).hexdigest(), dates[-1] + "T09:35:00+08:00",
                            dates[-1] + "T09:34:58+08:00", int(index >= 3), states[index],
                            dates[-1] + "T01:35:01", dates[-1] + "T01:35:02" if states[index] == "sent" else None,
                            "timeout" if states[index] == "unknown_delivery" else None))
            recommended = "2026-08-14" if index < 5 else "2026-09-11"
            base = prices[recommended]
            db.execute("INSERT INTO recommendation_records VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                       ("demo-rec-" + code, code, names[index], recommended, recommended + "T08:00:00",
                        "public", "global", version, "synthetic-plan-v1", "validated",
                        "comparable" if index != 6 else "unknown", "unadjusted", 1 if index != 6 else None,
                        base, base, base * 1.035, base * .965))
            for horizon in (1, 3, 5, 10):
                state = "unknown_order" if index == 2 else "complete" if index < 5 else "unknown" if index == 6 else "pending"
                db.execute("INSERT INTO recommendation_outcomes VALUES(?,?,?)", ("demo-rec-" + code, horizon, state))
        for i, day in enumerate(dates[-5:]):
            run_id = "demo-close" if i == 4 else "demo-close-" + str(i)
            db.execute("INSERT INTO screen_runs VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
                       (run_id, "automatic_close", day, day, "synthetic_fixture", day + "T07:10:00",
                        day + "T07:12:00", 8, 8, "completed", "good", 1.0, None))
            db.execute("INSERT INTO batches VALUES(?,?,?,?,?,?,?,?)", ("demo-batch-" + str(i), day, i + 1, "published", len(dates) * 8, "unadjusted", day + "T07:00:00", day + "T07:10:00"))
        db.execute("INSERT INTO market_contexts VALUES(?,?,?,?)", (dates[-1], json.dumps({
            "regime": "neutral", "breadth": .5, "advancing": 4, "declining": 3, "flat": 1,
            "median_return": .18, "total_amount": 180000000}), "synthetic_fixture", "good"))
        for provider, error in (("daily", None), ("trade_cal", None), ("index_daily", "permission denied")):
            db.execute("INSERT INTO provider_health VALUES(?,?,?,?,?)", (provider, dates[-1] + "T07:00:00" if not error else None,
                       dates[-1] + "T07:00:00" if error else None, "good" if not error else "unknown", error))
        db.execute("INSERT INTO job_runs VALUES('demo-daily','automatic_close',? ,?,'completed',NULL)", (dates[-1], dates[-1] + "T07:00:00"))
        db.execute("INSERT INTO automatic_close_deliveries VALUES('demo','sent',?)", (dates[-1] + "T08:00:00",))
        db.execute("INSERT INTO risk_events VALUES('DEMO07','data_unverified','watch_only',?)", (dates[-1] + "T01:35:00",))
        db.commit()
    finally:
        db.close()
    if settings is not None:
        Path(settings).write_text(json.dumps({"kind": "synthetic_demo", "values": {
            "min_score": 20, "price_min": 2, "price_max": 80, "intraday_confirmation_periods": 2,
            "intraday_cooldown_seconds": 1800, "intraday_min_amount": 5000000, "paper_trading_only": True,
            "market_comparison_enabled": False}}, indent=2), encoding="utf-8")
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path)
    parser.add_argument("--settings", type=Path)
    args = parser.parse_args()
    print(create_demo(args.database, args.settings))


if __name__ == "__main__":
    main()
