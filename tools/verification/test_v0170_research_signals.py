"""Evening research signals: overheat label (scheme F), index trend (scheme D), the Web route and the unit wiring."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json
from pathlib import Path
import sqlite3

import numpy as np
import pandas as pd

from webapp import research_signals as rs
from webapp.data import SIGNALS_SCHEMA, Dashboard
from webapp.deploy.update import server_arguments

ROOT = Path(__file__).resolve().parents[2]
NOW = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)


def _panel(n=80, m=60, seed=7):
    """Random walks for m main-board codes over n sessions, with one code per exclusion reason."""
    rng = np.random.default_rng(seed)
    codes = [f"60{i:04d}" for i in range(m - 1)] + ["688001"]
    days = [(date(2026, 5, 1) + timedelta(days=i)).isoformat() for i in range(n)]
    close = 10 * np.exp(np.cumsum(rng.normal(0, 0.02, (n, m)), axis=0))
    pre_close = np.vstack([close[:1], close[:-1]])
    high, low = close * 1.01, close * 0.99
    market = {"open": close.copy(), "high": high, "low": low, "close": close, "pre_close": pre_close.copy(),
              "volume": rng.uniform(1e5, 5e5, (n, m)), "amount": np.full((n, m), 5e7)}
    names = {code: "测试" for code in codes}
    names["600001"] = "*ST测试"
    market["pre_close"][-10, 2] = market["close"][-11, 2] * 0.8          # ex-dividend inside the 60-session window
    for key in market:
        market[key][-1, 3] = np.nan                                     # no bar on the latest session
    market["close"][-1, 4] = market["high"][-1, 4] = 60.0               # above the price cap
    market["low"][-1, 4] = market["open"][-1, 4] = 59.0
    market["amount"][-1, 5] = 1e6                                       # below the amount floor
    for key in market:
        market[key][-30, 6] = np.nan                                    # one missing session in the window
    return market, codes, names, days


def test_overheat_picks_the_top_decile_of_the_eight_ranks_inside_the_research_pool():
    market, codes, names, days = _panel()
    result = rs.overheat(market, codes, names, days)
    assert result["status"] == "available" and result["trade_date"] == days[-1]
    assert result["excluded"] == {"600001": "st_name", "600002": "corporate_action_60d", "600003": "no_bar_today",
                                  "600004": "price_above_50", "600005": "amount_below_20m", "600006": "history_lt_60",
                                  "688001": "not_main_or_chinext"}
    stocks = result["stocks"]
    assert len(stocks) == result["evaluated"] == len(codes) - len(result["excluded"]) == 53
    frame = pd.DataFrame({code: item["indicators"] for code, item in stocks.items()}).T
    total = frame[list(rs.COOL8)].rank(pct=True).mean(axis=1)
    expected = set(total[total >= np.quantile(total, 0.9)].index)
    assert {item["code"] for item in result["hot"]} == expected and len(expected) == result["hot_count"] > 0
    t, j = len(days) - 1, codes.index("600010")
    close, volume = market["close"][:, j], market["volume"][:, j]
    assert np.isclose(stocks["600010"]["indicators"]["R20"], close[t] / close[t - 20] - 1, atol=1e-6)
    assert np.isclose(stocks["600010"]["indicators"]["VOLR5_60"], volume[-5:].mean() / volume[-60:].mean(), atol=1e-6)
    assert np.isclose(stocks["600010"]["indicators"]["ABTURN"], volume[-20:].mean() / volume[-60:].mean(), atol=1e-6)
    assert result["approximations"] == ["turnover_ratio_from_volume", "st_from_current_name"]


def test_overheat_reports_short_history_and_small_pools_instead_of_labelling():
    market, codes, names, days = _panel(n=40)
    assert rs.overheat(market, codes, names, days)["reason"] == "history_lt_60"
    market, codes, names, days = _panel(m=20)
    small = rs.overheat(market, codes, names, days)
    assert small["status"] == "unavailable" and small["reason"] == "pool_lt_30" and small["hot"] == []


def _index_frame(start, step, rows=260):
    dates = pd.bdate_range("2025-09-01", periods=rows)
    return pd.DataFrame({"date": dates, "close": [start + step * i for i in range(rows)]})


def test_index_trend_compares_the_close_with_its_200_session_mean_and_survives_one_failure():
    def fetch(code):
        if code == "000852":
            raise ConnectionError("sina unreachable")
        return _index_frame(5000, 2)

    trend = rs.index_trend(fetch)
    item = trend["indices"]["000905"]
    closes = [5000 + 2 * i for i in range(260)]
    assert trend["status"] == "available" and set(trend["errors"]) == {"000852"}
    assert item["above_ma200"] is True and item["sessions_on_side"] == 61
    assert np.isclose(item["ma200"], np.mean(closes[-200:]), atol=1e-3)
    assert np.isclose(item["distance_ma200"], closes[-1] / np.mean(closes[-200:]) - 1, atol=1e-6)
    falling = rs.index_trend(lambda code: _index_frame(9000, -3))
    assert all(x["above_ma200"] is False for x in falling["indices"].values())
    assert rs.index_trend(lambda code: _index_frame(9000, -3, rows=150))["status"] == "unavailable"


def _active_raw_db(path, market, codes, names, days):
    db = sqlite3.connect(path)
    db.executescript("""
        CREATE TABLE datasets(dataset_id TEXT, dataset_key TEXT);
        CREATE TABLE batches(batch_id TEXT, dataset_id TEXT, actual_trade_date TEXT, status TEXT, source TEXT,
                             published_at TEXT, shadow INTEGER, publication_mode TEXT, quality TEXT);
        CREATE TABLE active_generations(dataset_id TEXT, active_batch_id TEXT, generation INTEGER);
        CREATE TABLE batch_days(batch_id TEXT, trade_date TEXT, partition_id TEXT);
        CREATE TABLE partition_bars(partition_id TEXT, trade_date TEXT, code TEXT, ts_code TEXT, name TEXT, open REAL,
                                    high REAL, low REAL, close REAL, pre_close REAL, pct_change REAL, volume REAL,
                                    amount REAL, source TEXT, basis TEXT);
        CREATE TABLE stock_symbols(code TEXT, name TEXT, normalized_name TEXT, source TEXT, updated_at TEXT);
        INSERT INTO datasets VALUES('d1','tushare_daily');
        INSERT INTO active_generations VALUES('d1','b1',21);
    """)
    db.execute("INSERT INTO batches VALUES('b1','d1',?,'published','tushare','2026-10-07T01:00:00+00:00',0,'active','good')",
               (days[-1],))
    for t, day in enumerate(days):
        db.execute("INSERT INTO batch_days VALUES('b1',?,?)", (day, "p" + day))
        for j, code in enumerate(codes):
            if np.isfinite(market["close"][t, j]):
                db.execute("INSERT INTO partition_bars VALUES(?,?,?,?,'',?,?,?,?,?,0,?,?,'tushare','unadjusted')",
                           ("p" + day, day, code, code + ".SH", *(float(market[k][t, j]) for k in
                            ("open", "high", "low", "close", "pre_close", "volume", "amount"))))
    db.executemany("INSERT INTO stock_symbols VALUES(?,?,?,'tushare','2026-10-02')",
                   [(code, name, name) for code, name in names.items()])
    db.commit()
    db.close()


def test_build_reads_active_raw_writes_atomically_and_records_each_date_once(tmp_path):
    market, codes, names, days = _panel()
    database = tmp_path / "snapshot.sqlite3"
    _active_raw_db(database, market, codes, names, days)
    output = tmp_path / "state" / "research_signals.json"
    fetch = lambda code: _index_frame(5000, 2)  # noqa: E731
    quiet = {"unlock_fetch": lambda start, end: pd.DataFrame(), "margin_fetch": lambda exchange, day: pd.DataFrame()}
    first = rs.build(database, output, now=NOW, fetch=fetch, **quiet)
    direct = rs.overheat(market, codes, names, days)
    written = json.loads(output.read_text(encoding="utf-8"))
    assert written["schema"] == SIGNALS_SCHEMA and written["inputs"]["batch_id"] == "b1" and written["inputs"]["sessions"] == 80
    assert [x["code"] for x in written["overheat"]["hot"]] == [x["code"] for x in direct["hot"]]
    assert written["unlock"]["status"] == "available" and written["margin"]["reason"] == "margin_unpublished"
    assert first["forward_recorded"] == {"overheat": True, "index_trend": True, "unlock": True, "margin": False}
    again = rs.build(database, output, now=NOW + timedelta(hours=3), fetch=fetch, **quiet)
    assert again["forward_recorded"] == {"overheat": False, "index_trend": False, "unlock": False, "margin": False}
    lines = (tmp_path / "state" / "research_overheat_forward.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 1 and json.loads(lines[0])["trade_date"] == days[-1]
    assert not list((tmp_path / "state").glob("*.tmp"))
    broken = rs.build(tmp_path / "missing.sqlite3", tmp_path / "other" / "research_signals.json", now=NOW, skip_index=True)
    assert broken["overheat"]["status"] == "unavailable" and broken["index_trend"]["status"] == "skipped"
    assert broken["unlock"] == {"status": "skipped"} and broken["margin"] == {"status": "skipped"}
    assert not (tmp_path / "other" / "research_overheat_forward.jsonl").exists()


def _signals_file(path, generated_at=NOW, **overrides):
    payload = {"schema": SIGNALS_SCHEMA, "generated_at": generated_at.isoformat(),
               "inputs": {"trade_date": "2026-09-30", "generation": 21, "sessions": 120, "snapshot_revision": "0cbc8b57a4c010e1"},
               "overheat": {"status": "available", "rule": "F_HOT_Q90", "trade_date": "2026-09-30", "pool_size": 3000,
                            "evaluated": 2990, "quantile": 0.9, "threshold": 0.71, "approximations": ["turnover_ratio_from_volume", "free text"],
                            "hot": [{"code": "600010", "name": "测试", "score": 0.83, "pct": 0.99},
                                    {"code": "../etc", "name": "bad", "score": 1, "pct": 1}],
                            "stocks": {"600010": {"score": 0.83, "pct": 0.99, "hot": True, "indicators": {"R20": 0.31, "RSI14": 81.2}}},
                            "excluded": {"600002": "corporate_action_60d", "600009": "/home/pi/secret"},
                            "excluded_counts": {"corporate_action_60d": 1, "other": 9}},
               "index_trend": {"status": "available", "rule": "D_MA200", "source": "akshare.stock_zh_index_daily (sina)",
                               "indices": {"000905": {"name": "中证500", "date": "2026-09-30", "close": 7000, "ma200": 6500,
                                                      "above_ma200": True, "distance_ma200": 0.0769, "sessions_on_side": 12},
                                           "399001": {"name": "other"}},
                               "errors": {"000852": "ConnectionError: /path/leak"}}}
    payload.update(overrides)
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return path


def test_dashboard_serves_signals_with_status_lookup_and_no_free_text(tmp_path):
    db = tmp_path / "db.sqlite3"
    assert Dashboard(db, now=lambda: NOW).research_signals()["status"] == "not_configured"
    missing = Dashboard(db, signals_path=tmp_path / "none.json", now=lambda: NOW)
    assert missing.research_signals()["status"] == "missing"
    path = _signals_file(tmp_path / "research_signals.json")
    board = Dashboard(db, signals_path=path, now=lambda: NOW + timedelta(hours=2))
    data = board.research_signals("600010")
    assert data["status"] == "available" and data["inputs"]["generation"] == 21
    assert [x["code"] for x in data["overheat"]["hot"]] == ["600010"] and data["overheat"]["approximations"] == ["turnover_ratio_from_volume"]
    assert data["stock"]["status"] == "evaluated" and data["stock"]["hot"] is True and data["stock"]["indicators"]["R20"] == 0.31
    assert data["overheat"]["excluded_counts"] == {"corporate_action_60d": 1}
    assert data["index_trend"]["failed"] == ["000852"] and set(data["index_trend"]["indices"]) == {"000905"}
    assert board.research_signals("600002")["stock"] == {"code": "600002", "status": "excluded", "hot": False, "score": None,
                                                        "pct": None, "reason": "corporate_action_60d", "indicators": {}}
    assert board.research_signals("600009")["stock"]["status"] == "not_evaluated"
    text = json.dumps(board.query("research_signals", {"code": "600009"}), ensure_ascii=False)
    assert "/home/pi" not in text and "/path/leak" not in text and "free text" not in text
    assert board.query("research_signals", {"code": "60001x"})["meta"]["reason"] == "invalid_stock_code"
    late = Dashboard(db, signals_path=path, now=lambda: NOW + timedelta(days=4))
    assert late.research_signals()["status"] == "stale"
    _signals_file(path, overheat={"status": "unavailable", "reason": "RuntimeError: /var/lib/x"})
    assert board.research_signals()["overheat"]["reason"] == "job_error"
    path.write_text(json.dumps({"schema": "other"}), encoding="utf-8")
    assert board.research_signals()["status"] == "invalid"
    assert "research_signals" in board.version()["capabilities"]


def test_units_wire_the_job_output_into_the_dashboard():
    web_unit = (ROOT / "webapp/deploy/stock-watch-web.service").read_text(encoding="utf-8")
    job_unit = (ROOT / "webapp/deploy/stock-watch-research-signals.service").read_text(encoding="utf-8")
    timer = (ROOT / "webapp/deploy/stock-watch-research-signals.timer").read_text(encoding="utf-8")
    signals = server_arguments(web_unit)["signals"]
    assert f"--output {signals}" in job_unit and "-m webapp.research_signals" in job_unit
    assert "--database " + server_arguments(web_unit)["database"] in job_unit
    assert "OnCalendar=*-*-* 18:40:00" in timer and "Persistent=true" in timer
    package = (ROOT / "webapp/deploy/package.py").read_text(encoding="utf-8")
    for name in ("webapp/research_signals.py", "webapp/deploy/stock-watch-research-signals.service",
                 "webapp/deploy/stock-watch-research-signals.timer"):
        assert f'"{name}"' in package
    assert f"--signals {signals}".encode() in (ROOT / "webapp/deploy/install.py").read_bytes()
