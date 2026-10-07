"""Stock-page MACD state: display-only, chained prices, the same cross rule as research schemes H and I."""
from datetime import date, datetime, timedelta, timezone
import hashlib
import sqlite3

import numpy as np
import pandas as pd
import pytest

from webapp.data import MACD_MIN_SESSIONS, Dashboard, macd_state


NOW = datetime(2026, 10, 7, 2, 30, tzinfo=timezone.utc)


def series(closes, pre_closes=None, start=date(2026, 1, 5)):
    days = [(start + timedelta(days=i)).isoformat() for i in range(len(closes))]
    rows = []
    for i, (day, close) in enumerate(zip(days, closes)):
        previous = pre_closes[i] if pre_closes is not None else (closes[i - 1] if i else close)
        rows.append({"trade_date": day, "open": float(close), "high": float(close), "low": float(close),
                     "close": float(close), "pre_close": float(previous), "basis": "unadjusted"})
    return days, rows


def reference(prices):
    """run_scheme_h.py's pandas definition."""
    p = pd.Series(np.asarray(prices, dtype=float))
    dif = p.ewm(span=12, adjust=False).mean() - p.ewm(span=26, adjust=False).mean()
    dea = dif.ewm(span=9, adjust=False).mean()
    above = (dif > dea).to_numpy()
    cross = np.zeros_like(above)
    cross[1:] = above[1:] & ~above[:-1]
    recent = cross.copy()
    for k in (1, 2):
        recent[k:] |= cross[:-k]
    return dif.to_numpy(), dea.to_numpy(), above, recent & above


def test_matches_pandas_and_the_scheme_h_cross_rule_on_every_window():
    prices = 10 * np.exp(np.cumsum(np.random.default_rng(7).normal(0, 0.02, 160)))
    days, rows = series(prices)
    dif, dea, above, recent = reference(prices)
    fresh = 0
    for n in range(MACD_MIN_SESSIONS, len(prices) + 1):
        state, t = macd_state(days[:n], rows[:n]), n - 1
        k = t
        while k > 0 and above[k - 1] == above[t]:
            k -= 1
        assert state["status"] == "available" and state["sessions"] == n and state["first_session"] == days[0]
        assert state["state"] == ("golden" if above[t] else "dead")
        assert state["recent_golden_cross"] == bool(recent[t])
        assert state["cross_date"] == (days[k] if k else None) and state["sessions_since_cross"] == (t - k if k else None)
        assert state["dif"] == pytest.approx(dif[t], abs=1e-4) and state["dea"] == pytest.approx(dea[t], abs=1e-4)
        assert state["histogram"] == pytest.approx(2 * (dif[t] - dea[t]), abs=2e-4)
        fresh += bool(recent[t])
    assert fresh >= 3


def test_ex_rights_day_is_not_a_death_cross():
    true = 20 * 1.003 ** np.arange(120)
    raw = true.copy()
    raw[100:] /= 2
    pre = np.r_[raw[0], raw[:-1]]
    pre[100] = raw[99] / 2
    chained = macd_state(*series(raw, pre))
    plain = macd_state(*series(true))
    for key in ("state", "cross_date", "sessions_since_cross", "recent_golden_cross", "sessions"):
        assert chained[key] == plain[key]
    assert chained["state"] == "golden"
    assert chained["dif"] == pytest.approx(plain["dif"] / 2, abs=1e-4)
    assert not reference(raw[:101])[2][-1]


def test_sessions_without_a_usable_bar_keep_the_price_unchanged():
    prices = 10 * np.exp(np.cumsum(np.random.default_rng(3).normal(0, 0.02, 100)))
    days, rows = series(prices)
    missing = set(days[70:75]) | {days[99]}
    rows = [dict(row) for row in rows if row["trade_date"] not in missing]
    next(row for row in rows if row["trade_date"] == days[75])["pre_close"] = float(prices[69])
    next(row for row in rows if row["trade_date"] == days[40])["basis"] = "qfq"
    filled = prices.copy()
    filled[70:75], filled[99] = prices[69], prices[98]
    filled[40] = prices[39]
    filled[41:] *= prices[39] / prices[40]
    state = macd_state(days, rows)
    dif, dea, above, _ = reference(filled)
    assert state["trade_date"] == days[99] and state["last_bar_date"] == days[98] and state["sessions"] == 100
    assert state["state"] == ("golden" if above[-1] else "dead")
    scale = prices[98] / filled[99]
    assert state["dif"] == pytest.approx(dif[-1] * scale, abs=1e-4) and state["dea"] == pytest.approx(dea[-1] * scale, abs=1e-4)


def test_short_or_late_history_is_not_padded():
    days, rows = series(np.linspace(10, 12, 80))
    short = MACD_MIN_SESSIONS - 1
    assert macd_state(days[:short], rows[:short]) == {"status": "insufficient_history", "sessions": short,
                                                      "min_sessions": MACD_MIN_SESSIONS}
    stopped = macd_state(days, rows[:short])
    assert stopped["sessions"] == 80 and stopped["last_bar_date"] == days[short - 1] and stopped["trade_date"] == days[-1]
    assert macd_state(days, rows[25:])["sessions"] == 55
    listed = macd_state(days, rows[20:])
    assert listed["status"] == "available" and listed["first_session"] == days[20] and listed["sessions"] == 60
    assert macd_state(days, [])["status"] == "insufficient_history"


def test_stock_route_returns_display_only_macd_without_writing(tmp_path):
    from test_v0139_intraday_m2 import _imports
    _, _, Store = _imports()
    database = tmp_path / "plugin.sqlite3"
    Store(database)
    prices = np.round(10 * np.exp(np.cumsum(np.random.default_rng(5).normal(0, 0.02, 70))), 2)
    days = [(date(2026, 6, 1) + timedelta(days=i)).isoformat() for i in range(70)]
    db = sqlite3.connect(database)
    try:
        db.execute("INSERT INTO datasets(dataset_id,dataset_key,provider,created_at) VALUES('ds','tushare_daily','tushare','2026-09-30T08:00:00')")
        db.execute("INSERT INTO batches(batch_id,dataset_id,requested_date,actual_trade_date,status,quality,source,generation,"
                   "row_count,manifest_hash,created_at,published_at) VALUES('batch-21','ds',?,?,'published',"
                   "'good','tushare',21,70,'digest','2026-09-30T08:00:00','2026-09-30T08:30:00+00:00')", (days[-1], days[-1]))
        for i, day in enumerate(days):
            partition, close = "p-" + day, float(prices[i])
            db.execute("INSERT INTO day_partitions(partition_id,dataset_id,trade_date,content_hash,source,row_count,created_at) "
                       "VALUES(?,?,?,?,?,?,?)", (partition, "ds", day, "h" + day, "tushare", 1, "2026-09-30T08:00:00"))
            db.execute("INSERT INTO partition_bars(partition_id,trade_date,code,ts_code,name,open,high,low,close,pre_close,"
                       "pct_change,volume,amount,source) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                       (partition, day, "600857", "600857.SH", "", close, close, close, close,
                        float(prices[i - 1]) if i else close, 0.0, 1000, 10000, "tushare"))
            db.execute("INSERT INTO batch_days(batch_id,trade_date,partition_id,row_count) VALUES('batch-21',?,?,1)", (day, partition))
        db.execute("INSERT INTO active_generations(dataset_id,active_batch_id,generation,updated_at) VALUES('ds','batch-21',21,'2026-09-30T08:30:00')")
        db.commit()
        db.execute("PRAGMA journal_mode=DELETE")
    finally:
        db.close()
    before = hashlib.sha256(database.read_bytes()).digest()
    data = Dashboard(database, now=lambda: NOW).query("stocks/600857")["data"]
    expected = macd_state(*series(prices, start=date(2026, 6, 1)))
    assert data["macd"] == {**expected, "verified": False, "display_only": True}
    assert expected["status"] == "available" and expected["sessions"] == 70 and expected["trade_date"] == days[-1]
    assert hashlib.sha256(database.read_bytes()).digest() == before
    db = sqlite3.connect(database)
    try:
        db.execute("DELETE FROM active_generations")
        db.execute("INSERT INTO stock_symbols(code,name,normalized_name,source,updated_at) "
                   "VALUES('600857','宁波中百','宁波中百','stock_basic','2026-09-30T08:00:00')")
        db.commit()
    finally:
        db.close()
    legacy = Dashboard(database, now=lambda: NOW).query("stocks/600857")["data"]
    assert legacy["macd"] == {"status": "unavailable", "reason": "no_active_raw", "verified": False, "display_only": True}
