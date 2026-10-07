"""Lockup-expiry and margin-crowding reminders: display-only research signals with forward records."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import json

import numpy as np
import pandas as pd

from webapp import research_signals as rs
from webapp.data import SIGNALS_SCHEMA, Dashboard

NOW = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)


def unlock_frame():
    return pd.DataFrame([
        {"股票代码": "301683", "解禁时间": "2026-10-08", "限售股类型": "首发机构配售股份", "实际解禁市值": 7.9e7, "占解禁前流通市值比例": 0.0568},
        {"股票代码": 1, "解禁时间": "2026-10-20", "限售股类型": "定向增发机构配售股份", "实际解禁市值": 1e9, "占解禁前流通市值比例": 0.03},
        {"股票代码": "000001", "解禁时间": "2026-10-28", "限售股类型": "股权激励限售股份", "实际解禁市值": 1e8, "占解禁前流通市值比例": 0.025},
        {"股票代码": "603515", "解禁时间": "2026-10-09", "限售股类型": "股权激励限售股份", "实际解禁市值": 2.6e7, "占解禁前流通市值比例": 0.0023},
        {"股票代码": "600000", "解禁时间": "2026-12-01", "限售股类型": "首发原股东限售股份", "实际解禁市值": 9e9, "占解禁前流通市值比例": 0.4},
        {"股票代码": "ETF", "解禁时间": "2026-10-10", "限售股类型": "x", "实际解禁市值": 1, "占解禁前流通市值比例": 1},
    ])


def test_unlock_schedule_sums_the_next_30_days_per_code():
    asked = []

    def fetch(start, end):
        asked.append((start, end))
        return unlock_frame()

    result = rs.unlock_schedule(date(2026, 10, 7), fetch)
    assert asked == [("20261007", "20261106")] and result["window"] == ["2026-10-07", "2026-11-06"]
    assert result["events"] == 4 and set(result["stocks"]) == {"301683", "000001", "603515"}
    pingan = result["stocks"]["000001"]
    assert [event["date"] for event in pingan["events"]] == ["2026-10-20", "2026-10-28"]
    assert pingan["ratio_total"] == 0.055 and pingan["heavy"] is True and pingan["next_date"] == "2026-10-20"
    assert result["stocks"]["603515"]["heavy"] is False
    assert result["heavy"] == [["301683", "2026-10-08", 0.0568], ["000001", "2026-10-20", 0.055]] and result["heavy_count"] == 2


def margin_panel(n=10, m=60):
    codes = [f"60{i:04d}" for i in range(m // 2)] + [f"00{i:04d}" for i in range(m // 2)]
    days = [(date(2026, 9, 14) + timedelta(days=i)).isoformat() for i in range(n)]
    market = {"amount": np.full((n, m), 1e8)}
    market["amount"][-2, 5] = 0.0
    return days, market, codes


def test_margin_crowding_uses_the_newest_day_both_exchanges_published():
    days, market, codes = margin_panel()
    asked = []

    def fetch(exchange, day):
        asked.append((exchange, day))
        if day == days[-1].replace("-", "") and exchange == "szse":
            return pd.DataFrame(columns=["证券代码", "融资买入额", "融资余额"])
        if exchange == "sse":
            return pd.DataFrame([{"标的证券代码": code, "融资买入额": 1e6 * (i + 1), "融资余额": 1e9}
                                 for i, code in enumerate(codes[:30])] + [{"标的证券代码": "510050", "融资买入额": 1e9, "融资余额": 1e9}])
        return pd.DataFrame([{"证券代码": code, "融资买入额": 1e6 * (i + 31), "融资余额": 2e9} for i, code in enumerate(codes[30:])])

    result = rs.margin_crowding(days, market, codes, fetch)
    assert result["status"] == "available" and result["trade_date"] == days[-2]
    assert result["attempts"] == {days[-1]: {"sse": 31, "szse": 0}, days[-2]: {"sse": 31, "szse": 30}}
    assert asked[:2] == [("sse", days[-1].replace("-", "")), ("szse", days[-1].replace("-", ""))]
    assert result["eligible"] == 59 and "600005" not in result["stocks"] and "510050" not in result["stocks"]
    top = result["stocks"]["000029"]
    assert top["buy_share"] == 0.6 and top["crowded"] is True and top["pct"] == 1.0 and top["balance"] == 2_000_000_000
    shares = sorted(item["buy_share"] for item in result["stocks"].values())
    assert result["threshold"] == round(float(np.quantile(shares, 0.9)), 6)
    assert result["crowded_count"] == sum(share >= result["threshold"] for share in shares) == len(result["crowded"])
    assert result["crowded"][0] == ["000029", 0.6]


def test_margin_crowding_reports_missing_publication_and_thin_lists():
    days, market, codes = margin_panel()
    empty = rs.margin_crowding(days, market, codes, lambda exchange, day: pd.DataFrame())
    assert empty["status"] == "unavailable" and empty["reason"] == "margin_unpublished" and len(empty["attempts"]) == 5
    thin = rs.margin_crowding(days, market, codes, lambda exchange, day: pd.DataFrame(
        [{"标的证券代码": codes[0], "证券代码": codes[1], "融资买入额": 1e6, "融资余额": 1e9}]))
    assert thin["reason"] == "margin_rows_lt_30"


def test_dashboard_passes_reminders_through_with_per_code_entries(tmp_path):
    payload = {"schema": SIGNALS_SCHEMA, "generated_at": NOW.isoformat(), "inputs": {"trade_date": "2026-09-30"},
               "overheat": {"status": "unavailable", "reason": "pool_lt_30"}, "index_trend": {"status": "skipped"},
               "unlock": {"status": "available", "window": ["2026-10-07", "2026-11-06"], "events": 3, "heavy_threshold": 0.05,
                          "heavy": [["301683", "2026-10-08", 0.0568], ["../x", "2026-10-08", 1], ["000001", "bad", 1]],
                          "stocks": {"301683": {"heavy": True, "ratio_total": 0.0568, "next_date": "2026-10-08",
                                                "events": [{"date": "2026-10-08", "type": "<b>首发</b>", "ratio": 0.0568, "value": 7.9e7},
                                                           {"date": "later", "ratio": 1}]}}},
               "margin": {"status": "available", "trade_date": "2026-09-29", "eligible": 3500, "quantile": 0.9, "threshold": 0.12,
                          "crowded": [["600519", 0.2], ["bad", 1]],
                          "stocks": {"600519": {"buy": 229076790, "balance": 17318617925, "buy_share": 0.2, "pct": 0.99, "crowded": True}}}}
    path = tmp_path / "research_signals.json"
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    board = Dashboard(tmp_path / "db.sqlite3", signals_path=path, now=lambda: NOW + timedelta(hours=1))
    data = board.research_signals("301683")
    assert data["unlock"]["heavy"] == [{"code": "301683", "date": "2026-10-08", "ratio": 0.0568}] and data["unlock"]["events"] == 3
    assert data["risk"]["unlock"]["status"] == "scheduled" and data["risk"]["unlock"]["heavy"] is True
    assert data["risk"]["unlock"]["events"] == [{"date": "2026-10-08", "type": "<b>首发</b>", "ratio": 0.0568, "value": 7.9e7}]
    assert data["risk"]["margin"] == {"code": "301683", "status": "not_listed"}
    assert data["margin"]["crowded"] == [{"code": "600519", "buy_share": 0.2}] and data["margin"]["trade_date"] == "2026-09-29"
    moutai = board.research_signals("600519")["risk"]
    assert moutai["margin"]["crowded"] is True and moutai["margin"]["balance"] == 17318617925
    assert moutai["unlock"] == {"code": "600519", "status": "none", "heavy": False, "ratio_total": None, "events": []}
    failed = {**payload, "margin": {"status": "unavailable", "reason": "ConnectionError: /secret/path"}}
    path.write_text(json.dumps(failed, ensure_ascii=False), encoding="utf-8")
    assert board.research_signals("600519")["margin"]["reason"] == "job_error"
