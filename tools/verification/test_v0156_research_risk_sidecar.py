from __future__ import annotations

import asyncio
import importlib
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from test_v0139_intraday_m2 import _imports, _main

Main, _, StockStore = _imports()
risk = importlib.import_module("astrbot_stock_watch.research_risk")


def _pools():
    return {"trade_date": "2026-09-23", "batch_id": "batch-1",
            "frozen_at": "2026-09-23T15:03:28", "source": "tushare",
            "picks": {"primary": [{"rank": 1, "code": "600000", "name": "Alpha",
                                   "score": 20, "close": 8.98, "risk_level": "unknown"}], "radar": []}}


def _bundle():
    return {"version": 1, "trade_date": "2026-09-23", "batch_id": "batch-1",
            "captured_at": "2026-09-24T01:45:23+00:00",
            "sources": {"trading": "baostock:daily:unadjusted", "limit": "akshare:dated-pools"},
            "rows": {"600000": {"close": 8.98, "suspended": False, "st": False,
                                "limit_up": None, "limit_down": None}}}


def test_sidecar_keeps_frozen_unknown_and_shows_retrospective_facts(tmp_path):
    path = tmp_path / "2026-09-23.json"
    path.write_text(json.dumps(_bundle()), encoding="utf-8")
    pools = _pools()
    evidence = risk.load_evidence(path, pools)
    text = Main._research_pool_text(pools, "primary", evidence)
    assert "风险unknown" in text and "事后核验" in text
    assert "停牌否 ST否 涨停待核 跌停待核" in text
    assert pools["picks"]["primary"][0]["risk_level"] == "unknown"


def test_sidecar_rejects_mismatch_and_invalid_field(tmp_path):
    path = tmp_path / "2026-09-23.json"
    good = _bundle()
    for mutation in ({"batch_id": "wrong"}, {"rows": {"600000": {**good["rows"]["600000"], "close": 9.99}}},
                     {"rows": {"600000": {**good["rows"]["600000"], "suspended": 1}}},
                     {"captured_at": "2026-09-23T12:00:00"}):
        path.write_text(json.dumps({**good, **mutation}), encoding="utf-8")
        assert not risk.load_evidence(path, _pools()).get("rows")
    path.write_text("x" * 65537, encoding="utf-8")
    assert risk.load_evidence(path, _pools()) == {}


def test_main_loads_only_matching_store_sidecar(tmp_path):
    store = StockStore(tmp_path / "stock.sqlite3")
    folder = tmp_path / "research_risk_evidence"
    folder.mkdir()
    (folder / "2026-09-23.json").write_text(json.dumps(_bundle()), encoding="utf-8")
    main = _main(Main, store)
    evidence = asyncio.run(main._research_evidence(_pools()))
    assert evidence["rows"]["600000"]["suspended"] is False
    assert asyncio.run(main._research_evidence({**_pools(), "batch_id": "other"})) == {}
