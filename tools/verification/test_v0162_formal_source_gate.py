"""Default-closed formal source behavior without the synthetic license fixture."""
from __future__ import annotations

import asyncio
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

from test_v0160_risk_qualification import (
    Main, StockStore, _clock_at, _eligible_paper_pick, _observation,
    _observe, _store, core, policy, risk, storage_module,
)
from test_v0139_intraday_m2 import _main


def _persisted(item):
    return {**item, "recorded_at": "2026-09-24T10:46:00+00:00"}


def test_unlicensed_negative_observations_remain_unknown_and_positive_blocks():
    assert policy.ACCEPTED_NEGATIVE_SCENARIOS == frozenset()
    rows = [_persisted(row) for item in (
        _observation("suspended", False, source="baostock:daily:unadjusted"),
        _observation("st", False, source="baostock:daily:unadjusted"),
        _observation("limit_up", False),
        _observation("limit_down", False),
    ) for row in risk.normalize_observation(item, trade_date="2026-09-24", batch_id="batch-a", close=10.0)]
    resolved = risk.resolve_as_of(rows, trade_date="2026-09-24", batch_id="batch-a",
                                  closes={"600000": 10.0}, decision_at="2026-09-24T11:00:00+00:00")
    assert resolved.get("600000", {}).get("suspended") is None
    assert resolved.get("600000", {}).get("limit_up") is None
    assert resolved.get("600000", {}).get("limit_down") is None
    assert resolved.get("600000", {}).get("st") is None
    assert risk.resolve_as_of(rows, trade_date="2026-09-25", batch_id="batch-a",
                              closes={"600000": 10.0}, decision_at="2026-09-25T11:00:00+00:00") == {}
    positive = [_persisted(row) for row in risk.normalize_observation(
        _observation("limit_up", True), trade_date="2026-09-24", batch_id="batch-a", close=10.0)]
    blocked = risk.resolve_as_of(rows + positive, trade_date="2026-09-24", batch_id="batch-a",
                                 closes={"600000": 10.0}, decision_at="2026-09-24T11:00:00+00:00")
    assert blocked["600000"]["limit_up"] is True
    assert risk.resolve_as_of(positive, trade_date="2026-09-24", batch_id="batch-b",
                              closes={"600000": 10.0}, decision_at="2026-09-24T11:00:00+00:00") == {}


def test_persisted_negatives_do_not_qualify_research_pick(tmp_path, monkeypatch):
    store = _store(tmp_path)
    snapshot = {"trade_date": "2026-09-24", "batch_id": "batch-a", "source": "tushare",
                "basis": "unadjusted", "published_at": "2026-09-24T08:30:00+00:00"}
    pick = [{"code": "600000", "name": "Synthetic", "score": 40, "close": 10.0,
             "amount": 1e8, "risk_level": "unknown", "risk_flags": [], "reasons": []}]
    with monkeypatch.context() as clock:
        clock.setattr(storage_module, "_utcnow_naive", lambda: datetime(2026, 9, 24, 9, 30))
        store.save_research_pools(snapshot, pick, [], {})
    with monkeypatch.context() as clock:
        clock.setattr(storage_module, "datetime", _clock_at("2026-09-24T10:46:00+00:00"))
        store.record_daily_risk_observations({
            "trade_date": "2026-09-24", "batch_id": "batch-a",
            "observations": [
                _observation("suspended", False, source="baostock:daily:unadjusted"),
                _observation("st", False, source="baostock:daily:unadjusted"),
                _observation("limit_up", False), _observation("limit_down", False),
            ],
        })
    quote = core.Quote("600000", "Synthetic", 10.0)
    assert store.daily_risk_evidence_for_quotes(
        "2026-09-24", "batch-a", [quote], "2026-09-24T11:00:00+00:00") == {}
    event = store.qualify_research_pick("research:batch-a", "600000", "2026-09-24T11:00:00+00:00")
    assert event["state"] == "unknown"
    with store._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM paper_simulated_entries").fetchone()[0] == 0


def test_old_quote_cache_is_masked_before_both_score_paths(tmp_path):
    main = _main(Main, StockStore(tmp_path / "score.sqlite3"))
    main.quotes = SimpleNamespace(tushare_token="")
    quote = core.Quote("600000", "Cached", 10.0, source="eastmoney",
                       suspended=False, limit_up=False, limit_down=False, st=False)
    ordinary = asyncio.run(main._score_quotes_result(
        [quote], 1, "2026-09-24", include_factors=False))
    assert (quote.suspended, quote.limit_up, quote.limit_down, quote.st) == (None, None, None, None)
    assert ordinary.diagnostics["risk_tuple_complete"] == 0
    assert ordinary.candidates == ()
    old = core.Quote("600000", "Cached", 10.0, source="eastmoney",
                     suspended=False, limit_up=False, limit_down=False, st=False)
    preview = asyncio.run(main._score_quotes_result(
        [old], 1, "2026-09-24", data_mode="eastmoney_transient"))
    assert (old.suspended, old.limit_up, old.limit_down, old.st) == (None, None, None, None)
    assert preview.candidates == ()


def test_st_is_required_for_formal_screen_and_cache_round_trip(tmp_path):
    quote = core.Quote("600000", "普通名", 10.0,
                       suspended=False, limit_up=False, limit_down=False)
    assert not core.is_screenable(quote)
    assert core.review_risk(quote).verdict == "unknown"
    quote.st = True
    assert not core.is_screenable(quote)
    assert "ST" in core.review_risk(quote).flags

    store = StockStore(tmp_path / "st.sqlite3")
    quote.st = False
    assert store.save_daily_quotes("2026-09-24", [quote]) == 1
    loaded = store.daily_quotes("2026-09-24")[0]
    assert loaded.st is False
    assert loaded.risk_source == ""
    assert loaded.risk_scenario_version == ""
    loaded.st = False
    policy.mask_quote_negatives(loaded)
    assert loaded.st is None


def test_pre_st_quote_database_migrates_as_unknown_after_restart(tmp_path):
    path = tmp_path / "legacy.sqlite3"
    store = StockStore(path)
    store.save_daily_quotes("2026-09-24", [core.Quote(
        "600000", "旧缓存", 10.0, suspended=False, limit_up=False, limit_down=False)])
    with sqlite3.connect(path) as db:
        for column in ("st", "risk_source", "risk_scenario_version"):
            db.execute(f"ALTER TABLE daily_quotes DROP COLUMN {column}")
    restarted = StockStore(path)
    cached = restarted.daily_quotes("2026-09-24")[0]
    assert cached.st is None
    assert cached.risk_source == cached.risk_scenario_version == ""
    policy.mask_quote_negatives(cached)
    assert (cached.suspended, cached.limit_up, cached.limit_down, cached.st) == (None,) * 4
    assert not core.is_screenable(cached)


def test_provider_explicit_st_is_tri_state_and_true_blocks():
    quote = core.Quote("600000", "普通名", 10.0)
    from astrbot_stock_watch.providers import SinaQuoteProvider
    SinaQuoteProvider._apply_authoritative_risk_fields([quote], [{
        "f12": "600000", "trade_status": "active", "st": True,
        "f43": 10, "f51": 11, "f52": 9,
    }])
    assert quote.st is True
    assert not core.is_screenable(quote)
    unknown = core.Quote("600001", "普通名", 10.0)
    SinaQuoteProvider._apply_authoritative_risk_fields([unknown], [{
        "f12": "600001", "trade_status": "active",
        "f43": 10, "f51": 11, "f52": 9,
    }])
    assert unknown.st is None


def test_automatic_collection_cannot_promote_unlicensed_companion(tmp_path):
    store = _store(tmp_path)
    main = _main(Main, store)

    async def companion(quotes, *, collect_observations):
        assert collect_observations is True
        for quote in quotes:
            quote.suspended = quote.limit_up = quote.limit_down = False
        return {"requested": 1, "complete": 1, "observations": [{
            "code": "600000", "reference_close": 10.0,
            "first_observed_at": "2026-09-24T10:45:00+00:00",
            "source_timestamp": "2026-09-24T07:00:00+00:00",
            "fields": {"suspended": False, "limit_up": False, "limit_down": False},
        }]}

    main.quotes = SimpleNamespace(enrich_daily_risk_fields=companion)
    cached = core.Quote("600000", "Old cache", 10.0,
                        suspended=False, limit_up=False, limit_down=False)
    result = asyncio.run(main._collect_automatic_close_risk_evidence(
        "2026-09-24", "batch-a", [cached]))
    assert (result[0].suspended, result[0].limit_up, result[0].limit_down) == (None, None, None)
    assert store.daily_risk_evidence_for_quotes(
        "2026-09-24", "batch-a", result, datetime.now(timezone.utc)) == {}


def test_automatic_collection_keeps_explicit_positive_block(tmp_path):
    store = _store(tmp_path)
    main = _main(Main, store)

    async def companion(quotes, *, collect_observations):
        assert collect_observations is True
        quotes[0].limit_up = True
        return {"requested": 1, "complete": 0, "observations": [{
            "code": "600000", "reference_close": 10.0,
            "first_observed_at": "2026-09-24T10:45:00+00:00",
            "source_timestamp": "2026-09-24T07:00:00+00:00",
            "fields": {"limit_up": True},
        }]}

    main.quotes = SimpleNamespace(enrich_daily_risk_fields=companion)
    result = asyncio.run(main._collect_automatic_close_risk_evidence(
        "2026-09-24", "batch-a", [core.Quote("600000", "Unknown ST", 10.0)]))
    assert result[0].limit_up is True
    assert result[0].st is None
    assert not core.is_screenable(result[0])


def test_old_eligible_event_cannot_start_new_paper_entry(tmp_path, monkeypatch):
    # Create a historical eligible event, then restore the real empty license.
    with monkeypatch.context() as historical_license:
        historical_license.setattr(policy, "accepted_negative",
                                   lambda source, version, field: field in policy.RISK_FIELDS)
        store, eligible, common = _eligible_paper_pick(tmp_path, monkeypatch)
    restarted = StockStore(tmp_path / "risk.sqlite3")
    result = _observe(restarted, **common, bar_start="2026-09-25T09:35:00+08:00",
                      quote_at="2026-09-25T09:36:10+08:00", now="2026-09-25T09:36:11+08:00")
    assert eligible["state"] == "eligible"
    assert result == {"state": "unknown", "reason": "formal_risk_source_unlicensed"}
    with restarted._connect() as db:
        assert db.execute("SELECT COUNT(*) FROM paper_bar_observations").fetchone()[0] == 0
        assert db.execute("SELECT COUNT(*) FROM paper_simulated_entries").fetchone()[0] == 0
