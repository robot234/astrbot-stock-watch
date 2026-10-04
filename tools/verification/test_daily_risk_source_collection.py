from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date, datetime, timedelta, timezone
import importlib.util
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT.parent))
from astrbot_stock_watch import formal_source_policy
from astrbot_stock_watch.daily_risk_sources import (
    DailyRiskSourceCollector, FIELDS, collect_companion_snapshots, parse_companion_snapshot,
)
from astrbot_stock_watch.derived_daily_risk import derive_daily_risk
from astrbot_stock_watch.providers import TusharePermissionError, TushareRequestGateway


NOW = datetime(2026, 10, 4, 2, tzinfo=timezone.utc)
FIXTURE_TOKEN = "fixture-runtime-token"


def _sessions():
    return {"2026-09-30": {"batch_id": "fixture-batch", "rows": [{
        "code": "600000", "trade_date": "2026-09-30", "batch_id": "fixture-batch",
        "source": "tushare", "basis": "unadjusted", "close": 9.48, "pre_close": 9.18,
        "volume": 1000, "amount": 9480,
    }]}}


def _body(api, rows):
    return {"code": 0, "data": {"fields": list(FIELDS[api]), "items": rows}}


class _Gateway:
    def __init__(self):
        self.calls = []

    async def request_existing_api(self, api, params, fields):
        self.calls.append((api, dict(params), fields))
        if params["offset"]:
            return _body(api, [])
        if api == "namechange":
            return _body(api, [["600000.SH", "普通名称", "20200101", None, "20191231"]])
        if api == "stock_basic":
            if params["list_status"] == "L":
                return _body(api, [["600000.SH", "20200101", None, "L"]])
            return _body(api, [])
        if api == "suspend_d":
            return _body(api, [["600001.SH", "20260930", None, "S"]])
        begin = datetime.strptime(params["start_date"], "%Y%m%d").date()
        end = datetime.strptime(params["end_date"], "%Y%m%d").date()
        rows = []
        while begin <= end:
            rows.append([params["exchange"], begin.strftime("%Y%m%d"),
                         int(begin.weekday() < 5 and begin != date(2026, 9, 25))])
            begin += timedelta(days=1)
        return _body(api, rows)


def test_collection_preserves_source_proof_but_never_invents_normal_regime():
    gateway = _Gateway()
    result = asyncio.run(DailyRiskSourceCollector(gateway, clock=lambda: NOW).collect(_sessions()))
    evidence = result["evidence"]["2026-09-30"]["600000"]
    assert evidence["names"]["complete"] is True
    assert evidence["suspensions"]["complete"] is True
    assert evidence["listing"]["open_dates"] == [
        "2026-09-22", "2026-09-23", "2026-09-24", "2026-09-28", "2026-09-29", "2026-09-30"]
    assert "regime" not in evidence
    row = _sessions()["2026-09-30"]["rows"][0]
    derived = derive_daily_risk(row, evidence, observed_at=result["observed_at"])
    assert derived["fields"] == {"suspended": False, "limit_up": None, "limit_down": None, "st": False}
    assert result["licensed"] is False and result["formal_calls"] == 0
    assert formal_source_policy.ACCEPTED_NEGATIVE_SCENARIOS == frozenset()
    assert all(formal_source_policy.formal_value(False, source=derived["risk_source"],
        scenario_version=derived["risk_scenario_version"], field=field) is None
        for field in formal_source_policy.RISK_FIELDS)
    assert all("token" not in params for _, params, _ in gateway.calls)


def test_empty_first_response_is_not_full_suspension_proof():
    gateway = _Gateway()
    async def empty(api, params, fields):
        return _body(api, [])
    gateway.request_existing_api = empty
    result = asyncio.run(DailyRiskSourceCollector(gateway, clock=lambda: NOW)._table(
        "suspend_d", {"trade_date": "20260930", "suspend_type": "S"}))
    assert result["complete"] is False
    assert result["reason"] == "empty_without_fullness_proof"


def test_offset_ignored_and_conflicting_calendar_are_rejected():
    async def ignored(api, params, fields):
        return _body(api, [["600001.SH", "20260930", None, "S"]])
    gateway = _Gateway()
    gateway.request_existing_api = ignored
    result = asyncio.run(DailyRiskSourceCollector(gateway, clock=lambda: NOW)._table(
        "suspend_d", {"trade_date": "20260930", "suspend_type": "S"}))
    assert result["complete"] is False and result["rows"] == []
    async def conflict(api, params, fields):
        return _body(api, [["SSE", "20260930", 1], ["SSE", "20260930", 0]])
    gateway.request_existing_api = conflict
    result = asyncio.run(DailyRiskSourceCollector(gateway, clock=lambda: NOW)._table(
        "trade_cal", {"exchange": "SSE", "start_date": "20260929", "end_date": "20260930"}))
    assert result["complete"] is False and result["rows"] == []


def test_permission_failures_do_not_repeat_for_each_date_or_echo_errors():
    gateway = _Gateway()
    calls = []
    async def denied(api, params, fields):
        calls.append(api)
        raise TusharePermissionError(FIXTURE_TOKEN)
    gateway.request_existing_api = denied
    collector = DailyRiskSourceCollector(gateway, clock=lambda: NOW)
    first = asyncio.run(collector._table("suspend_d", {"trade_date": "20260930", "suspend_type": "S"}))
    second = asyncio.run(collector._table("suspend_d", {"trade_date": "20260929", "suspend_type": "S"}))
    assert len(calls) == 1
    assert first["error_type"] == "TusharePermissionError"
    assert second["reason"] == "endpoint_blocked_after_failure"
    assert FIXTURE_TOKEN not in json.dumps([first, second])


def test_request_and_page_budget_never_prove_completeness():
    gateway = _Gateway()
    collector = DailyRiskSourceCollector(gateway, request_budget=1, clock=lambda: NOW)
    result = asyncio.run(collector._table("namechange", {}))
    assert collector.requests == 1 and result["complete"] is False
    collector = DailyRiskSourceCollector(gateway, max_pages=1, clock=lambda: NOW)
    result = asyncio.run(collector._table("namechange", {}))
    assert result["reason"] == "page_budget_exhausted" and result["complete"] is False


def test_cancellation_propagates_without_synthesising_safe_data():
    gateway = _Gateway()
    async def cancel(api, params, fields):
        raise asyncio.CancelledError()
    gateway.request_existing_api = cancel
    with pytest.raises(asyncio.CancelledError):
        asyncio.run(DailyRiskSourceCollector(gateway, clock=lambda: NOW)._table("namechange", {}))


def test_runtime_gateway_supplies_secret_only_to_transport_and_disables_delayed_retry():
    gateway = TushareRequestGateway(FIXTURE_TOKEN, storage=None)
    captured = []
    async def request(payload, **kwargs):
        captured.append((payload, kwargs))
        return _body("suspend_d", [])
    gateway.request_json = request
    asyncio.run(gateway.request_existing_api("suspend_d", {"trade_date": "20260930"}, "ts_code"))
    payload, options = captured[0]
    assert payload["token"] == FIXTURE_TOKEN
    assert options["rate_retry_enabled"] is False and options["cache_ttl"] == 0
    assert gateway.request_digest(payload) == gateway.request_digest({**payload, "token": "another-fixture"})
    with pytest.raises(TusharePermissionError):
        asyncio.run(TushareRequestGateway().request_existing_api("suspend_d", {}, "ts_code"))


def _companion():
    return {"f57": "600000", "f59": 2, "f43": 948, "f51": 1010, "f52": 826,
            "f86": int(datetime(2026, 9, 30, 7, 1, tzinfo=timezone.utc).timestamp())}


def _parse(data):
    return parse_companion_snapshot(data, code="600000", trade_date="2026-09-30",
        batch_id="fixture-batch", close=9.48, observed_at=NOW.isoformat())


def test_stock_get_scale_and_date_validated_without_guessing_suspension_or_st():
    result = _parse(_companion())
    assert result["validated"] is True and result["reference_close"] == 9.48
    assert result["fields"] == {"suspended": None, "limit_up": False, "limit_down": False, "st": None}
    assert result["licensed"] is False


@pytest.mark.parametrize("changed", [
    {"f43": 20144000000.0, "f86": 2194}, {"f59": 1}, {"f57": "600001"},
    {"f43": 949}, {"f51": 0}, {"f86": 0},
    {"f86": int(datetime(2026, 9, 29, 7, tzinfo=timezone.utc).timestamp())},
    {"f86": int(datetime(2026, 9, 30, 6, tzinfo=timezone.utc).timestamp())},
])
def test_invalid_or_intraday_companion_snapshot_stays_unknown(changed):
    result = _parse({**_companion(), **changed})
    assert result["validated"] is False
    assert all(value is None for value in result["fields"].values())


def test_companion_capture_is_bounded_and_uses_stock_get_without_mutating_inputs():
    calls = []
    class Response:
        def raise_for_status(self):
            return None
        def json(self):
            return {"rc": 0, "data": _companion()}
    class Client:
        async def get(self, url, params):
            calls.append((url, params))
            return Response()
    class Http:
        @asynccontextmanager
        async def slot(self):
            yield Client()
    rows = _sessions()["2026-09-30"]["rows"]
    before = json.dumps(rows)
    result = asyncio.run(collect_companion_snapshots(Http(), rows, trade_date="2026-09-30",
        batch_id="fixture-batch", max_symbols=1, clock=lambda: NOW))
    assert len(calls) == 1 and "/stock/get" in calls[0][0] and "secids" not in calls[0][1]
    assert result["observations"][0]["validated"] is True
    assert result["requested"] == 1 and json.dumps(rows) == before


def test_offline_runner_without_environment_credential_makes_zero_api_calls(monkeypatch):
    monkeypatch.delenv("TUSHARE_TOKEN", raising=False)
    monkeypatch.delenv("TUSHARE_URL", raising=False)
    spec = importlib.util.spec_from_file_location("source_capture", ROOT / "tools/capture_derived_risk_sources.py")
    runner = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(runner)
    result = asyncio.run(runner.capture(_sessions()))
    assert result["tushare"]["status"] == "not_executed"
    assert result["tushare"]["requests"] == 0 and result["credential_files_read"] is False
    assert result["production_mutation"] is False and result["formal_calls"] == 0
