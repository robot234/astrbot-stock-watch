from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import importlib.util
from pathlib import Path
import sys
import types

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT.parent))
from astrbot_stock_watch import formal_source_policy
from astrbot_stock_watch.baostock_daily_risk import (
    BaoStockRiskCollector, DAILY_FIELDS, derive_daily_risk, parse_daily_observation,
)


NOW = "2026-10-04T10:00:00+00:00"


def _row():
    return {"code": "600000", "trade_date": "2026-09-30", "batch_id": "fixture-batch",
            "basis": "unadjusted", "source": "tushare", "close": 9.48,
            "pre_close": 9.18, "volume": 1000, "amount": 9480}


def _raw():
    return {"date": "2026-09-30", "code": "sh.600000", "close": "9.4800",
            "preclose": "9.1800", "volume": "100000", "amount": "948000",
            "tradestatus": "1", "isST": "0"}


def _evidence(raw=None):
    return {"batch_id": "fixture-batch", "baostock": parse_daily_observation(
        raw or _raw(), _row(), batch_id="fixture-batch", observed_at=NOW)}


def _fallback():
    return {"batch_id": "fixture-batch", "names": {"source": "tushare:namechange",
        "trade_date": "2026-09-30", "observed_at": NOW, "complete": True,
        "rows": [{"code": "600000", "name": "普通股票", "start_date": "2020-01-01",
                  "end_date": None, "ann_date": "2019-12-31"}]},
        "suspensions": {"source": "tushare:suspend_d", "trade_date": "2026-09-30",
                        "observed_at": NOW, "complete": True, "rows": []}}


def _limit_evidence():
    return {**_evidence(), "listing": {"source": "baostock:listing-calendar",
        "trade_date": "2026-09-30", "observed_at": NOW, "complete": True, "code": "600000",
        "list_date": "2020-01-01", "evidence_hash": "fixture-listing",
        "open_dates": ["2026-09-22", "2026-09-23", "2026-09-24", "2026-09-28", "2026-09-29", "2026-09-30"]},
        "regime": {"source": "exchange:trading-regime", "trade_date": "2026-09-30",
                   "code": "600000", "observed_at": NOW, "ordinary": True,
                   "evidence_hash": "fixture-regime"}}


def test_baostock_first_without_tushare_metadata_and_without_inventing_limits():
    result = derive_daily_risk(_row(), _evidence(), observed_at=NOW)
    assert result["risk_source"] == "derived:baostock-first"
    assert result["risk_scenario_version"] == "2026-10-v2"
    assert result["fields"] == {"suspended": False, "limit_up": None, "limit_down": None, "st": False}
    assert result["field_sources"]["st"] == ["baostock:daily:unadjusted"]
    assert result["licensed"] is False
    assert formal_source_policy.ACCEPTED_NEGATIVE_SCENARIOS == frozenset()
    assert formal_source_policy.formal_value(False, source=result["risk_source"],
        scenario_version=result["risk_scenario_version"], field="st") is None


def test_missing_primary_uses_only_already_supplied_tushare_fallback():
    result = derive_daily_risk(_row(), _fallback(), observed_at=NOW)
    assert result["fields"]["st"] is False and result["fields"]["suspended"] is False
    assert result["field_sources"]["st"] == ["tushare:dated-risk"]
    assert result["risk_source"] == "derived:tushare-daily"
    assert result["risk_scenario_version"] == "2026-10-v1"
    assert "baostock_missing_optional_tushare_fallback" in result["reasons"]
    empty = derive_daily_risk(_row(), {"batch_id": "fixture-batch"}, observed_at=NOW)
    assert all(value is None for value in empty["fields"].values())


def test_future_baostock_license_cannot_license_tushare_fallback(monkeypatch):
    monkeypatch.setattr(formal_source_policy, "ACCEPTED_NEGATIVE_SCENARIOS",
        frozenset({("derived:baostock-first", "2026-10-v2", "st")}))
    fallback = derive_daily_risk(_row(), _fallback(), observed_at=NOW)
    assert fallback["fields"]["st"] is False
    assert formal_source_policy.formal_value(fallback["fields"]["st"], source=fallback["risk_source"],
        scenario_version=fallback["risk_scenario_version"], field="st") is None


@pytest.mark.parametrize("raw_change", [
    {"tradestatus": ""}, {"tradestatus": "0"}, {"volume": "0", "amount": "0"},
    {"volume": "0"}, {"amount": "0"}, {"tradestatus": True},
])
def test_uncertain_primary_suspension_does_not_fall_back_to_false(raw_change):
    evidence = {**_fallback(), **_evidence({**_raw(), **raw_change})}
    result = derive_daily_risk(_row(), evidence, observed_at=NOW)
    assert result["fields"]["suspended"] is None


def test_positive_suspension_and_st_are_retained_without_fallback():
    evidence = _evidence({**_raw(), "volume": "0", "amount": "0", "tradestatus": "0", "isST": "1"})
    result = derive_daily_risk(_row(), evidence, observed_at=NOW)
    assert result["fields"]["st"] is True and result["fields"]["suspended"] is True
    assert result["fields"]["limit_up"] is None


def test_explicit_cross_source_conflict_is_unknown_not_fallback_false():
    evidence = {**_fallback(), **_evidence({**_raw(), "isST": "1"})}
    result = derive_daily_risk(_row(), evidence, observed_at=NOW)
    assert result["fields"]["st"] is None
    assert "st:cross_source_conflict" in result["reasons"]


@pytest.mark.parametrize("change", [
    {"code": "sh.600001"}, {"date": "2026-09-29"}, {"close": "9.49"},
    {"preclose": "9.19"}, {"close": "NaN"}, {"volume": "-1"}, {"amount": "inf"},
])
def test_invalid_price_symbol_date_or_values_reject_observation(change):
    with pytest.raises((ValueError, ArithmeticError)):
        parse_daily_observation({**_raw(), **change}, _row(), batch_id="fixture-batch", observed_at=NOW)


@pytest.mark.parametrize("changed", [
    {"batch_id": "other"}, {"evidence_hash": "wrong"},
    {"first_observed_at": "2026-09-30T06:00:00+00:00"},
    {"first_observed_at": "2026-10-05T00:00:00+00:00"}, {"source": "other"},
    {"validated": False},
])
def test_invalid_primary_envelope_does_not_use_tushare_safety_fallback(changed):
    evidence = {**_fallback(), **_evidence()}
    evidence["baostock"].update(changed)
    result = derive_daily_risk(_row(), evidence, observed_at=NOW)
    assert all(value is None for value in result["fields"].values())
    assert "baostock_primary_invalid_no_safety_fallback" in result["reasons"]


def test_malformed_raw_row_cannot_supply_risk_facts():
    with pytest.raises(ValueError):
        parse_daily_observation([], _row(), batch_id="fixture-batch", observed_at=NOW)


def test_verified_normal_limits_require_listing_and_regime_evidence():
    evidence = _limit_evidence()
    result = derive_daily_risk(_row(), evidence, observed_at=NOW)
    assert result["fields"] == dict.fromkeys(("suspended", "limit_up", "limit_down", "st"), False)
    evidence["listing"]["open_dates"] = evidence["listing"]["open_dates"][-5:]
    assert derive_daily_risk(_row(), evidence, observed_at=NOW)["fields"]["limit_up"] is None
    evidence = _limit_evidence()
    evidence["regime"]["ordinary"] = False
    assert derive_daily_risk(_row(), evidence, observed_at=NOW)["fields"]["limit_down"] is None


@pytest.mark.parametrize("code,close,upper,lower", [
    ("600000", "10.10", True, False), ("600000", "8.26", False, True),
    ("300750", "11.02", True, False), ("300750", "7.34", False, True),
])
def test_verified_main_board_and_chinext_bands_preserve_risk_positives(code, close, upper, lower):
    row = {**_row(), "code": code, "close": float(close)}
    raw = {**_raw(), "code": ("sh." if code.startswith("6") else "sz.") + code, "close": close}
    evidence = _limit_evidence()
    evidence["baostock"] = parse_daily_observation(raw, row, batch_id="fixture-batch", observed_at=NOW)
    evidence["listing"]["code"] = code
    evidence["regime"]["code"] = code
    result = derive_daily_risk(row, evidence, observed_at=NOW)
    assert result["fields"]["limit_up"] is upper
    assert result["fields"]["limit_down"] is lower


class Cursor:
    error_code = "0"

    def __init__(self, rows, fields):
        self.rows = rows
        self.fields = fields
        self.index = -1

    def next(self):
        self.index += 1
        return self.index < len(self.rows)

    def get_row_data(self):
        return [self.rows[self.index][field] for field in self.fields]


class Client:
    def __init__(self):
        self.calls = []
        self.daily_rows = [_raw()]

    def query_trade_dates(self, **params):
        self.calls.append(("calendar", params))
        current = date.fromisoformat(params["start_date"])
        finish = date.fromisoformat(params["end_date"])
        rows = []
        while current <= finish:
            rows.append({"calendar_date": current.isoformat(), "is_trading_day": str(int(current.weekday() < 5))})
            current += timedelta(days=1)
        return Cursor(rows, ["calendar_date", "is_trading_day"])

    def query_history_k_data_plus(self, **params):
        self.calls.append(("daily", params))
        assert params["adjustflag"] == "3" and params["frequency"] == "d"
        assert params["fields"] == DAILY_FIELDS
        return Cursor(self.daily_rows, DAILY_FIELDS.split(","))

    def query_stock_basic(self, **params):
        self.calls.append(("listing", params))
        return Cursor([{"code": "sh.600000", "ipoDate": "2020-01-01"}], ["code", "ipoDate"])


def _sessions():
    return {"2026-09-30": {"batch_id": "fixture-batch", "rows": [_row()]}}


def test_collector_binds_listing_and_daily_sources_without_tushare_or_regime():
    client = Client()
    result = BaoStockRiskCollector(client, clock=lambda: datetime.fromisoformat(NOW)).collect(_sessions())
    evidence = result["evidence"]["2026-09-30"]["600000"]
    assert evidence["listing"]["source"] == "baostock:listing-calendar"
    assert evidence["baostock"]["fields"] == {"suspended": False, "st": False}
    assert "regime" not in evidence
    assert result["requests"] == 3 and result["tushare_requests"] == 0
    assert result["credential_files_read"] is False and result["formal_calls"] == 0


def test_duplicate_daily_rows_are_rejected_and_budget_is_bounded():
    client = Client()
    client.daily_rows = [_raw(), _raw()]
    result = BaoStockRiskCollector(client, clock=lambda: datetime.fromisoformat(NOW)).collect(_sessions())
    assert result["evidence"]["2026-09-30"] == {}
    assert result["errors"][0]["reason"] == "daily_duplicate_or_mismatched"
    result = BaoStockRiskCollector(Client(), request_budget=1,
        clock=lambda: datetime.fromisoformat(NOW)).collect(_sessions())
    assert result["requests"] == 1 and result["evidence"]["2026-09-30"] == {}


def test_comparison_does_not_count_primary_baostock_as_its_own_reference():
    spec = importlib.util.spec_from_file_location("compare_baostock", ROOT / "tools/compare_derived_daily_risk.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    evidence = _evidence()
    session = _sessions()["2026-09-30"]
    session["evidence"] = {"600000": evidence}
    session["references"] = {"600000": [evidence["baostock"]]}
    protocol = {"source": "derived:baostock-first", "scenario_version": "2026-10-v2",
        "dates": ["2026-09-30"], "fields": ["suspended", "limit_up", "limit_down", "st"],
        "thresholds": {"minimum_sessions": 20, "minimum_daily_universe": 2500,
            "minimum_daily_complete_tuple_fraction": 0.8, "minimum_daily_field_reference_fraction": 0.8,
            "minimum_field_agreement": 0.999, "minimum_positive_reference_samples_per_field": 10,
            "maximum_unsafe_false": 0, "maximum_unexplained_disagreements": 0}}
    report = module.compare({"sessions": {"2026-09-30": session}, "observed_at": NOW}, protocol)
    assert report["verdict"] == "incomplete"
    for field in ("suspended", "st"):
        assert report["totals"][field]["same_source_reference_ignored"] == 1
        assert report["totals"][field]["agreement_ppm"] is None
        assert report["totals"][field].get("reference_known", 0) == 0


def _runner():
    spec = importlib.util.spec_from_file_location("capture_baostock", ROOT / "tools/capture_baostock_daily_risk.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_live_runner_requires_shared_guard_and_refuses_external_budget_owner(tmp_path, monkeypatch):
    module = _runner()
    monkeypatch.setattr(module, "ROOT", tmp_path)
    result = module.capture(_sessions(), max_symbols=1, request_budget=3, max_seconds=10)
    assert result["status"] == "shared_budget_guard_required" and result["requests"] == 0
    (tmp_path / ".local_records").mkdir()
    (tmp_path / ".local_records/baostock_budget_owner.json").write_text("{}", encoding="utf-8")
    result = module.capture(_sessions(), max_symbols=1, request_budget=3, max_seconds=10, guard_dir=tmp_path)
    assert result["status"] == "external_budget_owner_requires_owner_host_execution"


@pytest.mark.parametrize("blacklist_ok,budget", [(False, 10), (True, 0)])
def test_live_guard_preflight_failure_never_logs_in(tmp_path, monkeypatch, blacklist_ok, budget):
    module = _runner()
    monkeypatch.setattr(module, "ROOT", tmp_path)
    class Guard:
        def __init__(self, **kwargs):
            pass
        def __enter__(self):
            return self
        def __exit__(self, *args):
            return False
        def remaining(self):
            return budget
        def login(self, client):
            raise AssertionError("must not connect")
    guard_module = types.SimpleNamespace(BaoStockGuard=Guard, detect_ipv4=lambda: "fixture-ip",
        release_status=lambda ips: {"ok": blacklist_ok})
    monkeypatch.setattr(module, "_load_guard", lambda path: guard_module)
    result = module.capture(_sessions(), max_symbols=1, request_budget=3, max_seconds=10, guard_dir=tmp_path)
    assert result["status"] in ("shared_daily_budget_insufficient", "official_blacklist_check_unverified")
    assert result["requests"] == 0


def test_guarded_capture_records_total_calls_and_always_logs_out(tmp_path, monkeypatch):
    module = _runner()
    monkeypatch.setattr(module, "ROOT", tmp_path)
    client = Client()
    actions = []
    class Guard:
        def __init__(self, **kwargs):
            assert kwargs["max_calls"] == 5
        def __enter__(self):
            actions.append("locked")
            return self
        def __exit__(self, *args):
            actions.append("unlocked")
            return False
        def remaining(self):
            return 5
        def login(self, sdk):
            assert sdk is client
            actions.append("login")
            return types.SimpleNamespace(error_code="0")
        def logout(self, sdk):
            actions.append("logout")
        def snapshot(self):
            return {"run_calls": len(client.calls) + 2, "stop_reason": None}
    guard_module = types.SimpleNamespace(BaoStockGuard=Guard, detect_ipv4=lambda: "fixture-ip",
        release_status=lambda ips: {"ok": True})
    monkeypatch.setattr(module, "_load_guard", lambda path: guard_module)
    monkeypatch.setitem(sys.modules, "baostock", client)
    result = module.capture(_sessions(), max_symbols=1, request_budget=3, max_seconds=10, guard_dir=tmp_path)
    assert result["status"] == "captured_unlicensed" and result["guard"]["run_calls"] == 5
    assert result["tushare_requests"] == 0
    assert actions == ["locked", "login", "logout", "unlocked"]
