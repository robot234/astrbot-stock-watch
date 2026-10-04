from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT.parent))
from astrbot_stock_watch import formal_source_policy
from astrbot_stock_watch.baostock_daily_risk import parse_daily_observation
from astrbot_stock_watch.split_source_daily_risk import derive_daily_risk


NOW = "2026-10-04T12:00:00+00:00"


def _hash(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def _row():
    return {"code": "600000", "trade_date": "2026-09-30", "batch_id": "fixture-batch",
            "source": "tushare", "basis": "unadjusted", "open": 9.40, "high": 9.60,
            "low": 9.30, "close": 9.48, "pre_close": 9.18, "volume": 1000, "amount": 9480}


def _evidence():
    raw = {"date": "2026-09-30", "code": "sh.600000", "open": "9.4000", "high": "9.6000",
           "low": "9.3000", "close": "9.4800", "preclose": "9.1800", "volume": "100000",
           "amount": "948000", "tradestatus": "1", "isST": "0"}
    primary = parse_daily_observation(raw, _row(), batch_id="fixture-batch", observed_at=NOW)
    primary["query"] = {"code": "sh.600000", "start_date": "2026-09-30", "end_date": "2026-09-30",
                        "frequency": "d", "adjustflag": "3"}
    companion_raw = {"f57": "600000", "f59": 2, "f43": 948, "f60": 918,
                     "f51": 1010, "f52": 826,
                     "f86": int(datetime(2026, 9, 30, 7, 1, tzinfo=timezone.utc).timestamp())}
    companion = {"source": "eastmoney:companion", "code": "600000", "trade_date": "2026-09-30",
                 "batch_id": "fixture-batch", "response_rc": 0, "first_observed_at": NOW,
                 "raw": companion_raw, "evidence_hash": _hash(companion_raw)}
    return {"batch_id": "fixture-batch", "baostock": primary, "companion": companion}


def test_full_same_session_tuple_without_guessed_normal_regime():
    result = derive_daily_risk(_row(), _evidence(), observed_at=NOW)
    assert result["fields"] == dict.fromkeys(("suspended", "limit_up", "limit_down", "st"), False)
    assert result["primary_price_complete"] is True
    assert result["primary_prices"]["open"] == "9.4000"
    assert result["field_sources"]["st"] == ["baostock:daily:unadjusted"]
    assert result["field_sources"]["limit_up"] == ["eastmoney:companion"]
    assert "ipo_st_suspension_or_special_regime_unverified" not in result["reasons"]
    assert result["licensed"] is False
    assert formal_source_policy.ACCEPTED_NEGATIVE_SCENARIOS == frozenset()
    assert all(formal_source_policy.formal_value(False, source=result["risk_source"],
        scenario_version=result["risk_scenario_version"], field=field) is None for field in result["fields"])


def test_missing_primary_price_is_not_patched_from_tushare_or_companion():
    evidence = _evidence()
    del evidence["baostock"]["raw"]["open"]
    evidence["baostock"]["evidence_hash"] = _hash(evidence["baostock"]["raw"])
    result = derive_daily_risk(_row(), evidence, observed_at=NOW)
    assert result["primary_price_complete"] is False
    assert "primary_prices" not in result
    assert result["fields"]["limit_up"] is None
    assert result["fields"]["st"] is False


@pytest.mark.parametrize("change", [{"high": "9.40"}, {"low": "9.50"}, {"open": "9.41"}])
def test_invalid_or_cross_supplier_conflicting_primary_prices_clear_risk_facts(change):
    evidence = _evidence()
    evidence["baostock"]["raw"].update(change)
    evidence["baostock"]["evidence_hash"] = _hash(evidence["baostock"]["raw"])
    result = derive_daily_risk(_row(), evidence, observed_at=NOW)
    assert all(value is None for value in result["fields"].values())


@pytest.mark.parametrize("change", [
    {"frequency": "5"}, {"adjustflag": "2"}, {"code": "sh.600001"}, {"start_date": "2026-10-01"},
])
def test_primary_query_provenance_must_match_daily_unadjusted_source(change):
    evidence = _evidence()
    evidence["baostock"]["query"].update(change)
    assert all(value is None for value in derive_daily_risk(_row(), evidence, observed_at=NOW)["fields"].values())


@pytest.mark.parametrize("change", [
    {"f60": 919}, {"f43": 949}, {"f51": 0}, {"f52": 0}, {"f59": 3},
    {"f57": "000001"}, {"f51": 947}, {"f52": 949}, {"f43": True},
    {"f86": int(datetime(2026, 9, 29, 7, 1, tzinfo=timezone.utc).timestamp())},
    {"f86": int(datetime(2026, 9, 30, 6, 59, tzinfo=timezone.utc).timestamp())},
])
def test_companion_mismatch_cannot_become_safe_negative_limit_state(change):
    evidence = _evidence()
    evidence["companion"]["raw"].update(change)
    evidence["companion"]["evidence_hash"] = _hash(evidence["companion"]["raw"])
    result = derive_daily_risk(_row(), evidence, observed_at=NOW)
    assert result["fields"]["limit_up"] is None and result["fields"]["limit_down"] is None
    assert result["fields"]["st"] is False and result["fields"]["suspended"] is False


@pytest.mark.parametrize("change", [
    {"response_rc": 1}, {"response_rc": True}, {"batch_id": "wrong"}, {"evidence_hash": "wrong"},
    {"first_observed_at": "2026-10-05T00:00:00+00:00"}, {"trade_date": "2026-09-29"},
])
def test_companion_envelope_is_dated_and_bound_to_original_batch(change):
    evidence = _evidence()
    evidence["companion"].update(change)
    result = derive_daily_risk(_row(), evidence, observed_at=NOW)
    assert result["fields"]["limit_up"] is None and result["fields"]["limit_down"] is None


@pytest.mark.parametrize("bound,close", [("f51", "10.10"), ("f52", "8.26")])
def test_supplied_bound_equality_proves_positive_limit_without_theoretical_rule(bound, close):
    row = _row()
    row.update(close=float(close), high=10.20, low=8.20)
    evidence = _evidence()
    evidence["baostock"]["raw"].update(close=close, high="10.20", low="8.20")
    evidence["baostock"]["evidence_hash"] = _hash(evidence["baostock"]["raw"])
    evidence["companion"]["raw"]["f43"] = int(float(close) * 100)
    evidence["companion"]["evidence_hash"] = _hash(evidence["companion"]["raw"])
    result = derive_daily_risk(row, evidence, observed_at=NOW)
    assert result["fields"]["limit_up" if bound == "f51" else "limit_down"] is True


def test_primary_st_positive_remains_positive_even_with_negative_limit_states():
    evidence = _evidence()
    evidence["baostock"]["raw"]["isST"] = "1"
    evidence["baostock"]["evidence_hash"] = _hash(evidence["baostock"]["raw"])
    result = derive_daily_risk(_row(), evidence, observed_at=NOW)
    assert result["fields"]["st"] is True and result["fields"]["limit_up"] is False


def test_fallback_retains_v1_identity():
    result = derive_daily_risk(_row(), {"batch_id": "fixture-batch"}, observed_at=NOW)
    assert result["risk_source"] == "derived:tushare-daily"
    assert result["risk_scenario_version"] == "2026-10-v1"


def test_malformed_query_provenance_stays_unknown_instead_of_crashing():
    evidence = _evidence()
    evidence["baostock"]["query"] = []
    assert all(value is None for value in derive_daily_risk(_row(), evidence, observed_at=NOW)["fields"].values())


def test_malformed_companion_row_stays_unknown_instead_of_crashing():
    evidence = _evidence()
    evidence["companion"]["raw"] = []
    result = derive_daily_risk(_row(), evidence, observed_at=NOW)
    assert result["fields"]["limit_up"] is None and result["fields"]["limit_down"] is None


def test_primary_companion_does_not_validate_its_own_limit_output():
    spec = importlib.util.spec_from_file_location("compare_split", ROOT / "tools/compare_derived_daily_risk.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    evidence = _evidence()
    reference = deepcopy(evidence["companion"])
    reference.update(validated=True, reference_close=9.48, source_timestamp="2026-09-30T07:01:00+00:00",
                     fields={"limit_up": False, "limit_down": False})
    session = {"batch_id": "fixture-batch", "rows": [_row()], "evidence": {"600000": evidence},
               "references": {"600000": [reference]}}
    protocol = json.loads((ROOT / "docs/FORMAL_RISK_SPLIT_SOURCE_ACCEPTANCE_20261004.json").read_text())
    protocol["dates"] = ["2026-09-30"]
    report = module.compare({"sessions": {"2026-09-30": session}, "observed_at": NOW}, protocol)
    for field in ("limit_up", "limit_down"):
        assert report["totals"][field]["same_source_reference_ignored"] == 1
        assert report["totals"][field].get("reference_known", 0) == 0
        assert report["totals"][field]["agreement_ppm"] is None
    assert report["verdict"] == "incomplete"
