from __future__ import annotations

from copy import deepcopy
import hashlib
import importlib.util
import json
from pathlib import Path
import sys

import pytest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT.parent))
from astrbot_stock_watch.baostock_rule_daily_risk import derive_daily_risk
from astrbot_stock_watch import formal_source_policy

NOW = "2026-10-08T09:00:00+00:00"


def hash_raw(raw):
    return hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def inputs(code="600000", close="11.01", previous="10.01"):
    row = {"code": code, "trade_date": "2026-10-08", "source": "baostock",
           "basis": "unadjusted", "batch_id": "fixture", "close": close, "pre_close": previous}
    raw_code = ("sh." if code.startswith("6") else "sz.") + code
    raw = {"code": raw_code, "date": row["trade_date"], "close": close,
           "preclose": previous, "volume": "100", "amount": "", "tradestatus": "1", "isST": "0"}
    primary = {"source": "baostock:daily:unadjusted", "trade_date": row["trade_date"],
               "batch_id": "fixture", "observed_at": NOW, "raw": raw, "evidence_hash": hash_raw(raw),
               "query": {"code": raw_code, "start_date": row["trade_date"],
                         "end_date": row["trade_date"], "frequency": "d", "adjustflag": "3"}}
    dated = {"code": code, "trade_date": row["trade_date"], "observed_at": NOW,
             "evidence_hash": "synthetic-fixture-not-live-proof"}
    listing = {**dated, "source": "baostock:listing-calendar", "complete": True,
               "list_date": "2010-01-01", "open_dates": ["2026-09-24", "2026-09-25",
               "2026-09-28", "2026-09-29", "2026-09-30", "2026-10-08"]}
    regime = {**dated, "source": "exchange:trading-regime", "ordinary": True}
    return row, {"batch_id": "fixture", "baostock": primary, "listing": listing, "regime": regime}


def derive_changed(changes):
    row, evidence = inputs()
    evidence["baostock"]["raw"].update(changes)
    evidence["baostock"]["evidence_hash"] = hash_raw(evidence["baostock"]["raw"])
    return derive_daily_risk(row, evidence, observed_at=NOW)


@pytest.mark.parametrize("code,previous,close,upper,lower", [
    ("600000", "10.01", "11.01", "11.01", "9.01"),
    ("000001", "10.05", "11.06", "11.06", "9.05"),
    ("300750", "10.01", "12.01", "12.01", "8.01"),
    ("301001", "10.01", "8.01", "12.01", "8.01"),
])
def test_original_v1_cent_rounding_and_boards(code, previous, close, upper, lower):
    row, evidence = inputs(code, close, previous)
    result = derive_daily_risk(row, evidence, observed_at=NOW)
    assert result["limit_prices"] == {"upper": upper, "lower": lower}
    assert result["fields"]["limit_up"] is (close == upper)
    assert result["fields"]["limit_down"] is (close == lower)
    assert result["fields"]["suspended"] is False
    assert result["fields"]["st"] is False
    assert result["licensed"] is False
    assert formal_source_policy.ACCEPTED_NEGATIVE_SCENARIOS == frozenset()
    assert all(formal_source_policy.formal_value(False, source=result["risk_source"],
        scenario_version=result["risk_scenario_version"], field=field) is None for field in result["fields"])


def test_suspended_empty_volume_amount_are_positive_not_zero():
    result = derive_changed({"tradestatus": "0", "volume": "", "amount": ""})
    assert result["fields"]["suspended"] is True
    assert result["fields"]["st"] is False
    assert result["fields"]["limit_up"] is None


@pytest.mark.parametrize("volume", ["", None, "0", "-1", "NaN", True])
def test_normal_trading_needs_strictly_positive_finite_volume(volume):
    assert derive_changed({"volume": volume})["fields"]["suspended"] is None


@pytest.mark.parametrize("status", ["", None, "2", True])
def test_missing_or_unrecognized_status_is_unknown(status):
    assert derive_changed({"tradestatus": status})["fields"]["suspended"] is None


@pytest.mark.parametrize("status,expected", [("1", True), ("0", False), ("", None), (None, None)])
def test_baostock_st_not_current_name(status, expected):
    assert derive_changed({"isST": status})["fields"]["st"] is expected


@pytest.mark.parametrize("change", ["missing_regime", "special", "missing_listing", "ipo", "unsupported"])
def test_unproven_regime_ipo_and_unsupported_board_never_false(change):
    row, evidence = inputs("688001" if change == "unsupported" else "600000")
    if change == "missing_regime":
        del evidence["regime"]
    elif change == "special":
        evidence["regime"]["ordinary"] = False
    elif change == "missing_listing":
        del evidence["listing"]
    elif change == "ipo":
        evidence["listing"]["open_dates"] = evidence["listing"]["open_dates"][-5:]
    result = derive_daily_risk(row, evidence, observed_at=NOW)
    assert result["fields"]["limit_up"] is None and result["fields"]["limit_down"] is None


@pytest.mark.parametrize("change", [
    {"evidence_hash": "wrong"}, {"observed_at": "2026-10-09T09:00:00+00:00"},
    {"observed_at": "2026-10-08T06:59:00+00:00"}, {"batch_id": "wrong"},
    {"query": {"frequency": "5"}}, {"raw": []},
])
def test_identity_hash_and_time_errors_clear_all_states(change):
    row, evidence = inputs()
    evidence["baostock"].update(change)
    assert all(value is None for value in derive_daily_risk(row, evidence, observed_at=NOW)["fields"].values())


def test_tushare_is_only_price_crosscheck_and_conflict_blocks_limits():
    row, evidence = inputs()
    evidence["price_crosscheck"] = {"source": "tushare:daily", "code": row["code"],
        "batch_id": row["batch_id"], "basis": "unadjusted", "trade_date": row["trade_date"],
        "observed_at": NOW, "evidence_hash": "fixture", "close": "11.02", "pre_close": "10.01"}
    result = derive_daily_risk(row, evidence, observed_at=NOW)
    assert result["price_crosscheck"] == "conflict"
    assert result["fields"]["limit_up"] is None
    assert result["fields"]["st"] is False


def test_new_protocol_preserves_all_thresholds_and_twenty_dates():
    protocol = json.loads((ROOT / "docs/FORMAL_RISK_FORWARD_ACCEPTANCE_20261004.json").read_text(encoding="utf-8"))
    old = json.loads((ROOT / "docs/FORMAL_RISK_SPLIT_SOURCE_ACCEPTANCE_20261004.json").read_text(encoding="utf-8"))
    assert protocol["thresholds"] == old["thresholds"]
    assert len(protocol["dates"]) == len(set(protocol["dates"])) == 20
    assert protocol["dates"][0] == "2026-10-08"
    assert protocol["status"] == "draft_pending_user_confirmation"


def test_missing_namechange_reference_does_not_stop_other_field_comparison():
    spec = importlib.util.spec_from_file_location("compare_v4", ROOT / "tools/compare_derived_daily_risk.py")
    evaluator = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(evaluator)
    row, evidence = inputs()
    reference = {"source": "eastmoney:companion", "code": row["code"], "trade_date": row["trade_date"],
        "batch_id": "fixture", "validated": True, "evidence_hash": "fixture",
        "first_observed_at": NOW, "source_timestamp": "2026-10-08T07:01:00+00:00",
        "reference_close": 11.01, "reference_pre_close": 10.01,
        "fields": {"limit_up": True, "limit_down": False}}
    protocol = json.loads((ROOT / "docs/FORMAL_RISK_FORWARD_ACCEPTANCE_20261004.json").read_text(encoding="utf-8"))
    protocol = deepcopy(protocol)
    protocol["dates"] = [row["trade_date"]]
    session = {"batch_id": "fixture", "rows": [row], "evidence": {row["code"]: evidence},
               "references": {row["code"]: [reference]}}
    result = evaluator.compare({"sessions": {row["trade_date"]: session}, "observed_at": NOW}, protocol)
    assert result["totals"]["limit_up"]["compared"] == 1
    assert result["totals"]["limit_up"]["agreement_ppm"] == 1000000
    assert result["totals"]["st"]["reference_missing"] == 1
    assert result["verdict"] == "incomplete"
