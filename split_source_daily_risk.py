"""Unlicensed same-session BaoStock price/risk and Eastmoney limit audit."""
from __future__ import annotations

from datetime import datetime
from decimal import Decimal
import hashlib
import json

from .baostock_daily_risk import DAILY_SOURCE, derive_daily_risk as derive_baostock_risk
from .derived_daily_risk import CHINA, FIELDS, _number


SOURCE = "mixed:baostock-eastmoney"
SCENARIO_VERSION = "2026-10-v3"
PRICE_KEYS = ("open", "high", "low", "close", "preclose", "volume", "amount")


def _hash(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def derive_daily_risk(row: dict, evidence: dict, *, observed_at: str) -> dict:
    result = derive_baostock_risk(row, evidence, observed_at=observed_at)
    if evidence.get("baostock") is None:
        result["reasons"].append("split_audit_primary_missing_retains_fallback_identity")
        return result
    fields = result["fields"]
    result.update(risk_source=SOURCE, risk_scenario_version=SCENARIO_VERSION,
                  primary_price_source=DAILY_SOURCE, primary_price_complete=False)
    result.pop("limit_prices", None)
    for field in ("limit_up", "limit_down"):
        fields[field] = None
        result["field_sources"].pop(field, None)
    result["reasons"] = [reason for reason in result["reasons"] if reason not in (
        "ipo_st_suspension_or_special_regime_unverified", "normal_band_or_close_unverified")]
    reasons = result["reasons"]
    try:
        result["input_hash"] = _hash({"row": row, "evidence": evidence, "observed_at": observed_at})
        primary = evidence["baostock"]
        query = primary["query"]
        if not isinstance(query, dict):
            raise ValueError("primary_query_shape_invalid")
        expected_symbol = ("sh." if row["code"].startswith("6") else "sz.") + row["code"]
        if (query.get("frequency") != "d" or query.get("adjustflag") != "3"
                or query.get("code") != expected_symbol
                or not query["start_date"] <= row["trade_date"] <= query["end_date"]
                or "baostock_primary_invalid_no_safety_fallback" in reasons):
            raise ValueError("primary_query_or_envelope_invalid")
        raw = primary["raw"]
        if any(key not in raw for key in PRICE_KEYS):
            reasons.append("primary_price_tuple_missing_no_cross_supplier_fill")
            return result
        prices = {key: _number(raw[key]) for key in PRICE_KEYS}
        if (any(prices[key] <= 0 for key in PRICE_KEYS[:5])
                or prices["volume"] < 0 or prices["amount"] < 0
                or not prices["low"] <= min(prices["open"], prices["close"])
                or not max(prices["open"], prices["close"]) <= prices["high"]
                or any(prices[key] != prices[key].quantize(Decimal("0.01"))
                       for key in ("open", "high", "low", "close"))):
            raise ValueError("primary_price_tuple_invalid")
        crosscheck = {}
        for key in PRICE_KEYS[:5]:
            baseline_key = "pre_close" if key == "preclose" else key
            if baseline_key in row:
                delta = abs(prices[key] - _number(row[baseline_key]))
                crosscheck[key] = str(delta)
                if delta > Decimal("0.005"):
                    raise ValueError("baseline_price_conflict")
        result.update(primary_price_complete=True,
                      primary_prices={key: str(value) for key, value in prices.items()},
                      baseline_price_differences=crosscheck)
    except (KeyError, TypeError, ValueError, ArithmeticError):
        fields.update(dict.fromkeys(FIELDS))
        result["field_sources"] = {}
        reasons.append("primary_price_tuple_or_provenance_invalid")
        return result
    companion = evidence.get("companion")
    if companion is None:
        reasons.append("same_session_companion_missing")
        return result
    try:
        observed = datetime.fromisoformat(observed_at)
        captured = datetime.fromisoformat(companion["first_observed_at"])
        raw = companion["raw"]
        if not isinstance(raw, dict):
            raise ValueError("companion_row_shape_invalid")
        if (companion.get("source") != "eastmoney:companion"
                or companion.get("code") != row["code"]
                or companion.get("trade_date") != row["trade_date"]
                or companion.get("batch_id") != row["batch_id"]
                or type(companion.get("response_rc")) is not int or companion["response_rc"] != 0
                or companion.get("evidence_hash") != _hash(raw)
                or observed.tzinfo is None or captured.tzinfo is None or captured > observed
                or raw.get("f57") != row["code"] or type(raw.get("f59")) is not int or raw["f59"] != 2
                or type(raw.get("f86")) is not int):
            raise ValueError("companion_identity_or_scale_unverified")
        source_time = datetime.fromtimestamp(raw["f86"], CHINA)
        if (source_time > captured or source_time.date().isoformat() != row["trade_date"]
                or source_time.hour < 15):
            raise ValueError("companion_session_or_close_unverified")
        last, previous, upper, lower = (_number(raw[key]) / 100 for key in ("f43", "f60", "f51", "f52"))
        if (min(last, previous, upper, lower) <= 0 or not lower <= last <= upper or not lower < upper
                or any(value != value.quantize(Decimal("0.01")) for value in (last, upper, lower))
                or abs(last - prices["close"]) > Decimal("0.005")
                or abs(previous - prices["preclose"]) > Decimal("0.005")):
            raise ValueError("companion_price_or_bound_conflict")
        fields.update(limit_up=last == upper, limit_down=last == lower)
        for field in ("limit_up", "limit_down"):
            result["field_sources"][field] = ["eastmoney:companion"]
        result.update(limit_prices={"upper": str(upper), "lower": str(lower)},
                      companion_price=str(last), companion_pre_close=str(previous),
                      companion_source_time=source_time.isoformat())
    except (KeyError, TypeError, ValueError, OverflowError, ArithmeticError):
        reasons.append("same_session_companion_unverified")
    return result
