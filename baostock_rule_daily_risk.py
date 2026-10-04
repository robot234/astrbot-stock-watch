"""Unlicensed v4 research using BaoStock prices and the original v1 arithmetic."""
from __future__ import annotations

from datetime import datetime
from decimal import InvalidOperation
import hashlib
import json
import re

from .derived_daily_risk import CHINA, FIELDS, _day, _dated, _number, normal_price_band


SOURCE = "derived:baostock-v1-rules"
SCENARIO_VERSION = "2026-10-v4"


def derive_daily_risk(row: dict, evidence: dict, *, observed_at: str) -> dict:
    fields = dict.fromkeys(FIELDS)
    reasons = []
    code = str(row.get("code", ""))
    trade_date = str(row.get("trade_date", ""))
    result = {"risk_source": SOURCE, "risk_scenario_version": SCENARIO_VERSION,
              "code": code, "trade_date": trade_date, "observed_at": observed_at,
              "fields": fields, "reasons": reasons, "licensed": False,
              "field_sources": {field: ["baostock:daily:unadjusted"] for field in FIELDS}}
    try:
        payload = json.dumps({"row": row, "evidence": evidence, "observed_at": observed_at},
                             sort_keys=True, separators=(",", ":"), allow_nan=False)
        result["input_hash"] = hashlib.sha256(payload.encode()).hexdigest()
        observed = datetime.fromisoformat(observed_at)
        day = _day(trade_date)
        primary = evidence["baostock"]
        raw = primary["raw"]
        raw_hash = hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(",", ":"),
                                            allow_nan=False).encode()).hexdigest()
        query = primary["query"]
        expected_code = ("sh." if code.startswith("6") else "sz.") + code
        if (not re.fullmatch(r"[0-9]{6}", code) or row.get("source") != "baostock"
                or row.get("basis") != "unadjusted" or not row.get("batch_id")
                or evidence.get("batch_id") != row["batch_id"]
                or primary.get("batch_id") != row["batch_id"]
                or not _dated(primary, trade_date, "baostock:daily:unadjusted", observed)
                or raw.get("date") != trade_date or raw.get("code") != expected_code
                or primary.get("evidence_hash") != raw_hash or query.get("code") != expected_code
                or query.get("frequency") != "d" or query.get("adjustflag") != "3"
                or not _day(query["start_date"]) <= day <= _day(query["end_date"])):
            raise ValueError("invalid_identity")
    except (KeyError, TypeError, ValueError, AttributeError, InvalidOperation):
        reasons.append("invalid_baostock_identity_or_observation")
        return result

    status = raw.get("tradestatus")
    if status == "0":
        fields["suspended"] = True
    elif status == "1":
        try:
            if _number(raw.get("volume")) > 0:
                fields["suspended"] = False
        except (ValueError, TypeError, InvalidOperation):
            pass
    fields["st"] = {"0": False, "1": True}.get(str(raw.get("isST")))
    for field in ("suspended", "st"):
        if fields[field] is None:
            reasons.append(field + "_state_unproven")

    try:
        close = _number(raw.get("close"))
        previous = _number(raw.get("preclose"))
        if close <= 0 or previous <= 0:
            raise ValueError("invalid_prices")
        result["primary_prices"] = {"close": str(close), "pre_close": str(previous)}
    except (ValueError, TypeError, InvalidOperation):
        reasons.append("invalid_primary_prices")
        return result

    cross = evidence.get("price_crosscheck")
    result["price_crosscheck"] = "unavailable"
    if cross is not None:
        try:
            if (not _dated(cross, trade_date, "tushare:daily", observed)
                    or cross.get("code") != code or cross.get("batch_id") != row["batch_id"]
                    or cross.get("basis") != "unadjusted" or not cross.get("evidence_hash")):
                raise ValueError("invalid_crosscheck")
            matched = (abs(_number(cross["close"]) - close) <= _number("0.005")
                       and abs(_number(cross["pre_close"]) - previous) <= _number("0.005"))
            result["price_crosscheck"] = "matched" if matched else "conflict"
            if not matched:
                reasons.append("tushare_price_conflict")
                return result
        except (KeyError, TypeError, ValueError, InvalidOperation):
            result["price_crosscheck"] = "invalid"
            reasons.append("tushare_price_crosscheck_invalid")
            return result

    listing = evidence.get("listing")
    regime = evidence.get("regime")
    try:
        sessions = listing["open_dates"]
        listed = _day(listing["list_date"])
        eligible = (_dated(listing, trade_date, "baostock:listing-calendar", observed)
                    and listing.get("code") == code and listing.get("complete") is True
                    and bool(listing.get("evidence_hash")) and isinstance(sessions, list)
                    and len(sessions) > 5 and sessions == sorted(set(sessions))
                    and sessions[-1] == trade_date
                    and all(listed <= _day(value) <= day for value in sessions)
                    and _dated(regime, trade_date, "exchange:trading-regime", observed)
                    and regime.get("code") == code and regime.get("ordinary") is True
                    and bool(regime.get("evidence_hash"))
                    and fields["st"] is False and fields["suspended"] is False)
    except (KeyError, TypeError, ValueError, AttributeError, IndexError):
        eligible = False
    if not eligible:
        reasons.append("ipo_st_suspension_or_special_regime_unverified")
        return result
    band = normal_price_band(code, previous)
    if band is None:
        reasons.append("normal_band_uncomputable")
        return result
    upper, lower = band
    if not lower <= close <= upper or close != close.quantize(_number("0.01")):
        reasons.append("price_outside_verified_normal_band")
        return result
    fields["limit_up"] = close == upper
    fields["limit_down"] = close == lower
    result["limit_prices"] = {"upper": str(upper), "lower": str(lower)}
    return result
