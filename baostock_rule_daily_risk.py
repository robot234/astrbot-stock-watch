"""Unlicensed v4 research using BaoStock prices and the original v1 arithmetic."""
from __future__ import annotations

from datetime import datetime
from decimal import InvalidOperation
import hashlib
import json
import re

from .derived_daily_risk import CHINA, FIELDS, MAIN_PREFIXES, _day, _dated, _number, normal_price_band


SOURCE = "derived:baostock-v1-rules"
SCENARIO_VERSION = "2026-10-v4"


def supported_code(code: str) -> bool:
    return bool(re.fullmatch(r"[0-9]{6}", code) and code.startswith(MAIN_PREFIXES + ("300", "301")))


def ordinary_regime(evidence: dict, code: str, trade_date: str, observed: datetime) -> dict:
    result = {"ordinary": None, "source": "derived:baostock-basic-daily-calendar",
              "rule_version": "2026-10-v4-draft-r2", "reason": "basic_or_calendar_unverified"}
    try:
        basic = evidence["basic"]
        raw = basic["raw"]
        symbol = ("sh." if code.startswith("6") else "sz.") + code
        raw_hash = hashlib.sha256(json.dumps(raw, sort_keys=True, separators=(",", ":"),
                                            allow_nan=False).encode()).hexdigest()
        captured = datetime.fromisoformat(basic["observed_at"])
        if (not supported_code(code)
                or not _dated(basic, trade_date, "baostock:query_stock_basic", observed)
                or basic.get("batch_id") != evidence.get("batch_id")
                or basic.get("complete") is not True or basic.get("evidence_hash") != raw_hash
                or captured.astimezone(CHINA).date().isoformat() != trade_date
                or raw.get("code") != symbol or raw.get("type") != "1"
                or any(field not in raw for field in ("code_name", "ipoDate", "outDate", "status"))):
            return result
        name = raw.get("code_name")
        if not isinstance(name, str) or not name.strip() or name.strip() in ("-", "--") or name.strip().isdigit():
            return result
        listed = _day(raw["ipoDate"])
        listing = evidence["listing"]
        sessions = listing["open_dates"]
        calendar_valid = (_dated(listing, trade_date, "baostock:listing-calendar", observed)
                          and listing.get("complete") is True and listing.get("code") == code
                          and listing.get("list_date") == listed.isoformat()
                          and bool(listing.get("evidence_hash")) and isinstance(sessions, list)
                          and bool(sessions) and sessions == sorted(set(sessions))
                          and sessions[-1] == trade_date
                          and all(listed <= _day(value) <= _day(trade_date) for value in sessions))
        if not calendar_valid:
            return result
        daily = evidence["baostock"]["raw"]
        excluded = (len(sessions) <= 5 or raw.get("status") != "1"
                    or raw.get("outDate") not in ("", None)
                    or "退" in name or "ST" in name.upper()
                    or name.upper().startswith(("N", "C"))
                    or daily.get("isST") != "0" or daily.get("tradestatus") != "1"
                    or evidence.get("known_special_regime") is True)
        result.update(ordinary=not excluded, basic_hash=raw_hash,
                      calendar_hash=listing["evidence_hash"], listing_date=listed.isoformat(),
                      listed_sessions=len(sessions), security_name=name,
                      reason="ordinary_by_basic_daily_calendar" if not excluded else "ipo_st_or_delisting_excluded")
    except (KeyError, TypeError, ValueError, AttributeError, IndexError):
        pass
    return result


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

    regime = ordinary_regime(evidence, code, trade_date, observed)
    result["regime_derivation"] = regime
    eligible = regime["ordinary"] is True and fields["st"] is False and fields["suspended"] is False
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
