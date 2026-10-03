"""Unlicensed, versioned daily-risk derivation for source acceptance research."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
import hashlib
import json
import re


SOURCE = "derived:tushare-daily"
SCENARIO_VERSION = "2026-10-v1"
FIELDS = ("suspended", "limit_up", "limit_down", "st")
CHINA = timezone(timedelta(hours=8))
MAIN_PREFIXES = ("000", "001", "002", "003", "600", "601", "603", "605")


def _day(value: object) -> date:
    return date.fromisoformat(str(value))


def _number(value: object) -> Decimal:
    if isinstance(value, bool) or value is None:
        raise ValueError("invalid_number")
    number = Decimal(str(value))
    if not number.is_finite():
        raise ValueError("invalid_number")
    return number


def _dated(bundle: object, trade_date: str, source: str, observed: datetime) -> bool:
    if not isinstance(bundle, dict):
        return False
    try:
        captured = datetime.fromisoformat(str(bundle.get("observed_at", "")))
        return bool(bundle.get("source") == source and bundle.get("trade_date") == trade_date
                    and captured.tzinfo is not None and captured <= observed
                    and captured >= datetime.combine(_day(trade_date), datetime.min.time(), CHINA).replace(hour=15))
    except (TypeError, ValueError):
        return False


def normal_price_band(code: str, pre_close: object) -> tuple[Decimal, Decimal] | None:
    """Arithmetic only, not proof that the normal regime applies to a stock."""
    if not re.fullmatch(r"[0-9]{6}", code) or not code.startswith(MAIN_PREFIXES + ("300", "301")):
        return None
    try:
        previous = _number(pre_close)
        rate = Decimal("0.20") if code.startswith(("300", "301")) else Decimal("0.10")
        upper = (previous * (1 + rate)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        lower = (previous * (1 - rate)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        return (upper, lower) if 0 < lower < previous < upper else None
    except (ValueError, TypeError, InvalidOperation):
        return None


def derive_daily_risk(row: dict, evidence: dict, *, observed_at: str) -> dict:
    """Return research facts only; never modify Quote or bypass formal policy.

    ``names`` contains full namechange rows for this code. ``suspensions``
    contains the complete dated suspend_d S response. ``listing`` contains
    list_date and at least six consecutive open dates ending on this day.
    ``regime`` explicitly excludes special regimes; absence is not evidence.
    Every input bundle retains its actual retrieval time and target date.
    """
    fields = dict.fromkeys(FIELDS)
    reasons = []
    trade_date = str(row.get("trade_date", ""))
    code = str(row.get("code", ""))
    result = {"risk_source": SOURCE, "risk_scenario_version": SCENARIO_VERSION,
              "trade_date": trade_date, "code": code, "fields": fields,
              "observed_at": observed_at, "reasons": reasons, "licensed": False}
    try:
        canonical = json.dumps({"row": row, "evidence": evidence, "observed_at": observed_at},
                               sort_keys=True, separators=(",", ":"), allow_nan=False)
        result["input_hash"] = hashlib.sha256(canonical.encode()).hexdigest()
        day = _day(trade_date)
        observed = datetime.fromisoformat(observed_at)
        close_at = datetime.combine(day, datetime.min.time(), CHINA).replace(hour=15)
        if (observed.tzinfo is None or observed < close_at or row.get("source") != "tushare"
                or not row.get("batch_id") or evidence.get("batch_id") != row.get("batch_id")
                or row.get("basis") != "unadjusted" or not re.fullmatch(r"[0-9]{6}", code)
                or not code.startswith(MAIN_PREFIXES + ("300", "301"))):
            raise ValueError("invalid_daily_identity_or_observation")
        close = _number(row.get("close"))
        previous = _number(row.get("pre_close"))
        volume = _number(row.get("volume"))
        amount = _number(row.get("amount"))
        if close <= 0 or previous <= 0 or volume < 0 or amount < 0:
            raise ValueError("invalid_daily_values")
    except (ValueError, TypeError, InvalidOperation):
        reasons.append("invalid_daily_input")
        return result

    names = evidence.get("names")
    if (_dated(names, trade_date, "tushare:namechange", observed)
            and names.get("complete") is True):
        try:
            active = [item for item in names["rows"] if item["code"] == code
                      and _day(item["start_date"]) <= day
                      and (not item.get("end_date") or day <= _day(item["end_date"]))]
            if len(active) == 1 and _day(active[0]["ann_date"]) <= day:
                if not isinstance(active[0]["name"], str):
                    raise ValueError("invalid_security_name")
                name = active[0]["name"].strip()
                if name and not re.fullmatch(r"[0-9]{6}", name):
                    fields["st"] = bool(re.search(r"(?:\*?ST|退)", name.upper()))
        except (KeyError, TypeError, ValueError):
            pass
    if fields["st"] is None:
        reasons.append("dated_name_missing_or_conflicting")

    suspensions = evidence.get("suspensions")
    if _dated(suspensions, trade_date, "tushare:suspend_d", observed):
        try:
            items = suspensions["rows"]
            valid = isinstance(items, list) and all(
                isinstance(item, dict) and re.fullmatch(r"[0-9]{6}", str(item.get("code", "")))
                and item.get("trade_date") == trade_date and item.get("suspend_type") == "S"
                for item in items)
            if valid:
                if any(item["code"] == code for item in items):
                    fields["suspended"] = True
                elif suspensions.get("complete") is True and volume > 0 and amount > 0:
                    fields["suspended"] = False
        except (KeyError, TypeError):
            pass
    if fields["suspended"] is None:
        reasons.append("suspension_response_missing_or_nontrading_bar")

    listing = evidence.get("listing")
    regime = evidence.get("regime")
    try:
        sessions = listing["open_dates"]
        listed = _day(listing["list_date"])
        eligible = bool(_dated(listing, trade_date, "tushare:listing-calendar", observed)
                        and listing.get("complete") is True and listing.get("code") == code
                        and isinstance(sessions, list) and len(sessions) == len(set(sessions))
                        and sessions == sorted(sessions) and listed <= _day(sessions[0])
                        and sessions[-1] == trade_date and len(sessions) > 5
                        and all(listed <= _day(value) <= day for value in sessions)
                        and _dated(regime, trade_date, "exchange:trading-regime", observed)
                        and regime.get("code") == code and regime.get("ordinary") is True
                        and bool(regime.get("evidence_hash")) and fields["st"] is False
                        and fields["suspended"] is False)
    except (KeyError, TypeError, ValueError, IndexError):
        eligible = False
    if not eligible:
        reasons.append("ipo_st_suspension_or_special_regime_unverified")
        return result
    band = normal_price_band(code, previous)
    if band is None:
        reasons.append("normal_band_uncomputable")
        return result
    upper, lower = band
    if not (lower <= close <= upper
            and close == close.quantize(Decimal("0.01"))):
        reasons.append("price_outside_verified_normal_band")
        return result
    fields["limit_up"] = close == upper
    fields["limit_down"] = close == lower
    result["limit_prices"] = {"upper": str(upper), "lower": str(lower)}
    return result
