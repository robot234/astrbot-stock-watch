"""Unlicensed BaoStock-first historical risk research; no production calls."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
import hashlib
import json
import re
import time

from .derived_daily_risk import (
    CHINA, FIELDS, MAIN_PREFIXES, _dated, _number, derive_daily_risk as derive_tushare_risk,
    normal_price_band,
)


SOURCE = "derived:baostock-first"
SCENARIO_VERSION = "2026-10-v2"
DAILY_SOURCE = "baostock:daily:unadjusted"
DAILY_FIELDS = "date,code,close,preclose,volume,amount,tradestatus,isST"


def _hash(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def parse_daily_observation(raw: dict, baseline: dict, *, batch_id: str,
                            observed_at: str) -> dict:
    if not isinstance(raw, dict) or not isinstance(baseline, dict):
        raise ValueError("baostock_daily_row_shape_unverified")
    code = str(baseline.get("code", ""))
    day = str(baseline.get("trade_date", ""))
    observed = datetime.fromisoformat(observed_at)
    expected_symbol = ("sh." if code.startswith("6") else "sz.") + code
    if (not re.fullmatch(r"[0-9]{6}", code)
            or not code.startswith(MAIN_PREFIXES + ("300", "301"))
            or not batch_id or baseline.get("batch_id") != batch_id
            or baseline.get("basis") != "unadjusted" or baseline.get("source") != "tushare"
            or raw.get("date") != day or raw.get("code") != expected_symbol
            or observed.tzinfo is None
            or observed < datetime.combine(date.fromisoformat(day), datetime.min.time(), CHINA).replace(hour=15)):
        raise ValueError("baostock_daily_identity_unverified")
    close, previous, volume, amount = (_number(raw.get(key))
                                       for key in ("close", "preclose", "volume", "amount"))
    reference, reference_previous = (_number(baseline.get(key)) for key in ("close", "pre_close"))
    if (close <= 0 or previous <= 0 or reference <= 0 or reference_previous <= 0
            or volume < 0 or amount < 0
            or abs(close - reference) > Decimal("0.005")
            or abs(previous - reference_previous) > Decimal("0.005")):
        raise ValueError("baostock_daily_price_or_volume_unverified")
    fields = {"suspended": None, "st": None}
    if raw.get("isST") in ("0", "1"):
        fields["st"] = raw["isST"] == "1"
    if raw.get("tradestatus") == "0" and volume == 0 and amount == 0:
        fields["suspended"] = True
    elif raw.get("tradestatus") == "1" and volume > 0 and amount > 0:
        fields["suspended"] = False
    return {"source": DAILY_SOURCE, "code": code, "trade_date": day, "batch_id": batch_id,
            "first_observed_at": observed_at, "reference_close": float(close),
            "pre_close": float(previous), "fields": fields, "raw": dict(raw),
            "evidence_hash": _hash(raw), "validated": True, "licensed": False}


def derive_daily_risk(row: dict, evidence: dict, *, observed_at: str) -> dict:
    fields = dict.fromkeys(FIELDS)
    result = {"risk_source": SOURCE, "risk_scenario_version": SCENARIO_VERSION,
              "trade_date": row.get("trade_date"), "code": row.get("code"),
              "observed_at": observed_at, "fields": fields, "reasons": [],
              "field_sources": {}, "licensed": False}
    reasons = result["reasons"]
    primary = evidence.get("baostock")
    if primary is None:
        fallback = derive_tushare_risk(row, evidence, observed_at=observed_at)
        result.update(risk_source=fallback["risk_source"],
                      risk_scenario_version=fallback["risk_scenario_version"])
        fields.update(fallback["fields"])
        reasons.extend(["baostock_missing_optional_tushare_fallback", *fallback["reasons"]])
        for field, value in fields.items():
            if value is not None:
                result["field_sources"][field] = ["tushare:dated-risk" if field in ("suspended", "st") else "tushare:daily"]
        if "limit_prices" in fallback:
            result["limit_prices"] = fallback["limit_prices"]
        result["input_hash"] = fallback.get("input_hash")
        return result
    try:
        observed = datetime.fromisoformat(observed_at)
        captured = datetime.fromisoformat(primary["first_observed_at"])
        if (observed.tzinfo is None or captured.tzinfo is None or captured > observed
                or primary.get("source") != DAILY_SOURCE
                or primary.get("code") != row.get("code")
                or primary.get("trade_date") != row.get("trade_date")
                or primary.get("batch_id") != row.get("batch_id")
                or primary.get("validated") is not True
                or evidence.get("batch_id") != row.get("batch_id")
                or not primary.get("evidence_hash")
                or primary["evidence_hash"] != _hash(primary["raw"])):
            raise ValueError("baostock_primary_envelope_unverified")
        parsed = parse_daily_observation(primary["raw"], row, batch_id=row["batch_id"],
                                         observed_at=primary["first_observed_at"])
        result["input_hash"] = _hash({"row": row, "evidence": evidence, "observed_at": observed_at})
        fields.update(parsed["fields"])
        for field, value in parsed["fields"].items():
            if value is not None:
                result["field_sources"][field] = [DAILY_SOURCE]
        secondary = derive_tushare_risk(row, evidence, observed_at=observed_at)
        for field in ("suspended", "st"):
            if (fields[field] is not None and secondary["fields"][field] is not None
                    and fields[field] != secondary["fields"][field]):
                fields[field] = None
                reasons.append(field + ":cross_source_conflict")
            if fields[field] is None:
                reasons.append(field + ":baostock_state_unverified")
    except (KeyError, TypeError, ValueError, ArithmeticError):
        reasons.append("baostock_primary_invalid_no_safety_fallback")
        return result
    listing, regime = evidence.get("listing"), evidence.get("regime")
    try:
        day = row["trade_date"]
        code = row["code"]
        sessions = listing["open_dates"]
        listed = date.fromisoformat(listing["list_date"])
        listing_source = listing.get("source")
        eligible = bool(listing_source in ("baostock:listing-calendar", "tushare:listing-calendar")
                        and _dated(listing, day, listing_source, observed)
                        and listing.get("complete") is True and listing.get("code") == code
                        and listing.get("evidence_hash") and len(sessions) > 5
                        and sessions == sorted(set(sessions)) and sessions[-1] == day
                        and all(listed <= date.fromisoformat(value) <= date.fromisoformat(day) for value in sessions)
                        and _dated(regime, day, "exchange:trading-regime", observed)
                        and regime.get("code") == code and regime.get("ordinary") is True
                        and regime.get("evidence_hash") and fields["st"] is False
                        and fields["suspended"] is False)
    except (KeyError, TypeError, ValueError, IndexError):
        eligible = False
    if not eligible:
        reasons.append("ipo_st_suspension_or_special_regime_unverified")
        return result
    band = normal_price_band(row["code"], parsed["pre_close"])
    close = _number(parsed["raw"]["close"])
    if (band is None or not band[1] <= close <= band[0]
            or close != close.quantize(Decimal("0.01"))):
        reasons.append("normal_band_or_close_unverified")
        return result
    upper, lower = band
    fields.update(limit_up=close == upper, limit_down=close == lower)
    for field in ("limit_up", "limit_down"):
        result["field_sources"][field] = [DAILY_SOURCE, listing_source, "exchange:trading-regime"]
    result["limit_prices"] = {"upper": str(upper), "lower": str(lower)}
    return result


class BaoStockRiskCollector:
    """Serial, bounded public SDK queries; no Tushare calls or credential access."""

    def __init__(self, client, *, max_symbols: int = 3, request_budget: int = 10,
                 max_seconds: float = 90, clock=None):
        self.client = client
        self.max_symbols = max(1, min(int(max_symbols), 6000))
        self.request_budget = max(1, min(int(request_budget), 12001))
        self.deadline = time.monotonic() + max(1, min(float(max_seconds), 1800))
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.requests = 0

    def _now(self):
        instant = self.clock()
        if instant.tzinfo is None:
            raise ValueError("baostock_capture_time_unverified")
        return instant.astimezone(timezone.utc).isoformat()

    def _query(self, method: str, **params) -> dict:
        result = {"method": method, "params": params, "rows": [], "complete": False,
                  "observed_at": self._now()}
        if self.requests >= self.request_budget or time.monotonic() >= self.deadline:
            result["reason"] = "capture_budget_exhausted"
            return result
        self.requests += 1
        try:
            cursor = getattr(self.client, method)(**params)
            if cursor.error_code != "0":
                raise ValueError("baostock_endpoint_failed")
            fields = list(cursor.fields)
            if not fields or len(set(fields)) != len(fields):
                raise ValueError("baostock_fields_invalid")
            rows = []
            while cursor.next():
                if time.monotonic() >= self.deadline or len(rows) >= 10000:
                    raise ValueError("baostock_response_budget_exhausted")
                values = cursor.get_row_data()
                if len(values) != len(fields):
                    raise ValueError("baostock_row_shape_invalid")
                rows.append(dict(zip(fields, values)))
            if cursor.error_code != "0":
                raise ValueError("baostock_cursor_failed")
            result.update(rows=rows, complete=True, observed_at=self._now())
        except Exception as exc:
            result.update(error_type=type(exc).__name__, reason="baostock_query_unverified")
        result["evidence_hash"] = _hash({"method": method, "params": params, "rows": result["rows"],
                                        "complete": result["complete"]})
        return result

    def collect(self, sessions: dict) -> dict:
        if not sessions:
            raise ValueError("baostock_sessions_missing")
        dates = sorted(sessions)
        expected = {}
        for day, session in sessions.items():
            date.fromisoformat(day)
            if not session.get("batch_id") or not isinstance(session.get("rows"), list):
                raise ValueError("baostock_session_identity_missing")
            seen = set()
            for row in session["rows"]:
                code = row.get("code")
                if (not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code)
                        or row.get("trade_date") != day or row.get("batch_id") != session["batch_id"]
                        or row.get("source") != "tushare" or row.get("basis") != "unadjusted"
                        or code in seen):
                    raise ValueError("baostock_baseline_identity_mismatch")
                seen.add(code)
                expected.setdefault(code, {})[day] = row
        codes = sorted(expected)[:self.max_symbols]
        begin = (date.fromisoformat(dates[0]) - timedelta(days=45)).isoformat()
        tables = {"calendar": self._query("query_trade_dates", start_date=begin, end_date=dates[-1])}
        result = {"source": SOURCE, "scenario_version": SCENARIO_VERSION, "licensed": False,
                  "tables": tables, "evidence": {day: {} for day in dates}, "errors": [],
                  "total_symbols": len(expected), "selected_symbols": codes,
                  "tushare_requests": 0, "credential_files_read": False, "formal_calls": 0}
        for code in codes:
            if self.requests >= self.request_budget or time.monotonic() >= self.deadline:
                break
            symbol = ("sh." if code.startswith("6") else "sz.") + code
            daily = self._query("query_history_k_data_plus", code=symbol, fields=DAILY_FIELDS,
                                start_date=dates[0], end_date=dates[-1], frequency="d", adjustflag="3")
            tables["daily:" + code] = daily
            if not daily["complete"]:
                result["errors"].append({"code": code, "reason": "daily_query_unverified"})
                if sum(item["reason"] == "daily_query_unverified" for item in result["errors"]) >= 2:
                    break
                continue
            basic = self._query("query_stock_basic", code=symbol)
            tables["listing:" + code] = basic
            rows = {}
            invalid = False
            for raw in daily["rows"]:
                if (raw.get("code") != symbol or raw.get("date") in rows
                        or not dates[0] <= str(raw.get("date", "")) <= dates[-1]):
                    invalid = True
                    break
                rows[raw["date"]] = raw
            if invalid:
                result["errors"].append({"code": code, "reason": "daily_duplicate_or_mismatched"})
                continue
            for day, baseline in expected[code].items():
                if day not in rows:
                    result["errors"].append({"code": code, "trade_date": day, "reason": "daily_missing"})
                    continue
                evidence = {"batch_id": baseline["batch_id"]}
                try:
                    evidence["baostock"] = parse_daily_observation(rows[day], baseline,
                        batch_id=baseline["batch_id"], observed_at=daily["observed_at"])
                except (TypeError, ValueError, ArithmeticError):
                    result["errors"].append({"code": code, "trade_date": day,
                                             "reason": "daily_values_unverified"})
                    continue
                try:
                    calendar = tables["calendar"]
                    values = calendar["rows"]
                    calendar_dates = [entry["calendar_date"] for entry in values]
                    expected_size = (date.fromisoformat(dates[-1]) - date.fromisoformat(begin)).days + 1
                    valid = (calendar["complete"] and len(values) == expected_size
                             and len(set(calendar_dates)) == len(values)
                             and all(begin <= date.fromisoformat(value).isoformat() <= dates[-1]
                                     for value in calendar_dates)
                             and all(entry["is_trading_day"] in ("0", "1") for entry in values))
                    opened = sorted(entry["calendar_date"] for entry in values
                                    if entry["is_trading_day"] == "1" and entry["calendar_date"] <= day)[-6:]
                    if (valid and opened and opened[-1] == day and basic["complete"]
                            and len(basic["rows"]) == 1 and basic["rows"][0]["code"] == symbol):
                        listed = date.fromisoformat(basic["rows"][0]["ipoDate"]).isoformat()
                        evidence["listing"] = {"source": "baostock:listing-calendar", "trade_date": day,
                            "code": code, "list_date": listed, "open_dates": opened, "complete": True,
                            "observed_at": max(calendar["observed_at"], basic["observed_at"]),
                            "evidence_hash": _hash([calendar["evidence_hash"], basic["evidence_hash"]])}
                except (KeyError, TypeError, ValueError):
                    pass
                result["evidence"][day][code] = evidence
        result.update(observed_at=self._now(), requests=self.requests,
                      blockers=["special_trading_regime_evidence_not_collected",
                                "independent_historical_risk_references_required"])
        return result
