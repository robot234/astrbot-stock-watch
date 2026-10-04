"""Bounded, unlicensed source collection with no configuration-file access."""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import re
import time


CONTRACT_VERSION = "derived-risk-inputs/2026-10-04-v1"
FIELDS = {
    "namechange": ("ts_code", "name", "start_date", "end_date", "ann_date"),
    "suspend_d": ("ts_code", "trade_date", "suspend_timing", "suspend_type"),
    "stock_basic": ("ts_code", "list_date", "delist_date", "list_status"),
    "trade_cal": ("exchange", "cal_date", "is_open"),
}


def _hash(value: object) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


def _date(value: object, *, optional: bool = False) -> str | None:
    if optional and value in (None, ""):
        return None
    text = str(value)
    return (datetime.strptime(text, "%Y%m%d").date().isoformat()
            if re.fullmatch(r"[0-9]{8}", text) else date.fromisoformat(text).isoformat())


def _code(value: object) -> str:
    match = re.fullmatch(r"([0-9]{6})\.(SH|SZ|BJ)", str(value))
    if not match:
        raise ValueError("source_symbol_invalid")
    code, exchange = match.groups()
    expected = "BJ" if code.startswith(("4", "8", "920")) else "SH" if code.startswith(("6", "9")) else "SZ"
    if exchange != expected:
        raise ValueError("source_exchange_mismatch")
    return code


def _normalise(api: str, item: dict) -> dict:
    if api == "trade_cal":
        value = item["is_open"]
        if type(value) not in (int, str) or str(value) not in ("0", "1"):
            raise ValueError("calendar_state_invalid")
        if item["exchange"] not in ("SSE", "SZSE"):
            raise ValueError("calendar_exchange_invalid")
        return {"exchange": item["exchange"], "cal_date": _date(item["cal_date"]),
                "is_open": int(value)}
    result = {"code": _code(item["ts_code"])}
    if api == "namechange":
        result.update(name=item["name"], start_date=_date(item["start_date"]),
                      end_date=_date(item["end_date"], optional=True),
                      ann_date=_date(item["ann_date"], optional=True))
        if not isinstance(item["name"], str) or not item["name"].strip():
            raise ValueError("security_name_invalid")
    elif api == "suspend_d":
        if item["suspend_type"] != "S":
            raise ValueError("suspension_type_invalid")
        result.update(trade_date=_date(item["trade_date"]),
                      suspend_type="S", suspend_timing=item["suspend_timing"])
    else:
        if item["list_status"] not in ("L", "D", "P"):
            raise ValueError("listing_status_invalid")
        result.update(list_date=_date(item["list_date"]),
                      delist_date=_date(item["delist_date"], optional=True),
                      list_status=item["list_status"])
    return result


class DailyRiskSourceCollector:
    """Fetch source tables through an existing gateway; never infer a regime."""

    def __init__(self, gateway, *, page_size: int = 1000, max_pages: int = 50,
                 request_budget: int = 150, clock=None, max_seconds: float = 1800):
        self.gateway = gateway
        self.page_size = max(1, min(int(page_size), 1000))
        self.max_pages = max(1, min(int(max_pages), 100))
        self.request_budget = max(1, min(int(request_budget), 500))
        self.clock = clock or (lambda: datetime.now(timezone.utc))
        self.requests = 0
        self.deadline = time.monotonic() + max(1, min(float(max_seconds), 1800))
        self.blocked_apis = set()
        self.failures = {}

    def _now(self) -> str:
        instant = self.clock()
        if not isinstance(instant, datetime) or instant.tzinfo is None:
            raise ValueError("source_observation_time_unverified")
        return instant.astimezone(timezone.utc).isoformat()

    async def _table(self, api: str, params: dict) -> dict:
        rows = []
        pages = []
        seen = set()
        result = {"api": api, "params": dict(params), "rows": rows, "pages": pages,
                  "complete": False, "observed_at": self._now()}
        if api in self.blocked_apis:
            result.update(reason="endpoint_blocked_after_failure",
                          evidence_hash=_hash({"api": api, "params": params, "complete": False}))
            return result
        try:
            for _ in range(self.max_pages):
                if time.monotonic() >= self.deadline:
                    result["reason"] = "collection_deadline_exhausted"
                    break
                if self.requests >= self.request_budget:
                    result["reason"] = "request_budget_exhausted"
                    break
                self.requests += 1
                body = await self.gateway.request_existing_api(
                    api, {**params, "limit": self.page_size, "offset": len(rows)},
                    ",".join(FIELDS[api]))
                result["observed_at"] = self._now()
                if not isinstance(body, dict) or type(body.get("code")) is not int or body["code"] != 0:
                    raise ValueError("source_response_unverified")
                data = body.get("data")
                if not isinstance(data, dict):
                    raise ValueError("source_data_missing")
                fields, items = data.get("fields"), data.get("items")
                if (not isinstance(fields, list) or set(fields) != set(FIELDS[api])
                        or len(fields) != len(set(fields)) or not isinstance(items, list)):
                    raise ValueError("source_fields_unverified")
                if len(items) > self.page_size:
                    raise ValueError("source_page_limit_unverified")
                page_rows = []
                for values in items:
                    if not isinstance(values, list) or len(values) != len(fields):
                        raise ValueError("source_row_shape_invalid")
                    row = _normalise(api, dict(zip(fields, values)))
                    if api == "suspend_d" and row["trade_date"] != _date(params["trade_date"]):
                        raise ValueError("suspension_date_mismatch")
                    if api == "stock_basic" and row["list_status"] != params["list_status"]:
                        raise ValueError("listing_query_mismatch")
                    if api == "trade_cal" and (row["exchange"] != params["exchange"]
                            or not _date(params["start_date"]) <= row["cal_date"] <= _date(params["end_date"])):
                        raise ValueError("calendar_query_mismatch")
                    if api == "namechange":
                        identity = (row["code"], row["start_date"])
                    elif api == "suspend_d":
                        identity = (row["code"], row["trade_date"], row["suspend_timing"])
                    elif api == "stock_basic":
                        identity = row["code"]
                    else:
                        identity = (row["exchange"], row["cal_date"])
                    if identity in seen:
                        raise ValueError("source_pagination_repeated_row")
                    seen.add(identity)
                    page_rows.append(row)
                pages.append({"offset": len(rows), "rows": page_rows,
                              "evidence_hash": _hash({"fields": fields, "items": items}),
                              "observed_at": result["observed_at"]})
                rows.extend(page_rows)
                if not items:
                    result["complete"] = bool(rows)
                    result["reason"] = "terminal_empty_page" if rows else "empty_without_fullness_proof"
                    break
            else:
                result["reason"] = "page_budget_exhausted"
        except Exception as exc:
            result["reason"] = "source_request_or_validation_failed"
            result["error_type"] = type(exc).__name__
            result["rows"] = []
            result["complete"] = False
            self.failures[api] = self.failures.get(api, 0) + 1
            if (self.failures[api] >= 2
                    or type(exc).__name__ in ("TusharePermissionError", "TushareCircuitOpen", "TushareRateLimitError")):
                self.blocked_apis.add(api)
        if result["complete"]:
            self.failures[api] = 0
        result["evidence_hash"] = _hash({"api": api, "params": result["params"],
                                        "pages": pages, "complete": result["complete"]})
        return result

    async def collect(self, sessions: dict) -> dict:
        if not sessions:
            raise ValueError("risk_sessions_missing")
        dates = sorted(sessions)
        for day in dates:
            date.fromisoformat(day)
            session = sessions[day]
            codes = set()
            if not session.get("batch_id") or not isinstance(session.get("rows"), list):
                raise ValueError("risk_session_batch_missing")
            for row in session["rows"]:
                if (row.get("trade_date") != day or row.get("batch_id") != session["batch_id"]
                        or row.get("source") != "tushare" or row.get("basis") != "unadjusted"
                        or not re.fullmatch(r"[0-9]{6}", str(row.get("code", "")))
                        or row["code"] in codes):
                    raise ValueError("risk_session_identity_mismatch")
                codes.add(row["code"])
        tables = {"names": await self._table("namechange", {})}
        for status in ("L", "D", "P"):
            tables["listing:" + status] = await self._table("stock_basic", {"list_status": status})
        start = (date.fromisoformat(dates[0]) - timedelta(days=45)).strftime("%Y%m%d")
        end = date.fromisoformat(dates[-1]).strftime("%Y%m%d")
        for exchange in ("SSE", "SZSE"):
            tables["calendar:" + exchange] = await self._table(
                "trade_cal", {"exchange": exchange, "start_date": start, "end_date": end})
        for day in dates:
            tables["suspensions:" + day] = await self._table(
                "suspend_d", {"trade_date": day.replace("-", ""), "suspend_type": "S"})
        evidence = {}
        names_by_code = {}
        for item in tables["names"]["rows"]:
            names_by_code.setdefault(item["code"], []).append(item)
        listed_by_code = {}
        for status in ("L", "D", "P"):
            table = tables["listing:" + status]
            if table["complete"]:
                for item in table["rows"]:
                    listed_by_code.setdefault(item["code"], []).append((item, table))
        for day in dates:
            evidence[day] = {}
            suspension = tables["suspensions:" + day]
            for row in sessions[day]["rows"]:
                code = row["code"]
                names = tables["names"]
                item = {"batch_id": sessions[day]["batch_id"],
                        "names": {"source": "tushare:namechange", "trade_date": day,
                                  "observed_at": names["observed_at"], "complete": names["complete"],
                                  "rows": names_by_code.get(code, []), "evidence_hash": names["evidence_hash"]},
                        "suspensions": {"source": "tushare:suspend_d", "trade_date": day,
                                        "observed_at": suspension["observed_at"],
                                        "complete": suspension["complete"],
                                        "rows": [entry for entry in suspension["rows"] if entry["code"] == code],
                                        "evidence_hash": suspension["evidence_hash"]}}
                exchange = "SSE" if code.startswith("6") else "SZSE"
                calendar = tables["calendar:" + exchange]
                rows_by_date = {entry["cal_date"]: entry["is_open"] for entry in calendar["rows"]}
                expected_days = (date.fromisoformat(day) - datetime.strptime(start, "%Y%m%d").date()).days + 1
                through = {value: state for value, state in rows_by_date.items() if value <= day}
                opened = sorted(value for value, state in through.items() if state == 1)[-6:]
                listing = listed_by_code.get(code, [])
                if (len(listing) == 1 and calendar["complete"] and len(through) == expected_days
                        and opened and opened[-1] == day):
                    record, listing_table = listing[0]
                    item["listing"] = {"source": "tushare:listing-calendar", "trade_date": day,
                                       "observed_at": max(calendar["observed_at"], listing_table["observed_at"]),
                                       "complete": True, "code": code, "list_date": record["list_date"],
                                       "open_dates": opened,
                                       "evidence_hash": _hash([calendar["evidence_hash"], listing_table["evidence_hash"]])}
                evidence[day][code] = item
        return {"contract_version": CONTRACT_VERSION, "licensed": False,
                "observed_at": self._now(), "requests": self.requests,
                "tables": tables, "evidence": evidence,
                "blockers": ["special_trading_regime_evidence_not_collected"],
                "formal_calls": 0}


def parse_companion_snapshot(data: dict, *, code: str, trade_date: str,
                             batch_id: str, close: float, observed_at: str) -> dict:
    """Decode stock/get only; absent suspension/ST stays None, never False."""
    result = {"source": "eastmoney:companion", "code": code, "trade_date": trade_date,
              "batch_id": batch_id, "first_observed_at": observed_at,
              "fields": dict.fromkeys(("suspended", "limit_up", "limit_down", "st")),
              "validated": False, "licensed": False}
    try:
        if data.get("f57") != code or not batch_id:
            raise ValueError("companion_symbol_or_batch_mismatch")
        digits = data["f59"]
        if type(digits) is not int or digits != 2:
            raise ValueError("companion_price_scale_unverified")
        numbers = [Decimal(str(data[key])) / (10 ** digits) for key in ("f43", "f51", "f52")]
        if any(not number.is_finite() or number <= 0 for number in numbers):
            raise ValueError("companion_price_invalid")
        last, upper, lower = numbers
        reference = Decimal(str(close))
        if (not reference.is_finite() or reference <= 0 or abs(last - reference) > Decimal("0.005")
                or not lower <= last <= upper or not lower < upper):
            raise ValueError("companion_reference_or_limit_mismatch")
        observed = datetime.fromisoformat(observed_at)
        stamp = data["f86"]
        if type(stamp) is not int:
            raise ValueError("companion_timestamp_invalid")
        source_time = datetime.fromtimestamp(stamp, timezone.utc)
        china = timezone(timedelta(hours=8))
        if (observed.tzinfo is None or source_time > observed
                or source_time.astimezone(china).date().isoformat() != trade_date
                or source_time.astimezone(china).hour < 15):
            raise ValueError("companion_date_or_close_unverified")
        result["fields"]["limit_up"] = last == upper
        result["fields"]["limit_down"] = last == lower
        for field in ("suspended", "st"):
            if type(data.get(field)) is bool:
                result["fields"][field] = data[field]
        result.update(validated=True, reference_close=float(last),
                      source_timestamp=source_time.isoformat(), evidence_hash=_hash(data))
    except (KeyError, TypeError, ValueError, OverflowError, InvalidOperation):
        result["fields"] = dict.fromkeys(("suspended", "limit_up", "limit_down", "st"))
        result["validated"] = False
        result["reason"] = "companion_snapshot_unverified"
    return result


async def collect_companion_snapshots(http, rows: list[dict], *, trade_date: str,
                                      batch_id: str, max_symbols: int = 100, clock=None,
                                      max_seconds: float = 180) -> dict:
    """Explicit research call, bounded per-symbol requests; no Quote mutation."""
    clock = clock or (lambda: datetime.now(timezone.utc))
    bound = max(1, min(int(max_symbols), 5000))
    result = {"source": "eastmoney:companion", "endpoint": "stock/get",
              "trade_date": trade_date, "batch_id": batch_id, "requested": 0,
              "total_rows": len(rows), "observations": [], "errors": [],
              "licensed": False, "historical_reconstruction": False}
    seen = set()
    deadline = time.monotonic() + max(1, min(float(max_seconds), 1800))
    for row in rows[:bound]:
        if time.monotonic() >= deadline:
            result["reason"] = "companion_deadline_exhausted"
            break
        code = str(row.get("code", ""))
        if (code in seen or not re.fullmatch(r"[0-9]{6}", code) or row.get("trade_date") != trade_date
                or row.get("batch_id") != batch_id):
            raise ValueError("companion_input_identity_mismatch")
        seen.add(code)
        result["requested"] += 1
        try:
            async with http.slot() as client:
                response = await client.get("https://push2.eastmoney.com/api/qt/stock/get",
                    params={"secid": ("1." if code.startswith("6") else "0.") + code,
                            "fields": "f43,f51,f52,f57,f59,f86,suspended,st"})
                response.raise_for_status()
                body = response.json()
            data = (body.get("data") if isinstance(body, dict)
                    and type(body.get("rc")) is int and body["rc"] == 0 else None)
            if not isinstance(data, dict):
                raise ValueError("companion_payload_unverified")
            selected = {key: data[key] for key in ("f43", "f51", "f52", "f57", "f59", "f86", "suspended", "st")
                        if key in data}
            instant = clock()
            if not isinstance(instant, datetime) or instant.tzinfo is None:
                raise ValueError("companion_observation_time_unverified")
            observation = parse_companion_snapshot(
                selected, code=code, trade_date=trade_date, batch_id=batch_id,
                close=row["close"], observed_at=instant.isoformat())
            observation["raw"] = selected
            result["observations"].append(observation)
        except Exception as exc:
            result["errors"].append({"code": code, "error_type": type(exc).__name__})
    return result
