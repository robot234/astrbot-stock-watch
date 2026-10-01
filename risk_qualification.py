"""Point-in-time daily risk evidence for research and formal screening.

An absent dated-pool member is never a negative limit-state observation.
All timestamps are first local observations, not reconstructed publication times.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
import re
from pathlib import Path

from . import formal_source_policy


CHINA = timezone(timedelta(hours=8))
RISK_FIELDS = ("suspended", "limit_up", "limit_down", "st")
SOURCE_FIELDS = {
    "baostock:daily:unadjusted": frozenset({"suspended", "st"}),
    "akshare:dated-pools": frozenset({"limit_up", "limit_down"}),
    "eastmoney:companion": frozenset(RISK_FIELDS),
}
BOARDS = ("000", "001", "002", "003", "300", "301", "600", "601", "603", "605")


def supported_code(value: object) -> bool:
    code = str(value or "")
    return bool(re.fullmatch(r"[0-9]{6}", code) and code.startswith(BOARDS))


def aware_instant(value: object) -> datetime:
    if isinstance(value, datetime):
        instant = value
    else:
        instant = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    if instant.tzinfo is None or instant.utcoffset() is None:
        raise ValueError("risk_observation_time_unverified")
    return instant.astimezone(timezone.utc)


def stored_recorded_instant(value: object) -> datetime:
    """Read the old UTC-naive writer format as UTC; new rows are aware."""
    instant = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
    if instant.tzinfo is None:
        instant = instant.replace(tzinfo=timezone.utc)
    return aware_instant(instant)


def normalize_observation(item: dict, *, trade_date: str, batch_id: str, close: float) -> list[dict]:
    """Validate one source row and return only its explicitly proven fields."""
    if not isinstance(item, dict) or not batch_id or str(item.get("trade_date")) != trade_date:
        raise ValueError("risk_batch_or_date_mismatch")
    date.fromisoformat(trade_date)
    if str(item.get("batch_id")) != batch_id or not supported_code(item.get("code")):
        raise ValueError("risk_batch_or_code_mismatch")
    source = str(item.get("source") or "")
    if source not in SOURCE_FIELDS:
        raise ValueError("risk_source_unverified")
    observed = aware_instant(item.get("first_observed_at"))
    source_timestamp = ""
    if source == "eastmoney:companion":
        source_time = aware_instant(item.get("source_timestamp"))
        if source_time > observed or source_time.astimezone(CHINA).date().isoformat() != trade_date:
            raise ValueError("risk_source_timestamp_mismatch")
        source_timestamp = source_time.isoformat()
    close_value = float(item.get("reference_close"))
    if (not math.isfinite(close_value) or not math.isfinite(float(close)) or close <= 0
            or abs(close_value - close) > 0.005):
        raise ValueError("risk_reference_close_mismatch")
    # These APIs report the finished daily state. A retrospectively fetched
    # row may be used only for a new later decision, never for an earlier one.
    close_at = datetime.combine(date.fromisoformat(trade_date), datetime.min.time(), CHINA).replace(hour=15)
    if observed < close_at.astimezone(timezone.utc):
        raise ValueError("risk_observation_before_close")
    fields = item.get("fields")
    if not isinstance(fields, dict) or not fields or any(key not in SOURCE_FIELDS[source] for key in fields):
        raise ValueError("risk_fields_unverified")
    result = []
    for field, value in fields.items():
        if value is None:
            continue
        if not isinstance(value, bool) or (source == "akshare:dated-pools" and value is not True):
            raise ValueError("risk_field_not_explicit")
        canonical = {"trade_date": trade_date, "batch_id": batch_id, "code": item["code"],
                     "field": field, "value": int(value), "source": source,
                     "first_observed_at": observed.isoformat(), "source_timestamp": source_timestamp,
                     "reference_close": close_value}
        digest = hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()
        result.append({**canonical, "observation_id": "risk:" + digest, "evidence_hash": digest})
    return result


def usable_as_of_rows(rows: list[dict], *, trade_date: str, batch_id: str,
                      closes: dict[str, float], decision_at: object) -> list[dict]:
    """Select evidence both observed and persisted before the decision."""
    cutoff = aware_instant(decision_at)
    usable = []
    for row in rows:
        code = str(row.get("code") or "")
        if (not supported_code(code) or row.get("trade_date") != trade_date
                or row.get("batch_id") != batch_id or code not in closes):
            continue
        try:
            observed = aware_instant(row.get("first_observed_at"))
            recorded = stored_recorded_instant(row.get("recorded_at"))
            close = float(row.get("reference_close"))
            field = str(row.get("field") or "")
            value = row.get("value")
            source = str(row.get("source") or "")
            if source == "eastmoney:companion":
                source_time = aware_instant(row.get("source_timestamp"))
                if source_time > observed or source_time.astimezone(CHINA).date().isoformat() != trade_date:
                    continue
        except (TypeError, ValueError, OverflowError):
            continue
        if (observed > cutoff or recorded > cutoff or not math.isfinite(close)
                or abs(close - closes[code]) > 0.005
                or source not in SOURCE_FIELDS or field not in SOURCE_FIELDS[source]
                or value not in (0, 1) or (source == "akshare:dated-pools" and value != 1)):
            continue
        if value == 0 and not formal_source_policy.accepted_negative(
                source, row.get("source_policy_version"), field):
            continue
        usable.append(row)
    return usable


def resolve_as_of(rows: list[dict], *, trade_date: str, batch_id: str,
                  closes: dict[str, float], decision_at: object) -> dict[str, dict]:
    """Resolve exact-date/batch/price evidence; contradictions remain unknown."""
    values: dict[str, dict[str, set[bool]]] = {}
    for row in usable_as_of_rows(rows, trade_date=trade_date, batch_id=batch_id,
                                 closes=closes, decision_at=decision_at):
        code, field, value = str(row["code"]), str(row["field"]), row["value"]
        values.setdefault(code, {}).setdefault(field, set()).add(bool(value))
    return {code: {field: next(iter(seen)) if len(seen) == 1 else None
                   for field in RISK_FIELDS for seen in (by_field.get(field, set()),)}
            for code, by_field in values.items()}


def observations_from_research_sidecar(bundle: dict) -> dict:
    """Adapt Bao suspension and AK positive matches without inventing negatives."""
    if (not isinstance(bundle, dict) or bundle.get("version") != 1
            or bundle.get("sources") != {"trading": "baostock:daily:unadjusted",
                                          "limit": "akshare:dated-pools"}
            or bundle.get("failed_codes") or not isinstance(bundle.get("rows"), dict)
            or len(bundle["rows"]) > 100):
        raise ValueError("research_sidecar_unverified")
    trade_date = str(bundle.get("trade_date") or "")
    batch_id = str(bundle.get("batch_id") or "")
    date.fromisoformat(trade_date)
    times = bundle.get("source_captured_at") or {}
    if not isinstance(times, dict) or not batch_id:
        raise ValueError("research_sidecar_time_missing")
    observed = {key: aware_instant(times.get(key)).isoformat()
                for key in ("trading", "limit_up", "limit_down")}
    captured = aware_instant(bundle.get("captured_at"))
    if any(aware_instant(value) > captured for value in observed.values()):
        raise ValueError("research_sidecar_source_after_capture")
    observations = []
    for code, row in bundle["rows"].items():
        if not supported_code(code) or not isinstance(row, dict):
            raise ValueError("research_sidecar_code_invalid")
        close = float(row.get("close"))
        if (not math.isfinite(close) or close <= 0
                or not isinstance(row.get("suspended"), bool)
                or not any(row.get("st") is value for value in (True, False, None))):
            raise ValueError("research_sidecar_trading_invalid")
        observations.append({"trade_date": trade_date, "batch_id": batch_id, "code": code,
                             "source": "baostock:daily:unadjusted", "first_observed_at": observed["trading"],
                             "reference_close": close, "fields": {"suspended": row["suspended"],
                                                                      "st": row.get("st")}})
        for field in ("limit_up", "limit_down"):
            value = row.get(field)
            if value is True:
                observations.append({"trade_date": trade_date, "batch_id": batch_id, "code": code,
                                     "source": "akshare:dated-pools", "first_observed_at": observed[field],
                                     "reference_close": close, "fields": {field: True}})
            elif value is not None:
                raise ValueError("research_sidecar_negative_limit_unverified")
    return {"trade_date": trade_date, "batch_id": batch_id, "observations": observations}


def read_research_sidecar(path: Path, *, trade_date: str, batch_id: str) -> dict:
    """Read one bounded local artifact with an exact date and raw-batch identity."""
    if path.stat().st_size > 65536:
        raise ValueError("research_sidecar_oversized")
    bundle = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(bundle, dict) or bundle.get("trade_date") != trade_date or bundle.get("batch_id") != batch_id:
        raise ValueError("research_sidecar_batch_mismatch")
    return observations_from_research_sidecar(bundle)
