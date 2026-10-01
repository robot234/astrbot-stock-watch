"""File-based, public-data research for frozen same-day marked performance.

This tool is deliberately separate from plugin storage, configuration, and
providers.  It can capture an explicit public URL, but selection and outcome
evaluation consume local JSON files only.  A candidate freeze is created from
observations available no later than ``cutoff_at``.  Evaluation later consumes
an independent outcome snapshot and verifies the freeze hash before doing any
calculation.  It reports hypothetical marked performance, never validated
accuracy or executable trading results.

Selection input schema (JSON object):

    {
      "snapshot": {"source": "...", "data_date": "YYYY-MM-DD",
                   "available_at": "ISO timestamp with timezone",
                   "captured_at": "ISO timestamp with timezone",
                   "parser_version": "...", "raw_sha256": "canonical payload SHA-256",
                   "raw_response_sha256": "optional raw response SHA-256",
                   "transformation_id": "required for post-close reconstruction",
                   "transformation_details": {"...": "required for post-close reconstruction"}},
      "observations": [{"code": "600000", "name": "...",
        "data_date": "YYYY-MM-DD", "observed_at": "ISO timestamp",
        "risk_evidence": {"suspended": false, "limit_up": false,
          "limit_down": false, "st": false, "name_risk_status": "clear", "method": "...",
          "source": "...", "as_of": "ISO timestamp", "quality": "verified"},
        "bars": [{"trade_date": "YYYY-MM-DD", "open": 1, "high": 1,
                  "low": 1, "close": 1, "volume": 1, "amount": 1,
                  "price_basis": "unadjusted", "source": "...",
                  "available_at": "ISO timestamp"}]}]
    }

The universe is intentionally a separate, explicit local file.  It must have
``source``, ``date``, ``available_at``, ``complete: true``, and a unique
``codes`` list.  This avoids quietly using a current turnover/ranking list as
a historical broad-market universe.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import date, datetime, timezone
import hashlib
import json
import math
from pathlib import Path
import random
import sys
import time
from typing import Any, Iterable
import unicodedata
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
from urllib.request import Request, urlopen


REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from core import Quote, calculate_daily_indicators, candidate_rank_key, score_quote


TOOL_VERSION = "same-day-research-v1"
SCHEMA_VERSION = 1
_SECRET_QUERY_FRAGMENTS = ("api_key", "apikey", "token", "secret", "password", "authorization", "credential", "auth")


class ResearchInputError(ValueError):
    """Raised when a local research artifact cannot be proven internally consistent."""


def _canonical_json(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as exc:
        raise ResearchInputError("artifact must be canonical JSON without non-finite values") from exc


def _sha256(value: Any) -> str:
    return hashlib.sha256(_canonical_json(value).encode("utf-8")).hexdigest()


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="microseconds")


def _timestamp(value: Any, field: str) -> datetime:
    if not isinstance(value, str) or not value.strip():
        raise ResearchInputError(f"{field} must be an ISO timestamp with timezone")
    try:
        parsed = datetime.fromisoformat(value.strip().replace("Z", "+00:00"))
    except ValueError as exc:
        raise ResearchInputError(f"{field} must be an ISO timestamp with timezone") from exc
    if parsed.tzinfo is None:
        raise ResearchInputError(f"{field} must include a timezone")
    return parsed.astimezone(timezone.utc)


def _iso_date(value: Any, field: str) -> str:
    if not isinstance(value, str):
        raise ResearchInputError(f"{field} must be canonical YYYY-MM-DD")
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ResearchInputError(f"{field} must be canonical YYYY-MM-DD") from exc
    if parsed.isoformat() != value:
        raise ResearchInputError(f"{field} must be canonical YYYY-MM-DD")
    return value


def _finite(value: Any, field: str, *, positive: bool = False, nonnegative: bool = False) -> float:
    if isinstance(value, bool):
        raise ResearchInputError(f"{field} must be finite")
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise ResearchInputError(f"{field} must be finite") from exc
    if not math.isfinite(number) or (positive and number <= 0) or (nonnegative and number < 0):
        raise ResearchInputError(f"{field} must be finite" + (" and positive" if positive else ""))
    return number


def _code(value: Any, field: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ResearchInputError(f"{field} is required")
    return text


def _sha_text(value: Any, field: str) -> str:
    text = str(value or "").strip().lower()
    if len(text) != 64 or any(char not in "0123456789abcdef" for char in text):
        raise ResearchInputError(f"{field} must be a SHA-256 hex digest")
    return text


def _object_file(path: str | Path, label: str) -> dict[str, Any]:
    source = Path(path)
    try:
        payload = json.loads(source.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ResearchInputError(f"{label} must be a readable JSON object: {source}") from exc
    if not isinstance(payload, dict):
        raise ResearchInputError(f"{label} must be a JSON object")
    return payload


def _write_json(path: str | Path, value: dict[str, Any]) -> None:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _sealed(value: dict[str, Any]) -> dict[str, Any]:
    result = deepcopy(value)
    result.pop("artifact_sha256", None)
    result["artifact_sha256"] = _sha256(result)
    return result


def verify_artifact(value: dict[str, Any], expected_kind: str) -> dict[str, Any]:
    if value.get("artifact_kind") != expected_kind:
        raise ResearchInputError(f"expected {expected_kind} artifact")
    given = _sha_text(value.get("artifact_sha256"), "artifact_sha256")
    actual_value = deepcopy(value)
    actual_value.pop("artifact_sha256", None)
    if _sha256(actual_value) != given:
        raise ResearchInputError("artifact hash verification failed")
    return value


def _snapshot_metadata(value: Any, label: str, *, cutoff: datetime, exact_date: str | None = None) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ResearchInputError(f"{label}.snapshot is required")
    source = value.get("source")
    parser_version = value.get("parser_version")
    if not isinstance(source, str) or not source.strip() or not isinstance(parser_version, str) or not parser_version.strip():
        raise ResearchInputError(f"{label}.snapshot needs source and parser_version")
    data_date = _iso_date(value.get("data_date"), f"{label}.snapshot.data_date")
    if exact_date is not None and data_date != exact_date:
        raise ResearchInputError(f"{label}.snapshot.data_date does not match target_date")
    available_at = _timestamp(value.get("available_at"), f"{label}.snapshot.available_at")
    captured_at = _timestamp(value.get("captured_at"), f"{label}.snapshot.captured_at")
    if available_at > cutoff:
        raise ResearchInputError(f"{label}.snapshot was not available by the declared cutoff")
    transformation_id = value.get("transformation_id")
    transformation_details = value.get("transformation_details")
    if transformation_id in (None, "") and transformation_details in (None, ""):
        transformation_id, transformation_details = None, None
    else:
        if not isinstance(transformation_id, str) or not transformation_id.strip():
            raise ResearchInputError(f"{label}.snapshot.transformation_id is required with transformation_details")
        if not isinstance(transformation_details, dict) or not transformation_details:
            raise ResearchInputError(f"{label}.snapshot.transformation_details must be a non-empty object")
        _canonical_json(transformation_details)
        transformation_id = transformation_id.strip()
    captured_after_cutoff = captured_at > cutoff
    if captured_after_cutoff and (transformation_id is None or transformation_details is None):
        raise ResearchInputError(f"{label}.snapshot post-close reconstruction requires transformation_id and transformation_details")
    raw_response_sha256 = value.get("raw_response_sha256")
    return {
        "source": source.strip(),
        "data_date": data_date,
        "available_at": value["available_at"],
        "captured_at": value["captured_at"],
        "parser_version": parser_version.strip(),
        "raw_sha256": _sha_text(value.get("raw_sha256"), f"{label}.snapshot.raw_sha256"),
        "raw_sha256_semantics": "canonical_payload_sha256",
        "raw_response_sha256": (_sha_text(raw_response_sha256, f"{label}.snapshot.raw_response_sha256")
                                if raw_response_sha256 not in (None, "") else None),
        "transformation_id": transformation_id,
        "transformation_details": deepcopy(transformation_details) if transformation_details is not None else None,
        "captured_after_cutoff": captured_after_cutoff,
    }


def _universe(value: dict[str, Any], *, cutoff: datetime) -> dict[str, Any]:
    if value.get("complete") is not True:
        raise ResearchInputError("universe.complete must be true; an incomplete broad universe cannot be inferred")
    source = value.get("source")
    if not isinstance(source, str) or not source.strip():
        raise ResearchInputError("universe.source is required")
    universe_date = _iso_date(value.get("date"), "universe.date")
    if date.fromisoformat(universe_date) > cutoff.date():
        raise ResearchInputError("universe.date is after cutoff")
    if _timestamp(value.get("available_at"), "universe.available_at") > cutoff:
        raise ResearchInputError("universe was not available by the declared cutoff")
    raw_codes = value.get("codes")
    if not isinstance(raw_codes, list) or not raw_codes:
        raise ResearchInputError("universe.codes must be a non-empty explicit list")
    codes = [_code(code, "universe.code") for code in raw_codes]
    if len(set(codes)) != len(codes):
        raise ResearchInputError("universe contains duplicate codes")
    return {
        "source": source.strip(),
        "date": universe_date,
        "available_at": value["available_at"],
        "complete": True,
        "codes": codes,
        "sha256": _sha256(value),
    }


def _validate_bars(rows: Any, code: str, *, cutoff: datetime, expected_date: str) -> list[dict[str, Any]]:
    if not isinstance(rows, list) or len(rows) < 2:
        raise ResearchInputError(f"observation {code} requires at least two daily bars")
    bars: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, row in enumerate(rows, 1):
        if not isinstance(row, dict):
            raise ResearchInputError(f"bar {code}[{index}] must be an object")
        trade_date = _iso_date(row.get("trade_date"), f"bar {code}[{index}].trade_date")
        if trade_date in seen:
            raise ResearchInputError(f"duplicate bar for {code} {trade_date}")
        seen.add(trade_date)
        if date.fromisoformat(trade_date) > cutoff.date():
            raise ResearchInputError(f"future bar for {code} {trade_date}")
        if _timestamp(row.get("available_at"), f"bar {code}[{index}].available_at") > cutoff:
            raise ResearchInputError(f"future availability for {code} {trade_date}")
        source = row.get("source")
        if not isinstance(source, str) or not source.strip():
            raise ResearchInputError(f"bar {code}[{index}].source is required")
        if str(row.get("price_basis") or "").strip().casefold() != "unadjusted":
            raise ResearchInputError(f"bar {code}[{index}] must use unadjusted price basis")
        open_price = _finite(row.get("open"), f"bar {code}[{index}].open", positive=True)
        high = _finite(row.get("high"), f"bar {code}[{index}].high", positive=True)
        low = _finite(row.get("low"), f"bar {code}[{index}].low", positive=True)
        close = _finite(row.get("close"), f"bar {code}[{index}].close", positive=True)
        volume = _finite(row.get("volume", 0), f"bar {code}[{index}].volume", nonnegative=True)
        amount = _finite(row.get("amount", 0), f"bar {code}[{index}].amount", nonnegative=True)
        if high < low or high < max(open_price, close) or low > min(open_price, close):
            raise ResearchInputError(f"bar {code}[{index}] has inconsistent OHLC")
        bars.append({
            "trade_date": trade_date,
            "open": open_price,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
            "amount": amount,
            "price_basis": "unadjusted",
            "source": source.strip(),
            "available_at": row["available_at"],
        })
    bars.sort(key=lambda item: item["trade_date"])
    if bars[-1]["trade_date"] != expected_date:
        raise ResearchInputError(f"latest bar for {code} must match snapshot data_date")
    return bars


def _risk_evidence(row: dict[str, Any], code: str, *, cutoff: datetime, post_close_reconstruction: bool) -> dict[str, Any]:
    evidence = row.get("risk_evidence")
    if not isinstance(evidence, dict):
        raise ResearchInputError(f"observation {code}.risk_evidence must be an object")
    states: dict[str, bool] = {}
    for field in ("suspended", "limit_up", "limit_down", "st"):
        value = evidence.get(field)
        if not isinstance(value, bool):
            raise ResearchInputError(f"observation {code}.risk_evidence.{field} must be a boolean")
        states[field] = value
    name_risk_status = evidence.get("name_risk_status")
    if name_risk_status not in {"clear", "flagged"}:
        raise ResearchInputError(f"observation {code}.risk_evidence.name_risk_status must be clear or flagged")
    for field in ("method", "source"):
        if not isinstance(evidence.get(field), str) or not evidence[field].strip():
            raise ResearchInputError(f"observation {code}.risk_evidence.{field} is required")
    if _timestamp(evidence.get("as_of"), f"observation {code}.risk_evidence.as_of") > cutoff:
        raise ResearchInputError(f"observation {code}.risk_evidence.as_of is after cutoff")
    quality = evidence.get("quality")
    if quality not in {"verified", "reconstructed_assumption"}:
        raise ResearchInputError(f"observation {code}.risk_evidence.quality must be verified or reconstructed_assumption")
    if quality == "reconstructed_assumption" and not post_close_reconstruction:
        raise ResearchInputError(f"observation {code}.risk_evidence reconstructed_assumption requires post-close reconstruction")
    return {
        "suspended": states["suspended"],
        "limit_up": states["limit_up"],
        "limit_down": states["limit_down"],
        "st": states["st"],
        "name_risk_status": name_risk_status,
        "method": evidence["method"].strip(),
        "source": evidence["source"].strip(),
        "as_of": evidence["as_of"],
        "quality": quality,
    }


def _observation_map(selection: dict[str, Any], *, snapshot: dict[str, Any], cutoff: datetime, universe_codes: set[str]) -> dict[str, dict[str, Any]]:
    rows = selection.get("observations")
    if not isinstance(rows, list):
        raise ResearchInputError("selection.observations must be a list")
    result: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows, 1):
        if not isinstance(row, dict):
            raise ResearchInputError(f"observation[{index}] must be an object")
        code = _code(row.get("code"), f"observation[{index}].code")
        if code in result:
            raise ResearchInputError(f"duplicate observation for {code}")
        if code not in universe_codes:
            raise ResearchInputError(f"observation {code} is outside the explicit universe")
        if _iso_date(row.get("data_date"), f"observation[{index}].data_date") != snapshot["data_date"]:
            raise ResearchInputError(f"observation {code} has wrong data_date")
        if _timestamp(row.get("observed_at"), f"observation[{index}].observed_at") > cutoff:
            raise ResearchInputError(f"future observation for {code}")
        name = row.get("name", "")
        if not isinstance(name, str):
            raise ResearchInputError(f"observation {code}.name must be text")
        risk_evidence = _risk_evidence(
            row, code, cutoff=cutoff, post_close_reconstruction=snapshot["captured_after_cutoff"],
        )
        name_marker = _name_risk_exclusion(name)
        if name_marker and risk_evidence["name_risk_status"] != "flagged":
            raise ResearchInputError(f"observation {code}.risk_evidence.name_risk_status conflicts with name marker")
        result[code] = {
            "code": code,
            "name": name.strip(),
            "observed_at": row["observed_at"],
            "risk_evidence": risk_evidence,
            "bars": _validate_bars(row.get("bars"), code, cutoff=cutoff, expected_date=snapshot["data_date"]),
        }
    return result


def _candidate_from_observation(row: dict[str, Any]) -> tuple[Any, dict[str, Any]]:
    bars = row["bars"]
    values = calculate_daily_indicators(bars)
    latest, previous = bars[-1], bars[-2]
    quote = Quote(
        row["code"], row["name"], latest["close"], prev_close=previous["close"], amount=latest["amount"],
        pct_change=(latest["close"] / previous["close"] - 1.0) * 100.0,
        volume=latest["volume"], source=latest["source"],
        suspended=row["risk_evidence"]["suspended"], limit_up=row["risk_evidence"]["limit_up"],
        limit_down=row["risk_evidence"]["limit_down"], st=row["risk_evidence"]["st"],
    )
    for field in (
        "rsi6", "ma5", "ma10", "ma20", "volume_ratio", "atr14", "support20", "resistance20",
        "volatility20", "momentum5", "momentum20", "history_days", "indicator_last_date",
        "indicator_last_close", "indicator_price_basis", "indicator_source",
    ):
        setattr(quote, field, values[field])
    candidate = score_quote(quote)
    return candidate, values


def _name_risk_exclusion(name: str) -> str | None:
    """Reject explicit special-treatment and delisting markers before ranking.

    This research-only guard intentionally uses names as a hard risk signal.
    It does not modify the production scorer and does not guess from a generic
    Chinese character such as "退"; only explicit status markers qualify.
    """
    normalized = unicodedata.normalize("NFKC", str(name or "")).upper()
    compact = "".join(normalized.split())
    if compact.startswith("*ST") or compact.startswith("ST"):
        return "name_risk_st"
    if any(marker in normalized for marker in ("退市", "终止上市", "退市整理", "退市风险警示", "风险警示")):
        return "name_risk_delisting"
    return None


def freeze_candidates(
    selection: dict[str, Any],
    universe_file: dict[str, Any],
    *,
    cutoff_at: str,
    target_date: str,
    frozen_at: str,
    top_n: int = 10,
    random_seed: int = 20260922,
    benchmark_code: str = "000300.SH",
) -> dict[str, Any]:
    """Freeze the current rule prototype from a dated local snapshot.

    The freeze is evidence that this function used a supplied cutoff.  It is
    not evidence that the snapshot was actually captured before the cutoff,
    unless its independent ``available_at`` provenance is authentic.
    """
    cutoff = _timestamp(cutoff_at, "cutoff_at")
    target = _iso_date(target_date, "target_date")
    if date.fromisoformat(target) <= cutoff.date():
        raise ResearchInputError("target_date must be after the selection cutoff date")
    frozen_time = _timestamp(frozen_at, "frozen_at")
    if frozen_time < cutoff:
        raise ResearchInputError("frozen_at cannot precede cutoff_at")
    if isinstance(top_n, bool) or not isinstance(top_n, int) or top_n < 1:
        raise ResearchInputError("top_n must be a positive integer")
    if isinstance(random_seed, bool) or not isinstance(random_seed, int):
        raise ResearchInputError("random_seed must be an integer")
    benchmark_code = _code(benchmark_code, "benchmark_code")
    snapshot = _snapshot_metadata(selection.get("snapshot"), "selection", cutoff=cutoff)
    if _timestamp(snapshot["captured_at"], "selection.snapshot.captured_at") > frozen_time:
        raise ResearchInputError("selection.snapshot.captured_at cannot be after frozen_at")
    if date.fromisoformat(snapshot["data_date"]) > cutoff.date():
        raise ResearchInputError("selection snapshot data_date is after cutoff")
    universe = _universe(universe_file, cutoff=cutoff)
    observations = _observation_map(selection, snapshot=snapshot, cutoff=cutoff, universe_codes=set(universe["codes"]))
    ranked: list[tuple[Any, dict[str, Any], dict[str, Any]]] = []
    exclusions: list[dict[str, Any]] = []
    momentum_pool: list[tuple[str, float]] = []
    assumed_risk_count = 0
    for code in universe["codes"]:
        observation = observations.get(code)
        if observation is None:
            exclusions.append({"code": code, "reason": "missing_observation"})
            continue
        name_risk = _name_risk_exclusion(observation["name"])
        if name_risk:
            exclusions.append({"code": code, "name": observation["name"], "reason": name_risk})
            continue
        if observation["risk_evidence"]["name_risk_status"] == "flagged":
            exclusions.append({"code": code, "name": observation["name"], "reason": "risk_evidence_name_risk_flagged"})
            continue
        candidate, values = _candidate_from_observation(observation)
        if candidate.risk_level != "eligible":
            exclusions.append({
                "code": code,
                "reason": "risk_" + candidate.risk_level,
                "risk_flags": list(candidate.risk_flags),
            })
            continue
        risk_level = "eligible"
        if observation["risk_evidence"]["quality"] == "reconstructed_assumption":
            risk_level = "research_assumed"
            assumed_risk_count += 1
            candidate.risk_level = risk_level
            candidate.risk_flags = [*candidate.risk_flags, "risk_evidence_reconstructed_assumption"]
        details = {
            "code": code,
            "name": observation["name"],
            "score": int(candidate.score),
            "base_score": int(candidate.base_score),
            "reasons": list(candidate.reasons),
            "risk_level": risk_level,
            "risk_flags": list(candidate.risk_flags),
            "risk_evidence": deepcopy(observation["risk_evidence"]),
            "observed_at": observation["observed_at"],
            "indicator_last_date": str(values["indicator_last_date"]),
            "indicator_source": str(values["indicator_source"]),
            "momentum5_pct": values["momentum5"],
        }
        ranked.append((candidate, details, observation))
        if values["momentum5"] is not None:
            momentum_pool.append((code, float(values["momentum5"])))
    ranked.sort(key=lambda item: (*candidate_rank_key(item[0]), item[1]["code"]))
    selected = []
    for rank, (_, details, _) in enumerate(ranked[:top_n], 1):
        selected.append({**details, "rank": rank})
    risk_pool_codes = [details["code"] for _, details, _ in ranked]
    risk_pool_label = ("verified_eligible_plus_research_assumed_pool" if assumed_risk_count
                       else "verified_eligible_pool")
    sample_size = len(selected)
    random_codes = random.Random(random_seed).sample(sorted(risk_pool_codes), min(sample_size, len(risk_pool_codes)))
    momentum_codes = [code for code, _ in sorted(momentum_pool, key=lambda item: (-item[1], item[0]))[:sample_size]]
    mode = "post_close_reconstruction" if snapshot["captured_after_cutoff"] else "time_attested_research"
    result = {
        "schema_version": SCHEMA_VERSION,
        "tool_version": TOOL_VERSION,
        "artifact_kind": "same_day_candidate_freeze",
        "cutoff_at": cutoff_at,
        "target_date": target,
        "frozen_at": frozen_at,
        "benchmark_code": benchmark_code,
        "research_mode": mode,
        "historical_prediction_evidence": False,
        "strategy_quality_established": False,
        "selection_snapshot": snapshot,
        "selection_input_sha256": _sha256(selection),
        "universe": universe,
        "selector": {
            "name": "current_rule_prototype",
            "implementation": "core.calculate_daily_indicators + core.score_quote + core.candidate_rank_key",
            "top_n": top_n,
            "risk_pool_label": risk_pool_label,
            "risk_pool_size": len(risk_pool_codes),
            "verified_eligible_count": len(risk_pool_codes) - assumed_risk_count,
            "research_assumed_count": assumed_risk_count,
        },
        "candidates": selected,
        "exclusions": exclusions,
        "baselines": {
            "equal_weight_universe": {
                "codes": list(universe["codes"]),
                "population": "explicit_universe",
                "definition": "all explicit universe codes, including missing outcome coverage",
            },
            "fixed_seed_random": {
                "codes": random_codes,
                "seed": random_seed,
                "population": risk_pool_label,
                "definition": "sampled from the same frozen selector risk pool",
            },
            "simple_momentum": {
                "codes": momentum_codes,
                "population": risk_pool_label,
                "definition": "highest frozen five-session momentum in the same frozen selector risk pool",
            },
        },
        "limitations": [
            "explicit_universe_file_required_no_current_turnover_or_ranking_fallback",
            "post_close_reconstruction_is_not_historical_prediction" if mode == "post_close_reconstruction" else "snapshot_provenance_requires_external_review",
            "rule_selector_prototype_unvalidated",
        ],
    }
    return _sealed(result)


def _fill_model(entry_field: str, mark_field: str, entry_slippage_bps: Any, exit_slippage_bps: Any, fee_bps_per_side: Any) -> dict[str, Any]:
    if entry_field not in {"open", "close"} or mark_field not in {"open", "close"}:
        raise ResearchInputError("entry_field and mark_field must be open or close")
    values = {
        "entry_slippage_bps": _finite(entry_slippage_bps, "entry_slippage_bps", nonnegative=True),
        "exit_slippage_bps": _finite(exit_slippage_bps, "exit_slippage_bps", nonnegative=True),
        "fee_bps_per_side": _finite(fee_bps_per_side, "fee_bps_per_side", nonnegative=True),
    }
    if any(value >= 10000 for value in values.values()):
        raise ResearchInputError("slippage and fee parameters must be below 10000 bps")
    return {"entry_field": entry_field, "mark_field": mark_field, **values}


def _outcome_rows(value: dict[str, Any], *, target_date: str, cutoff: datetime) -> dict[str, dict[str, Any]]:
    rows = value.get("outcomes")
    if not isinstance(rows, list):
        raise ResearchInputError("outcomes must be a list")
    result: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows, 1):
        if not isinstance(row, dict):
            raise ResearchInputError(f"outcome[{index}] must be an object")
        code = _code(row.get("code"), f"outcome[{index}].code")
        if code in result:
            raise ResearchInputError(f"duplicate outcome for {code}")
        if _iso_date(row.get("date"), f"outcome[{index}].date") != target_date:
            raise ResearchInputError(f"outcome {code} has wrong date")
        if _timestamp(row.get("available_at"), f"outcome[{index}].available_at") > cutoff:
            raise ResearchInputError(f"outcome {code} was not available by outcome cutoff")
        result[code] = {
            "code": code,
            "date": target_date,
            "available_at": row["available_at"],
            "open": _finite(row.get("open"), f"outcome[{index}].open", positive=True),
            "close": _finite(row.get("close"), f"outcome[{index}].close", positive=True),
        }
    return result


def _marked_record(code: str, row: dict[str, Any] | None, fill: dict[str, Any]) -> dict[str, Any]:
    if row is None:
        return {"code": code, "status": "unknown", "reason": "outcome_missing", "gross_marked_return_pct": None, "net_marked_return_pct": None}
    entry = row[fill["entry_field"]]
    mark = row[fill["mark_field"]]
    buy_fill = entry * (1.0 + fill["entry_slippage_bps"] / 10000.0)
    mark_fill = mark * (1.0 - fill["exit_slippage_bps"] / 10000.0)
    all_in_cost = buy_fill * (1.0 + fill["fee_bps_per_side"] / 10000.0)
    marked_proceeds = mark_fill * (1.0 - fill["fee_bps_per_side"] / 10000.0)
    gross = (mark / entry - 1.0) * 100.0
    net = (marked_proceeds / all_in_cost - 1.0) * 100.0
    return {
        "code": code,
        "status": "complete",
        "reason": "",
        "entry_reference_price": entry,
        "mark_reference_price": mark,
        "hypothetical_buy_fill": buy_fill,
        "hypothetical_mark_proceeds": marked_proceeds,
        "gross_marked_return_pct": gross,
        "net_marked_return_pct": net,
    }


def _metrics(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    rows = list(records)
    complete = [row for row in rows if row["status"] == "complete"]
    gross = [row["gross_marked_return_pct"] for row in complete]
    net = [row["net_marked_return_pct"] for row in complete]
    total, valid = len(rows), len(complete)
    return {
        "sample_count": total,
        "valid_price_count": valid,
        "missing_price_count": total - valid,
        "coverage": valid / total if total else None,
        "up_count": sum(value > 0 for value in gross),
        "flat_count": sum(value == 0 for value in gross),
        "down_count": sum(value < 0 for value in gross),
        "same_day_up_share": sum(value > 0 for value in gross) / valid if valid else None,
        "mean_gross_marked_return_pct": sum(gross) / valid if valid else None,
        "median_gross_marked_return_pct": _median(gross),
        "mean_net_marked_return_pct": sum(net) / valid if valid else None,
        "median_net_marked_return_pct": _median(net),
    }


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    middle = len(ordered) // 2
    return ordered[middle] if len(ordered) % 2 else (ordered[middle - 1] + ordered[middle]) / 2.0


def _baseline(codes: list[str], outcomes: dict[str, dict[str, Any]], fill: dict[str, Any]) -> dict[str, Any]:
    records = [_marked_record(code, outcomes.get(code), fill) for code in codes]
    return {"codes": codes, "performance": _metrics(records), "records": records}


def evaluate_same_day(
    freeze: dict[str, Any],
    outcome_input: dict[str, Any],
    *,
    target_date: str,
    outcome_cutoff_at: str,
    evaluated_at: str,
    entry_field: str = "open",
    mark_field: str = "close",
    entry_slippage_bps: float = 0.0,
    exit_slippage_bps: float = 0.0,
    fee_bps_per_side: float = 0.0,
) -> dict[str, Any]:
    """Mark a frozen list using independent outcome observations without reranking."""
    verify_artifact(freeze, "same_day_candidate_freeze")
    target = _iso_date(target_date, "target_date")
    if freeze.get("target_date") != target:
        raise ResearchInputError("target_date does not match the frozen candidate artifact")
    frozen_time = _timestamp(freeze.get("frozen_at"), "freeze.frozen_at")
    frozen_benchmark_code = _code(freeze.get("benchmark_code"), "freeze.benchmark_code")
    outcome_cutoff = _timestamp(outcome_cutoff_at, "outcome_cutoff_at")
    evaluated_time = _timestamp(evaluated_at, "evaluated_at")
    if evaluated_time < outcome_cutoff:
        raise ResearchInputError("evaluated_at cannot precede outcome_cutoff_at")
    if evaluated_time < frozen_time:
        raise ResearchInputError("evaluated_at cannot precede freeze.frozen_at")
    snapshot = _snapshot_metadata(outcome_input.get("snapshot"), "outcomes", cutoff=outcome_cutoff, exact_date=target)
    if _timestamp(snapshot["captured_at"], "outcomes.snapshot.captured_at") > evaluated_time:
        raise ResearchInputError("outcomes.snapshot.captured_at cannot be after evaluated_at")
    outcomes = _outcome_rows(outcome_input, target_date=target, cutoff=outcome_cutoff)
    fill = _fill_model(entry_field, mark_field, entry_slippage_bps, exit_slippage_bps, fee_bps_per_side)
    frozen_candidates = freeze.get("candidates")
    if not isinstance(frozen_candidates, list):
        raise ResearchInputError("freeze candidates must be a list")
    candidate_codes = []
    assumed_risk_codes = []
    for index, candidate in enumerate(frozen_candidates, 1):
        if not isinstance(candidate, dict):
            raise ResearchInputError("freeze candidate must be an object")
        code = _code(candidate.get("code"), f"freeze.candidates[{index}].code")
        if code in candidate_codes:
            raise ResearchInputError("freeze contains duplicate candidate codes")
        if candidate.get("risk_level") == "research_assumed":
            if freeze.get("research_mode") != "post_close_reconstruction":
                raise ResearchInputError("research_assumed candidate requires post-close reconstruction artifact")
            assumed_risk_codes.append(code)
        elif candidate.get("risk_level") != "eligible":
            raise ResearchInputError("freeze contains a non-eligible candidate")
        candidate_codes.append(code)
    records = []
    for candidate in frozen_candidates:
        marked = _marked_record(candidate["code"], outcomes.get(candidate["code"]), fill)
        records.append({
            "rank": candidate["rank"], "score": candidate["score"], "code": candidate["code"], "name": candidate.get("name", ""),
            "risk_level": candidate["risk_level"], **{key: value for key, value in marked.items() if key != "code"},
        })
    selector_metrics = _metrics(records)
    benchmark = outcome_input.get("benchmark")
    benchmark_result: dict[str, Any]
    if benchmark is None:
        benchmark_result = {
            "status": "unknown", "reason": "benchmark_missing", "code": frozen_benchmark_code,
            "gross_marked_return_pct": None,
            "selector_mean_gross_marked_return_minus_gross_benchmark_return_pct": None,
            "selector_mean_net_marked_return_minus_gross_benchmark_return_pct": None,
        }
    elif not isinstance(benchmark, dict):
        raise ResearchInputError("benchmark must be an object")
    else:
        parsed = _outcome_rows({"outcomes": [benchmark]}, target_date=target, cutoff=outcome_cutoff)
        benchmark_code = next(iter(parsed))
        if benchmark_code != frozen_benchmark_code:
            raise ResearchInputError("outcome benchmark code does not match frozen benchmark_code")
        marked = _marked_record(benchmark_code, parsed[benchmark_code], fill)
        gross = marked["gross_marked_return_pct"]
        benchmark_result = {
            "status": "complete", "code": benchmark_code, "gross_marked_return_pct": gross,
            "selector_mean_gross_marked_return_minus_gross_benchmark_return_pct": (
                selector_metrics["mean_gross_marked_return_pct"] - gross
                if selector_metrics["mean_gross_marked_return_pct"] is not None else None
            ),
            "selector_mean_net_marked_return_minus_gross_benchmark_return_pct": (
                selector_metrics["mean_net_marked_return_pct"] - gross
                if selector_metrics["mean_net_marked_return_pct"] is not None else None
            ),
        }
    baselines = freeze.get("baselines")
    if not isinstance(baselines, dict):
        raise ResearchInputError("freeze baselines must be an object")
    baseline_results = {}
    for name in ("equal_weight_universe", "fixed_seed_random", "simple_momentum"):
        config = baselines.get(name)
        if not isinstance(config, dict) or not isinstance(config.get("codes"), list):
            raise ResearchInputError(f"freeze baseline {name} is invalid")
        codes = [_code(code, f"baseline {name}.code") for code in config["codes"]]
        if len(set(codes)) != len(codes):
            raise ResearchInputError(f"freeze baseline {name} has duplicate codes")
        result = _baseline(codes, outcomes, fill)
        result["population"] = config.get("population", "unknown")
        result["assumed_risk_count"] = sum(code in assumed_risk_codes for code in codes)
        result["definition"] = config.get("definition", "")
        if name == "fixed_seed_random":
            result["seed"] = config.get("seed")
        baseline_results[name] = result
    result = {
        "schema_version": SCHEMA_VERSION,
        "tool_version": TOOL_VERSION,
        "artifact_kind": "same_day_marked_evaluation",
        "freeze_sha256": freeze["artifact_sha256"],
        "outcome_input_sha256": _sha256(outcome_input),
        "target_date": target,
        "outcome_cutoff_at": outcome_cutoff_at,
        "evaluated_at": evaluated_at,
        "outcome_snapshot": snapshot,
        "fill_model": fill,
        "valuation": "hypothetical_same_day_mark_to_market_not_realized_trade_return",
        "strategy_quality_established": False,
        "accuracy_claim": "not_validated; report same_day_up_share and marked_performance only",
        "selector": {
            "risk_pool_label": freeze.get("selector", {}).get("risk_pool_label", "unknown"),
            "assumed_risk_count": len(assumed_risk_codes),
            "performance": selector_metrics,
            "records": records,
        },
        "benchmark": benchmark_result,
        "baselines": baseline_results,
        "limitations": [
            "evaluation_uses_frozen_candidates_without_reranking",
            "hypothetical_fills_do_not_prove_order_execution_or_liquidity",
            "benchmark_comparison_is_selector_net_marked_return_minus_gross_benchmark_return",
            "single_session_marked_performance_does_not_establish_strategy_quality",
        ],
    }
    return _sealed(result)


def _public_url(url: str, params: dict[str, Any] | None) -> str:
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password:
        raise ResearchInputError("capture URL must be an explicit public http(s) URL without embedded credentials")
    pairs = list(parse_qsl(parsed.query, keep_blank_values=True))
    for key, value in (params or {}).items():
        if isinstance(value, (dict, list)):
            raise ResearchInputError("capture params must be scalar values")
        pairs.append((str(key), str(value)))
    if any(any(fragment in key.strip().casefold() for fragment in _SECRET_QUERY_FRAGMENTS) for key, _ in pairs):
        raise ResearchInputError("capture URL/params must not contain credential-like query keys")
    return urlunsplit((parsed.scheme, parsed.netloc, parsed.path, urlencode(pairs), ""))


def capture_public_snapshot(
    url: str,
    *,
    source: str,
    data_date: str,
    output_dir: str | Path,
    params: dict[str, Any] | None = None,
    timeout_seconds: float = 10.0,
    retries: int = 1,
    captured_at: str | None = None,
) -> dict[str, Any]:
    """Capture a bounded public HTTP response without importing plugin settings or credentials."""
    if not isinstance(source, str) or not source.strip():
        raise ResearchInputError("capture source is required")
    _iso_date(data_date, "data_date")
    timeout = _finite(timeout_seconds, "timeout_seconds", positive=True)
    if timeout > 60:
        raise ResearchInputError("timeout_seconds must be at most 60")
    if isinstance(retries, bool) or not isinstance(retries, int) or not 0 <= retries <= 2:
        raise ResearchInputError("retries must be an integer from 0 to 2")
    url = _public_url(url, params)
    captured_at = captured_at or _utc_now()
    _timestamp(captured_at, "captured_at")
    error: Exception | None = None
    raw: bytes | None = None
    for attempt in range(retries + 1):
        try:
            request = Request(url, headers={"User-Agent": TOOL_VERSION, "Accept": "application/json,text/plain,*/*"})
            with urlopen(request, timeout=timeout) as response:
                raw = response.read()
            break
        except (HTTPError, URLError, TimeoutError, OSError) as exc:
            error = exc
            if attempt < retries:
                time.sleep(0.25 * (attempt + 1))
    if raw is None:
        raise ResearchInputError(f"public capture failed after {retries + 1} attempt(s): {error}")
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    digest = hashlib.sha256(raw).hexdigest()
    stem = f"public_snapshot_{data_date}_{digest[:12]}"
    raw_path = output / f"{stem}.raw"
    metadata_path = output / f"{stem}.json"
    raw_path.write_bytes(raw)
    metadata = {
        "schema_version": SCHEMA_VERSION,
        "tool_version": TOOL_VERSION,
        "artifact_kind": "public_raw_snapshot",
        "source": source.strip(),
        "url": url,
        "data_date": data_date,
        "captured_at": captured_at,
        "available_at": None,
        "parser_version": "not_parsed",
        "raw_file": raw_path.name,
        "raw_response_sha256": digest,
        "raw_response_sha256_semantics": "exact_http_response_bytes_sha256",
        "response_bytes": len(raw),
        "attempts": retries + 1,
    }
    _write_json(metadata_path, _sealed(metadata))
    return {**metadata, "metadata_file": str(metadata_path), "raw_path": str(raw_path)}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    commands = parser.add_subparsers(dest="command", required=True)
    capture = commands.add_parser("capture", help="Capture an explicit public URL to raw local evidence")
    capture.add_argument("--url", required=True)
    capture.add_argument("--source", required=True)
    capture.add_argument("--data-date", required=True)
    capture.add_argument("--output-dir", required=True)
    capture.add_argument("--params-json", default="{}")
    capture.add_argument("--timeout-seconds", type=float, default=10.0)
    capture.add_argument("--retries", type=int, default=1)
    capture.add_argument("--captured-at")
    freeze = commands.add_parser("freeze", help="Freeze candidates from local cutoff-valid inputs")
    freeze.add_argument("--input", required=True, help="Selection JSON")
    freeze.add_argument("--universe", required=True, help="Explicit point-in-time universe JSON")
    freeze.add_argument("--cutoff-at", required=True)
    freeze.add_argument("--target-date", required=True)
    freeze.add_argument("--frozen-at", required=True)
    freeze.add_argument("--top-n", type=int, default=10)
    freeze.add_argument("--random-seed", type=int, default=20260922)
    freeze.add_argument("--benchmark-code", default="000300.SH")
    freeze.add_argument("--output", required=True)
    evaluate = commands.add_parser("evaluate", help="Mark a frozen candidate list from a separate local outcome file")
    evaluate.add_argument("--freeze", required=True)
    evaluate.add_argument("--outcomes", required=True)
    evaluate.add_argument("--target-date", required=True)
    evaluate.add_argument("--outcome-cutoff-at", required=True)
    evaluate.add_argument("--evaluated-at", required=True)
    evaluate.add_argument("--entry-field", choices=("open", "close"), default="open")
    evaluate.add_argument("--mark-field", choices=("open", "close"), default="close")
    evaluate.add_argument("--entry-slippage-bps", type=float, default=0.0)
    evaluate.add_argument("--exit-slippage-bps", type=float, default=0.0)
    evaluate.add_argument("--fee-bps-per-side", type=float, default=0.0)
    evaluate.add_argument("--output", required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "capture":
        try:
            params = json.loads(args.params_json)
        except json.JSONDecodeError as exc:
            raise ResearchInputError("params-json must be a JSON object") from exc
        if not isinstance(params, dict):
            raise ResearchInputError("params-json must be a JSON object")
        result = capture_public_snapshot(
            args.url, source=args.source, data_date=args.data_date, output_dir=args.output_dir,
            params=params, timeout_seconds=args.timeout_seconds, retries=args.retries, captured_at=args.captured_at,
        )
        print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
        return 0
    if args.command == "freeze":
        result = freeze_candidates(
            _object_file(args.input, "selection input"), _object_file(args.universe, "universe"),
            cutoff_at=args.cutoff_at, target_date=args.target_date, frozen_at=args.frozen_at,
            top_n=args.top_n, random_seed=args.random_seed, benchmark_code=args.benchmark_code,
        )
        _write_json(args.output, result)
        return 0
    result = evaluate_same_day(
        _object_file(args.freeze, "freeze"), _object_file(args.outcomes, "outcomes"),
        target_date=args.target_date, outcome_cutoff_at=args.outcome_cutoff_at, evaluated_at=args.evaluated_at,
        entry_field=args.entry_field, mark_field=args.mark_field,
        entry_slippage_bps=args.entry_slippage_bps, exit_slippage_bps=args.exit_slippage_bps,
        fee_bps_per_side=args.fee_bps_per_side,
    )
    _write_json(args.output, result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
