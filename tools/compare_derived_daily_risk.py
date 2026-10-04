"""Compare an immutable, dated input bundle without licensing its output."""
from __future__ import annotations

import argparse
from collections import Counter
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import importlib
import json
import math
from pathlib import Path
import sys


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("derived_daily_risk", ROOT / "derived_daily_risk.py")
DERIVER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DERIVER)
ARCHIVE_SPEC = importlib.util.spec_from_file_location("archive_reference", ROOT / "tools/archive_eastmoney_close.py")
ARCHIVE_REFERENCE = importlib.util.module_from_spec(ARCHIVE_SPEC)
ARCHIVE_SPEC.loader.exec_module(ARCHIVE_REFERENCE)


def _deriver(protocol: dict):
    if protocol["source"] == DERIVER.SOURCE and protocol["scenario_version"] == DERIVER.SCENARIO_VERSION:
        return DERIVER
    if protocol["source"] == "derived:baostock-first" and protocol["scenario_version"] == "2026-10-v2":
        sys.path.insert(0, str(ROOT.parent))
        return importlib.import_module("astrbot_stock_watch.baostock_daily_risk")
    if protocol["source"] == "mixed:baostock-eastmoney" and protocol["scenario_version"] == "2026-10-v3":
        sys.path.insert(0, str(ROOT.parent))
        return importlib.import_module("astrbot_stock_watch.split_source_daily_risk")
    if protocol["source"] == "derived:baostock-v1-rules" and protocol["scenario_version"] == "2026-10-v4":
        sys.path.insert(0, str(ROOT.parent))
        return importlib.import_module("astrbot_stock_watch.baostock_rule_daily_risk")
    raise ValueError("protocol_scenario_mismatch")


def _reference_valid(reference: dict, row: dict, batch_id: str, observed_at: str,
                     *, allow_tushare: bool = False, forward: bool = False) -> bool:
    source_fields = {"eastmoney:companion": DERIVER.FIELDS,
                     "akshare:dated-pools": ("limit_up", "limit_down"),
                     "baostock:daily:unadjusted": ("suspended", "st")}
    if allow_tushare:
        source_fields["tushare:dated-risk"] = ("suspended", "st")
    if forward:
        source_fields["tushare:namechange"] = ("st",)
        source_fields["eastmoney:companion"] = DERIVER.FIELDS
    try:
        source = reference.get("source")
        close = float(reference["reference_close"])
        observed = datetime.fromisoformat(reference["first_observed_at"])
        cutoff = datetime.fromisoformat(observed_at)
        close_at = datetime.fromisoformat(row["trade_date"]).replace(
            hour=15, tzinfo=timezone(timedelta(hours=8)))
        if (source not in source_fields or not reference.get("evidence_hash")
                or reference.get("validated") is not True
                or reference.get("trade_date") != row["trade_date"]
                or reference.get("batch_id") != batch_id or reference.get("code") != row["code"]
                or not math.isfinite(close) or close <= 0
                or abs(close - float(row["close"])) > 0.005
                or observed.tzinfo is None or cutoff.tzinfo is None or not close_at <= observed <= cutoff
                or any(field not in source_fields[source] for field in reference.get("fields", {}))):
            return False
        if source == "eastmoney:companion":
            stamp = datetime.fromisoformat(reference["source_timestamp"])
            if (stamp.tzinfo is None or stamp > observed
                    or stamp.astimezone(close_at.tzinfo).date() != close_at.date()):
                return False
            if forward and (stamp < close_at or observed.astimezone(close_at.tzinfo).date() != close_at.date()):
                return False
            if forward:
                previous = float(reference["reference_pre_close"])
                if not math.isfinite(previous) or previous <= 0 or abs(previous - float(row["pre_close"])) > 0.005:
                    return False
                raw = reference["raw_response"]
                if hashlib.sha256(raw.encode()).hexdigest() != reference["evidence_hash"]:
                    return False
                candidate = ARCHIVE_REFERENCE.limit_reference(json.loads(raw), row["code"], row["trade_date"],
                                                             reference["first_observed_at"])
                if candidate["price_valid"] is not True:
                    return False
                if (abs(float(candidate["close"]) - close) > 0.005
                        or abs(float(candidate["pre_close"]) - previous) > 0.005
                        or datetime.fromisoformat(candidate["source_timestamp"]) != stamp):
                    return False
                for field, value in reference.get("fields", {}).items():
                    if type(value) is not bool or candidate[field] is not value:
                        return False
                    if field in ("st", "suspended") and reference.get("status_mapping_confirmed") is not True:
                        return False
        if source == "tushare:namechange":
            if (reference.get("complete") is not True
                    or reference.get("ann_date", "9999-12-31") > row["trade_date"]
                    or reference.get("start_date", "9999-12-31") > row["trade_date"]
                    or (reference.get("end_date") and reference["end_date"] < row["trade_date"])):
                return False
        return True
    except (KeyError, TypeError, ValueError, OverflowError):
        return False


def compare(bundle: dict, protocol: dict) -> dict:
    """Missing inputs are incomplete; unknowns never improve agreement."""
    limits = protocol["thresholds"]
    dates = protocol["dates"]
    fields = protocol["fields"]
    daily = []
    disagreements = []
    blockers = []
    totals = {field: Counter() for field in fields}
    inputs = bundle.get("sessions", {})
    deriver = _deriver(protocol)
    forward = deriver.SOURCE == "derived:baostock-v1-rules"
    for trade_date in dates:
        session = inputs.get(trade_date, {})
        rows = session.get("rows", [])
        archived_total = len(rows)
        denominator = None
        if forward:
            universe = session.get("universe_codes")
            if (session.get("universe_complete") is not True or not isinstance(universe, list)
                    or any(not isinstance(code, str) or len(code) != 6 or not code.isascii() or not code.isdigit()
                           for code in universe)):
                universe = []
                blockers.append(f"{trade_date}:dated_universe_missing")
            if len(universe) != len(set(universe)):
                raise ValueError("duplicate_universe_code")
            denominator = {code for code in universe if deriver.supported_code(code)}
            rows = [row for row in rows if deriver.supported_code(str(row.get("code", "")))]
            if any(row.get("code") not in denominator for row in rows):
                raise ValueError("row_outside_dated_universe")
        evidence = session.get("evidence", {})
        references = session.get("references", {})
        counts = {field: Counter() for field in fields}
        boards = {}
        reasons = Counter()
        complete = 0
        seen = set()
        if not session.get("batch_id"):
            blockers.append(f"{trade_date}:missing_batch")
        for row in rows:
            code = str(row.get("code", ""))
            if code in seen or row.get("trade_date") != trade_date or row.get("batch_id") != session.get("batch_id"):
                raise ValueError("duplicate_or_mismatched_daily_row")
            seen.add(code)
            derived = deriver.derive_daily_risk(row, evidence.get(code, {}),
                                               observed_at=bundle["observed_at"])
            values = derived["fields"]
            reasons.update(derived["reasons"])
            complete += all(values[field] is not None for field in fields)
            board = "chinext" if code.startswith(("300", "301")) else "main"
            board_counts = boards.setdefault(board, {"rows": 0, "complete": 0})
            board_counts["rows"] += 1
            board_counts["complete"] += all(values[field] is not None for field in fields)
            for field in fields:
                predicted = values[field]
                counts[field]["rows"] += 1
                counts[field]["unknown"] += predicted is None
                source_values = []
                for reference in references.get(code, []):
                    if not _reference_valid(reference, row, session.get("batch_id"), bundle["observed_at"],
                                            allow_tushare=deriver.SOURCE in (
                                                "derived:baostock-first", "mixed:baostock-eastmoney"),
                                            forward=forward):
                        continue
                    if reference.get("source") in derived.get("field_sources", {}).get(field, []):
                        counts[field]["same_source_reference_ignored"] += 1
                        continue
                    value = reference.get("fields", {}).get(field)
                    if type(value) is bool:
                        if reference.get("source") == "akshare:dated-pools" and value is not True:
                            raise ValueError("pool_absence_cannot_prove_false")
                        source_values.append(value)
                if not source_values:
                    counts[field]["reference_missing"] += 1
                    continue
                if len(set(source_values)) > 1:
                    counts[field]["reference_conflict"] += 1
                    continue
                expected = source_values[0]
                counts[field]["reference_known"] += 1
                counts[field]["reference_positive"] += expected is True
                if predicted is None:
                    counts[field]["unknown_with_reference"] += 1
                    continue
                counts[field]["compared"] += 1
                counts[field]["agree"] += predicted is expected
                if predicted is not expected:
                    counts[field]["disagree"] += 1
                    counts[field]["unsafe_false"] += predicted is False and expected is True
                    disagreements.append({"trade_date": trade_date, "code": code, "field": field,
                                          "predicted": predicted, "reference": expected,
                                          "input_hash": derived.get("input_hash")})
        size = len(denominator) if forward else len(rows)
        if forward:
            missing = size - len(seen)
            for field in fields:
                counts[field]["rows"] += missing
                counts[field]["unknown"] += missing
                counts[field]["reference_missing"] += missing
            reasons["missing_supported_primary_row"] += missing
        if size < limits["minimum_daily_universe"]:
            blockers.append(f"{trade_date}:universe_insufficient")
        if not size or complete / size < limits["minimum_daily_complete_tuple_fraction"]:
            blockers.append(f"{trade_date}:complete_tuple_insufficient")
        for field in fields:
            if not size or counts[field]["reference_known"] / size < limits["minimum_daily_field_reference_fraction"]:
                blockers.append(f"{trade_date}:{field}:reference_insufficient")
            if not size or counts[field]["compared"] / size < limits["minimum_daily_field_reference_fraction"]:
                blockers.append(f"{trade_date}:{field}:comparison_insufficient")
            totals[field].update(counts[field])
        for source in ("eastmoney:companion", "akshare:dated-pools"):
            if session.get("source_checks", {}).get(source) != "verified":
                blockers.append(f"{trade_date}:{source}:unverified")
        daily.append({"trade_date": trade_date, "rows": size, "complete": complete,
                      "fields": {field: dict(counts[field]) for field in fields}, "boards": boards,
                      "unknown_reasons": dict(reasons)})
        if forward:
            daily[-1].update(archive_rows=archived_total, supported_returned_rows=len(rows),
                             supported_universe=size, excluded_universe=len(universe) - size)
    for field, counts in totals.items():
        compared = counts["compared"]
        counts["agreement_ppm"] = round(counts["agree"] / compared * 1000000) if compared else None
        if not compared or counts["agree"] / compared < limits["minimum_field_agreement"]:
            blockers.append(f"{field}:agreement_unproven")
        if counts["reference_positive"] < limits["minimum_positive_reference_samples_per_field"]:
            blockers.append(f"{field}:positive_samples_insufficient")
    failed = any(counts["unsafe_false"] > limits["maximum_unsafe_false"] for counts in totals.values())
    failed |= len(disagreements) > limits["maximum_unexplained_disagreements"]
    if len(dates) < limits["minimum_sessions"]:
        blockers.append("sessions_insufficient")
    return {"verdict": "fail" if failed else "incomplete" if blockers else "pending_user_acceptance",
            "licensed": False, "daily": daily, "totals": {field: dict(counts) for field, counts in totals.items()},
            "disagreements": disagreements, "blockers": blockers}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    raw = args.input.read_bytes()
    protocol_raw = args.protocol.read_bytes()
    protocol = json.loads(protocol_raw)
    report = compare(json.loads(raw), protocol)
    report["hashes"] = {"input": hashlib.sha256(raw).hexdigest(),
                        "protocol": hashlib.sha256(protocol_raw).hexdigest(),
                        "deriver": hashlib.sha256(Path(_deriver(protocol).__file__).read_bytes()).hexdigest(),
                        "evaluator": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    with args.output.open("x", encoding="utf-8", newline="\n") as output:
        json.dump(report, output, ensure_ascii=False, indent=2)
        output.write("\n")
    print(json.dumps({"verdict": report["verdict"], "licensed": False,
                      "sessions": len(report["daily"]), "blockers": len(report["blockers"])}))
    return 1 if report["verdict"] == "fail" else 5 if report["verdict"] == "incomplete" else 0


if __name__ == "__main__":
    raise SystemExit(main())
