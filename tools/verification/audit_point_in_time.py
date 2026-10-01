"""Strict, file-only research audit; never imports or tunes production scores."""
from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import statistics

from offline_walk_forward import WalkForwardInputError, _candidate_rows, _iso_date, _spearman


HORIZONS = (1, 5, 20)
DEFAULT_POLICY = {"min_decision_dates": 252, "min_test_dates": 60, "min_test_samples": 1000, "min_coverage": 0.9}
CHINA = timezone(timedelta(hours=8))


def _time(value):
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise WalkForwardInputError("explicit availability timestamp required") from exc
    if result.tzinfo is None:
        raise WalkForwardInputError("availability timestamp must include timezone")
    return result.astimezone(CHINA)


def _positive(value):
    if isinstance(value, bool):
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) and value > 0 else None
    except (TypeError, ValueError, OverflowError):
        return None


def _digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, allow_nan=False).encode()).hexdigest()


def _summary(rows):
    complete = [r for r in rows if r["status"] == "complete"]
    returns = [r["return_pct"] for r in complete]
    excess = [r["excess_pct"] for r in complete if r["excess_pct"] is not None]
    daily = defaultdict(list)
    for row in complete:
        daily[row["date"]].append(row)
    correlations = [
        _spearman([r["rank"] for r in group], [r["return_pct"] for r in group])
        for group in daily.values()
    ]
    correlations = [r for r in correlations if r is not None]
    return {
        "count": len(rows), "status_counts": dict(Counter(r["status"] for r in rows)),
        "price_coverage": len(complete) / len(rows) if rows else 0,
        "benchmark_coverage": len(excess) / len(rows) if rows else 0,
        "mean_gross_return_pct": statistics.fmean(returns) if returns else None,
        "median_gross_return_pct": statistics.median(returns) if returns else None,
        "mean_excess_pct": statistics.fmean(excess) if excess else None,
        "mean_daily_rank_correlation": statistics.fmean(correlations) if correlations else None,
    }


def audit_dataset(dataset, *, as_of, policy=None):
    """Require attested availability, exact sessions, fixed benchmark and purged folds.

    Hashes make supplied inputs auditable, not authentic. Real-data attestations
    still require external provenance review. Close-to-close returns are gross
    diagnostics, not executable fills or evidence of profitable trading.
    """
    cutoff = _iso_date(as_of, "as_of")
    cutoff_time = _time(cutoff + "T23:59:59.999999+08:00")
    policy = {**DEFAULT_POLICY, **(policy or {})}
    if (not isinstance(policy["min_decision_dates"], int) or policy["min_decision_dates"] < 1
            or not isinstance(policy["min_test_samples"], int) or policy["min_test_samples"] < 1
            or not isinstance(policy["min_test_dates"], int) or policy["min_test_dates"] < 1
            or not 0 < policy["min_coverage"] <= 1):
        raise WalkForwardInputError("invalid sufficiency policy")
    manifest = dataset.get("manifest") or {}
    blockers = []
    for field in ("source", "universe_provenance", "ranking_version", "benchmark_code"):
        if not manifest.get(field):
            blockers.append("manifest_missing:" + field)
    if manifest.get("kind") != "real":
        blockers.append("not_attested_real_data")
    raw_candidates = list(dataset.get("candidates") or [])
    allowed_fields = {
        "date", "candidate_date", "code", "rank", "score", "market_regime", "entry_price",
        "decision_at", "recorded_at", "features_available_at", "source_ref",
    }
    if any(set(row) - allowed_fields for row in raw_candidates):
        raise WalkForwardInputError("candidate contains fields outside the ranking input contract")
    if any(not str(row.get("rank", "")).isdigit() for row in raw_candidates):
        raise WalkForwardInputError("rank must be an integer without coercion")
    candidates = _candidate_rows(raw_candidates)
    calendars = {}
    for row in dataset.get("calendar") or []:
        day = _iso_date(row.get("date"), "calendar.date")
        if day in calendars:
            raise WalkForwardInputError("duplicate calendar date")
        calendars[day] = row
    universes = {}
    for row in dataset.get("universes") or []:
        day = _iso_date(row.get("date"), "universe.date")
        if day in universes:
            raise WalkForwardInputError("duplicate universe date")
        universes[day] = row
    bars = {}
    for row in dataset.get("bars") or []:
        key = (_iso_date(row.get("date"), "bar.date"), str(row.get("code") or ""))
        if key in bars:
            raise WalkForwardInputError("duplicate bar")
        bars[key] = row
    benchmark = {}
    for row in dataset.get("benchmark") or []:
        key = (_iso_date(row.get("date"), "benchmark.date"), str(row.get("code") or ""))
        if key in benchmark:
            raise WalkForwardInputError("duplicate benchmark row")
        benchmark[key] = row

    def known(row):
        return bool(row and row.get("source") and row.get("available_at")
                    and _time(row["available_at"]) <= cutoff_time)

    def usable(row, base_factor):
        return bool(known(row) and _positive(row.get("close"))
                    and row.get("price_basis") == "unadjusted"
                    and row.get("factor_evidence") and _positive(row.get("factor"))
                    and math.isclose(float(row["factor"]), base_factor, rel_tol=1e-10))

    records = []
    for candidate, raw in zip(candidates, raw_candidates):
        day, code = candidate["date"], candidate["code"]
        decision = _time(raw.get("decision_at"))
        if decision.date().isoformat() != day or decision > cutoff_time:
            raise WalkForwardInputError("decision date/cutoff mismatch")
        for field in ("recorded_at", "features_available_at"):
            if _time(raw.get(field)) > decision:
                raise WalkForwardInputError("ranking contains post-decision evidence")
        if not raw.get("source_ref"):
            raise WalkForwardInputError("ranking source_ref is required")
        universe = universes.get(day)
        if (not universe or not universe.get("source") or universe.get("complete") is not True
                or code not in universe.get("codes", [])
                or _time(universe.get("available_at")) > decision):
            raise WalkForwardInputError("point-in-time universe membership unverified")
        entry = bars.get((day, code))
        base_factor = _positive((entry or {}).get("factor"))
        entry_valid = bool(base_factor and usable(entry, base_factor)
                           and _time(entry["available_at"]) <= decision)
        if entry_valid and candidate.get("entry_price") is not None:
            if not math.isclose(float(entry["close"]), candidate["entry_price"], rel_tol=1e-9):
                raise WalkForwardInputError("entry_price does not match same-date close")
        calendar_entry = calendars.get(day)
        entry_valid = bool(entry_valid and known(calendar_entry)
                           and calendar_entry.get("verified") is True
                           and calendar_entry.get("is_open") is True
                           and _time(calendar_entry["available_at"]) <= decision)
        future, calendar_gap = [], False
        cursor = date.fromisoformat(day) + timedelta(days=1)
        while cursor.isoformat() <= cutoff:
            session = calendars.get(cursor.isoformat())
            if (not known(session) or session.get("verified") is not True
                    or not isinstance(session.get("is_open"), bool)):
                calendar_gap = True
                break
            if session["is_open"]:
                future.append(cursor.isoformat())
            cursor += timedelta(days=1)
        for horizon in HORIZONS:
            window = future[:horizon]
            row = {
                **candidate, "horizon": horizon, "source_ref": raw["source_ref"],
                "status": "pending", "reason": "calendar_unverified" if calendar_gap else "window_not_mature",
                "label_end": window[-1] if len(window) == horizon else None,
                "return_pct": None, "excess_pct": None,
            }
            if len(window) == horizon:
                if not entry_valid or any(not usable(bars.get((d, code)), base_factor) for d in window):
                    row.update(status="unknown", reason="price_or_factor_evidence_missing")
                else:
                    value = (float(bars[window[-1], code]["close"]) / float(entry["close"]) - 1) * 100
                    row.update(status="complete", reason="", return_pct=value)
                    bench_rows = [benchmark.get((d, manifest.get("benchmark_code"))) for d in [day, *window]]
                    if (all(known(b) and b.get("price_basis") == "index_close" and _positive(b.get("close")) for b in bench_rows)
                            and _time(bench_rows[0]["available_at"]) <= decision):
                        baseline = (float(bench_rows[-1]["close"]) / float(bench_rows[0]["close"]) - 1) * 100
                        row["excess_pct"] = value - baseline
            records.append(row)

    folds, previous_end = [], ""
    for fold in manifest.get("folds") or []:
        start, end, test_start, test_end = [
            _iso_date(fold.get(k), k) for k in ("train_start", "train_end", "test_start", "test_end")
        ]
        if not start <= end < test_start <= test_end <= cutoff or test_start <= previous_end:
            raise WalkForwardInputError("folds must be ordered, nonoverlapping and within cutoff")
        previous_end = test_end
        training = [r for r in records if start <= r["date"] <= end]
        purged = [r for r in training if r["label_end"] is None or r["label_end"] >= test_start]
        kept = [r for r in training if r["label_end"] is not None and r["label_end"] < test_start]
        test = [r for r in records if test_start <= r["date"] <= test_end]
        folds.append({
            **fold, "purged_training_labels": len(purged),
            "train": {str(h): _summary([r for r in kept if r["horizon"] == h]) for h in HORIZONS},
            "test": {str(h): _summary([r for r in test if r["horizon"] == h]) for h in HORIZONS},
        })
    if not folds:
        blockers.append("temporal_folds_missing")
    decision_dates = len({r["date"] for r in candidates})
    if decision_dates < policy["min_decision_dates"]:
        blockers.append("decision_history_too_short")
    test_rows = [r for r in records if any(f["test_start"] <= r["date"] <= f["test_end"] for f in folds)]
    if len({r["date"] for r in test_rows}) < policy["min_test_dates"]:
        blockers.append("out_of_sample_history_too_short")
    for horizon in HORIZONS:
        summary = _summary([r for r in test_rows if r["horizon"] == horizon])
        if summary["count"] < policy["min_test_samples"]:
            blockers.append(f"T+{horizon}:test_samples_insufficient")
        if summary["price_coverage"] < policy["min_coverage"]:
            blockers.append(f"T+{horizon}:price_coverage_insufficient")
        if summary["benchmark_coverage"] < policy["min_coverage"]:
            blockers.append(f"T+{horizon}:benchmark_coverage_insufficient")
    groups = {}
    for name, field in (("market_regimes", "market_regime"), ("rank_bands", "rank_band")):
        for row in test_rows:
            row["rank_band"] = "top10" if row["rank"] <= 10 else "11plus"
        groups[name] = {
            value: {str(h): _summary([r for r in test_rows if r[field] == value and r["horizon"] == h]) for h in HORIZONS}
            for value in sorted({r[field] for r in test_rows})
        }
    return {
        "schema_version": 1, "as_of": cutoff, "input_sha256": _digest(dataset),
        "policy": policy, "manifest": manifest, "decision_dates": decision_dates,
        "candidate_count": len(candidates), "blockers": blockers,
        "status": "insufficient_data" if blockers else "ready_for_provenance_review",
        "strategy_quality_established": False, "production_tuning_allowed": False,
        "return_basis": "gross_close_to_close_diagnostic_not_executable_fills",
        "rank_correlation_population": "recorded_candidates_only",
        "folds": folds, **groups, "records": records,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--as-of", required=True)
    parser.add_argument("--output")
    args = parser.parse_args()
    source = Path(args.dataset)
    content = source.read_bytes()
    report = audit_dataset(json.loads(content), as_of=args.as_of)
    report["input_file_sha256"] = hashlib.sha256(content).hexdigest()
    text = json.dumps(report, indent=2, sort_keys=True, allow_nan=False) + "\n"
    if args.output:
        Path(args.output).write_text(text, encoding="utf-8")
    else:
        print(text, end="")


if __name__ == "__main__":
    main()
