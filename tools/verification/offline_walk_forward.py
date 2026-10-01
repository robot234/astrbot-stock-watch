"""Offline point-in-time walk-forward evaluation for stock-screen rankings.

Inputs are candidate rows (date, code, rank, optional score and market_regime)
and dated close rows (date, code, close).  Entry is always the close recorded on
the candidate date.  Outcomes use the strictly later 1st, 5th, and 20th dated
bar for the same code; unavailable horizons stay null and are counted missing.

Returns are percentage changes from the entry close.  Win rate is the fraction
of available returns greater than zero.  Profit/loss ratio is mean positive
return divided by the absolute mean negative return and is null when either
side is absent.  Max drawdown is the worst peak-to-trough percentage from entry
through that horizon.  Rank correlation is Spearman correlation between the
input rank (lower is better) and forward return, so a negative value means the
ordering agrees with better future returns.

This module uses only supplied files and the Python standard library.  It does
not import production ranking code, open the plugin database, or tune scores.
"""

from __future__ import annotations

import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Iterable


HORIZONS = {"next_day": 1, "5d": 5, "20d": 20}
LEAKAGE_FIELDS = {
    "outcome_date",
    "exit_date",
    "future_date",
    "future_close",
    "next_close",
    "return_1d",
    "return_5d",
    "return_20d",
}


class WalkForwardInputError(ValueError):
    """Raised when an input row violates the point-in-time contract."""


def _iso_date(value, field: str) -> str:
    text = str(value or "").strip()
    try:
        parsed = date.fromisoformat(text)
    except ValueError as exc:
        raise WalkForwardInputError(f"{field} must be an ISO date: {text!r}") from exc
    if parsed.isoformat() != text:
        raise WalkForwardInputError(f"{field} must be canonical YYYY-MM-DD: {text!r}")
    return text


def _finite(value, field: str, *, required: bool = True) -> float | None:
    if value in (None, "") and not required:
        return None
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError) as exc:
        raise WalkForwardInputError(f"{field} must be numeric") from exc
    if not math.isfinite(number):
        raise WalkForwardInputError(f"{field} must be finite")
    return number


def _code(value) -> str:
    text = str(value or "").strip()
    if not text:
        raise WalkForwardInputError("code is required")
    return text


def load_rows(path: str | Path, collection: str) -> list[dict]:
    """Load a CSV list or a JSON list/object collection without side effects."""
    source = Path(path)
    if source.suffix.lower() == ".csv":
        with source.open("r", encoding="utf-8-sig", newline="") as handle:
            return [dict(row) for row in csv.DictReader(handle)]
    if source.suffix.lower() == ".json":
        with source.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        rows = payload.get(collection) if isinstance(payload, dict) else payload
        if not isinstance(rows, list) or not all(isinstance(row, dict) for row in rows):
            raise WalkForwardInputError(f"JSON {collection} must be a list of objects")
        return [dict(row) for row in rows]
    raise WalkForwardInputError(f"unsupported input format for {source}; use CSV or JSON")


def _candidate_rows(rows: Iterable[dict]) -> list[dict]:
    normalized = []
    seen = set()
    for index, row in enumerate(rows, 1):
        leaked = sorted(field for field in LEAKAGE_FIELDS if row.get(field) not in (None, ""))
        if leaked:
            raise WalkForwardInputError(f"candidate row {index} contains outcome fields: {','.join(leaked)}")
        candidate_date = _iso_date(row.get("date") or row.get("candidate_date"), f"candidate[{index}].date")
        entry_date = row.get("entry_date")
        if entry_date not in (None, "") and _iso_date(entry_date, f"candidate[{index}].entry_date") != candidate_date:
            raise WalkForwardInputError(f"candidate row {index} entry_date must equal candidate date")
        code = _code(row.get("code"))
        key = (candidate_date, code)
        if key in seen:
            raise WalkForwardInputError(f"duplicate candidate for {candidate_date} {code}")
        seen.add(key)
        try:
            rank = int(row.get("rank"))
        except (TypeError, ValueError, OverflowError) as exc:
            raise WalkForwardInputError(f"candidate[{index}].rank must be a positive integer") from exc
        if rank <= 0:
            raise WalkForwardInputError(f"candidate[{index}].rank must be a positive integer")
        normalized.append({
            "date": candidate_date,
            "code": code,
            "rank": rank,
            "score": _finite(row.get("score"), f"candidate[{index}].score", required=False),
            "market_regime": str(row.get("market_regime") or "unknown").strip() or "unknown",
            "entry_price": _finite(row.get("entry_price"), f"candidate[{index}].entry_price", required=False),
        })
    return normalized


def _bar_rows(rows: Iterable[dict]) -> dict[str, list[dict]]:
    by_code: dict[str, list[dict]] = defaultdict(list)
    seen = set()
    for index, row in enumerate(rows, 1):
        bar_date = _iso_date(row.get("date") or row.get("trade_date"), f"bar[{index}].date")
        code = _code(row.get("code"))
        key = (bar_date, code)
        if key in seen:
            raise WalkForwardInputError(f"duplicate bar for {bar_date} {code}")
        seen.add(key)
        close = _finite(row.get("close"), f"bar[{index}].close")
        if close is None or close <= 0:
            raise WalkForwardInputError(f"bar[{index}].close must be positive")
        by_code[code].append({"date": bar_date, "close": close})
    for bars in by_code.values():
        bars.sort(key=lambda item: item["date"])
    return dict(by_code)


def _drawdown(entry: float, future: list[float]) -> float:
    peak = entry
    worst = 0.0
    for price in future:
        peak = max(peak, price)
        worst = min(worst, (price / peak - 1.0) * 100.0)
    return _metric(worst)


def _metric(value: float | None) -> float | None:
    """Normalize binary floating-point tails for stable JSON artifacts."""
    if value is None:
        return None
    rounded = round(float(value), 10)
    return 0.0 if rounded == 0 else rounded


def _average_ranks(values: list[float]) -> list[float]:
    order = sorted(range(len(values)), key=lambda index: (values[index], index))
    ranks = [0.0] * len(values)
    cursor = 0
    while cursor < len(order):
        end = cursor + 1
        while end < len(order) and values[order[end]] == values[order[cursor]]:
            end += 1
        average = (cursor + 1 + end) / 2.0
        for position in order[cursor:end]:
            ranks[position] = average
        cursor = end
    return ranks


def _spearman(ranks: list[float], returns: list[float]) -> float | None:
    if len(ranks) < 2 or len(ranks) != len(returns):
        return None
    x = _average_ranks(ranks)
    y = _average_ranks(returns)
    mean_x = statistics.fmean(x)
    mean_y = statistics.fmean(y)
    covariance = sum((a - mean_x) * (b - mean_y) for a, b in zip(x, y))
    spread_x = sum((a - mean_x) ** 2 for a in x)
    spread_y = sum((b - mean_y) ** 2 for b in y)
    if spread_x <= 0 or spread_y <= 0:
        return None
    return _metric(covariance / math.sqrt(spread_x * spread_y))


def _summary(records: list[dict], horizon: str) -> dict:
    available = [record for record in records if record["returns_pct"][horizon] is not None]
    returns = [record["returns_pct"][horizon] for record in available]
    gains = [value for value in returns if value > 0]
    losses = [value for value in returns if value < 0]
    ratio = None
    if gains and losses:
        ratio = _metric(statistics.fmean(gains) / abs(statistics.fmean(losses)))
    return {
        "available": len(available),
        "missing": len(records) - len(available),
        "mean_return_pct": _metric(statistics.fmean(returns)) if returns else None,
        "median_return_pct": _metric(statistics.median(returns)) if returns else None,
        "max_drawdown_pct": min((record["max_drawdown_pct"][horizon] for record in available), default=None),
        "win_rate": sum(value > 0 for value in returns) / len(returns) if returns else None,
        "profit_loss_ratio": ratio,
        "rank_correlation": _spearman(
            [float(record["rank"]) for record in available],
            returns,
        ),
    }


def evaluate_walk_forward(candidate_rows: Iterable[dict], bar_rows: Iterable[dict]) -> dict:
    """Evaluate point-in-time rankings without consulting production state."""
    candidates = _candidate_rows(candidate_rows)
    bars_by_code = _bar_rows(bar_rows)
    records = []
    for candidate in candidates:
        bars = bars_by_code.get(candidate["code"], [])
        positions = {bar["date"]: index for index, bar in enumerate(bars)}
        if candidate["date"] not in positions:
            raise WalkForwardInputError(
                f"candidate {candidate['date']} {candidate['code']} has no same-date entry close"
            )
        entry_index = positions[candidate["date"]]
        entry = bars[entry_index]["close"]
        supplied_entry = candidate.get("entry_price")
        if supplied_entry is not None and not math.isclose(supplied_entry, entry, rel_tol=1e-9, abs_tol=1e-9):
            raise WalkForwardInputError(
                f"candidate {candidate['date']} {candidate['code']} entry_price does not match same-date close"
            )
        returns_pct = {}
        drawdowns_pct = {}
        outcome_dates = {}
        for label, offset in HORIZONS.items():
            outcome_index = entry_index + offset
            if outcome_index >= len(bars):
                returns_pct[label] = None
                drawdowns_pct[label] = None
                outcome_dates[label] = None
                continue
            outcome = bars[outcome_index]
            if outcome["date"] <= candidate["date"]:
                raise WalkForwardInputError("outcome date must be strictly after candidate date")
            returns_pct[label] = _metric((outcome["close"] / entry - 1.0) * 100.0)
            drawdowns_pct[label] = _drawdown(
                entry,
                [bar["close"] for bar in bars[entry_index + 1:outcome_index + 1]],
            )
            outcome_dates[label] = outcome["date"]
        records.append({
            "date": candidate["date"],
            "code": candidate["code"],
            "rank": candidate["rank"],
            "score": candidate["score"],
            "market_regime": candidate["market_regime"],
            "entry_close": entry,
            "outcome_dates": outcome_dates,
            "returns_pct": returns_pct,
            "max_drawdown_pct": drawdowns_pct,
        })

    regimes = {}
    for regime in sorted({record["market_regime"] for record in records}):
        subset = [record for record in records if record["market_regime"] == regime]
        regimes[regime] = {
            "candidate_count": len(subset),
            "horizons": {label: _summary(subset, label) for label in HORIZONS},
        }
    return {
        "schema_version": 1,
        "evaluation_scope": "mechanics_only_unverified_session_and_factor_inputs",
        "strategy_quality_established": False,
        "candidate_count": len(records),
        "metric_units": {"returns": "percent", "max_drawdown": "percent", "win_rate": "fraction"},
        "horizons": {label: _summary(records, label) for label in HORIZONS},
        "market_regimes": regimes,
        "records": records,
    }


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Offline point-in-time walk-forward evaluation; lower rank is better, returns are percentages.",
        epilog=(
            "Candidates: date,code,rank[,score,market_regime,entry_price]. "
            "Bars: date,code,close. entry_price, when supplied, must equal the same-date close."
        ),
    )
    parser.add_argument("--candidates", required=True, help="Candidate CSV or JSON file")
    parser.add_argument("--bars", required=True, help="Dated close CSV or JSON file")
    parser.add_argument("--output", help="Write JSON here instead of stdout")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    result = evaluate_walk_forward(
        load_rows(args.candidates, "candidates"),
        load_rows(args.bars, "bars"),
    )
    payload = json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        Path(args.output).write_text(payload, encoding="utf-8")
    else:
        print(payload, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
