from __future__ import annotations

import csv
import importlib.util
import json
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

import pytest


TOOL_PATH = Path(__file__).with_name("offline_walk_forward.py")
SPEC = importlib.util.spec_from_file_location("offline_walk_forward", TOOL_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


def _dates(count: int) -> list[str]:
    start = date(2026, 1, 1)
    return [(start + timedelta(days=index)).isoformat() for index in range(count)]


def _bars(code: str, closes: list[float]) -> list[dict]:
    return [
        {"date": trade_date, "code": code, "close": close}
        for trade_date, close in zip(_dates(len(closes)), closes)
    ]


def test_metric_math_regimes_drawdown_and_rank_correlation():
    candidates = [
        {"date": "2026-01-01", "code": "A", "rank": 1, "score": 20, "market_regime": "risk_on"},
        {"date": "2026-01-01", "code": "B", "rank": 2, "score": 10, "market_regime": "risk_off"},
    ]
    bars = _bars("A", [100.0] + [100.0 + index for index in range(1, 21)])
    bars += _bars("B", [100.0, 90.0, 95.0, 80.0, 85.0, 75.0] + [75.0 - (index * 2.0) for index in range(1, 16)])

    result = MODULE.evaluate_walk_forward(candidates, bars)

    one = result["horizons"]["next_day"]
    assert one["available"] == 2
    assert one["missing"] == 0
    assert one["mean_return_pct"] == pytest.approx(-4.5)
    assert one["win_rate"] == 0.5
    assert one["profit_loss_ratio"] == pytest.approx(0.1)
    assert one["rank_correlation"] == pytest.approx(-1.0)
    assert result["horizons"]["5d"]["mean_return_pct"] == pytest.approx(-10.0)
    assert result["horizons"]["20d"]["mean_return_pct"] == pytest.approx(-17.5)
    assert result["horizons"]["20d"]["max_drawdown_pct"] == pytest.approx(-55.0)
    assert result["market_regimes"]["risk_on"]["candidate_count"] == 1
    assert result["market_regimes"]["risk_off"]["horizons"]["next_day"]["mean_return_pct"] == pytest.approx(-10.0)


def test_missing_horizons_are_explicit_and_never_fabricated():
    candidates = [{"date": "2026-01-01", "code": "A", "rank": 1, "score": "", "market_regime": "neutral"}]
    result = MODULE.evaluate_walk_forward(candidates, _bars("A", [100.0, 110.0]))
    record = result["records"][0]
    assert record["score"] is None
    assert record["returns_pct"] == {"next_day": pytest.approx(10.0), "5d": None, "20d": None}
    assert result["horizons"]["next_day"]["available"] == 1
    assert result["horizons"]["5d"]["missing"] == 1
    assert result["horizons"]["20d"]["mean_return_pct"] is None


def test_tied_ranks_or_constant_returns_have_null_rank_correlation():
    candidates = [
        {"date": "2026-01-01", "code": "A", "rank": 1},
        {"date": "2026-01-01", "code": "B", "rank": 1},
    ]
    bars = _bars("A", [100.0, 110.0]) + _bars("B", [100.0, 90.0])
    result = MODULE.evaluate_walk_forward(candidates, bars)
    assert result["horizons"]["next_day"]["rank_correlation"] is None


@pytest.mark.parametrize(
    "candidate",
    [
        {"date": "2026-01-01", "entry_date": "2026-01-02", "code": "A", "rank": 1},
        {"date": "2026-01-01", "code": "A", "rank": 1, "return_1d": 99},
        {"date": "2026-01-01", "code": "A", "rank": 1, "outcome_date": "2026-01-02"},
        {"date": "2026-01-01", "code": "A", "rank": 1, "entry_price": 101},
    ],
)
def test_future_or_wrong_cutoff_candidate_fields_are_rejected(candidate):
    with pytest.raises(MODULE.WalkForwardInputError):
        MODULE.evaluate_walk_forward([candidate], _bars("A", [100.0, 110.0]))


def test_missing_same_date_entry_and_duplicate_bars_are_rejected():
    candidate = {"date": "2026-01-02", "code": "A", "rank": 1}
    with pytest.raises(MODULE.WalkForwardInputError, match="same-date entry"):
        MODULE.evaluate_walk_forward([candidate], _bars("A", [100.0]))
    duplicate = [
        {"date": "2026-01-01", "code": "A", "close": 100},
        {"date": "2026-01-01", "code": "A", "close": 101},
    ]
    with pytest.raises(MODULE.WalkForwardInputError, match="duplicate bar"):
        MODULE.evaluate_walk_forward([{"date": "2026-01-01", "code": "A", "rank": 1}], duplicate)


def test_cli_accepts_csv_and_json_and_emits_json(tmp_path):
    candidates_path = tmp_path / "candidates.csv"
    with candidates_path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=["date", "code", "rank", "score", "market_regime"])
        writer.writeheader()
        writer.writerow({"date": "2026-01-01", "code": "A", "rank": 1, "score": 10, "market_regime": "neutral"})
    bars_path = tmp_path / "bars.json"
    bars_path.write_text(json.dumps({"bars": _bars("A", [100.0, 105.0])}), encoding="utf-8")

    completed = subprocess.run(
        [sys.executable, str(TOOL_PATH), "--candidates", str(candidates_path), "--bars", str(bars_path)],
        check=True,
        capture_output=True,
        text=True,
    )
    payload = json.loads(completed.stdout)
    assert payload["candidate_count"] == 1
    assert payload["horizons"]["next_day"]["mean_return_pct"] == pytest.approx(5.0)
    assert payload["horizons"]["5d"]["available"] == 0
