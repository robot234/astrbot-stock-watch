from __future__ import annotations

from copy import deepcopy

import pytest

from same_day_research import ResearchInputError, _public_url, evaluate_same_day, freeze_candidates


CUTOFF = "2026-09-21T15:30:00+08:00"
FROZEN_AT = "2026-09-22T08:00:00+08:00"
OUTCOME_CUTOFF = "2026-09-22T15:10:00+08:00"
EVALUATED_AT = "2026-09-22T15:11:00+08:00"


def _bars(code: str, step: float) -> list[dict]:
    rows = []
    for day in range(1, 22):
        date = f"2026-09-{day:02d}"
        close = 10 + day * step
        rows.append({
            "trade_date": date,
            "open": close - 0.03,
            "high": close + 0.08,
            "low": close - 0.08,
            "close": close,
            "volume": 100 if day < 21 else 240,
            "amount": close * (100 if day < 21 else 240),
            "price_basis": "unadjusted",
            "source": "public_fixture",
            "available_at": date + "T15:00:00+08:00",
        })
    return rows


def _selection() -> dict:
    return {
        "snapshot": {
            "source": "public_fixture", "data_date": "2026-09-21",
            "available_at": "2026-09-21T15:20:00+08:00",
            "captured_at": "2026-09-22T07:30:00+08:00",
            "parser_version": "fixture-v1", "raw_sha256": "a" * 64,
            "transformation_id": "fixture-post-close-rebuild-v1",
            "transformation_details": {"fixture": "selection reconstruction"},
        },
        "observations": [
            {"code": "600000", "name": "Alpha", "data_date": "2026-09-21", "observed_at": "2026-09-21T15:20:00+08:00",
             "risk_evidence": {"suspended": False, "limit_up": False, "limit_down": False, "st": False,
                               "name_risk_status": "clear", "method": "fixture", "source": "fixture",
                               "as_of": "2026-09-21T15:20:00+08:00", "quality": "verified"},
             "bars": _bars("600000", 0.20)},
            {"code": "600001", "name": "Beta", "data_date": "2026-09-21", "observed_at": "2026-09-21T15:20:00+08:00",
             "risk_evidence": {"suspended": False, "limit_up": False, "limit_down": False, "st": False,
                               "name_risk_status": "clear", "method": "fixture", "source": "fixture",
                               "as_of": "2026-09-21T15:20:00+08:00", "quality": "verified"},
             "bars": _bars("600001", 0.15)},
        ],
    }


def _universe() -> dict:
    return {
        "source": "public_fixture_universe", "date": "2026-09-21",
        "available_at": "2026-09-21T15:20:00+08:00", "complete": True,
        "codes": ["600000", "600001"],
    }


def _freeze(**kwargs) -> dict:
    random_seed = kwargs.pop("random_seed", 17)
    benchmark_code = kwargs.pop("benchmark_code", "000300.SH")
    return freeze_candidates(
        _selection(), _universe(), cutoff_at=CUTOFF, target_date="2026-09-22", frozen_at=FROZEN_AT,
        top_n=2, random_seed=random_seed, benchmark_code=benchmark_code, **kwargs,
    )


def _outcomes(*, first_close: float = 11.0, include_second: bool = True, benchmark: bool = True,
              benchmark_code: str = "000300.SH") -> dict:
    rows = [{"code": "600000", "date": "2026-09-22", "available_at": "2026-09-22T15:05:00+08:00", "open": 10.0, "close": first_close}]
    if include_second:
        rows.append({"code": "600001", "date": "2026-09-22", "available_at": "2026-09-22T15:05:00+08:00", "open": 20.0, "close": 19.0})
    value = {
        "snapshot": {
            "source": "public_fixture", "data_date": "2026-09-22",
            "available_at": "2026-09-22T15:06:00+08:00",
            "captured_at": "2026-09-22T15:07:00+08:00",
            "parser_version": "fixture-v1", "raw_sha256": "b" * 64,
        },
        "outcomes": rows,
    }
    if benchmark:
        value["benchmark"] = {"code": benchmark_code, "date": "2026-09-22", "available_at": "2026-09-22T15:05:00+08:00", "open": 100.0, "close": 102.0}
    return value


def _evaluate(freeze: dict, outcomes: dict, **kwargs) -> dict:
    return evaluate_same_day(
        freeze, outcomes, target_date="2026-09-22", outcome_cutoff_at=OUTCOME_CUTOFF, evaluated_at=EVALUATED_AT,
        **kwargs,
    )


def test_freeze_is_independent_of_later_outcomes_and_evaluation_keeps_frozen_rank():
    freeze = _freeze()
    first = _evaluate(freeze, _outcomes(first_close=11.0))
    second = _evaluate(freeze, _outcomes(first_close=8.0))
    assert freeze["research_mode"] == "post_close_reconstruction"
    assert freeze["historical_prediction_evidence"] is freeze["strategy_quality_established"] is False
    assert freeze["benchmark_code"] == "000300.SH"
    assert freeze["selection_snapshot"]["raw_sha256_semantics"] == "canonical_payload_sha256"
    assert freeze["selection_snapshot"]["transformation_id"] == "fixture-post-close-rebuild-v1"
    assert [item["code"] for item in freeze["candidates"]] == [item["code"] for item in first["selector"]["records"]]
    assert [item["rank"] for item in first["selector"]["records"]] == [item["rank"] for item in second["selector"]["records"]]
    assert first["selector"]["performance"]["mean_gross_marked_return_pct"] != second["selector"]["performance"]["mean_gross_marked_return_pct"]


def test_tampered_freeze_hash_and_future_cutoff_data_fail_closed():
    freeze = _freeze()
    freeze["candidates"][0]["score"] += 1
    with pytest.raises(ResearchInputError, match="hash"):
        _evaluate(freeze, _outcomes())
    selection = _selection()
    selection["observations"][0]["bars"][-1]["available_at"] = "2026-09-22T09:30:00+08:00"
    with pytest.raises(ResearchInputError, match="future availability"):
        freeze_candidates(selection, _universe(), cutoff_at=CUTOFF, target_date="2026-09-22", frozen_at=FROZEN_AT)


@pytest.mark.parametrize(
    ("mutate", "match"),
    [
        (lambda value: value["outcomes"].append(deepcopy(value["outcomes"][0])), "duplicate outcome"),
        (lambda value: value["outcomes"][0].update(close=float("nan")), "finite"),
        (lambda value: value["outcomes"][0].update(date="2026-09-21"), "wrong date"),
        (lambda value: value["outcomes"][0].update(available_at="2026-09-22T16:00:00+08:00"), "outcome cutoff"),
    ],
)
def test_duplicate_nonfinite_wrong_date_and_future_outcomes_are_rejected(mutate, match):
    outcomes = _outcomes()
    mutate(outcomes)
    with pytest.raises(ResearchInputError, match=match):
        _evaluate(_freeze(), outcomes)


def test_missing_benchmark_is_unknown_not_substituted():
    result = _evaluate(_freeze(), _outcomes(benchmark=False))
    assert result["benchmark"] == {
        "status": "unknown", "reason": "benchmark_missing", "code": "000300.SH", "gross_marked_return_pct": None,
        "selector_mean_gross_marked_return_minus_gross_benchmark_return_pct": None,
        "selector_mean_net_marked_return_minus_gross_benchmark_return_pct": None,
    }


def test_frozen_benchmark_code_is_required_and_outcome_mismatch_is_rejected():
    freeze = _freeze(benchmark_code="000001.SH")
    assert freeze["benchmark_code"] == "000001.SH"
    with pytest.raises(ResearchInputError, match="benchmark code"):
        _evaluate(freeze, _outcomes(benchmark_code="000300.SH"))
    result = _evaluate(freeze, _outcomes(benchmark_code="000001.SH"))
    assert result["benchmark"]["selector_mean_net_marked_return_minus_gross_benchmark_return_pct"] is not None


def test_fixed_seed_baseline_is_deterministic_and_persisted_before_outcomes():
    one = _freeze(random_seed=23)
    two = _freeze(random_seed=23)
    assert one["baselines"]["fixed_seed_random"] == two["baselines"]["fixed_seed_random"]
    result = _evaluate(one, _outcomes())
    assert result["baselines"]["fixed_seed_random"]["seed"] == 23
    assert result["baselines"]["fixed_seed_random"]["population"] == "verified_eligible_pool"
    assert result["baselines"]["simple_momentum"]["population"] == "verified_eligible_pool"
    assert result["baselines"]["equal_weight_universe"]["population"] == "explicit_universe"
    assert result["baselines"]["simple_momentum"]["codes"] == one["baselines"]["simple_momentum"]["codes"]


def test_fee_and_slippage_math_and_marked_not_realized_language():
    result = _evaluate(
        _freeze(), _outcomes(), entry_slippage_bps=10, exit_slippage_bps=10, fee_bps_per_side=10,
    )
    row = next(item for item in result["selector"]["records"] if item["code"] == "600000")
    assert row["gross_marked_return_pct"] == pytest.approx(10)
    assert row["hypothetical_buy_fill"] == pytest.approx(10.01)
    assert row["hypothetical_mark_proceeds"] == pytest.approx(10.978011)
    assert row["net_marked_return_pct"] == pytest.approx((10.978011 / 10.02001 - 1) * 100)
    assert result["valuation"] == "hypothetical_same_day_mark_to_market_not_realized_trade_return"
    assert result["strategy_quality_established"] is False


def test_empty_and_partial_coverage_use_null_metrics_for_zero_valid_samples():
    freeze = _freeze()
    partial = _evaluate(freeze, _outcomes(include_second=False))
    assert partial["selector"]["performance"]["valid_price_count"] == 1
    assert partial["selector"]["performance"]["missing_price_count"] == 1
    assert partial["selector"]["performance"]["coverage"] == pytest.approx(0.5)
    empty = _evaluate(freeze, {**_outcomes(include_second=False), "outcomes": []})
    performance = empty["selector"]["performance"]
    assert performance["valid_price_count"] == 0 and performance["coverage"] == 0
    assert performance["same_day_up_share"] is performance["mean_gross_marked_return_pct"] is performance["mean_net_marked_return_pct"] is None


def test_missing_risk_evidence_fails_closed():
    selection = _selection()
    selection["observations"][0].pop("risk_evidence")
    with pytest.raises(ResearchInputError, match="risk_evidence"):
        freeze_candidates(selection, _universe(), cutoff_at=CUTOFF, target_date="2026-09-22", frozen_at=FROZEN_AT, top_n=2)


@pytest.mark.parametrize(
    ("name", "reason"),
    [
        ("*ST皇庭", "name_risk_st"),
        ("ST皇庭", "name_risk_st"),
        ("退市皇庭", "name_risk_delisting"),
        ("终止上市皇庭", "name_risk_delisting"),
    ],
)
def test_name_special_treatment_or_delisting_risk_is_hard_excluded_from_eligible_baselines(name, reason):
    selection = _selection()
    selection["observations"][0]["name"] = name
    selection["observations"][0]["risk_evidence"]["name_risk_status"] = "flagged"
    freeze = freeze_candidates(selection, _universe(), cutoff_at=CUTOFF, target_date="2026-09-22", frozen_at=FROZEN_AT, top_n=2, random_seed=1)
    exclusion = next(row for row in freeze["exclusions"] if row["code"] == "600000")
    assert exclusion == {"code": "600000", "name": name, "reason": reason}
    assert "600000" not in [row["code"] for row in freeze["candidates"]]
    assert "600000" not in freeze["baselines"]["fixed_seed_random"]["codes"]
    assert "600000" not in freeze["baselines"]["simple_momentum"]["codes"]
    assert "600000" in freeze["baselines"]["equal_weight_universe"]["codes"]


def test_reconstructed_risk_assumption_is_post_close_only_and_reported_without_eligible_promotion():
    selection = _selection()
    selection["observations"][0]["risk_evidence"]["quality"] = "reconstructed_assumption"
    freeze = freeze_candidates(selection, _universe(), cutoff_at=CUTOFF, target_date="2026-09-22", frozen_at=FROZEN_AT, top_n=2)
    candidate = next(row for row in freeze["candidates"] if row["code"] == "600000")
    assert candidate["risk_level"] == "research_assumed"
    assert freeze["selector"]["risk_pool_label"] == "verified_eligible_plus_research_assumed_pool"
    result = _evaluate(freeze, _outcomes())
    assert result["selector"]["assumed_risk_count"] == 1
    assert result["baselines"]["fixed_seed_random"]["population"] == result["selector"]["risk_pool_label"]
    assert result["baselines"]["simple_momentum"]["population"] == result["selector"]["risk_pool_label"]


def test_reconstructed_risk_assumption_requires_post_close_transformation_evidence():
    selection = _selection()
    selection["snapshot"]["captured_at"] = CUTOFF
    selection["snapshot"].pop("transformation_id")
    selection["snapshot"].pop("transformation_details")
    selection["observations"][0]["risk_evidence"]["quality"] = "reconstructed_assumption"
    with pytest.raises(ResearchInputError, match="requires post-close"):
        freeze_candidates(selection, _universe(), cutoff_at=CUTOFF, target_date="2026-09-22", frozen_at=FROZEN_AT, top_n=2)


def test_snapshot_capture_and_evaluation_times_cannot_run_ahead_of_actions():
    selection = _selection()
    selection["snapshot"]["captured_at"] = "2026-09-22T08:01:00+08:00"
    with pytest.raises(ResearchInputError, match="captured_at cannot be after frozen_at"):
        freeze_candidates(selection, _universe(), cutoff_at=CUTOFF, target_date="2026-09-22", frozen_at=FROZEN_AT)
    outcomes = _outcomes()
    outcomes["snapshot"]["captured_at"] = "2026-09-22T15:12:00+08:00"
    outcomes["snapshot"]["transformation_id"] = "fixture-outcome-rebuild-v1"
    outcomes["snapshot"]["transformation_details"] = {"fixture": "outcome reconstruction"}
    with pytest.raises(ResearchInputError, match="captured_at cannot be after evaluated_at"):
        _evaluate(_freeze(), outcomes)
    future_freeze = freeze_candidates(
        _selection(), _universe(), cutoff_at=CUTOFF, target_date="2026-09-22", frozen_at="2026-09-22T15:12:00+08:00",
    )
    with pytest.raises(ResearchInputError, match="evaluated_at cannot precede freeze"):
        _evaluate(future_freeze, _outcomes())


def test_post_close_snapshot_requires_transformation_id_and_details():
    selection = _selection()
    selection["snapshot"].pop("transformation_details")
    with pytest.raises(ResearchInputError, match="transformation"):
        freeze_candidates(selection, _universe(), cutoff_at=CUTOFF, target_date="2026-09-22", frozen_at=FROZEN_AT)


def test_outcome_mutation_cannot_rerank_or_change_frozen_candidate_set():
    freeze = _freeze()
    outcome = _outcomes(first_close=1.0)
    outcome["outcomes"].reverse()
    result = _evaluate(freeze, outcome)
    assert [(row["rank"], row["code"]) for row in result["selector"]["records"]] == [
        (row["rank"], row["code"]) for row in freeze["candidates"]
    ]


def test_public_capture_url_rejects_credential_like_query_parameters():
    with pytest.raises(ResearchInputError, match="credential"):
        _public_url("https://example.invalid/data", {"access_token": "must-not-be-used"})
