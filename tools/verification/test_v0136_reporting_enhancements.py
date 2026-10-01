from __future__ import annotations

import sys
from pathlib import Path


REPOSITORY_ROOT = Path(__file__).resolve().parents[3]
if str(REPOSITORY_ROOT) not in sys.path:
    sys.path.insert(0, str(REPOSITORY_ROOT))

from astrbot_stock_watch.core import (
    Candidate,
    FactorOverlay,
    PricePlan,
    Quote,
    assess_market_context,
    candidate_rank_key,
    format_compact_candidate,
    format_plan_distance,
    format_watch_range_distance,
)


def _validated_plan() -> PricePlan:
    return PricePlan(
        "watch",
        10.0,
        0.5,
        9.5,
        11.5,
        9.0,
        11.0,
        12.0,
        13.0,
        14.0,
        8.0,
        "good",
        ["fixture"],
        {
            "context": "daily_close",
            "actual_date": "2026-09-08",
            "last_date": "2026-09-08",
            "last_close": 10.0,
            "basis": "unadjusted",
            "source": "tushare",
            "tolerance_pct": 1.0,
            "deviation_pct": 0.0,
            "anchor_price": 10.0,
            "reference_price": 10.0,
            "anchor": 10.0,
        },
        True,
    )


def _candidate(code: str, *, score=12, base=12, risk="eligible", amount=1000.0, coverage=1.0) -> Candidate:
    quote = Quote(
        code,
        code,
        10.0,
        amount=amount,
        pct_change=1.0,
        history_days=60,
        rsi6=50.0,
        ma5=10.0,
        ma10=9.9,
        ma20=9.8,
        momentum5=2.0,
        momentum20=4.0,
        atr14=0.5,
        suspended=False,
        limit_up=False,
        limit_down=False,
    )
    return Candidate(
        quote,
        score,
        ["趋势确认+2"],
        score_max=70 if score != base else 50,
        base_score=base,
        composite_score=score if score != base else None,
        risk_level=risk,
        factor_overlay=FactorOverlay(
            fundamental_coverage=coverage,
            risk_factor_coverage=coverage,
        ),
    )


def test_market_context_and_report_include_same_snapshot_median():
    quotes = [
        Quote("000001", "A", 10.0, pct_change=-1.0),
        Quote("000002", "B", 10.0, pct_change=0.0),
        Quote("000003", "C", 10.0, pct_change=3.0),
        Quote("000004", "D", 10.0, pct_change=5.0),
    ]
    market = assess_market_context(quotes)
    assert (market.advancing, market.declining, market.flat, market.sample_size) == (2, 1, 1, 4)
    assert market.median_return == 1.5
    assert "涨跌幅中位数+1.50%" in market.evidence

def test_tied_candidate_order_uses_explicit_stable_keys():
    code_high = _candidate("600002")
    code_low = _candidate("600001")
    better_base = _candidate("600003", score=12, base=13)
    safer = _candidate("600004", risk="watch_only")
    missing = _candidate("600005", risk="unknown", coverage=float("nan"), amount=float("nan"))
    ordered = sorted([missing, safer, code_high, better_base, code_low], key=candidate_rank_key)
    assert [item.quote.code for item in ordered] == ["600003", "600001", "600002", "600004", "600005"]


def test_compact_score_breakdown_matches_base_and_actual_adjustment():
    candidate = _candidate("600001", score=15, base=12)
    text = format_compact_candidate(candidate, 1)
    assert "综合 15/70(技术12+3)" in text
    assert "技术12+3" in text


def test_watch_range_distance_and_reference_deviation_are_bounded():
    plan = _validated_plan()
    assert format_watch_range_distance(plan, 10.5) == "位于风险区间｜较参考价+5.0%"
    assert format_watch_range_distance(plan, 8.1) == "低于风险区间10.0%｜较参考价-19.0%"
    assert format_watch_range_distance(plan, 12.1) == "高于风险区间10.0%｜较参考价+21.0%"
    assert format_watch_range_distance(plan, 25.0) == "现价与参考价偏离异常，距离未展示"
    assert format_plan_distance(plan, 25.0) == "现价与参考价偏离异常，距离未展示"
    assert format_watch_range_distance(plan, float("nan")) == "价位数据异常，距离未展示"


def test_invalid_plan_range_never_renders_implausible_levels():
    plan = _validated_plan()
    plan.attention_low = 11.0
    plan.attention_high = 9.0
    assert format_watch_range_distance(plan, 10.0) == ""
    candidate = _candidate("600001")
    candidate.price_plan = plan
    text = format_compact_candidate(candidate, 1)
    assert "无已验证收盘计划" in text
    assert "风险区间 11.00-9.00" not in text
