"""Deterministic factor calculations. Network providers only supply raw rows."""
from __future__ import annotations
import math

def _finite(value):
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError):
        return None

def percentile_centered(values, value):
    data = [_finite(item) for item in values]
    data = sorted(item for item in data if item is not None)
    current = _finite(value)
    if current is None or len(data) < 5:
        return None
    less = sum(item < current for item in data)
    equal = sum(item == current for item in data)
    rank = (less + 0.5 * equal) / len(data)
    return round(2 * rank - 1, 4)

def industry_strength(industry_return5, benchmark_return5, breadth, amount_ratio):
    """Return a bounded -10..10 score; missing inputs remain unknown."""
    vals = [_finite(industry_return5), _finite(benchmark_return5), _finite(breadth), _finite(amount_ratio)]
    if any(item is None for item in vals):
        return None
    excess = vals[0] - vals[1]
    score = 10 * (0.5 * max(-1, min(1, excess / 10)) + 0.3 * (vals[2] * 2 - 1) + 0.2 * max(-1, min(1, (vals[3] - 1) / 2)))
    return max(-10, min(10, round(score)))

def fundamental_score(roe, profit_growth, cash_quality, valuation_score, st_flag=False, audit_flag=False):
    if st_flag or audit_flag:
        return -10
    values = [_finite(item) for item in (roe, profit_growth, cash_quality, valuation_score)]
    usable = [item for item in values if item is not None]
    if len(usable) < 2:
        return None
    normalized = [max(-1, min(1, item / 20)) if item is not None else None for item in values]
    weights = (0.35, 0.3, 0.2, 0.15)
    total = sum(w * n for w, n in zip(weights, normalized) if n is not None)
    used = sum(w for w, n in zip(weights, normalized) if n is not None)
    return max(-10, min(10, round(10 * total / used)))

def market_adjustment(regime):
    return {"risk_on": 3, "neutral": 0, "risk_off": -5}.get(str(regime), 0)
