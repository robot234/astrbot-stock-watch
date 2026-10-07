"""Screening funnel (S04) and per-stock audit rows (S03) for one bounded screen.

Display data only: built from the objects the screen already used and never read back as a gate.
Scores rank setups; they are not probabilities.
"""
from __future__ import annotations

from collections import Counter
import math
import re

FUNNEL_VERSION = 1
AUDIT_ROWS = 60
HARD_STATES = ("suspended", "limit_up", "limit_down", "st")
INDICATOR_OK = frozenset({"network", "memory_cache", "persistent_cache", "raw_batch"})
# Every quote field core.score_quote reads; a missing one scores 0 instead of its rule points.
SCORE_INPUTS = ("rsi6", "ma5", "ma10", "ma20", "momentum5", "momentum20", "volume_ratio", "volatility20")
RISK_EXCLUSIONS = ("risk_blocked", "technical_incomplete", "factor_not_checked", "st_audit_unknown", "risk_unknown")


def _finite(value):
    try:
        number = float(value)
    except (TypeError, ValueError, OverflowError):
        return None
    return number if math.isfinite(number) else None


def _text(value, limit):
    return re.sub(r"[\x00-\x1f\x7f]", "", str(value or "")).strip()[:limit]


def _nonzero(counts):
    return {key: int(value) for key, value in counts.items() if value}


def quote_gate(quote, price_min, price_max):
    """First pre-filter that keeps a quote out of the deep screen, or None; same rule as core.is_screenable."""
    price = _finite(getattr(quote, "price", None))
    if price is None or price <= 0 or not price_min <= price <= price_max:
        return "price_out_of_range"
    for field in HARD_STATES:
        if getattr(quote, field, None) is True:
            return field
    if any(getattr(quote, field, None) is not False for field in HARD_STATES):
        return "risk_state_unknown"
    return None


def scored_statuses(scored, *, minimum, factor_codes, qualified, final, fallback):
    """Where each deep-screen candidate ended up, keyed by id(candidate)."""
    final_ids, qualified_ids, fallback_ids = ({id(item) for item in group} for group in (final, qualified, fallback))
    statuses = {}
    for item in scored:
        key, flags = id(item), set(item.risk_flags or ())
        if key in final_ids:
            status = "fallback_candidate" if key in fallback_ids else "candidate"
        elif key in qualified_ids:
            status = "beyond_candidate_limit"
        elif item.risk_level == "blocked":
            status = "risk_blocked"
        elif item.risk_level == "unknown":
            if "技术数据不完整" in flags:
                status = "technical_incomplete"
            elif item.quote.code not in factor_codes:
                status = "factor_not_checked"
            elif "ST/审计状态未知" in flags:
                status = "st_audit_unknown"
            else:
                status = "risk_unknown"
        elif item.base_score < minimum:
            status = "below_min_score"
        else:
            status = "not_selected"
        statuses[key] = status
    return statuses


def screen_funnel(quotes, scored, statuses, *, price_min, price_max, deep_limit, indicator_status, minimum, limit):
    """Counts after each step with the reasons for every exclusion; `indicators` is shown but is not a gate."""
    gates = Counter(quote_gate(quote, price_min, price_max) for quote in quotes)
    counts = Counter(statuses.values())
    indicators = Counter(indicator_status.get(item.quote.code, "not_computed") for item in scored)
    tradable = gates[None]
    risk_excluded = _nonzero({key: counts[key] for key in RISK_EXCLUSIONS})
    qualified = counts["candidate"] + counts["beyond_candidate_limit"]
    return {"version": FUNNEL_VERSION, "stages": [
        {"key": "input", "count": len(quotes)},
        {"key": "price", "count": len(quotes) - gates["price_out_of_range"],
         "excluded": _nonzero({"price_out_of_range": gates["price_out_of_range"]}),
         "price_min": price_min, "price_max": price_max},
        {"key": "risk_state", "count": tradable,
         "excluded": _nonzero({key: gates[key] for key in HARD_STATES + ("risk_state_unknown",)})},
        {"key": "deep_screen", "count": len(scored), "excluded": _nonzero({"beyond_deep_limit": tradable - len(scored)}),
         "limit": deep_limit},
        {"key": "indicators", "count": sum(n for key, n in indicators.items() if key in INDICATOR_OK), "gate": False,
         "excluded": _nonzero({_text(key, 30): n for key, n in indicators.items() if key not in INDICATOR_OK})},
        {"key": "risk_review", "count": len(scored) - sum(risk_excluded.values()), "excluded": risk_excluded},
        {"key": "min_score", "count": qualified,
         "excluded": _nonzero({"below_min_score": counts["below_min_score"] + counts["fallback_candidate"]}),
         "min_score": minimum},
        {"key": "candidates", "count": counts["candidate"] + counts["fallback_candidate"],
         "excluded": _nonzero({"beyond_candidate_limit": counts["beyond_candidate_limit"]}),
         "limit": limit, "fallback": counts["fallback_candidate"]},
    ]}


def audit_rows(scored, statuses, indicator_status, *, final=(), limit=AUDIT_ROWS):
    """Rank-ordered rows: every final candidate plus the best-ranked others, at most `limit` rows."""
    final_ids = {id(item) for item in final}
    room = max(0, limit - len(final_ids))
    rows = []
    for rank, item in enumerate(scored, 1):
        if id(item) not in final_ids:
            if room <= 0:
                continue
            room -= 1
        quote = item.quote
        indicators = {}
        for key in SCORE_INPUTS + ("atr14",):
            value = _finite(getattr(quote, key, None))
            if value is not None:
                indicators[key] = round(value, 4)
        missing = [key for key in SCORE_INPUTS if key not in indicators]
        source = indicator_status.get(quote.code, "not_computed")
        rows.append({
            "code": _text(quote.code, 12), "name": _text(quote.name, 20), "rank": rank, "status": statuses[id(item)],
            "base_score": int(item.base_score), "score": int(item.score), "score_max": int(item.score_max),
            "risk_level": _text(item.risk_level, 20),
            "risk_flags": [_text(flag, 40) for flag in list(item.risk_flags or ())[:6]],
            "reasons": [_text(reason, 40) for reason in list(item.reasons or ())[:8]],
            "indicators": indicators, "history_days": int(quote.history_days or 0),
            "price": _finite(quote.price), "amount": _finite(quote.amount), "indicator_status": _text(source, 30),
            "comparable": source in INDICATOR_OK and not missing, "missing_inputs": missing,
        })
    return rows
