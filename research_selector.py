"""Bounded, non-trading research pools from one published unadjusted batch."""

from __future__ import annotations

import math

from .core import Quote, apply_daily_indicators, score_quote


def supported_board(code: str) -> bool:
    return len(code) == 6 and code.isdigit() and code.startswith(
        ("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688", "689")
    )


def select_pools(day_rows: list[dict], histories: dict[str, list[dict]], *,
                 deep_limit: int = 300, primary_limit: int = 7, radar_limit: int = 20,
                 price_min: float = 2, price_max: float = 80) -> tuple[list[dict], list[dict], dict]:
    """Rank visible technical setups. Unknown risk remains unknown, never safe."""
    eligible = []
    for row in day_rows:
        code = str(row.get("code") or "")
        name = str(row.get("name") or "")
        try:
            price, amount, volume = (float(row[key]) for key in ("close", "amount", "volume"))
        except (KeyError, TypeError, ValueError, OverflowError):
            continue
        if (not supported_board(code) or not math.isfinite(price) or not math.isfinite(amount)
                or not math.isfinite(volume) or not price_min <= price <= price_max
                or amount <= 0 or volume <= 0 or "ST" in name.upper() or "退" in name):
            continue
        eligible.append(row)
    eligible.sort(key=lambda row: (-float(row["amount"]), str(row["code"])))
    scored = []
    for row in eligible[:deep_limit]:
        code = str(row["code"])
        bars = histories.get(code, [])
        if not bars or str(bars[-1].get("trade_date") or "") != str(row.get("trade_date") or ""):
            continue
        quote = Quote(code, str(row.get("name") or ""), float(row["close"]),
                      float(row.get("pre_close") or 0), float(row["amount"]),
                      float(row.get("pct_change") or 0), float(row["volume"]),
                      source=str(row.get("source") or ""))
        if not apply_daily_indicators(quote, bars):
            continue
        candidate = score_quote(quote)
        if candidate.risk_level == "blocked":
            continue
        scored.append({"code": code, "name": quote.name, "score": candidate.score,
                       "close": quote.price, "amount": quote.amount, "risk_level": candidate.risk_level,
                       "risk_flags": list(candidate.risk_flags), "reasons": list(candidate.reasons)})
    scored.sort(key=lambda row: (-row["score"], -row["amount"], row["code"]))
    return (scored[:primary_limit], scored[primary_limit:primary_limit + radar_limit],
            {"universe": len(day_rows), "liquid": len(eligible), "ranked": len(scored),
             "examined": min(len(eligible), deep_limit)})


def radar_crossed(quote: Quote, frozen_close: float, *, threshold_pct: float, now, max_age: int = 90) -> bool:
    """An observation threshold, not a signal that the stock is tradable."""
    fetched = getattr(quote, "fetched_at", None)
    source = getattr(quote, "provider_ts", None)
    if not fetched or not source or fetched.tzinfo is None or source.tzinfo is None:
        return False
    if any(not 0 <= (now - stamp.astimezone(now.tzinfo)).total_seconds() <= max_age for stamp in (fetched, source)):
        return False
    try:
        price = float(quote.price)
        close = float(frozen_close)
    except (TypeError, ValueError, OverflowError):
        return False
    return (math.isfinite(price) and math.isfinite(close) and close > 0
            and price >= close * (1 + threshold_pct / 100)
            and quote.suspended is not True and quote.limit_up is not True
            and quote.limit_down is not True and quote.st is not True)
