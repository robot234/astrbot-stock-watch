from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Iterable
from zoneinfo import ZoneInfo

CHINA_TZ = ZoneInfo("Asia/Shanghai")


@dataclass(slots=True)
class Quote:
    code: str
    name: str
    price: float
    prev_close: float = 0.0
    amount: float = 0.0
    pct_change: float = 0.0
    volume: float = 0.0
    rsi6: float | None = None
    ma5: float | None = None
    ma10: float | None = None
    ma20: float | None = None
    volume_ratio: float | None = None
    suspended: bool = False
    limit_up: bool = False
    limit_down: bool = False
    fetched_at: datetime = field(default_factory=lambda: datetime.now(CHINA_TZ))


@dataclass(slots=True)
class Candidate:
    quote: Quote
    score: int
    reasons: list[str]


@dataclass(slots=True)
class NewsItem:
    title: str
    link: str = ""
    summary: str = ""
    published_at: str = ""
    source: str = ""


def normalize_code(code: str) -> str:
    value = str(code or "").strip().lower()
    for prefix in ("sh", "sz", "bj"):
        if value.startswith(prefix):
            value = value[2:]
    return value


def parse_codes(raw: str | Iterable[str]) -> list[str]:
    values = raw.replace("，", ",").replace("\n", ",").replace(" ", ",").split(",") if isinstance(raw, str) else list(raw)
    result: list[str] = []
    seen: set[str] = set()
    for item in values:
        code = normalize_code(item)
        if code.isdigit() and len(code) == 6 and code not in seen:
            result.append(code)
            seen.add(code)
    return result


def calc_rsi(closes: Iterable[float], period: int = 6) -> float | None:
    data = [float(value) for value in closes]
    if period <= 0 or len(data) <= period:
        return None
    changes = [data[index] - data[index - 1] for index in range(1, len(data))][-period:]
    gains = sum(max(change, 0.0) for change in changes) / period
    losses = sum(max(-change, 0.0) for change in changes) / period
    if losses == 0:
        return 100.0 if gains > 0 else 50.0
    return round(100 - 100 / (1 + gains / losses), 2)


def is_tradable(quote: Quote, price_min: float = 2, price_max: float = 80) -> bool:
    return price_min <= quote.price <= price_max and quote.price > 0 and not quote.suspended and not quote.limit_up and not quote.limit_down


def score_quote(quote: Quote) -> Candidate:
    score = 0
    reasons: list[str] = []
    if quote.rsi6 is not None:
        if quote.rsi6 < 25:
            score += 15
            reasons.append("RSI6超卖+15")
        elif quote.rsi6 < 40:
            score += 8
            reasons.append("RSI6低位+8")
    if quote.ma5 is not None and quote.ma10 is not None and quote.ma20 is not None:
        if quote.ma5 > quote.ma10 > quote.ma20:
            score += 10
            reasons.append("均线多头+10")
        elif quote.ma5 < quote.ma10 < quote.ma20:
            score -= 10
            reasons.append("均线空头-10")
    if quote.volume_ratio is not None:
        if quote.pct_change > 0 and quote.volume_ratio > 1:
            score += 5
            reasons.append("价涨量增+5")
        elif quote.pct_change < 0 and quote.volume_ratio < 1:
            score -= 5
            reasons.append("放量下跌-5")
    return Candidate(quote, score, reasons)


def in_trading_session(now: datetime | None = None) -> bool:
    current = (now or datetime.now(CHINA_TZ)).astimezone(CHINA_TZ)
    if current.weekday() >= 5:
        return False
    value = current.time()
    return time(9, 30) <= value < time(11, 30) or time(13, 0) <= value < time(15, 0)


def format_candidate(candidate: Candidate) -> str:
    quote = candidate.quote
    why = "、".join(candidate.reasons) or "暂无技术加分"
    return f"{quote.code} {quote.name} 现价{quote.price:.2f} 涨跌{quote.pct_change:+.2f}% 成交额{quote.amount:.0f} 分数{candidate.score}（{why}）"

