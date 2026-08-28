from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Iterable
from zoneinfo import ZoneInfo
import re

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


@dataclass(slots=True)
class MinuteBar:
    code: str
    start: datetime
    open: float
    high: float
    low: float
    close: float
    volume: float
    amount: float


class MinuteBarAggregator:
    def __init__(self, max_bars_per_code: int = 120):
        self.max_bars_per_code = max(10, max_bars_per_code)
        self.current: dict[str, MinuteBar] = {}
        self.history: dict[str, list[MinuteBar]] = {}
        self._baseline: dict[str, tuple[object, float, float]] = {}

    def update(self, quote: Quote) -> MinuteBar | None:
        if not isinstance(quote.fetched_at, datetime):
            return None
        timestamp = quote.fetched_at.astimezone(CHINA_TZ) if quote.fetched_at.tzinfo else quote.fetched_at.replace(tzinfo=CHINA_TZ)
        start = timestamp.replace(second=0, microsecond=0)
        previous = self.current.get(quote.code)
        if previous and start < previous.start:
            return None
        day = start.date()
        try:
            volume = max(0.0, float(quote.volume))
            amount = max(0.0, float(quote.amount))
        except (TypeError, ValueError):
            return None
        baseline = self._baseline.get(quote.code)
        day_changed = baseline is not None and baseline[0] != day
        if day_changed:
            previous = None
            self.current.pop(quote.code, None)
        if baseline is None or day_changed:
            delta_volume = delta_amount = 0.0
        else:
            delta_volume = volume - baseline[1] if volume >= baseline[1] else volume
            delta_amount = amount - baseline[2] if amount >= baseline[2] else amount
        self._baseline[quote.code] = (day, volume, amount)
        if previous and previous.start == start:
            previous.high = max(previous.high, quote.price)
            previous.low = min(previous.low, quote.price)
            previous.close = quote.price
            previous.volume += delta_volume
            previous.amount += delta_amount
            return None
        completed = previous
        self.current[quote.code] = MinuteBar(quote.code, start, quote.price, quote.price, quote.price, quote.price, delta_volume, delta_amount)
        if completed:
            bars = self.history.setdefault(quote.code, [])
            bars.append(completed)
            del bars[:-self.max_bars_per_code]
        return completed

    def bars(self, code: str) -> list[MinuteBar]:
        return list(self.history.get(code, []))

    def reset(self) -> None:
        self.current.clear()
        self.history.clear()
        self._baseline.clear()

    def symbol_count(self) -> int:
        return len(set(self.current) | set(self.history))

    def bar_count(self) -> int:
        return sum(len(values) for values in self.history.values())


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


def simple_moving_average(values: Iterable[float], period: int) -> float | None:
    data = [float(value) for value in values]
    if period <= 0 or len(data) < period:
        return None
    return sum(data[-period:]) / period


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
        elif quote.pct_change < 0 and quote.volume_ratio > 1:
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
    name = re.sub(r"[\x00-\x1f\x7f]", "", str(quote.name or "")).strip()[:40]
    why = "、".join(candidate.reasons) or "暂无技术加分"
    metrics = []
    if quote.rsi6 is not None:
        metrics.append(f"RSI6={quote.rsi6:.1f}")
    if quote.ma5 is not None and quote.ma10 is not None and quote.ma20 is not None:
        metrics.append(f"MA5/10/20={quote.ma5:.2f}/{quote.ma10:.2f}/{quote.ma20:.2f}")
    if quote.volume_ratio is not None:
        metrics.append(f"量比={quote.volume_ratio:.2f}")
    evidence = "、".join(metrics) or "当前可用技术指标不足"
    risk_items: list[str] = []
    if any("空头" in reason or "放量下跌" in reason for reason in candidate.reasons):
        risk_items.append("均线偏弱或出现放量下跌")
    if quote.rsi6 is not None and quote.rsi6 >= 70:
        risk_items.append("RSI6处于高位，短线波动风险较高")
    if not metrics:
        risk_items.append("技术数据不足，评分参考价值有限")
    risk_text = "；".join(risk_items) if risk_items else "暂未触发机械风险项，但指标可能滞后或不完整"
    conclusion = "进入观察池，先复核日线趋势、基本面、公告和流动性；这不是买卖指令"
    return (
        f"候选：{name or quote.code}（{quote.code}）\n"
        f"行情：现价{quote.price:.2f}，涨跌{quote.pct_change:+.2f}%，成交额{quote.amount:.0f}\n"
        f"技术评分：{candidate.score}/30（规则加分：{why}）\n"
        f"技术证据：{evidence}\n"
        f"风险审核：{risk_text}\n"
        f"研究结论：{conclusion}。"
    )
