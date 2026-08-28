from __future__ import annotations

import asyncio
import json
import math
import re
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star, register
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

from .core import CHINA_TZ, MinuteBarAggregator, assess_market_context, format_candidate, in_trading_session, is_tradable, parse_codes, score_quote
from .factors import market_adjustment
from .providers import OpenAICompatibleClient, RssNewsProvider, SinaQuoteProvider, news_fingerprint
from .storage import StockStore

PLUGIN_NAME = "astrbot_stock_watch"


@register(PLUGIN_NAME, "DIO", "A股收盘选股与自选股监听", "0.8.0")
class Main(Star):
    def __init__(self, context: Context, config=None, **kwargs):
        super().__init__(context, config=config)
        self.context, self.config = context, config or {}
        data_dir = Path(get_astrbot_data_path()) / "plugin_data" / PLUGIN_NAME
        self.store = StockStore(data_dir / "stock_watch.sqlite3")
        timeout = self._float("request_timeout", 10, 3, 60)
        self.quotes = SinaQuoteProvider(
            timeout,
            str(self.config.get("tushare_url", "")),
            str(self.config.get("tushare_token", "")),
        )
        self.news = RssNewsProvider(str(self.config.get("news_rss_url", "")), timeout)
        self.llm = OpenAICompatibleClient(
            str(self.config.get("llm_base_url", "https://api.openai.com/v1")),
            str(self.config.get("llm_api_key", "")),
            str(self.config.get("llm_model", "gpt-4o-mini")),
            max(timeout, 15),
            self._float("llm_min_interval_seconds", 10, 0, 3600),
            self._int("llm_daily_request_limit", 100, 1, 10000),
        )
        self.tasks: list[asyncio.Task] = []
        self.last_daily_scan: str | None = None
        self._daily_snapshot_lock = asyncio.Lock()
        self._daily_date_alias: dict[str, str] = {}
        self._daily_retry_after: datetime | None = None
        self._annotation_task: asyncio.Task | None = None
        self._annotation_cache: dict[str, tuple[datetime, dict]] = {}
        self._last_annotation_at: datetime | None = None
        self.minute_bars = MinuteBarAggregator(self._int("minute_bar_history", 120, 10, 2000))
        self._intraday_date: str | None = None
        self._intraday_health = {
            "last_cycle_at": None,
            "last_success_at": None,
            "last_error_at": None,
            "cycles": 0,
            "successful_cycles": 0,
            "failed_cycles": 0,
            "stale_quotes": 0,
            "accepted_quotes": 0,
            "completed_bars": 0,
            "consecutive_failures": 0,
        }
        self._source_health = {
            "sina": {
                "batches": 0,
                "successes": 0,
                "failures": 0,
                "last_success_at": None,
                "last_error_at": None,
            }
        }
        self._last_screen_run_id: str | None = None
        self._last_screen_diagnostics: dict[str, int] = {}

    def _minute_signal_text(self, quote, completed) -> str:
        if not completed or not self._bool("minute_trigger_enabled", False):
            return ""
        bars = self.minute_bars.bars(quote.code)
        lookback = self._int("minute_trigger_lookback", 5, 1, 60)
        minimum = self._int("minute_trigger_min_bars", 5, 1, 120)
        consecutive = self._int("minute_trigger_consecutive_up", 3, 1, 20)
        if len(bars) < max(minimum, lookback + 1, consecutive + 1):
            return ""
        step_pct = self._float("minute_trigger_step_pct", 0.1, 0.0, 20.0)
        window = bars[-(consecutive + 1):]
        if any(current.close < previous.close * (1 + step_pct / 100) for previous, current in zip(window, window[1:])):
            return ""
        prior = bars[-(lookback + 1):-1]
        breakout_pct = self._float("minute_trigger_breakout_pct", 0.5, 0.0, 20.0)
        reference = max(bar.high for bar in prior)
        if completed.close < reference * (1 + breakout_pct / 100):
            return ""
        return (
            "分钟触发\n"
            f"{quote.code} {quote.name} {completed.start:%H:%M} 收盘{completed.close:.2f}："
            f"连续上涨{consecutive}根，突破近{lookback}根高点+{breakout_pct:.2f}%\n"
            f"依据：每根涨幅至少{step_pct:.2f}%，最新收盘高于参考高点{breakout_pct:.2f}%。\n"
            "风险：分钟级波动和假突破较多，当前规则未确认后续成交量。\n"
            "研究动作建议：复核日线趋势、量价和公告后再决定观望或跟踪；仅研究/模拟盘，不自动下单。"
        )

    async def initialize(self):
        if not self._bool("enabled", True):
            logger.info("[%s] 后台任务已关闭", PLUGIN_NAME)
            return
        self.tasks = [
            asyncio.create_task(self._daily_loop(), name=f"{PLUGIN_NAME}:daily"),
            asyncio.create_task(self._intraday_loop(), name=f"{PLUGIN_NAME}:quotes"),
            asyncio.create_task(self._news_loop(), name=f"{PLUGIN_NAME}:news"),
        ]
        logger.info("[%s] 已加载，研究/模拟盘模式=%s", PLUGIN_NAME, self._bool("paper_trading_only", True))

    async def terminate(self):
        if self._annotation_task:
            task = self._annotation_task
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            self._annotation_task = None
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        self.tasks.clear()

    def _int(self, key: str, default: int, minimum: int, maximum: int) -> int:
        try:
            return max(minimum, min(int(self.config.get(key, default)), maximum))
        except (TypeError, ValueError):
            return default

    def _float(self, key: str, default: float, minimum: float, maximum: float) -> float:
        try:
            value = float(self.config.get(key, default))
            return default if not math.isfinite(value) else max(minimum, min(value, maximum))
        except (TypeError, ValueError):
            return default

    def _bool(self, key: str, default: bool) -> bool:
        value = self.config.get(key, default)
        return value.lower() in {"1", "true", "yes", "on"} if isinstance(value, str) else bool(value)

    @staticmethod
    def _origin(event: AstrMessageEvent) -> str:
        return str(getattr(event, "unified_msg_origin", "") or "")

    def _configured_whitelist(self) -> set[str]:
        raw = self.config.get("push_whitelist", "")
        if isinstance(raw, (list, tuple, set)):
            values = raw
        else:
            values = str(raw or "").replace("，", ",").replace("\n", ",").split(",")
        return {str(value).strip() for value in values if str(value).strip()}

    def _push_allowed(self, origin: str) -> bool:
        origin = str(origin or "").strip()
        if not origin:
            return False
        configured = self._configured_whitelist()
        return "*" in configured or origin in configured or self.store.is_whitelisted(origin)

    def _message_chunks(self, text: str) -> list[str]:
        """Split long reports at line boundaries so chat adapters do not truncate them."""
        limit = self._int("push_max_chars", 3500, 500, 12000)
        lines = str(text or "").splitlines() or [""]
        chunks: list[str] = []
        current = ""
        for line in lines:
            pieces = [line[index:index + limit] for index in range(0, max(1, len(line)), limit)]
            for piece in pieces:
                candidate = piece if not current else current + "\n" + piece
                if current and len(candidate) > limit:
                    chunks.append(current)
                    current = piece
                else:
                    current = candidate
        if current or not chunks:
            chunks.append(current)
        return chunks

    @staticmethod
    def _model_text_is_research_safe(text: str) -> bool:
        """Reject model output that turns a research summary into an action instruction."""
        return not re.search(r"(?:建议|推荐|应当|适合买|考虑买|买入|卖出|止损|止盈|加仓|减仓|目标价|仓位|下单)", text or "")

    @staticmethod
    def _clean_external_text(value, limit: int) -> str:
        return re.sub(r"[\x00-\x1f\x7f]", "", str(value or "")).strip()[:limit]

    async def _push(self, origin: str, text: str) -> bool:
        if not self._push_allowed(origin):
            logger.debug("[%s] 已跳过非白名单会话推送：%s", PLUGIN_NAME, origin or "<empty>")
            return False
        try:
            for chunk in self._message_chunks(text):
                try:
                    await self.context.send_message(origin, MessageChain([Plain(chunk)]))
                except TypeError:
                    await self.context.send_message(origin, chunk)
            return True
        except Exception:
            logger.exception("[%s] 推送失败：%s", PLUGIN_NAME, origin or "<empty>")
            return False

    def _universe(self) -> list[str]:
        configured = parse_codes(str(self.config.get("universe_codes", "")))
        if configured:
            return configured
        merged: list[str] = []
        for codes in self.store.all_watch().values():
            merged.extend(codes)
        return list(dict.fromkeys(merged))

    async def _score_quotes(self, quotes, limit: int, as_of: str = ""):
        tradable = [quote for quote in quotes if is_tradable(
            quote,
            self._float("price_min", 2, 0.01, 100000),
            self._float("price_max", 80, 0.01, 100000),
        )]
        tradable.sort(key=lambda quote: quote.amount, reverse=True)
        enrich_targets = tradable[: min(300, max(80, limit * 5))]
        # 日线历史请求只对流动性靠前的有限集合执行，控制网络和内存开销。
        await self.quotes.enrich_indicators(enrich_targets, self._int("max_concurrency", 5, 1, 20), as_of)
        for quote in enrich_targets:
            bars = self.quotes.history_bars.get(quote.code, [])
            if bars:
                self.store.save_daily_bars(quote.code, bars, quote.source or "eastmoney")
        self._last_screen_diagnostics = {"input": len(quotes), "tradable": len(tradable), "enriched": sum(1 for q in enrich_targets if q.history_days >= 20)}
        scored = [score_quote(quote) for quote in tradable]
        market = assess_market_context(quotes)
        factor_url = str(self.config.get("factor_data_url", "")).strip()
        if factor_url:
            try:
                raw_factors = await self.quotes.fetch_custom_factors(factor_url, [q.code for q in enrich_targets], as_of)
                for item in scored:
                    row = raw_factors.get(item.quote.code)
                    if row:
                        item.quote.industry_score = float(row.get("industry_score")) if row.get("industry_score") is not None else None
                        item.quote.fundamental_score = float(row.get("fundamental_score")) if row.get("fundamental_score") is not None else None
                        if str(self.config.get("factor_mode", "report_only")) == "score":
                            extra = int(round((item.quote.industry_score or 0) + (item.quote.fundamental_score or 0)))
                            item.score += max(-20, min(20, extra))
                            if extra:
                                item.reasons.append(f"行业/基本面修正{extra:+d}")
            except Exception:
                logger.warning("[%s] 自定义因子源不可用，继续技术筛选", PLUGIN_NAME)
        elif str(self.config.get("factor_source", "auto")) in {"auto", "eastmoney"}:
            try:
                raw_factors = await self.quotes.fetch_eastmoney_factors([q.code for q in enrich_targets])
                for item in scored:
                    row = raw_factors.get(item.quote.code)
                    if row:
                        item.quote.fundamental_score = round(max(-10, min(10, float(row.get("roe") or 0) / 2)), 1) if row.get("roe") is not None else None
                        item.quote.industry_score = None
            except Exception:
                logger.warning("[%s] 东方财富因子源不可用，继续技术筛选", PLUGIN_NAME)
        adjustment = market_adjustment(market.regime) if str(self.config.get("factor_mode", "report_only")) == "score" else 0
        if adjustment:
            for item in scored:
                if item.score > 0:
                    item.score += adjustment
                    item.reasons.append(f"市场环境修正{adjustment:+d}")
        minimum = self._int("min_score", 10, -100, 100)
        scored.sort(key=lambda item: (item.score, item.quote.amount), reverse=True)
        qualified = [item for item in scored if item.score >= minimum and item.risk_level != "blocked"]
        fallback_limit = self._int("fallback_limit", 5, 0, 30)
        fallback = []
        if not qualified and fallback_limit and scored:
            # 只从有历史指标且未触发硬风险的项目中给出“观察候选”，不绕过风险门禁。
            fallback = [item for item in scored if item.risk_level not in {"blocked", "unknown"} and item.quote.history_days >= 20][:fallback_limit]
            for item in fallback:
                item.reasons = ["未达到最低分，列入观察候选"] + item.reasons
        self._last_screen_diagnostics.update({
            "qualified": len(qualified), "fallback": len(fallback),
            "max_score": max((item.score for item in scored), default=0), "min_score": minimum,
            "market_regime": market.regime, "market_breadth": round(market.breadth, 4),
        })
        return (qualified or fallback)[:limit]

    async def _scan(self, codes: list[str], limit: int):
        return await self._score_quotes(await self.quotes.fetch_quotes(codes), limit)

    async def _annotate_batch(self, candidates):
        annotations = await self.llm.annotate_candidates(
            candidates,
            self._int("llm_annotation_max_tokens", 800, 200, 2000),
        )
        now = datetime.now(CHINA_TZ)
        for code, annotation in annotations.items():
            self._annotation_cache[code] = (now, annotation)

    async def _annotate_batches(self, batches):
        for candidates in batches:
            await self._annotate_batch(candidates)

    def _annotation_text(self, code: str) -> str:
        cached = self._annotation_cache.get(code)
        if not cached:
            return ""
        created_at, annotation = cached
        max_age = max(60, self._int("llm_annotation_interval_seconds", 180, 30, 3600) * 2)
        if (datetime.now(CHINA_TZ) - created_at).total_seconds() > max_age:
            self._annotation_cache.pop(code, None)
            return ""
        raw_evidence = annotation.get("evidence", [])
        if not isinstance(raw_evidence, list):
            raw_evidence = []
        cleaned_evidence = [self._clean_external_text(item, 80) for item in raw_evidence[:3] if str(item).strip()]
        if any(not self._model_text_is_research_safe(item) for item in cleaned_evidence):
            return ""
        evidence = "、".join(cleaned_evidence)
        summary = self._clean_external_text(annotation.get("summary", ""), 300)
        if not self._model_text_is_research_safe(summary):
            return ""
        return f"模型补充（未验证）：{summary}；风险{self._clean_external_text(annotation.get('risk_level', 'unknown'), 20)}；参考：{evidence or '未提供'}"

    def _cost_signal(self, quote, cost_price: float | None) -> str:
        if not cost_price or not math.isfinite(cost_price) or not math.isfinite(quote.price) or quote.price <= 0:
            return ""
        change = (quote.price - cost_price) / cost_price * 100
        profit = self._float("cost_profit_threshold_pct", 5.0, 0.1, 1000)
        risk = self._float("cost_risk_threshold_pct", 5.0, 0.1, 1000)
        label = f"{quote.name or quote.code}（{quote.code}）"
        if change >= profit:
            return (
                f"成本观察：{label} 现价{quote.price:.2f}，成本{cost_price:.2f}，相对成本{change:+.2f}%\n"
                f"依据：相对成本达到 +{profit:.2f}% 的收益阈值事件。\n"
                "风险：成本价只是单点参考，未计手续费和滑点，行情可能继续波动或反转。\n"
                "研究动作建议：结合 RSI、均线、量价和公告复核，记录后续观察结论；仅研究/模拟盘，不自动下单。"
            )
        if change <= -risk:
            return (
                f"成本观察：{label} 现价{quote.price:.2f}，成本{cost_price:.2f}，相对成本{change:+.2f}%\n"
                f"依据：相对成本达到 -{risk:.2f}% 的亏损阈值事件。\n"
                "风险：成本价不代表合理价值，未计手续费和滑点，弱势行情可能继续下探。\n"
                "研究动作建议：先复核日线趋势、基本面和自身风险承受，记录观望或继续研究的理由；仅研究/模拟盘，不自动下单。"
            )
        return ""

    def _fresh_quotes(self, quotes):
        now = datetime.now(CHINA_TZ)
        max_age = max(30, self._int("quote_interval_seconds", 30, 10, 600) * 2)
        fresh = []
        for quote in quotes:
            fetched_at = quote.fetched_at
            if not isinstance(fetched_at, datetime):
                continue
            if fetched_at.tzinfo is None:
                fetched_at = fetched_at.replace(tzinfo=CHINA_TZ)
            if 0 <= (now - fetched_at).total_seconds() <= max_age:
                fresh.append(quote)
        return fresh

    def _health_text(self) -> str:
        health = self._intraday_health
        def display(value):
            if not value:
                return "暂无"
            try:
                return datetime.fromisoformat(value).astimezone(CHINA_TZ).strftime("%m-%d %H:%M:%S")
            except (TypeError, ValueError):
                return str(value)
        threshold = self._int("intraday_failure_threshold", 0, 0, 100)
        suppressed = threshold > 0 and health["consecutive_failures"] >= threshold
        state = "信号推送暂缓（行情源连续失败）" if suppressed else "正常"
        sina = self._source_health["sina"]
        return (
            f"行情健康：{state}\n"
            f"最近成功：{display(health['last_success_at'])}；最近错误：{display(health['last_error_at'])}；最近轮询：{display(health['last_cycle_at'])}\n"
            f"轮询 {health['cycles']} 次，成功 {health['successful_cycles']} 次，失败 {health['failed_cycles']} 次，"
            f"连续失败 {health['consecutive_failures']} 次\n"
            f"最近累计接收 {health['accepted_quotes']} 条有效行情，过期/丢弃 {health['stale_quotes']} 条；"
            f"分钟线 {self.minute_bars.symbol_count()} 只股票/{self.minute_bars.bar_count()} 根已完成\n"
            f"新浪行情批次：{sina['successes']}/{sina['batches']} 成功，失败 {sina['failures']} 次；"
            f"最近错误：{display(sina['last_error_at'])}"
        )

    async def _calendar_open(self, trade_date: str) -> bool:
        cached = self.store.calendar_status(trade_date)
        if cached is not None:
            return cached
        try:
            online = await self.quotes.fetch_trade_calendar(trade_date)
        except Exception:
            online = None
        if online is not None:
            self.store.save_calendar(trade_date, online, "tushare")
            return online
        approximate = datetime.strptime(trade_date, "%Y-%m-%d").weekday() < 5
        self.store.save_calendar(trade_date, approximate, "weekday_approximate")
        return approximate

    async def _daily_snapshot(self, trade_date: str) -> tuple[list, bool, str]:
        """Return a snapshot, whether it was fetched now, and its actual trade date."""
        lookup_date = self._daily_date_alias.get(trade_date, trade_date)
        cached = self.store.daily_quotes(lookup_date)
        if cached:
            return cached, False, lookup_date
        async with self._daily_snapshot_lock:
            # Re-check after waiting so the background loop and manual command do not fetch twice.
            lookup_date = self._daily_date_alias.get(trade_date, trade_date)
            cached = self.store.daily_quotes(lookup_date)
            if cached:
                self._daily_retry_after = None
                return cached, False, lookup_date
            try:
                result = await self.quotes.fetch_market_snapshot_result(
                    str(self.config.get("daily_market_url", "")), trade_date
                )
                self.store.update_provider_health(result.source or "unknown", bool(result.quotes), result.quality)
            except Exception:
                self.store.update_provider_health("unknown", False, "unknown", "fetch failed")
                result = None
            if result is None or not result.quotes:
                # A transient source failure should not discard a usable prior snapshot.
                actual_date = self.store.latest_daily_trade_date(trade_date)
                if actual_date:
                    fallback = self.store.daily_quotes(actual_date)
                    if fallback:
                        self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                        logger.warning("[%s] 当日快照不可用，使用最近缓存交易日：%s", PLUGIN_NAME, actual_date)
                        return fallback, False, actual_date
                if result is None:
                    raise RuntimeError("daily snapshot source unavailable")
                return [], True, trade_date
            actual_date = result.trade_date
            if not actual_date:
                cached_date = self.store.latest_daily_trade_date(trade_date)
                if cached_date:
                    self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                    return self.store.daily_quotes(cached_date), False, cached_date
                self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                logger.warning("[%s] 行情源未提供真实交易日，拒绝将快照伪装为请求日期", PLUGIN_NAME)
                return [], True, trade_date
            saved = self.store.save_daily_quotes(
                actual_date,
                result.quotes,
                self._int("daily_cache_keep_days", 180, 7, 730),
            )
            logger.info("[%s] 全市场日快照已缓存：%s 只", PLUGIN_NAME, saved)
            if actual_date == trade_date:
                self._daily_date_alias[trade_date] = actual_date
                self._daily_retry_after = None
                fetched = True
            else:
                self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                fetched = False
            return self.store.daily_quotes(actual_date), fetched, actual_date

    async def _daily_candidates(self, limit: int):
        if self._bool("daily_cache_enabled", True):
            trade_date = datetime.now(CHINA_TZ).date().isoformat()
            try:
                cached, _, actual_date = await self._daily_snapshot(trade_date)
            except Exception:
                logger.exception("[%s] 全市场日快照失败，退回股票池扫描", PLUGIN_NAME)
                cached = []
                actual_date = trade_date
            if cached:
                return await self._score_quotes(cached, limit, actual_date or trade_date)
        return await self._scan(self._universe(), limit)

    def _record_screen(self, requested_date: str, actual_date: str, source: str, quotes, candidates, status: str = "completed", quality: str = "good", error: str | None = None) -> str:
        run_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        self.store.save_screen_bundle((run_id, "daily_screen", requested_date, actual_date, source, now, now, len(quotes), len(candidates), status, quality, error), candidates)
        self._last_screen_run_id = run_id
        return run_id

    async def _daily_loop(self):
        while True:
            now = datetime.now(CHINA_TZ)
            target = str(self.config.get("daily_scan_time", "15:10"))
            retry_ready = self._daily_retry_after is None or now >= self._daily_retry_after
            if now.strftime("%H:%M") >= target and now.hour < 16 and self.last_daily_scan != now.date().isoformat() and retry_ready and await self._calendar_open(now.date().isoformat()):
                job_key = f"daily_screen:{now.date().isoformat()}"
                if not self.store.begin_job(job_key, "daily_screen", now.date().isoformat()):
                    await asyncio.sleep(20)
                    continue
                try:
                    requested_date = now.date().isoformat()
                    candidates = await self._daily_candidates(self._int("candidate_limit", 30, 1, 100))
                    actual_date = self.store.latest_daily_trade_date(requested_date) or requested_date
                    cached_for_record = self.store.daily_quotes(actual_date) if actual_date else []
                    source = next((str(item.source) for item in cached_for_record if getattr(item, "source", "")), "unknown")
                    self._record_screen(requested_date, actual_date, source, cached_for_record, candidates)
                    lines = [
                        "收盘选股（仅研究/模拟盘）",
                        "筛选口径：价格区间过滤 → 成交额流动性预筛选 → RSI6/均线/量价规则评分 → 人工复核",
                        *[format_candidate(item) for item in candidates],
                    ]
                    if not candidates:
                        d = self._last_screen_diagnostics
                        lines.append(f"暂无达标候选：可交易{d.get('tradable', 0)}只，补齐历史指标{d.get('enriched', 0)}只，最高分{d.get('max_score', 0)}，最低门槛{d.get('min_score', 15)}。")
                    push_failed = False
                    for origin in self.store.subscriptions():
                        if not await self._push(origin, "\n".join(lines)):
                            push_failed = True
                    if not push_failed and self._daily_retry_after is None:
                        self.last_daily_scan = now.date().isoformat()
                    self.store.finish_job(job_key, "completed")
                except Exception:
                    self.store.finish_job(job_key, "failed", "收盘扫描异常")
                    logger.exception("[%s] 收盘扫描失败", PLUGIN_NAME)
            await asyncio.sleep(20)

    async def _intraday_loop(self):
        while True:
            cycle_failed = False
            health_counted = False
            try:
                if self._annotation_task and self._annotation_task.done():
                    task = self._annotation_task
                    self._annotation_task = None
                    try:
                        await task
                    except asyncio.CancelledError:
                        logger.info("[%s] 模型盘中解释任务已取消", PLUGIN_NAME)
                    except Exception:
                        logger.exception("[%s] 模型盘中解释失败", PLUGIN_NAME)
                if in_trading_session():
                    today = datetime.now(CHINA_TZ).date().isoformat()
                    if self._intraday_date != today:
                        self.minute_bars.reset()
                        self._intraday_date = today
                    watch = self.store.all_watch()
                    watch_details = self.store.all_watch_details()
                    active = {origin: list(codes) for origin, codes in watch.items() if self.store.is_subscribed(origin) and codes}
                    union = list(dict.fromkeys(code for codes in active.values() for code in codes))
                    if self._bool("auto_watch_candidates", True):
                        candidate_codes = [item["code"] for item in self.store.latest_screen_candidates(self._int("candidate_limit", 30, 1, 100)) if item.get("code")]
                        union.extend(candidate_codes)
                        for origin in active:
                            active[origin] = list(dict.fromkeys(active[origin] + candidate_codes))
                        union = list(dict.fromkeys(union))[: self._int("intraday_focus_limit", 200, 10, 500)]
                    if union:
                        health = self._intraday_health
                        health["cycles"] += 1
                        health["last_cycle_at"] = datetime.now(CHINA_TZ).isoformat()
                        raw_quotes = []
                        for start in range(0, len(union), 100):
                            source = self._source_health["sina"]
                            source["batches"] += 1
                            try:
                                raw_quotes.extend(await self.quotes.fetch_quotes(union[start:start + 100]))
                                source["successes"] += 1
                                source["last_success_at"] = datetime.now(CHINA_TZ).isoformat()
                            except Exception:
                                cycle_failed = True
                                source["failures"] += 1
                                source["last_error_at"] = datetime.now(CHINA_TZ).isoformat()
                                logger.exception("[%s] 盘中行情分批抓取失败：批次 %s", PLUGIN_NAME, start // 100 + 1)
                        quotes = self._fresh_quotes(raw_quotes)
                        health["stale_quotes"] += max(0, len(raw_quotes) - len(quotes))
                        health["accepted_quotes"] += len(quotes)
                        minute_signals = {}
                        completed_codes = set()
                        if self._bool("minute_enabled", True):
                            for quote in quotes:
                                completed = self.minute_bars.update(quote)
                                if completed is not None:
                                    completed_codes.add(quote.code)
                                    health["completed_bars"] += 1
                                    signal = self._minute_signal_text(quote, completed)
                                    if signal:
                                        minute_signals[quote.code] = signal
                        if quotes:
                            if cycle_failed:
                                health["failed_cycles"] += 1
                                health["consecutive_failures"] += 1
                                health["last_error_at"] = datetime.now(CHINA_TZ).isoformat()
                                health_counted = True
                            quotes_by_code = {quote.code: quote for quote in quotes}
                            candidates = await self._score_quotes(quotes, len(quotes))
                            by_code = {candidate.quote.code: candidate for candidate in candidates}
                            if not cycle_failed:
                                health["successful_cycles"] += 1
                                health["last_success_at"] = datetime.now(CHINA_TZ).isoformat()
                                health["consecutive_failures"] = 0
                                health_counted = True
                            if self._bool("llm_annotation_enabled", False) and by_code and not self._annotation_task:
                                now = datetime.now(CHINA_TZ)
                                interval = self._int("llm_annotation_interval_seconds", 180, 30, 3600)
                                if not self._last_annotation_at or (now - self._last_annotation_at).total_seconds() >= interval:
                                    limit = self._int("llm_annotation_limit", 10, 5, 20)
                                    batches = []
                                    for codes in active.values():
                                        top = [by_code[code] for code in codes if code in by_code]
                                        top.sort(key=lambda item: (item.score, item.quote.amount), reverse=True)
                                        if top:
                                            batches.append(top[:limit])
                                    self._last_annotation_at = now
                                    if batches:
                                        self._annotation_task = asyncio.create_task(self._annotate_batches(batches))
                            for origin, codes in active.items():
                                for code in codes:
                                    quote = quotes_by_code.get(code)
                                    candidate = by_code.get(code)
                                    minute_signal = minute_signals.get(code, "")
                                    cost_signal = self._cost_signal(quote, watch_details.get(origin, {}).get(code)) if quote else ""
                                    is_minute = bool(minute_signal and not candidate and not cost_signal)
                                    if quote and not candidate:
                                        self.store.reset_confirmation(origin, code)
                                    if quote and code in completed_codes and not is_minute:
                                        self.store.reset_confirmation(origin, "minute:" + code)
                                    if not candidate and not cost_signal and not minute_signal:
                                        continue
                                    if candidate and candidate.risk_level in {"blocked", "unknown"} and not cost_signal and not minute_signal:
                                        continue
                                    threshold = self._int("intraday_failure_threshold", 0, 0, 100)
                                    if threshold > 0 and health["consecutive_failures"] >= threshold:
                                        continue
                                    if not cost_signal and self._bool("confirmation_enabled", False):
                                        confirmation_code = "minute:" + code if is_minute else code
                                        confirmed = self.store.observe_confirmation(
                                            origin,
                                            confirmation_code,
                                            self._int("confirmation_periods", 2, 1, 5),
                                            self._int("confirmation_max_gap_seconds", 90, 30, 600),
                                        )
                                        if not confirmed:
                                            continue
                                    claim_time = datetime.now(timezone.utc)
                                    if not self.store.claim_signal(origin, code, 600, now=claim_time):
                                        continue
                                    if cost_signal:
                                        text = cost_signal
                                    elif is_minute:
                                        text = minute_signal
                                    else:
                                        prefix = "盘中观察信号（需人工复核）" if candidate.risk_level == "watch_only" else "盘中信号"
                                        text = prefix + "（仅研究/模拟盘）\n" + format_candidate(candidate)
                                        annotation = self._annotation_text(code)
                                        if annotation:
                                            text += "\n" + annotation
                                    sent = await self._push(origin, text)
                                    if not sent:
                                        self.store.release_signal(origin, code, claimed_at=claim_time)
                        else:
                            health["failed_cycles"] += 1
                            health["consecutive_failures"] += 1
                            health["last_error_at"] = datetime.now(CHINA_TZ).isoformat()
                            health_counted = True
            except Exception:
                health = self._intraday_health
                if not health_counted:
                    health["failed_cycles"] += 1
                    health["consecutive_failures"] += 1
                    health["last_error_at"] = datetime.now(CHINA_TZ).isoformat()
                logger.exception("[%s] 盘中监听失败", PLUGIN_NAME)
            await asyncio.sleep(self._int("quote_interval_seconds", 30, 10, 600))

    async def _news_loop(self):
        while True:
            try:
                items = await self.news.fetch()
                fresh = [item for item in items if self.store.mark_news_seen(news_fingerprint(item))]
                if fresh:
                    summary = ""
                    if self._bool("llm_enabled", False):
                        try:
                            summary = self._clean_external_text(await self.llm.summarize(fresh), 3000)
                            if not self._model_text_is_research_safe(summary):
                                summary = ""
                        except Exception:
                            logger.exception("[%s] 模型摘要失败", PLUGIN_NAME)
                    text = summary or "\n".join(f"资讯：{item.title}\n{item.link}" for item in fresh[:5])
                    for origin in self.store.subscriptions():
                        await self._push(origin, "市场故事提醒（仅供研究）\n" + text)
            except Exception:
                logger.exception("[%s] 新闻监听失败", PLUGIN_NAME)
            await asyncio.sleep(self._int("news_interval_seconds", 180, 30, 3600))

    @filter.command("选股", alias={"收盘选股"})
    async def pick(self, event: AstrMessageEvent, count: int = 0):
        try:
            candidates = await self._scan(self._universe(), max(1, min(int(count or self._int("candidate_limit", 30, 1, 100)), 100)))
            lines = [
                "选股结果（仅研究/模拟盘）",
                "筛选口径：价格区间过滤 → 成交额流动性预筛选 → RSI6/均线/量价规则评分 → 人工复核",
                *[format_candidate(item) for item in candidates],
            ]
            if not candidates:
                lines.append("暂无结果。请先配置 universe_codes 或添加自选股。")
            yield event.plain_result("\n".join(lines))
        except Exception:
            logger.exception("[%s] 手动选股失败", PLUGIN_NAME)
            yield event.plain_result("选股暂时失败，请检查行情接口和插件配置。")

    @filter.command("股票帮助", alias={"选股帮助", "股票指令"})
    async def stock_help(self, event: AstrMessageEvent):
        yield event.plain_result(
            "A股研究助手指令\n"
            "/全市场选股 或 /股票同步：同步全市场并生成候选\n"
            "/选股 [数量]：按配置股票池选股\n"
            "/候选池：查看最近保存的候选和风险状态\n"
            "/行情 600000：查看单只股票指标和参考价位\n"
            "/自选 添加 600000 [成本价]：加入自选\n"
            "/自选 删除 600000：移除自选\n"
            "/监听 开启|关闭|状态：控制盘中和故事提醒\n"
            "/白名单 开启|关闭|列表：管理推送会话\n"
            "/研究状态 或 /数据质量：查看运行、来源和质量\n"
            "/验证 [天数] 或 /回放 [天数]：回放候选后续行情\n"
            "/故事 [关键词]：查看新闻故事\n"
            "所有结果仅供研究和模拟盘，不自动下单。"
        )

    @filter.command("全市场选股", alias={"全市场同步", "全市场股票同步", "股票同步"})
    async def market_sync(self, event: AstrMessageEvent, count: int = 0):
        """Manually fetch/cache today's full-market snapshot and score it immediately."""
        limit = max(1, min(int(count or self._int("candidate_limit", 30, 1, 100)), 100))
        trade_date = datetime.now(CHINA_TZ).date().isoformat()
        try:
            if self._bool("daily_cache_enabled", True):
                quotes, fetched, actual_date = await self._daily_snapshot(trade_date)
                source = ("已同步交易日 " if fetched else "已使用交易日 ") + actual_date + " 全市场数据"
            else:
                result = await self.quotes.fetch_market_snapshot_result(
                    str(self.config.get("daily_market_url", "")), trade_date
                )
                actual_date = result.trade_date
                if not actual_date:
                    yield event.plain_result("行情源没有返回真实交易日，已拒绝把数据标记为今天；请使用 Tushare 或先同步可验证的缓存。")
                    return
                quotes = result.quotes
                source = "已抓取交易日 " + actual_date + " 全市场数据（未启用缓存）"
            if not quotes:
                yield event.plain_result(
                    f"未找到可用的全市场日行情（请求日期：{trade_date}）。\n"
                    "可能是 Tushare 尚未发布该日期数据，或行情接口暂时不可用。"
                )
                return
            candidates = await self._score_quotes(quotes, limit, actual_date)
            self._record_screen(trade_date, actual_date, "cache/eastmoney/tushare", quotes, candidates)
            lines = [
                f"{source}：{len(quotes)} 只",
                "全市场选股结果（仅研究/模拟盘）",
                f"市场环境：{self._last_screen_diagnostics.get('market_regime', 'unknown')}（上涨占比{self._last_screen_diagnostics.get('market_breadth', 0):.1%}）",
                "筛选口径：硬过滤 → 趋势/动量/量价/波动评分 → 市场环境修正 → 人工复核",
            ]
            lines.extend(format_candidate(item) for item in candidates)
            if not candidates:
                d = self._last_screen_diagnostics
                lines.append(f"暂无达标候选：可交易{d.get('tradable', 0)}只，成功补齐历史指标{d.get('enriched', 0)}只，最高分{d.get('max_score', 0)}，最低门槛{d.get('min_score', self._int('min_score', 10, -100, 100))}。")
                lines.append("若想扩大观察范围，可把 min_score 调低；指标补齐数为 0 时请检查行情接口限流或配置。")
            yield event.plain_result("\n".join(lines))
        except Exception:
            logger.exception("[%s] 手动全市场同步失败", PLUGIN_NAME)
            yield event.plain_result("全市场同步失败，请检查行情接口和插件配置。")

    @filter.command("候选池", alias={"候选详情"})
    async def candidate_pool(self, event: AstrMessageEvent, count: int = 0):
        rows = self.store.latest_screen_candidates(max(1, min(int(count or self._int("candidate_limit", 30, 1, 100)), 100)))
        if not rows:
            yield event.plain_result("暂无已保存候选池，请先执行 /全市场选股。")
            return
        lines = ["最近候选池（仅研究）"]
        for row in rows:
            flags = "、".join(json.loads(row.get("risk_flags") or "[]")) or "无"
            lines.append(f"{row['name']}（{row['code']}） 评分{row['score']}/{row['score_max']} 风险{row['risk_level']}（{flags}）")
        lines.append(f"数据日期：{rows[0].get('actual_trade_date') or '未知'}；来源：{rows[0].get('source') or '未知'}")
        yield event.plain_result("\n".join(lines))

    @filter.command("研究状态", alias={"数据质量"})
    async def research_status(self, event: AstrMessageEvent):
        runs = self.store.recent_screen_runs(5)
        if not runs:
            yield event.plain_result("研究状态：尚未运行收盘扫描。\n盘中监听只输出白名单会话，且不会自动下单。")
            return
        latest = runs[0]
        yield event.plain_result(
            f"研究状态：{latest['status']}\n"
            f"最近运行：{latest['started_at']}\n"
            f"请求日期：{latest['requested_date']}；实际交易日：{latest.get('actual_trade_date') or '未知'}\n"
            f"来源：{latest['source'] or '未知'}；行情{latest['quote_count']}条；候选{latest['candidate_count']}条\n"
            f"数据质量：{latest['quality']}\n"
            "边界：模型只做摘要解释，价位由规则计算；仅研究/模拟盘，不提供订单或自动交易。"
        )

    @filter.command("验证", alias={"结果", "回放"})
    async def evaluate(self, event: AstrMessageEvent, horizon: int = 5):
        rows = self.store.latest_screen_candidates(100)
        if not rows:
            yield event.plain_result("暂无候选运行记录，先执行 /全市场选股。")
            return
        horizon = max(1, min(int(horizon or 5), 20))
        as_of = str(rows[0].get("actual_trade_date") or "")
        future_dates = self.store.daily_trade_dates(as_of, horizon)
        if len(future_dates) < horizon:
            yield event.plain_result(f"验证暂不可用：候选日期 {as_of} 之后只有 {len(future_dates)} 个完整交易日，至少需要 {horizon} 个。")
            return
        run_id = str(rows[0].get("run_id") or "")
        evaluated = 0
        for row in rows:
            base = self.store.daily_quotes(as_of)
            base_quote = next((q for q in base if q.code == row["code"]), None)
            future_bars = self.store.daily_bars(row["code"], after=as_of)[:horizon]
            future = [item for item in future_bars]
            if not base_quote or len(future) < horizon:
                continue
            last = future[-1]
            ret = (last["close"] - base_quote.price) / base_quote.price * 100 if base_quote.price else None
            highs = [(item["high"] - base_quote.price) / base_quote.price * 100 for item in future]
            lows = [(item["low"] - base_quote.price) / base_quote.price * 100 for item in future]
            self.store.save_result_evaluation(uuid.uuid4().hex, run_id, row["code"], as_of, horizon, "complete", last["close"], ret, max(highs), min(lows), None, True)
            evaluated += 1
        yield event.plain_result(f"回放验证（仅研究）：基准日 {as_of}，周期 {horizon} 日，完成 {evaluated} 条。结果已写入本地历史；不代表未来收益。")

    @filter.command("自选")
    async def watch(self, event: AstrMessageEvent, action: str = "", code: str = "", cost: str = ""):
        origin, action = self._origin(event), str(action or "").strip().lower()
        raw_parts = (str(code or "") + " " + str(cost or "")).replace("，", " ").replace(",", " ").split()
        codes = parse_codes(raw_parts[:1])
        cost_price = None
        if len(raw_parts) > 1:
            try:
                parsed = float(raw_parts[1])
                if math.isfinite(parsed) and parsed > 0:
                    cost_price = parsed
            except (TypeError, ValueError):
                cost_price = None
        if action in {"添加", "add"} and codes:
            if len(raw_parts) > 1 and cost_price is None:
                yield event.plain_result("用法：/自选 添加 600000 [成本价]")
                return
            code_value = codes[0]
            stock_name = None
            try:
                matched = await self.quotes.fetch_quotes([code_value])
                if matched and matched[0].name.strip():
                    stock_name = matched[0].name.strip()
            except Exception:
                logger.debug("[%s] 添加自选时获取股票名称失败：%s", PLUGIN_NAME, code_value)
            ok = self.store.add_watch(
                origin,
                code_value,
                self._int("watchlist_limit", 100, 1, 1000),
                cost_price,
                stock_name,
            )
            suffix = f"，成本价 {cost_price:.2f}" if cost_price else ""
            label = f"{stock_name}（{code_value}）" if stock_name else code_value
            yield event.plain_result(("已加入自选：" if ok else "添加失败，可能已达到数量上限：") + label + suffix)
        elif action in {"删除", "移除", "del", "remove"} and codes:
            existing = {item_code: item_name for item_code, item_name, _ in self.store.list_watch_details_with_names(origin)}
            code_value = codes[0]
            label = f"{existing.get(code_value) or code_value}（{code_value}）" if existing.get(code_value) else code_value
            yield event.plain_result(("已移除：" if self.store.remove_watch(origin, code_value) else "自选中没有：") + label)
        else:
            current = self.store.list_watch_details_with_names(origin)
            values = [f"{name or code}（{code}）" + (f"(成本{cost:.2f})" if cost else "") for code, name, cost in current]
            yield event.plain_result("自选股：" + ("、".join(values) if values else "暂无。用 /自选 添加 600000 [成本价]"))

    @filter.command("监听")
    async def listen(self, event: AstrMessageEvent, action: str = "状态"):
        origin, action = self._origin(event), str(action or "状态").strip().lower()
        if action in {"开启", "开", "on", "start"}:
            if not self._push_allowed(origin):
                yield event.plain_result("当前会话不在推送白名单，请先执行 /白名单 开启。")
                return
            self.store.set_subscription(origin, True)
            yield event.plain_result("已开启盘中行情和故事提醒。")
        elif action in {"关闭", "关", "off", "stop"}:
            self.store.set_subscription(origin, False)
            yield event.plain_result("已关闭提醒。")
        else:
            yield event.plain_result(
                "监听状态：{}\n白名单：{}\n当前会话标识：{}".format(
                    "开启" if self.store.is_subscribed(origin) else "关闭",
                    "已加入" if self._push_allowed(origin) else "未加入",
                    origin or "无法读取",
                )
                + "\n" + self._health_text()
            )

    @filter.command("状态", alias={"行情状态"})
    async def status(self, event: AstrMessageEvent):
        origin = self._origin(event)
        yield event.plain_result(
            f"监听状态：{'开启' if self.store.is_subscribed(origin) else '关闭'}\n"
            f"白名单：{'已加入' if self._push_allowed(origin) else '未加入'}\n"
            + self._health_text()
        )

    @filter.command("白名单")
    async def whitelist(self, event: AstrMessageEvent, action: str = "状态"):
        origin, action = self._origin(event), str(action or "状态").strip().lower()
        if not origin:
            yield event.plain_result("无法读取当前会话标识，暂不能设置白名单。")
            return
        if action in {"开启", "开", "on", "add", "加入"}:
            self.store.set_whitelist(origin, True)
            yield event.plain_result("已加入推送白名单。\n会话标识：" + origin)
        elif action in {"关闭", "关", "off", "remove", "移除"}:
            self.store.set_whitelist(origin, False)
            yield event.plain_result("已移出推送白名单。")
        elif action in {"列表", "list"}:
            values = sorted(set(self._configured_whitelist()) | set(self.store.whitelist()))
            yield event.plain_result("推送白名单：\n" + ("\n".join(values) if values else "暂无（后台推送默认关闭）"))
        else:
            yield event.plain_result(
                "当前会话白名单：{}\n会话标识：{}\n用法：/白名单 开启|关闭|列表|状态".format(
                    "已加入" if self._push_allowed(origin) else "未加入", origin
                )
            )

    @filter.command("行情")
    async def quote(self, event: AstrMessageEvent, code: str = ""):
        codes = parse_codes(code)
        if not codes:
            yield event.plain_result("用法：/行情 600000")
            return
        try:
            quotes = await self.quotes.fetch_quotes(codes[:10])
            yield event.plain_result("\n".join(format_candidate(score_quote(item)) for item in quotes) or "没有拿到行情，可能是接口限流。")
        except Exception:
            logger.exception("[%s] 行情查询失败", PLUGIN_NAME)
            yield event.plain_result("行情查询失败，请稍后重试。")

    @filter.command("故事")
    async def stories(self, event: AstrMessageEvent, keyword: str = ""):
        try:
            items = await self.news.fetch()
            keyword = str(keyword or "").strip()
            if keyword:
                items = [item for item in items if keyword.lower() in (item.title + item.summary).lower()]
            if not items:
                yield event.plain_result("暂时没有匹配的故事；请先配置 news_rss_url。")
                return
            if self._bool("llm_enabled", False):
                try:
                    summary = self._clean_external_text(await self.llm.summarize(items[:10]), 3000)
                    if summary and self._model_text_is_research_safe(summary):
                        yield event.plain_result(summary)
                        return
                except Exception:
                    logger.exception("[%s] 手动故事摘要失败", PLUGIN_NAME)
            yield event.plain_result("\n".join(f"{item.title}\n{item.link}" for item in items[:10]))
        except Exception:
            logger.exception("[%s] 故事查询失败", PLUGIN_NAME)
            yield event.plain_result("故事查询失败，请检查 RSS 地址。")
