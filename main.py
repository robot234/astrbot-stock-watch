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

from .core import CHINA_TZ, Candidate, FactorOverlay, MinuteBarAggregator, PricePlan, assess_market_context, format_candidate, in_trading_session, is_tradable, parse_codes, score_quote
from .factors import fundamental_score, industry_strength, market_adjustment
from .providers import OpenAICompatibleClient, RssNewsProvider, SinaQuoteProvider, news_fingerprint
from .storage import StockStore

PLUGIN_NAME = "astrbot_stock_watch"


@register(PLUGIN_NAME, "DIO", "A股收盘选股与自选股监听", "0.10.0")
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
        self._last_screen_diagnostics: dict[str, object] = {}

    @staticmethod
    def _price_plan_from_payload(payload: str) -> PricePlan | None:
        try:
            raw = json.loads(payload or "{}")
            fields = PricePlan.__dataclass_fields__
            if not isinstance(raw, dict) or not raw.get("quality"):
                return None
            return PricePlan(**{key: raw.get(key) for key in fields})
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _price_state_for_plan(quote, plan: PricePlan | None) -> str:
        """Evaluate the immutable closing plan against a fresh intraday quote."""
        if not plan or plan.quality != "good" or quote.price <= 0:
            return "unknown"
        if plan.invalidation is not None and quote.price <= plan.invalidation:
            return "invalidated"
        if plan.sell_low is not None and quote.price >= plan.sell_low:
            return "near_sell"
        if plan.confirmation is not None and quote.price >= plan.confirmation and quote.volume_ratio is not None and quote.volume_ratio >= 1:
            return "confirmed"
        if plan.attention_low is not None and plan.attention_high is not None and plan.attention_low <= quote.price <= plan.attention_high:
            return "in_attention"
        return "between"

    def _snapshot_context(self, requested_date: str, actual_date: str, quotes: list | None = None) -> dict:
        meta = self.store.snapshot_meta(actual_date) or {}
        return {
            "requested_date": requested_date,
            "actual_date": actual_date,
            "source": str(meta.get("source") or next((str(q.source) for q in (quotes or []) if getattr(q, "source", "")), "unknown")),
            "quality": str(meta.get("quality") or "unknown"),
            "complete": bool(meta.get("complete")),
            "note": str(meta.get("note") or ""),
        }

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
        return "*" in configured or origin in configured or (self._bool("allow_self_whitelist", False) and self.store.is_whitelisted(origin))

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
        return not re.search(r"(?:建议|推荐|应当|适合买|考虑买|买入|卖出|止损|止盈|加仓|减仓|目标价|仓位|下单|\\bbuy\\b|\\bsell\\b|\\border\\b|target\\s*price|position)", text or "", flags=re.I)

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

    async def _score_quotes(self, quotes, limit: int, as_of: str = "", include_factors: bool = True):
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
        full_market = self.store.daily_quotes(as_of) if as_of else []
        market_minimum = self._int("market_min_snapshot_size", 4000, 1000, 10000)
        market_quotes = full_market if len(full_market) >= market_minimum else quotes
        market = assess_market_context(market_quotes)
        if as_of:
            self.store.save_market_context(as_of, {
                "regime": market.regime, "breadth": market.breadth, "advancing": market.advancing,
                "declining": market.declining, "total_amount": market.total_amount, "evidence": market.evidence,
            }, "daily_snapshot" if len(full_market) >= market_minimum else "input_subset", "good" if len(full_market) >= market_minimum else "partial")
        factor_url = str(self.config.get("factor_data_url", "")).strip() if include_factors else ""
        factor_source = str(self.config.get("factor_source", "auto")).strip().lower() if include_factors else "disabled"
        factor_mode = str(self.config.get("factor_mode", "report_only")).strip().lower()
        historical_factor_date = bool(as_of and as_of < datetime.now(CHINA_TZ).date().isoformat())
        raw_factors: dict[str, dict] = {}
        factor_name, factor_quality = "", "unknown"
        if factor_url:
            try:
                raw_factors = await self.quotes.fetch_custom_factors(factor_url, [q.code for q in enrich_targets], as_of)
                factor_name, factor_quality = "custom", "good" if raw_factors else "unknown"
            except Exception:
                logger.warning("[%s] 自定义因子源不可用，继续技术筛选", PLUGIN_NAME)
        if not raw_factors and factor_source == "tushare" and self.quotes.tushare_token:
            try:
                raw_factors = await self.quotes.fetch_tushare_factors([q.code for q in enrich_targets], as_of)
                factor_name, factor_quality = "tushare", "partial" if raw_factors else "unknown"
            except Exception:
                logger.warning("[%s] Tushare 因子源不可用，继续技术筛选", PLUGIN_NAME)
        if not raw_factors and not historical_factor_date and factor_source in {"auto", "eastmoney", "custom", "tushare"}:
            try:
                raw_factors = await self.quotes.fetch_eastmoney_factors([q.code for q in enrich_targets])
                factor_name, factor_quality = "eastmoney", "partial" if raw_factors else "unknown"
            except Exception:
                logger.warning("[%s] 东方财富因子源不可用，继续技术筛选", PLUGIN_NAME)
        if factor_source == "auto" and self.quotes.tushare_token:
            missing_codes = [quote.code for quote in enrich_targets if quote.code not in raw_factors]
            if missing_codes:
                try:
                    tushare_rows = await self.quotes.fetch_tushare_factors(missing_codes, as_of)
                    if tushare_rows:
                        raw_factors.update(tushare_rows)
                        factor_name = f"{factor_name}+tushare" if factor_name else "tushare"
                        factor_quality = "partial"
                except Exception:
                    logger.warning("[%s] Tushare 因子补充不可用", PLUGIN_NAME)
        if as_of:
            cached_rows = self.store.factor_snapshots(as_of)
            missing_codes = [quote.code for quote in enrich_targets if quote.code not in raw_factors]
            cache_hits = 0
            for code in missing_codes:
                if code in cached_rows:
                    raw_factors[code] = cached_rows[code]
                    cache_hits += 1
            if cache_hits:
                factor_name = f"{factor_name}+cache" if factor_name else "cache"
                factor_quality = "cached" if factor_name == "cache" else "partial"
        if as_of and raw_factors:
            self.store.save_factor_snapshots(as_of, raw_factors, factor_name or factor_source, factor_quality)
        adjustment = market_adjustment(market.regime)
        momentum_values = [q.momentum5 for q in enrich_targets if q.momentum5 is not None and math.isfinite(q.momentum5)]
        benchmark_momentum = sum(momentum_values) / len(momentum_values) if momentum_values else None
        industry_rows: dict[str, list] = {}
        for quote in enrich_targets:
            industry = str((raw_factors.get(quote.code) or {}).get("industry") or "").strip()
            if industry and quote.momentum5 is not None and math.isfinite(quote.momentum5):
                industry_rows.setdefault(industry, []).append(quote)
        for item in scored:
            row = raw_factors.get(item.quote.code) or {}
            factor_name_value = str(row.get("name") or "").strip()
            if factor_name_value and (not item.quote.name or item.quote.name == item.quote.code):
                item.quote.name = factor_name_value
            def number(key):
                try:
                    value = float(row.get(key))
                    return value if math.isfinite(value) else None
                except (TypeError, ValueError):
                    return None
            industry = number("industry_score")
            industry_name = str(row.get("industry") or "").strip()
            members = industry_rows.get(industry_name, [])
            if industry is None and benchmark_momentum is not None and len(members) >= 5:
                avg_momentum = sum(member.momentum5 for member in members if member.momentum5 is not None) / len(members)
                breadth = sum(1 for member in members if member.momentum5 is not None and member.momentum5 > 0) / len(members)
                mean_amount = sum(member.amount for member in enrich_targets) / max(1, len(enrich_targets))
                amount_ratio = (sum(member.amount for member in members) / len(members)) / mean_amount if mean_amount > 0 else 1.0
                industry = industry_strength(avg_momentum, benchmark_momentum, breadth, amount_ratio)
            fundamental = number("fundamental_score")
            roe, pe, pb = number("roe"), number("pe"), number("pb")
            profit_growth, cash_quality = number("profit_growth"), number("cash_quality")
            valuation = (2 - pe / 20 - (pb / 10 if pb is not None and pb > 0 else 0)) if pe is not None and pe > 0 else None
            flag = lambda value: str(value).strip().lower() in {"1", "true", "yes", "on", "是"}
            fundamental_risk = flag(row.get("st_flag")) or flag(row.get("audit_flag"))
            calculated_fundamental = fundamental_score(roe, profit_growth, cash_quality, valuation, flag(row.get("st_flag")), flag(row.get("audit_flag")))
            if fundamental is None and calculated_fundamental is not None:
                fundamental = calculated_fundamental
            if fundamental is None and roe is not None:
                fundamental = max(-10, min(10, round(roe / 2, 1)))
            if fundamental is None and pe is not None:
                fundamental = max(-3, min(3, round(2 - pe / 20, 1))) if pe is not None else None
            item.quote.industry_score, item.quote.fundamental_score = industry, fundamental
            item.factor_overlay = FactorOverlay(industry_name, industry, fundamental, market.regime, adjustment, str(row.get("source") or factor_name), as_of, str(row.get("quality") or factor_quality))
            if fundamental_risk:
                item.risk_level = "blocked"
                item.risk_flags.append("基本面硬风险")
            if factor_mode == "score" and item.risk_level != "blocked":
                extra = max(-20, min(20, item.factor_overlay.adjustment))
                item.composite_score = item.base_score + extra
                item.score = item.composite_score
                item.score_max = 70
                if extra:
                    item.reasons.append(f"因子综合修正{extra:+d}")
        minimum = self._int("min_score", 10, -100, 100)
        scored.sort(key=lambda item: (item.score, item.quote.amount), reverse=True)
        qualified = [item for item in scored if item.base_score >= minimum and item.risk_level != "blocked"]
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
            "market_regime": market.regime, "market_breadth": round(market.breadth, 4), "factor_source": factor_name or "unknown", "factor_quality": factor_quality,
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
        try:
            latest = await self.quotes.fetch_eastmoney_latest_trade_date()
        except Exception:
            latest = None
        if latest:
            is_open = latest == trade_date
            self.store.save_calendar(trade_date, is_open, "eastmoney_index")
            return is_open
        # A weekday is not proof of an open A-share session. Do not start a
        # background scan or intraday listener when the calendar is unknown.
        return False

    async def _daily_snapshot(self, trade_date: str) -> tuple[list, bool, str]:
        """Return a snapshot, whether it was fetched now, and its actual trade date."""
        lookup_date = self._daily_date_alias.get(trade_date, trade_date)
        cached = self.store.daily_quotes(lookup_date)
        cached_meta = self.store.snapshot_meta(lookup_date)
        if cached and (lookup_date != trade_date or not cached_meta or bool(cached_meta.get("complete"))):
            return cached, False, lookup_date
        async with self._daily_snapshot_lock:
            # Re-check after waiting so the background loop and manual command do not fetch twice.
            lookup_date = self._daily_date_alias.get(trade_date, trade_date)
            cached = self.store.daily_quotes(lookup_date)
            cached_meta = self.store.snapshot_meta(lookup_date)
            if cached and (lookup_date != trade_date or not cached_meta or bool(cached_meta.get("complete"))):
                self._daily_retry_after = None
                return cached, False, lookup_date
            try:
                result = await self.quotes.fetch_market_snapshot_result(
                    str(self.config.get("daily_market_url", "")), trade_date
                )
                minimum = self._int("daily_snapshot_min_size", 4000, 1000, 10000)
                valid_codes = {quote.code for quote in result.quotes if str(getattr(quote, "code", "")).isdigit() and len(str(quote.code)) == 6 and float(getattr(quote, "price", 0) or 0) > 0}
                if result.quotes and len(valid_codes) < minimum:
                    result.quality = "partial"
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
                if result.source == "eastmoney":
                    try:
                        actual_date = await self.quotes.fetch_eastmoney_latest_trade_date()
                    except Exception:
                        actual_date = None
                    if not actual_date:
                        self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                        logger.warning("[%s] 东方财富快照无法验证交易日，拒绝缓存", PLUGIN_NAME)
                        return [], True, trade_date
                    result.quality = "degraded"
                    logger.warning("[%s] 东方财富快照交易日由指数日线验证为 %s，标记 degraded", PLUGIN_NAME, actual_date)
                else:
                    self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                    return [], True, trade_date
            saved = self.store.save_daily_quotes(
                actual_date,
                result.quotes,
                self._int("daily_cache_keep_days", 180, 7, 730),
            )
            complete = actual_date == trade_date and result.quality == "good" and saved >= self._int("daily_snapshot_min_size", 4000, 1000, 10000)
            self.store.save_snapshot_meta(
                actual_date, result.source, result.quality, complete, trade_date,
                "" if complete else "交易日回退或来源未确认，不能视为当日完整收盘快照",
            )
            logger.info("[%s] 全市场日快照已缓存：%s 只", PLUGIN_NAME, saved)
            if complete:
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
            if now.strftime("%H:%M") >= target and self.last_daily_scan != now.date().isoformat() and retry_ready and await self._calendar_open(now.date().isoformat()):
                job_key = f"daily_screen:{now.date().isoformat()}"
                if not self.store.begin_job(job_key, "daily_screen", now.date().isoformat()):
                    await asyncio.sleep(20)
                    continue
                try:
                    requested_date = now.date().isoformat()
                    candidates = await self._daily_candidates(self._int("candidate_limit", 30, 1, 100))
                    actual_date = self.store.latest_daily_trade_date(requested_date) or requested_date
                    cached_for_record = self.store.daily_quotes(actual_date) if actual_date else []
                    snapshot = self._snapshot_context(requested_date, actual_date, cached_for_record)
                    source, quality = snapshot["source"], snapshot["quality"]
                    self._record_screen(requested_date, actual_date, source, cached_for_record, candidates, status="completed" if snapshot["complete"] else "degraded", quality=quality)
                    lines = [
                        "收盘选股（仅研究/模拟盘）",
                        f"请求日期：{requested_date}；实际数据交易日：{actual_date}",
                        f"行情来源：{source}；质量{quality}{'；非完整收盘快照' if not snapshot['complete'] else ''}",
                        f"市场环境：{self._last_screen_diagnostics.get('market_regime', 'unknown')}（上涨占比{self._last_screen_diagnostics.get('market_breadth', 0):.1%}）",
                        f"因子数据：{self._last_screen_diagnostics.get('factor_source', 'unknown')}，质量{self._last_screen_diagnostics.get('factor_quality', 'unknown')}，模式{self.config.get('factor_mode', 'report_only')}",
                        "筛选口径：硬过滤 → 趋势/动量/量价/波动评分 → 因子复核 → 人工复核",
                        *[format_candidate(item) for item in candidates],
                    ]
                    if not candidates:
                        d = self._last_screen_diagnostics
                        lines.append(f"暂无达标候选：可交易{d.get('tradable', 0)}只，补齐历史指标{d.get('enriched', 0)}只，最高分{d.get('max_score', 0)}，最低门槛{d.get('min_score', 15)}。")
                    push_failed = False
                    for origin in self.store.subscriptions():
                        if not self._push_allowed(origin):
                            continue
                        daily_signal = f"daily:{actual_date}"
                        claimed_at = datetime.now(timezone.utc)
                        if not self.store.claim_signal(origin, daily_signal, 86400, now=claimed_at):
                            continue
                        if not await self._push(origin, "\n".join(lines)):
                            self.store.release_signal(origin, daily_signal, claimed_at=claimed_at)
                            push_failed = True
                    completed = not push_failed and self._daily_retry_after is None and snapshot["complete"]
                    if completed:
                        self.last_daily_scan = now.date().isoformat()
                    self.store.finish_job(job_key, "completed" if completed else "failed", None if completed else "等待数据或推送重试")
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
                today_for_calendar = datetime.now(CHINA_TZ).date().isoformat()
                if in_trading_session() and await self._calendar_open(today_for_calendar):
                    today = datetime.now(CHINA_TZ).date().isoformat()
                    if self._intraday_date != today:
                        self.minute_bars.reset()
                        self._intraday_date = today
                    watch = self.store.all_watch()
                    watch_details = self.store.all_watch_details()
                    active = {origin: list(codes) for origin, codes in watch.items() if self.store.is_subscribed(origin) and codes}
                    union = list(dict.fromkeys(code for codes in active.values() for code in codes))
                    stored_candidates: dict[str, dict] = {}
                    if self._bool("auto_watch_candidates", True):
                        rows = self.store.latest_screen_candidates(self._int("candidate_limit", 30, 1, 100))
                        max_age = self._int("candidate_plan_valid_days", 10, 1, 60)
                        cutoff = (datetime.now(CHINA_TZ).date() - timedelta(days=max_age)).isoformat()
                        stored_candidates = {str(item["code"]): item for item in rows if item.get("code") and str(item.get("actual_trade_date") or "") >= cutoff}
                        candidate_codes = list(stored_candidates)
                        for origin in self.store.subscriptions():
                            active.setdefault(origin, [])
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
                            await self._score_quotes(quotes, len(quotes), include_factors=False)
                            # Monitoring evaluates every watched quote so risk states are not
                            # lost merely because the stock is not a screening candidate.
                            by_code = {quote.code: score_quote(quote) for quote in quotes}
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
                                    stored = stored_candidates.get(code)
                                    if candidate and stored:
                                        candidate.price_plan = self._price_plan_from_payload(str(stored.get("price_plan") or ""))
                                        candidate.risk_level = str(stored.get("risk_level") or candidate.risk_level)
                                        candidate.risk_flags = json.loads(stored.get("risk_flags") or "[]")
                                    minute_signal = minute_signals.get(code, "")
                                    cost_signal = self._cost_signal(quote, watch_details.get(origin, {}).get(code)) if quote else ""
                                    plan_candidate = candidate if stored else None
                                    if quote and not plan_candidate:
                                        self.store.reset_confirmation(origin, code)
                                    if quote and code in completed_codes and not minute_signal:
                                        self.store.reset_confirmation(origin, "minute:" + code)
                                    plan_state = self._price_state_for_plan(quote, plan_candidate.price_plan) if quote and plan_candidate else "unknown"
                                    price_signal = bool(plan_candidate)
                                    state_changed = price_signal and self.store.price_state(origin, code) != plan_state
                                    if plan_candidate and plan_candidate.risk_level == "unknown" and price_signal:
                                        price_signal = False
                                    if plan_candidate and plan_candidate.risk_level == "blocked" and plan_state != "invalidated" and price_signal:
                                        price_signal = False
                                    if price_signal and plan_state not in {"in_attention", "confirmed", "near_sell", "invalidated"}:
                                        price_signal = False
                                    if price_signal and not state_changed:
                                        price_signal = False
                                    threshold = self._int("intraday_failure_threshold", 0, 0, 100)
                                    if threshold > 0 and health["consecutive_failures"] >= threshold:
                                        continue
                                    events: list[tuple[str, str, bool]] = []
                                    if cost_signal:
                                        events.append((f"cost:{code}", cost_signal, False))
                                    if minute_signal:
                                        events.append((f"minute:{code}", minute_signal, False))
                                    if price_signal:
                                        state_text = {"in_attention": "进入关注区", "confirmed": "突破确认位", "near_sell": "进入参考卖出区", "invalidated": "跌破失效位"}.get(plan_state, "盘中价位变化")
                                        prefix = f"{state_text}（需人工复核）" if plan_candidate.risk_level == "watch_only" else state_text
                                        text = prefix + "（仅研究/模拟盘）\n" + format_candidate(plan_candidate)
                                        annotation = self._annotation_text(code)
                                        if annotation:
                                            text += "\n" + annotation
                                        if plan_state in {"near_sell", "invalidated"}:
                                            self.store.save_risk_event(f"{today}:{code}:{plan_state}", self._last_screen_run_id, code, plan_state, candidate.risk_level, json.dumps({"price": quote.price, "state": plan_state}, ensure_ascii=False), datetime.now(timezone.utc).isoformat())
                                        events.append((f"price:{stored.get('run_id')}:{code}:{plan_state}", text, True))
                                    for signal_key, text, is_price in events:
                                        if (is_price or signal_key.startswith("minute:")) and self._bool("confirmation_enabled", False):
                                            confirmation_code = "price:" + code + ":" + plan_state if is_price else "minute:" + code
                                            if not self.store.observe_confirmation(origin, confirmation_code, self._int("confirmation_periods", 2, 1, 5), self._int("confirmation_max_gap_seconds", 90, 30, 600)):
                                                continue
                                        claim_time = datetime.now(timezone.utc)
                                        if not self.store.claim_signal(origin, signal_key, 600, now=claim_time):
                                            continue
                                        sent = await self._push(origin, text)
                                        if sent and is_price:
                                            self.store.set_price_state(origin, code, plan_state)
                                        elif not sent:
                                            self.store.release_signal(origin, signal_key, claimed_at=claim_time)
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
            "/白名单 状态：查看当前会话是否在推送白名单\n"
            "/研究状态 或 /数据质量：查看运行、来源和质量\n"
            "/验证 [天数]：回放最近候选；/结果：查看已保存回放结果\n"
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
                self.store.save_snapshot_meta(actual_date, result.source, result.quality, actual_date == trade_date and result.quality == "good", trade_date, "未启用本地日快照缓存")
                source = "已抓取交易日 " + actual_date + " 全市场数据（未启用缓存）"
            if not quotes:
                yield event.plain_result(
                    f"未找到可用的全市场日行情（请求日期：{trade_date}）。\n"
                    "可能是 Tushare 尚未发布该日期数据，或行情接口暂时不可用。"
                )
                return
            candidates = await self._score_quotes(quotes, limit, actual_date)
            snapshot = self._snapshot_context(trade_date, actual_date, quotes)
            self._record_screen(trade_date, actual_date, snapshot["source"], quotes, candidates, status="completed" if snapshot["complete"] else "degraded", quality=snapshot["quality"])
            lines = [
                f"{source}：{len(quotes)} 只",
                f"来源：{snapshot['source']}；质量{snapshot['quality']}{'；非完整收盘快照' if not snapshot['complete'] else ''}",
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
        health_rows = self.store.provider_health_rows()
        factor_meta = self.store.factor_snapshot_meta(str(latest.get("actual_trade_date") or "")) or {}
        market_meta = self.store.market_context_meta(str(latest.get("actual_trade_date") or "")) or {}
        snapshot_meta = self.store.snapshot_meta(str(latest.get("actual_trade_date") or "")) or {}
        health_text = "；".join(f"{row['provider']}={row['last_quality']}（成功{row['success_count']}/失败{row['error_count']}）" for row in health_rows) or "暂无来源健康记录"
        yield event.plain_result(
            f"研究状态：{latest['status']}\n"
            f"最近运行：{latest['started_at']}\n"
            f"请求日期：{latest['requested_date']}；实际交易日：{latest.get('actual_trade_date') or '未知'}\n"
            f"来源：{latest['source'] or '未知'}；行情{latest['quote_count']}条；候选{latest['candidate_count']}条\n"
            f"数据质量：{latest['quality']}\n"
            f"快照：{snapshot_meta.get('source', '未知')}，质量{snapshot_meta.get('quality', '未知')}，完整收盘{bool(snapshot_meta.get('complete'))}\n"
            f"因子模式：{self.config.get('factor_mode', 'report_only')}；因子来源：{factor_meta.get('source', '未知')}；因子质量：{factor_meta.get('quality', '未知')}；覆盖{factor_meta.get('row_count', 0)}只\n"
            f"市场环境来源：{market_meta.get('source', '未知')}；质量{market_meta.get('quality', '未知')}\n"
            f"来源健康：{health_text}\n"
            "边界：模型只做摘要解释，价位由规则计算；仅研究/模拟盘，不提供订单或自动交易。"
        )

    @filter.command("验证", alias={"回放"})
    async def evaluate(self, event: AstrMessageEvent, horizon: int = 5):
        rows = self.store.latest_screen_candidates(100)
        if not rows:
            yield event.plain_result("暂无候选运行记录，先执行 /全市场选股。")
            return
        horizon = max(1, min(int(horizon or 5), 20))
        as_of = str(rows[0].get("actual_trade_date") or "")
        run_id = str(rows[0].get("run_id") or "")
        evaluated = 0
        details = []
        returns = []
        base = self.store.daily_quotes(as_of)
        for row in rows:
            base_quote = next((q for q in base if q.code == row["code"]), None)
            # This is deliberately evaluation-only. It never changes a prior screen.
            if base_quote:
                evaluation_end = (datetime.fromisoformat(as_of).date() + timedelta(days=horizon * 3 + 7)).isoformat()
                await self.quotes.enrich_indicators([base_quote], 1, evaluation_end)
                bars = self.quotes.history_bars.get(base_quote.code, [])
                if bars:
                    self.store.save_daily_bars(base_quote.code, bars, "eastmoney_evaluation")
            future_bars = self.store.daily_bars(row["code"], after=as_of)[:horizon]
            future = [item for item in future_bars]
            if not base_quote or len(future) < horizon:
                continue
            last = future[-1]
            ret = (last["close"] - base_quote.price) / base_quote.price * 100 if base_quote.price else None
            highs = [(item["high"] - base_quote.price) / base_quote.price * 100 for item in future]
            lows = [(item["low"] - base_quote.price) / base_quote.price * 100 for item in future]
            plan = json.loads(row.get("price_plan") or "{}")
            first_touch = None
            for item in future:
                touches = []
                if plan.get("invalidation") and item["low"] <= plan["invalidation"]:
                    touches.append("失效位")
                if plan.get("sell_low") and item["high"] >= plan["sell_low"]:
                    touches.append("参考卖出区")
                if plan.get("confirmation") and item["high"] >= plan["confirmation"]:
                    touches.append("确认位")
                if touches:
                    first_touch = "、".join(touches) if len(touches) == 1 else "同日多价位触达，日线无法判断先后（" + "、".join(touches) + "）"
                    break
            self.store.save_result_evaluation(f"{run_id}:{row['code']}:{horizon}", run_id, row["code"], as_of, horizon, "complete", last["close"], ret, max(highs), min(lows), first_touch, True)
            evaluated += 1
            returns.append(ret)
            details.append(f"{row['name']}（{row['code']}）：{ret:+.2f}%｜最大浮盈{max(highs):+.2f}%｜最大回撤{min(lows):+.2f}%｜关键位{first_touch or '未触达'}")
        if not details:
            yield event.plain_result(f"验证暂不可用：候选后续 K 线不足 {horizon} 个交易日。")
            return
        avg = sum(returns) / len(returns)
        yield event.plain_result(f"回放验证（仅研究）：基准日 {as_of}，周期 {horizon} 日，完成 {evaluated} 条，平均收益{avg:+.2f}%\n" + "\n".join(details[:20]))

    @filter.command("结果")
    async def results(self, event: AstrMessageEvent, count: int = 20):
        rows = self.store.evaluations(limit=max(1, min(int(count or 20), 100)))
        if not rows:
            yield event.plain_result("暂无已保存的回放结果，请先执行 /验证 [天数]。")
            return
        lines = ["已保存回放结果（仅研究）"]
        for row in rows:
            ret = row.get("return_pct")
            text = f"{row['code']}｜基准{row['as_of']}｜{row['horizon']}日收益{float(ret):+.2f}%" if ret is not None else f"{row['code']}｜基准{row['as_of']}｜结果未完成"
            lines.append(text + f"｜关键位{row.get('first_touch') or '未触达'}")
        yield event.plain_result("\n".join(lines))

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
            if not self._bool("allow_self_whitelist", False):
                yield event.plain_result("白名单由插件配置维护；当前不允许会话自行加入。")
                return
            self.store.set_whitelist(origin, True)
            yield event.plain_result("已加入推送白名单。\n会话标识：" + origin)
        elif action in {"关闭", "关", "off", "remove", "移除"}:
            if not self._bool("allow_self_whitelist", False):
                yield event.plain_result("白名单由插件配置维护；当前不允许会话自行修改。")
                return
            self.store.set_whitelist(origin, False)
            yield event.plain_result("已移出推送白名单。")
        elif action in {"列表", "list"}:
            yield event.plain_result("为保护会话标识，不提供白名单列表。可用 /白名单 状态 查看当前会话。")
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
            await self.quotes.enrich_indicators(quotes, self._int("max_concurrency", 5, 1, 20))
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
