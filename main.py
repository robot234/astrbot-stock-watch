from __future__ import annotations

import asyncio
import math
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path

from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star, register
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

from .core import CHINA_TZ, format_candidate, in_trading_session, is_tradable, parse_codes, score_quote
from .providers import OpenAICompatibleClient, RssNewsProvider, SinaQuoteProvider, news_fingerprint
from .storage import StockStore

PLUGIN_NAME = "astrbot_stock_watch"


@register(PLUGIN_NAME, "DIO", "A股收盘选股与自选股监听", "0.7.0")
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
        )
        self.tasks: list[asyncio.Task] = []
        self.last_daily_scan: str | None = None
        self._daily_snapshot_lock = asyncio.Lock()
        self._daily_date_alias: dict[str, str] = {}
        self._annotation_task: asyncio.Task | None = None
        self._annotation_cache: dict[str, tuple[datetime, dict]] = {}
        self._last_annotation_at: datetime | None = None
        self._signal_observations: dict[tuple[str, str], deque[datetime]] = defaultdict(deque)

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

    async def _push(self, origin: str, text: str) -> bool:
        if not self._push_allowed(origin):
            logger.debug("[%s] 已跳过非白名单会话推送：%s", PLUGIN_NAME, origin or "<empty>")
            return False
        try:
            await self.context.send_message(origin, MessageChain([Plain(text)]))
            return True
        except TypeError:
            try:
                await self.context.send_message(origin, text)
                return True
            except Exception:
                logger.exception("[%s] 推送失败：%s", PLUGIN_NAME, origin or "<empty>")
                return False
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

    async def _score_quotes(self, quotes, limit: int):
        tradable = [quote for quote in quotes if is_tradable(
            quote,
            self._float("price_min", 2, 0.01, 100000),
            self._float("price_max", 80, 0.01, 100000),
        )]
        tradable.sort(key=lambda quote: quote.amount, reverse=True)
        await self.quotes.enrich_indicators(tradable[: max(40, limit * 2)], self._int("max_concurrency", 5, 1, 20))
        candidates = [score_quote(quote) for quote in tradable]
        minimum = self._int("min_score", 15, -100, 100)
        candidates = [item for item in candidates if item.score >= minimum]
        candidates.sort(key=lambda item: (item.score, item.quote.amount), reverse=True)
        return candidates[:limit]

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
        evidence = "、".join(annotation.get("evidence", [])[:3])
        return f"模型解读：{annotation.get('summary', '')}；风险{annotation.get('risk_level', 'unknown')}；依据：{evidence}"

    def _cost_signal(self, quote, cost_price: float | None) -> str:
        if not cost_price or not math.isfinite(cost_price) or not math.isfinite(quote.price) or quote.price <= 0:
            return ""
        change = (quote.price - cost_price) / cost_price * 100
        profit = self._float("cost_profit_threshold_pct", 5.0, 0.1, 1000)
        risk = self._float("cost_risk_threshold_pct", 5.0, 0.1, 1000)
        if change >= profit:
            return f"成本观察：{quote.code} 现价{quote.price:.2f}，成本{cost_price:.2f}，相对成本{change:+.2f}%，达到止盈观察阈值（仅研究/模拟盘，不自动下单）"
        if change <= -risk:
            return f"成本观察：{quote.code} 现价{quote.price:.2f}，成本{cost_price:.2f}，相对成本{change:+.2f}%，达到风险观察阈值（仅研究/模拟盘，不自动下单）"
        return ""

    def _confirmed_candidate(self, origin: str, code: str) -> bool:
        key = (origin, code)
        now = datetime.now(CHINA_TZ)
        history = self._signal_observations[key]
        max_gap = self._int("confirmation_max_gap_seconds", 90, 30, 600)
        while history and (now - history[0]).total_seconds() > max_gap:
            history.popleft()
        history.append(now)
        required = self._int("confirmation_periods", 2, 1, 5)
        if len(history) >= required:
            history.clear()
            return True
        return False

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
                return cached, False, lookup_date
            result = await self.quotes.fetch_market_snapshot_result(
                str(self.config.get("daily_market_url", "")), trade_date
            )
            if not result.quotes:
                return [], True, trade_date
            actual_date = result.trade_date or trade_date
            saved = self.store.save_daily_quotes(
                actual_date,
                result.quotes,
                self._int("daily_cache_keep_days", 180, 7, 730),
            )
            logger.info("[%s] 全市场日快照已缓存：%s 只", PLUGIN_NAME, saved)
            self._daily_date_alias[trade_date] = actual_date
            return self.store.daily_quotes(actual_date), True, actual_date

    async def _daily_candidates(self, limit: int):
        if self._bool("daily_cache_enabled", True):
            trade_date = datetime.now(CHINA_TZ).date().isoformat()
            try:
                cached, _, _ = await self._daily_snapshot(trade_date)
            except Exception:
                logger.exception("[%s] 全市场日快照失败，退回股票池扫描", PLUGIN_NAME)
                cached = []
            if cached:
                return await self._score_quotes(cached, limit)
        return await self._scan(self._universe(), limit)

    async def _daily_loop(self):
        while True:
            now = datetime.now(CHINA_TZ)
            target = str(self.config.get("daily_scan_time", "15:10"))
            if now.weekday() < 5 and now.strftime("%H:%M") >= target and now.hour < 16 and self.last_daily_scan != now.date().isoformat():
                self.last_daily_scan = now.date().isoformat()
                try:
                    candidates = await self._daily_candidates(self._int("candidate_limit", 30, 1, 100))
                    lines = ["收盘选股（仅研究/模拟盘）", *[format_candidate(item) for item in candidates]]
                    if not candidates:
                        lines.append("暂无候选，或尚未配置股票池/行情接口。")
                    for origin in self.store.subscriptions():
                        await self._push(origin, "\n".join(lines))
                except Exception:
                    logger.exception("[%s] 收盘扫描失败", PLUGIN_NAME)
            await asyncio.sleep(20)

    async def _intraday_loop(self):
        while True:
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
                    watch = self.store.all_watch()
                    watch_details = self.store.all_watch_details()
                    active = {origin: codes for origin, codes in watch.items() if self.store.is_subscribed(origin) and codes}
                    union = list(dict.fromkeys(code for codes in active.values() for code in codes))
                    if union:
                        raw_quotes = []
                        for start in range(0, len(union), 100):
                            try:
                                raw_quotes.extend(await self.quotes.fetch_quotes(union[start:start + 100]))
                            except Exception:
                                logger.exception("[%s] 盘中行情分批抓取失败：批次 %s", PLUGIN_NAME, start // 100 + 1)
                        quotes = self._fresh_quotes(raw_quotes)
                        if quotes:
                            quotes_by_code = {quote.code: quote for quote in quotes}
                            candidates = await self._score_quotes(quotes, len(quotes))
                            by_code = {candidate.quote.code: candidate for candidate in candidates}
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
                                    cost_signal = self._cost_signal(quote, watch_details.get(origin, {}).get(code)) if quote else ""
                                    if not candidate and not cost_signal:
                                        continue
                                    claim_time = datetime.now(timezone.utc)
                                    if not self.store.claim_signal(origin, code, 600, now=claim_time):
                                        continue
                                    if cost_signal:
                                        text = cost_signal
                                    else:
                                        if self._bool("confirmation_enabled", False) and not self._confirmed_candidate(origin, code):
                                            continue
                                        text = "盘中信号（仅研究/模拟盘）\n" + format_candidate(candidate)
                                        annotation = self._annotation_text(code)
                                        if annotation:
                                            text += "\n" + annotation
                                    sent = await self._push(origin, text)
                                    if not sent:
                                        self.store.release_signal(origin, code, claimed_at=claim_time)
            except Exception:
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
                            summary = await self.llm.summarize(fresh)
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
            lines = ["选股结果（仅研究/模拟盘）", *[format_candidate(item) for item in candidates]]
            if not candidates:
                lines.append("暂无结果。请先配置 universe_codes 或添加自选股。")
            yield event.plain_result("\n".join(lines))
        except Exception:
            logger.exception("[%s] 手动选股失败", PLUGIN_NAME)
            yield event.plain_result("选股暂时失败，请检查行情接口和插件配置。")

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
                quotes, actual_date = result.quotes, result.trade_date or trade_date
                source = "已抓取交易日 " + actual_date + " 全市场数据（未启用缓存）"
            if not quotes:
                yield event.plain_result(
                    f"未找到可用的全市场日行情（请求日期：{trade_date}）。\n"
                    "可能是 Tushare 尚未发布该日期数据，或行情接口暂时不可用。"
                )
                return
            candidates = await self._score_quotes(quotes, limit)
            lines = [f"{source}：{len(quotes)} 只", "全市场选股结果（仅研究/模拟盘）"]
            lines.extend(format_candidate(item) for item in candidates)
            if not candidates:
                lines.append("暂无候选，可能是行情接口未返回数据或评分条件较严。")
            yield event.plain_result("\n".join(lines))
        except Exception:
            logger.exception("[%s] 手动全市场同步失败", PLUGIN_NAME)
            yield event.plain_result("全市场同步失败，请检查行情接口和插件配置。")

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
            ok = self.store.add_watch(origin, codes[0], self._int("watchlist_limit", 100, 1, 1000), cost_price)
            suffix = f"，成本价 {cost_price:.2f}" if cost_price else ""
            yield event.plain_result(("已加入自选：" if ok else "添加失败，可能已达到数量上限：") + codes[0] + suffix)
        elif action in {"删除", "移除", "del", "remove"} and codes:
            yield event.plain_result(("已移除：" if self.store.remove_watch(origin, codes[0]) else "自选中没有：") + codes[0])
        else:
            current = self.store.list_watch_details(origin)
            values = [f"{code}(成本{cost:.2f})" if cost else code for code, cost in current]
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
                    summary = await self.llm.summarize(items[:10])
                    if summary:
                        yield event.plain_result(summary)
                        return
                except Exception:
                    logger.exception("[%s] 手动故事摘要失败", PLUGIN_NAME)
            yield event.plain_result("\n".join(f"{item.title}\n{item.link}" for item in items[:10]))
        except Exception:
            logger.exception("[%s] 故事查询失败", PLUGIN_NAME)
            yield event.plain_result("故事查询失败，请检查 RSS 地址。")
