from __future__ import annotations

import asyncio
from datetime import datetime
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


@register(PLUGIN_NAME, "DIO", "A股收盘选股与自选股监听", "0.2.0")
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
        self.last_alert: dict[tuple[str, str], datetime] = {}
        self._daily_snapshot_lock = asyncio.Lock()
        self._daily_date_alias: dict[str, str] = {}

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
            return max(minimum, min(float(self.config.get(key, default)), maximum))
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

    async def _push(self, origin: str, text: str):
        if not self._push_allowed(origin):
            logger.debug("[%s] 已跳过非白名单会话推送：%s", PLUGIN_NAME, origin or "<empty>")
            return
        try:
            await self.context.send_message(origin, MessageChain([Plain(text)]))
        except TypeError:
            await self.context.send_message(origin, text)

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
                if in_trading_session():
                    for origin, codes in self.store.all_watch().items():
                        if self.store.is_subscribed(origin) and codes:
                            candidates = await self._scan(codes, min(len(codes), 20))
                            for candidate in candidates:
                                key = (origin, candidate.quote.code)
                                previous = self.last_alert.get(key)
                                if previous and (datetime.now(CHINA_TZ) - previous).total_seconds() < 600:
                                    continue
                                self.last_alert[key] = datetime.now(CHINA_TZ)
                                await self._push(origin, "盘中信号（仅研究/模拟盘）\n" + format_candidate(candidate))
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
    async def watch(self, event: AstrMessageEvent, action: str = "", code: str = ""):
        origin, action, codes = self._origin(event), str(action or "").strip().lower(), parse_codes(code)
        if action in {"添加", "add"} and codes:
            ok = self.store.add_watch(origin, codes[0], self._int("watchlist_limit", 100, 1, 1000))
            yield event.plain_result(("已加入自选：" if ok else "添加失败，可能已达到数量上限：") + codes[0])
        elif action in {"删除", "移除", "del", "remove"} and codes:
            yield event.plain_result(("已移除：" if self.store.remove_watch(origin, codes[0]) else "自选中没有：") + codes[0])
        else:
            current = self.store.list_watch(origin)
            yield event.plain_result("自选股：" + ("、".join(current) if current else "暂无。用 /自选 添加 600000"))

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
