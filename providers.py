from __future__ import annotations

import asyncio
import hashlib
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import Iterable

import httpx

from .core import CHINA_TZ, NewsItem, Quote, calc_rsi, normalize_code, simple_moving_average


def _sina_symbol(code: str) -> str:
    value = normalize_code(code)
    if value.startswith(("4", "8")):
        return "bj" + value
    return ("sh" if value.startswith(("6", "68", "9")) else "sz") + value


class SinaQuoteProvider:
    """Prototype provider; replace it with a licensed/stable source for production."""

    def __init__(self, timeout: float = 10):
        self.timeout = timeout
        self._indicator_cache: dict[str, tuple[datetime, dict[str, float | None]]] = {}

    async def fetch_market_snapshot(self) -> list[Quote]:
        """Fetch one daily snapshot for the A-share universe from Eastmoney."""
        url = "https://push2.eastmoney.com/api/qt/clist/get"
        fields = "f2,f3,f4,f5,f6,f12,f14"
        result: list[Quote] = []
        page = 1
        # Eastmoney may silently cap oversized pages; 200 keeps pagination predictable.
        page_size = 200
        async with httpx.AsyncClient(
            timeout=self.timeout,
            headers={
                "User-Agent": "Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
                "Referer": "https://quote.eastmoney.com/",
            },
        ) as client:
            while True:
                params = {
                    "pn": page,
                    "pz": page_size,
                    "po": 1,
                    "np": 1,
                    "ut": "bd1d9ddb04089700cf9c27f6f7426281",
                    "fltt": 2,
                    "invt": 2,
                    "fid": "f3",
                    "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
                    "fields": fields,
                }
                data = None
                for attempt in range(3):
                    try:
                        response = await client.get(url, params=params)
                        response.raise_for_status()
                        data = response.json().get("data") or {}
                        break
                    except (httpx.HTTPError, ValueError):
                        if attempt == 2:
                            raise
                        await asyncio.sleep(1.5 * (attempt + 1))
                diff = data.get("diff") or []
                if isinstance(diff, dict):
                    diff = diff.values()
                diff = list(diff)
                for row in diff:
                    quote = self._snapshot_row(row)
                    if quote:
                        result.append(quote)
                total = data.get("total")
                if not diff or (total is not None and page * page_size >= int(total)) or (total is None and len(diff) < page_size):
                    break
                page += 1
        return result

    @staticmethod
    def _snapshot_row(row: dict) -> Quote | None:
        code = str(row.get("f12") or "").zfill(6)
        if not code.isdigit() or len(code) != 6:
            return None
        try:
            price = float(row.get("f2") or 0)
            pct_change = float(row.get("f3") or 0)
            prev_close = price / (1 + pct_change / 100) if price and pct_change > -100 else 0.0
            volume = float(row.get("f5") or 0)
            amount = float(row.get("f6") or 0)
        except (TypeError, ValueError):
            return None
        return Quote(code, str(row.get("f14") or code), price, prev_close, amount, pct_change, volume, fetched_at=datetime.now(CHINA_TZ))

    async def fetch_quotes(self, codes: Iterable[str]) -> list[Quote]:
        values = [normalize_code(code) for code in codes]
        if not values:
            return []
        url = "https://hq.sinajs.cn/list=" + ",".join(_sina_symbol(code) for code in values)
        async with httpx.AsyncClient(timeout=self.timeout, headers={"Referer": "https://finance.sina.com.cn/"}) as client:
            response = await client.get(url)
            response.raise_for_status()
            payload = response.text
        result: list[Quote] = []
        for symbol, raw in re.findall(r'hq_str_([a-z0-9]+)="(.*?)";', payload, flags=re.I):
            fields = raw.split(",")
            if len(fields) < 10:
                continue
            try:
                price, prev_close = float(fields[3] or 0), float(fields[2] or 0)
                amount, volume = float(fields[9] or 0), float(fields[8] or 0)
            except (TypeError, ValueError):
                continue
            pct = (price - prev_close) / prev_close * 100 if prev_close else 0.0
            result.append(Quote(symbol[2:], fields[0].strip() or symbol[2:], price, prev_close, amount, pct, volume, fetched_at=datetime.now(CHINA_TZ)))
        return result

    async def enrich_indicators(self, quotes: list[Quote], max_concurrency: int = 5) -> None:
        """Fetch a short adjusted daily history for the small candidate set."""
        if not quotes:
            return
        semaphore = asyncio.Semaphore(max(1, max_concurrency))

        async def enrich(quote: Quote) -> None:
            cached = self._indicator_cache.get(quote.code)
            if cached and datetime.now(CHINA_TZ) - cached[0] < timedelta(minutes=15):
                values = cached[1]
                quote.rsi6, quote.ma5, quote.ma10, quote.ma20, quote.volume_ratio = (values["rsi6"], values["ma5"], values["ma10"], values["ma20"], values["volume_ratio"])
                return
            secid = ("1." if quote.code.startswith(("6", "68", "9")) else "0.") + quote.code
            url = "https://push2his.eastmoney.com/api/qt/stock/kline/get"
            params = {
                "secid": secid,
                "ut": "fa5fd1943c7b386f172d6893dbfba10b",
                "fields1": "f1,f2,f3,f4,f5,f6",
                "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
                "klt": "101",
                "fqt": "1",
                "beg": "0",
                "end": "20500101",
                "lmt": "60",
            }
            try:
                async with semaphore:
                    async with httpx.AsyncClient(timeout=self.timeout) as client:
                        response = await client.get(url, params=params)
                        response.raise_for_status()
                        payload = response.json()
                klines = ((payload.get("data") or {}).get("klines") or [])
                closes = [float(str(row).split(",")[2]) for row in klines if len(str(row).split(",")) > 6]
                volumes = [float(str(row).split(",")[5]) for row in klines if len(str(row).split(",")) > 6]
                if len(closes) < 20:
                    return
                values = {
                    "rsi6": calc_rsi(closes, 6),
                    "ma5": simple_moving_average(closes, 5),
                    "ma10": simple_moving_average(closes, 10),
                    "ma20": simple_moving_average(closes, 20),
                    "volume_ratio": (volumes[-1] / (sum(volumes[-6:-1]) / 5)) if len(volumes) >= 6 and sum(volumes[-6:-1]) > 0 else None,
                }
                self._indicator_cache[quote.code] = (datetime.now(CHINA_TZ), values)
                quote.rsi6, quote.ma5, quote.ma10, quote.ma20, quote.volume_ratio = (values["rsi6"], values["ma5"], values["ma10"], values["ma20"], values["volume_ratio"])
            except (httpx.HTTPError, ValueError, TypeError, KeyError, IndexError):
                return

        await asyncio.gather(*(enrich(quote) for quote in quotes))


class RssNewsProvider:
    def __init__(self, url: str, timeout: float = 10):
        self.url, self.timeout = url.strip(), timeout

    async def fetch(self) -> list[NewsItem]:
        if not self.url:
            return []
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            response = await client.get(self.url)
            response.raise_for_status()
            root = ET.fromstring(response.content)
        items: list[NewsItem] = []
        for node in root.findall(".//item"):
            def value(name: str) -> str:
                child = node.find(name)
                return (child.text or "").strip() if child is not None else ""
            title = value("title")
            if title:
                items.append(NewsItem(title, value("link"), value("description"), value("pubDate"), self.url))
        return items[:50]


class OpenAICompatibleClient:
    def __init__(self, base_url: str, api_key: str, model: str, timeout: float = 30):
        self.base_url, self.api_key, self.model, self.timeout = base_url.rstrip("/"), api_key, model, timeout

    async def summarize(self, items: list[NewsItem]) -> str:
        if not items or not self.api_key:
            return ""
        content = "\n".join(f"- {item.title}\n  {item.summary[:500]}" for item in items[:10])
        payload = {"model": self.model, "temperature": 0.1, "messages": [
            {"role": "system", "content": "你是A股资讯助手。只根据提供的新闻，简洁输出事件、涉及公司/行业、可能影响和可信度；不要给出买卖指令。"},
            {"role": "user", "content": content},
        ]}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
            response = await client.post(self.base_url + "/chat/completions", json=payload, headers={"Authorization": "Bearer " + self.api_key})
            response.raise_for_status()
            data = response.json()
        return str(data["choices"][0]["message"]["content"]).strip()


def news_fingerprint(item: NewsItem) -> str:
    return hashlib.sha256((item.title + "|" + item.link).encode("utf-8")).hexdigest()
