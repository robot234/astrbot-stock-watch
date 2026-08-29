from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable

import httpx

from .core import CHINA_TZ, Candidate, NewsItem, Quote, apply_daily_indicators, calculate_daily_indicators, normalize_code


def _sina_symbol(code: str) -> str:
    value = normalize_code(code)
    if value.startswith(("4", "8")):
        return "bj" + value
    return ("sh" if value.startswith(("6", "68", "9")) else "sz") + value


@dataclass(slots=True)
class MarketSnapshotResult:
    quotes: list[Quote]
    trade_date: str | None
    source: str = ""
    quality: str = "unknown"
    fetched_at: datetime = field(default_factory=lambda: datetime.now(CHINA_TZ))


class HttpRuntime:
    """One lazily opened client plus a process-local request gate.

    Providers share this runtime so a large screen cannot create one client
    and one unconstrained task per symbol.  The loop check keeps the object
    usable in short-lived test/event loops as well as AstrBot's long-lived
    loop.
    """

    def __init__(self, timeout: float = 10, max_concurrency: int = 8, headers: dict | None = None):
        self.timeout = timeout
        self.max_concurrency = max(1, int(max_concurrency))
        self.headers = dict(headers or {})
        self._semaphore: asyncio.Semaphore | None = None
        self._semaphore_loop = None
        self._client: httpx.AsyncClient | None = None
        self._entered = False
        self._loop = None
        self._client_lock: asyncio.Lock | None = None
        self._lock_loop = None

    def _gate(self) -> asyncio.Semaphore:
        loop = asyncio.get_running_loop()
        if self._semaphore is None or self._semaphore_loop is not loop:
            self._semaphore = asyncio.Semaphore(self.max_concurrency)
            self._semaphore_loop = loop
        return self._semaphore

    @staticmethod
    async def _dispose(client, entered: bool) -> None:
        if client is None:
            return
        exit_method = getattr(client, "__aexit__", None)
        if exit_method and entered:
            await exit_method(None, None, None)
            return
        close = getattr(client, "aclose", None)
        if close:
            await close()

    async def client(self) -> httpx.AsyncClient:
        loop = asyncio.get_running_loop()
        if self._client is not None and self._loop is loop:
            return self._client
        if self._client_lock is None or self._lock_loop is not loop:
            self._client_lock = asyncio.Lock()
            self._lock_loop = loop
        async with self._client_lock:
            if self._client is not None and self._loop is loop:
                return self._client
            # A prior asyncio.run() may have closed its loop.  Dispose of the
            # old client before opening a replacement on the current loop.
            previous, previous_entered = self._client, self._entered
            self._client, self._loop, self._entered = None, None, False
            if previous is not None:
                try:
                    await self._dispose(previous, previous_entered)
                except Exception:
                    # A client owned by a closed loop may no longer be
                    # closable; it must not prevent a fresh runtime.
                    pass
            client = httpx.AsyncClient(timeout=self.timeout, headers=self.headers)
            entered = False
            try:
                enter = getattr(client, "__aenter__", None)
                if enter:
                    opened = await enter()
                    if opened is not None:
                        client = opened
                    entered = True
            except Exception:
                try:
                    await self._dispose(client, entered)
                except Exception:
                    pass
                raise
            self._client, self._loop, self._entered = client, loop, entered
            return client

    @asynccontextmanager
    async def slot(self):
        async with self._gate():
            yield await self.client()

    async def close(self) -> None:
        client, self._client = self._client, None
        self._loop = None
        entered = self._entered
        self._entered = False
        await self._dispose(client, entered)


class SinaQuoteProvider:
    """Prototype provider; replace it with a licensed/stable source for production."""

    def __init__(self, timeout: float = 10, tushare_url: str = "", tushare_token: str = "", max_concurrency: int = 8, http_runtime: HttpRuntime | None = None):
        self.timeout = timeout
        self.tushare_url = str(tushare_url or "").strip() or "https://api.tushare.pro"
        self.tushare_token = str(tushare_token or "").strip()
        self.http = http_runtime or HttpRuntime(timeout, max_concurrency)
        self._indicator_cache: dict[str, tuple[datetime, dict[str, float | None]]] = {}
        self.history_bars: dict[str, list[dict[str, float | str]]] = {}
        self._last_tushare_date: str | None = None
        self._tushare_names: dict[str, str] = {}
        # Current industry labels are display annotations, not historical
        # factor snapshots.  Cache both successful lookups and short-lived
        # failures so a large candidate report cannot hammer the endpoint.
        self._industry_cache: dict[str, tuple[datetime, str, bool]] = {}
        # Futures are loop-bound.  The loop identity is checked before a
        # caller awaits one so providers remain safe across short-lived test
        # loops and AstrBot's long-lived event loop.
        self._industry_inflight: dict[str, asyncio.Future] = {}

    async def close(self) -> None:
        await self.http.close()

    async def fetch_market_snapshot(self, daily_market_url: str = "", trade_date: str = "") -> list[Quote]:
        """Fetch one daily snapshot, preferring Tushare when a token is configured."""
        return (await self.fetch_market_snapshot_result(daily_market_url, trade_date)).quotes

    async def fetch_eastmoney_latest_trade_date(self) -> str | None:
        """Verify the snapshot date from the latest completed Shanghai index bar."""
        params = {"secid": "1.000001", "fields1": "f1,f2,f3,f4,f5,f6", "fields2": "f51,f52,f53,f54,f55", "klt": "101", "fqt": "0", "lmt": "2", "end": "20500101"}
        async with self.http.slot() as client:
            response = await client.get("https://push2his.eastmoney.com/api/qt/stock/kline/get", params=params)
            response.raise_for_status()
            rows = ((response.json().get("data") or {}).get("klines") or [])
        if not rows:
            return None
        value = str(rows[-1]).split(",", 1)[0].strip()
        return self._normalize_trade_date(value) if re.fullmatch(r"\d{4}-?\d{2}-?\d{2}", value) else None

    async def fetch_market_snapshot_result(self, daily_market_url: str = "", trade_date: str = "") -> MarketSnapshotResult:
        """Fetch a snapshot and report the actual trading date represented by the data."""
        if self.tushare_token:
            try:
                return await self._fetch_tushare_snapshot_result(trade_date)
            except (httpx.HTTPError, ValueError, TypeError, KeyError, RuntimeError):
                # Tushare permissions, quota, or transient errors should not stop the daily job.
                pass
        quotes = await self._fetch_eastmoney_snapshot(daily_market_url)
        # Eastmoney snapshot contract does not expose a reliable trade date.
        return MarketSnapshotResult(quotes, None, "eastmoney", "degraded" if quotes else "unknown")

    async def _fetch_tushare_snapshot(self, trade_date: str = "") -> list[Quote]:
        return (await self._fetch_tushare_snapshot_result(trade_date)).quotes

    @staticmethod
    def _normalize_trade_date(value: str) -> str:
        digits = str(value or "").replace("-", "")
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}" if len(digits) == 8 else str(value)

    async def _fetch_tushare_snapshot_result(self, trade_date: str = "") -> MarketSnapshotResult:
        requested = str(trade_date or datetime.now(CHINA_TZ).strftime("%Y%m%d")).replace("-", "")
        async with self.http.slot() as client:
            self._last_tushare_date = None
            quotes = await self._fetch_tushare_daily(client, requested)
            if quotes:
                await self._apply_tushare_names(client, quotes)
                return MarketSnapshotResult(quotes, self._last_tushare_date or self._normalize_trade_date(requested), "tushare", "good")

            dates = await self._fetch_tushare_trade_dates(client, requested)
            if not dates:
                current = datetime.strptime(requested, "%Y%m%d")
                dates = [
                    (current - timedelta(days=offset)).strftime("%Y%m%d")
                    for offset in range(1, 31)
                    if (current - timedelta(days=offset)).weekday() < 5
                ]
            for date_value in dates[:30]:
                quotes = await self._fetch_tushare_daily(client, date_value)
                if quotes:
                    await self._apply_tushare_names(client, quotes)
                    return MarketSnapshotResult(quotes, self._last_tushare_date or self._normalize_trade_date(date_value), "tushare", "good")
        return MarketSnapshotResult([], None, "tushare", "unknown")

    async def _fetch_tushare_daily(self, client: httpx.AsyncClient, date_value: str) -> list[Quote]:
        payload = {
            "api_name": "daily",
            "token": self.tushare_token,
            "params": {"trade_date": date_value, "limit": 6000},
            "fields": "ts_code,trade_date,close,pre_close,pct_chg,vol,amount",
        }
        response = await client.post(self.tushare_url, json=payload)
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict):
            raise ValueError("Tushare response is not an object")
        if int(body.get("code") or 0) != 0:
            raise RuntimeError(str(body.get("msg") or "Tushare returned an error"))
        data = body.get("data") or {}
        fields = list(data.get("fields") or [])
        items = data.get("items") or []
        result: list[Quote] = []
        row_dates: set[str] = set()
        for values in items:
            row = dict(zip(fields, values))
            code = str(row.get("ts_code") or "").split(".", 1)[0].zfill(6)
            if not code.isdigit() or len(code) != 6:
                continue
            try:
                price = float(row.get("close") or 0)
                prev_close = float(row.get("pre_close") or 0)
                pct_change = float(row.get("pct_chg") or 0)
                volume = float(row.get("vol") or 0)
                amount = float(row.get("amount") or 0) * 1000
            except (TypeError, ValueError):
                continue
            row_date = str(row.get("trade_date") or "").replace("-", "")
            if len(row_date) != 8:
                raise ValueError("Tushare row missing trade_date")
            row_dates.add(row_date)
            result.append(Quote(code, code, price, prev_close, amount, pct_change, volume, source="tushare", provider_ts=datetime.now(CHINA_TZ), fetched_at=datetime.now(CHINA_TZ)))
        if len(row_dates) != 1:
            raise ValueError("Tushare response contains mixed or missing trade_date")
        self._last_tushare_date = self._normalize_trade_date(next(iter(row_dates)))
        return result

    async def _apply_tushare_names(self, client: httpx.AsyncClient, quotes: list[Quote]) -> int:
        missing = [quote.code for quote in quotes if not quote.name or quote.name == quote.code]
        if missing and (not self._tushare_names or any(code not in self._tushare_names for code in missing)):
            for status in ("L", "P", "D"):
                try:
                    payload = {"api_name": "stock_basic", "token": self.tushare_token, "params": {"list_status": status}, "fields": "ts_code,name"}
                    response = await client.post(self.tushare_url, json=payload)
                    response.raise_for_status()
                    body = response.json()
                    if not isinstance(body, dict) or int(body.get("code") or 0) != 0:
                        continue
                    data = body.get("data") or {}
                    fields, items = list(data.get("fields") or []), data.get("items") or []
                    for row in (dict(zip(fields, item)) for item in items if isinstance(item, list)):
                        code, name = str(row.get("ts_code") or "").split(".")[0], str(row.get("name") or "").strip()
                        if len(code) == 6 and name:
                            self._tushare_names[code] = name
                except (httpx.HTTPError, ValueError, TypeError, KeyError, IndexError):
                    continue
        updated = 0
        for quote in quotes:
            name = self._tushare_names.get(quote.code)
            if name and quote.name != name:
                quote.name = name
                updated += 1
        return updated

    async def enrich_names(self, quotes: list[Quote]) -> int:
        if not self.tushare_token or not quotes:
            return 0
        async with self.http.slot() as client:
            return await self._apply_tushare_names(client, quotes)

    async def _fetch_tushare_trade_dates(self, client: httpx.AsyncClient, end_date: str) -> list[str]:
        payload = {
            "api_name": "trade_cal",
            "token": self.tushare_token,
            "params": {"exchange": "SSE", "is_open": 1, "end_date": end_date, "limit": 1000},
            "fields": "cal_date,is_open",
        }
        response = await client.post(self.tushare_url, json=payload)
        response.raise_for_status()
        body = response.json()
        if not isinstance(body, dict):
            raise ValueError("Tushare trade_cal response is not an object")
        if int(body.get("code") or 0) != 0:
            raise RuntimeError(str(body.get("msg") or "Tushare returned an error"))
        data = body.get("data") or {}
        fields = list(data.get("fields") or [])
        dates = []
        for values in data.get("items") or []:
            row = dict(zip(fields, values))
            if str(row.get("is_open", "1")) not in {"1", "True", "true"}:
                continue
            value = str(row.get("cal_date") or "").replace("-", "")
            if len(value) == 8 and value < end_date:
                dates.append(value)
        return sorted(set(dates), reverse=True)

    async def fetch_trade_calendar(self, trade_date: str) -> bool | None:
        if not self.tushare_token:
            return None
        value = str(trade_date or datetime.now(CHINA_TZ).date().isoformat()).replace("-", "")
        async with self.http.slot() as client:
            payload = {"api_name": "trade_cal", "token": self.tushare_token, "params": {"exchange": "SSE", "start_date": value, "end_date": value}, "fields": "cal_date,is_open"}
            response = await client.post(self.tushare_url, json=payload); response.raise_for_status()
            body = response.json(); data = body.get("data") or {}; fields = list(data.get("fields") or [])
            for values in data.get("items") or []:
                row = dict(zip(fields, values))
                if str(row.get("cal_date") or "").replace("-", "") == value:
                    return str(row.get("is_open", "0")) in {"1", "True", "true"}
        return None

    async def _fetch_eastmoney_snapshot(self, daily_market_url: str = "") -> list[Quote]:
        """Fetch one daily snapshot; a custom URL may expose the same JSON shape."""
        url = str(daily_market_url or "").strip() or "https://push2.eastmoney.com/api/qt/clist/get"
        fields = "f2,f3,f4,f5,f6,f12,f14"
        result: list[Quote] = []
        page = 1
        # Eastmoney may silently cap oversized pages; 200 keeps pagination predictable.
        page_size = 200
        async with self.http.slot() as client:
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
                        response = await client.get(
                            url,
                            params=params,
                            headers={
                                "User-Agent": "Mozilla/5.0 (X11; Linux aarch64) AppleWebKit/537.36 Chrome/120 Safari/537.36",
                                "Referer": "https://quote.eastmoney.com/",
                            },
                        )
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
        return Quote(code, str(row.get("f14") or code), price, prev_close, amount, pct_change, volume, source="eastmoney", provider_ts=datetime.now(CHINA_TZ), fetched_at=datetime.now(CHINA_TZ))

    async def fetch_quotes(self, codes: Iterable[str]) -> list[Quote]:
        values = list(dict.fromkeys(normalize_code(code) for code in codes if normalize_code(code)))[:500]
        if not values:
            return []
        url = "https://hq.sinajs.cn/list=" + ",".join(_sina_symbol(code) for code in values)
        async with self.http.slot() as client:
            response = await client.get(url, headers={"Referer": "https://finance.sina.com.cn/"})
            response.raise_for_status()
            payload = response.text
        result: list[Quote] = []
        for symbol, raw in re.findall(r'hq_str_([a-z0-9]+)="(.*?)";', payload, flags=re.I):
            fields = raw.split(",")
            if len(fields) < 32:
                continue
            try:
                price, prev_close = float(fields[3] or 0), float(fields[2] or 0)
                amount, volume = float(fields[9] or 0), float(fields[8] or 0)
            except (TypeError, ValueError):
                continue
            pct = (price - prev_close) / prev_close * 100 if prev_close else 0.0
            try:
                quote_time = datetime.fromisoformat(f"{fields[30].strip()}T{fields[31].strip()}").replace(tzinfo=CHINA_TZ)
            except (TypeError, ValueError):
                continue
            result.append(Quote(symbol[2:], fields[0].strip() or symbol[2:], price, prev_close, amount, pct, volume, source="sina", provider_ts=quote_time, fetched_at=quote_time))
        return result

    async def fetch_custom_factors(self, url: str, codes: Iterable[str], as_of: str = "") -> dict[str, dict]:
        """Optional JSON factor source: {data:[{code,industry_score,fundamental_score,...}]}.
        Values are annotations only; unknown/malformed rows are ignored.
        """
        if not str(url or "").strip():
            return {}
        async with self.http.slot() as client:
            response = await client.get(url, params={"codes": ",".join(codes), "as_of": as_of}, follow_redirects=True)
            response.raise_for_status()
            payload = response.json()
        rows = payload.get("data", payload) if isinstance(payload, dict) else payload
        result = {}
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or not row.get("code"):
                continue
            result[str(row["code"])[-6:]] = row
        return result

    @staticmethod
    def _eastmoney_secid(code: str) -> str:
        return ("1." if str(code).startswith(("6", "68", "9")) else "0.") + str(code)

    @staticmethod
    def _clean_industry_text(value) -> str:
        cleaned = re.sub(r"[\x00-\x1f\x7f]", "", str(value or "")).strip()[:80]
        return "" if cleaned.lower() in {"-", "--", "null", "none", "n/a", "na"} else cleaned

    @staticmethod
    def _normalized_industry_code(value) -> str:
        text = str(value or "").strip()
        if text.endswith(".0") and text[:-2].isdigit():
            text = text[:-2]
        normalized = normalize_code(text)
        return normalized if re.fullmatch(r"\d{6}", normalized) else ""

    def _cache_industry(self, code: str, value: str = "", success: bool = True, now: datetime | None = None) -> None:
        timestamp = now or datetime.now(CHINA_TZ)
        self._industry_cache[str(code)] = (timestamp, self._clean_industry_text(value), bool(success))
        if len(self._industry_cache) <= 1000:
            return
        def cache_time(key: str):
            try:
                value = self._industry_cache[key][0]
                if not isinstance(value, datetime):
                    return datetime.min.replace(tzinfo=CHINA_TZ)
                return value if value.tzinfo is not None else value.replace(tzinfo=CHINA_TZ)
            except (IndexError, KeyError, TypeError):
                return datetime.min.replace(tzinfo=CHINA_TZ)
        for key in sorted(self._industry_cache, key=cache_time)[:-1000]:
            self._industry_cache.pop(key, None)

    def _cached_industry(self, code: str, now: datetime | None = None) -> tuple[bool, str] | None:
        cached = self._industry_cache.get(str(code))
        if not cached:
            return None
        try:
            timestamp, value, success = cached
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=CHINA_TZ)
            age = (now or datetime.now(CHINA_TZ)) - timestamp
            ttl = timedelta(hours=24) if success else timedelta(minutes=10)
            if age.total_seconds() < ttl.total_seconds():
                return bool(success), self._clean_industry_text(value)
        except (AttributeError, IndexError, TypeError, ValueError, OverflowError):
            pass
        self._industry_cache.pop(str(code), None)
        return None

    async def fetch_eastmoney_industries(self, codes: Iterable[str]) -> dict[str, str]:
        """Fetch current Eastmoney industry labels with bounded shared I/O.

        A response is accepted only when its ``f57`` code matches the request;
        malformed or mismatched responses become ten-minute negative-cache
        entries and never leak a label to another stock.
        """
        values: list[str] = []
        seen: set[str] = set()
        for raw in codes or []:
            code = normalize_code(raw)
            if not re.fullmatch(r"\d{6}", code) or code in seen:
                continue
            values.append(code)
            seen.add(code)
        values = values[:1000]
        if not values:
            return {}

        result: dict[str, str] = {}
        pending: list[str] = []
        now = datetime.now(CHINA_TZ)
        for code in values:
            cached = self._cached_industry(code, now=now)
            if cached is None:
                pending.append(code)
            elif cached[0] and cached[1]:
                result[code] = cached[1]
        if not pending:
            return result

        queue: asyncio.Queue[str] = asyncio.Queue()
        for code in pending:
            queue.put_nowait(code)

        async def fetch_one(code: str) -> None:
            loop = asyncio.get_running_loop()
            inflight = self._industry_inflight.get(code)
            if inflight is not None:
                try:
                    if inflight.get_loop() is loop:
                        outcome = await asyncio.shield(inflight)
                        if outcome[0] and outcome[1]:
                            result[code] = outcome[1]
                        return
                except AttributeError:
                    pass
                self._industry_inflight.pop(code, None)

            future = loop.create_future()
            self._industry_inflight[code] = future
            outcome: tuple[bool, str] = (False, "")
            try:
                async with self.http.slot() as client:
                    response = await client.get(
                        "https://push2.eastmoney.com/api/qt/stock/get",
                        params={"secid": self._eastmoney_secid(code), "fields": "f57,f127"},
                    )
                    response.raise_for_status()
                    body = response.json()
                    data = body.get("data") if isinstance(body, dict) else None
                    if not isinstance(data, dict):
                        raise ValueError("Eastmoney industry response has no data")
                    if self._normalized_industry_code(data.get("f57")) != code:
                        raise ValueError("Eastmoney industry response code mismatch")
                    industry = self._clean_industry_text(data.get("f127"))
                    if not industry:
                        raise ValueError("Eastmoney industry response has no usable label")
                self._cache_industry(code, industry, True)
                outcome = (True, industry)
                if industry:
                    result[code] = industry
            except asyncio.CancelledError:
                self._cache_industry(code, "", False)
                outcome = (False, "")
                raise
            except Exception:
                self._cache_industry(code, "", False)
            finally:
                if not future.done():
                    future.set_result(outcome)
                if self._industry_inflight.get(code) is future:
                    self._industry_inflight.pop(code, None)

        async def worker() -> None:
            while True:
                try:
                    code = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    await fetch_one(code)
                finally:
                    queue.task_done()

        await asyncio.gather(*(worker() for _ in range(min(len(pending), self.http.max_concurrency))))
        return result

    async def fetch_eastmoney_factors(self, codes: Iterable[str]) -> dict[str, dict]:
        """Best-effort public fields: industry, PE, PB and ROE."""
        result = {}
        values = list(dict.fromkeys(normalize_code(code) for code in codes if normalize_code(code)))[:300]
        queue: asyncio.Queue[str] = asyncio.Queue()
        for code in values:
            queue.put_nowait(code)

        async def fetch_one(code: str) -> None:
            secid = self._eastmoney_secid(code)
            try:
                async with self.http.slot() as client:
                    response = await client.get(
                        "https://push2.eastmoney.com/api/qt/stock/get",
                        params={"secid": secid, "fields": "f57,f58,f127,f162,f167,f173"},
                        headers={"Referer": "https://quote.eastmoney.com/"},
                    )
                    response.raise_for_status()
                    body = response.json()
                    if not isinstance(body, dict):
                        raise ValueError("Eastmoney factor response is not an object")
                    data = body.get("data") or {}
                    if not isinstance(data, dict) or self._normalized_industry_code(data.get("f57")) != str(code):
                        raise ValueError("Eastmoney factor response code mismatch")
                def finite(key):
                    try:
                        value = float(data.get(key))
                        return value if math.isfinite(value) else None
                    except (TypeError, ValueError):
                        return None
                pe, pb = finite("f162"), finite("f167")
                industry = self._clean_industry_text(data.get("f127"))
                if industry:
                    self._cache_industry(str(code), industry, True)
                result[str(code)] = {"name": str(data.get("f58") or ""), "industry": industry, "pe": pe / 100 if pe is not None else None, "pb": pb / 100 if pb is not None else None, "roe": finite("f173"), "source": "eastmoney", "quality": "partial"}
            except (httpx.HTTPError, ValueError, TypeError, KeyError, AttributeError):
                return

        async def worker() -> None:
            while True:
                try:
                    code = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    await fetch_one(code)
                finally:
                    queue.task_done()

        await asyncio.gather(*(worker() for _ in range(min(len(values), self.http.max_concurrency))))
        return result

    async def fetch_tushare_factors(self, codes: Iterable[str], trade_date: str = "") -> dict[str, dict]:
        """Optional point-in-time daily and financial factors; permission failures degrade safely."""
        if not self.tushare_token:
            return {}
        values = list(dict.fromkeys(normalize_code(c) for c in codes if normalize_code(c)))[:50]
        as_of = str(trade_date or datetime.now(CHINA_TZ).date().isoformat()).replace("-", "")
        async def call(client, api_name: str, params: dict, fields: str) -> list[dict]:
            payload = {"api_name": api_name, "token": self.tushare_token, "params": params, "fields": fields}
            async with self.http.slot():
                response = await client.post(self.tushare_url, json=payload)
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict) or int(body.get("code") or 0) != 0:
                return []
            data = body.get("data") or {}
            names = list(data.get("fields") or [])
            return [dict(zip(names, item)) for item in (data.get("items") or []) if isinstance(item, list)]
        client = await self.http.client()
        rows = await call(client, "daily_basic", {"trade_date": as_of}, "ts_code,trade_date,pe,pb,turnover_rate,total_mv")
        result = {}
        for row in rows:
            code = str(row.get("ts_code") or "").split(".")[0]
            if code in values:
                result[code] = {"pe": row.get("pe"), "pb": row.get("pb"), "source": "tushare", "quality": "partial", "as_of": self._normalize_trade_date(as_of)}

        async def financial(code: str):
            suffix = "SH" if code.startswith(("6", "9")) else ("BJ" if code.startswith(("4", "8")) else "SZ")
            ts_code = f"{code}.{suffix}"
            try:
                indicator, income, cashflow = await asyncio.gather(
                    call(client, "fina_indicator", {"ts_code": ts_code, "limit": 8}, "ts_code,ann_date,end_date,roe,debt_to_assets,ocf_to_or"),
                    call(client, "income", {"ts_code": ts_code, "limit": 8}, "ts_code,ann_date,end_date,revenue_yoy,operate_profit"),
                    call(client, "cashflow", {"ts_code": ts_code, "limit": 8}, "ts_code,ann_date,end_date,n_cashflow_act"),
                )
            except (httpx.HTTPError, ValueError, TypeError):
                return
            def latest_visible(rows: list[dict]) -> dict | None:
                valid = []
                for row in rows:
                    ann_date = str(row.get("ann_date") or "").replace("-", "")
                    if re.fullmatch(r"\d{8}", ann_date) and ann_date <= as_of:
                        valid.append(row)
                return max(valid, key=lambda row: str(row.get("ann_date")).replace("-", "")) if valid else None
            indicator_row = latest_visible(indicator)
            income_row = latest_visible(income)
            cashflow_row = latest_visible(cashflow)
            if not any((indicator_row, income_row, cashflow_row)):
                return
            target = result.setdefault(code, {"source": "tushare_financial", "quality": "partial", "as_of": self._normalize_trade_date(as_of)})
            if indicator_row:
                target.update({"roe": indicator_row.get("roe"), "roe_ann_date": indicator_row.get("ann_date"), "roe_report_period": indicator_row.get("end_date")})
            if income_row:
                target.update({"profit_growth": income_row.get("revenue_yoy"), "profit_ann_date": income_row.get("ann_date"), "profit_report_period": income_row.get("end_date")})
            if cashflow_row:
                target.update({"cash_quality": cashflow_row.get("n_cashflow_act"), "cash_ann_date": cashflow_row.get("ann_date"), "cash_report_period": cashflow_row.get("end_date")})
            target["source"] = "tushare+financial"
            target["quality"] = "partial"

        queue: asyncio.Queue[str] = asyncio.Queue()
        for code in values:
            queue.put_nowait(code)

        async def worker() -> None:
            while True:
                try:
                    code = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    await financial(code)
                finally:
                    queue.task_done()

        worker_count = min(8, self.http.max_concurrency, len(values))
        if worker_count:
            await asyncio.gather(*(worker() for _ in range(worker_count)))
        for code in list(result):
            row = result[code]
            # Financial data has a publication date; it is never silently treated as same-day data.
            announcement_dates = [str(row.get(key) or "").replace("-", "") for key in ("roe_ann_date", "profit_ann_date", "cash_ann_date")]
            if any(value and (not re.fullmatch(r"\d{8}", value) or value > as_of) for value in announcement_dates):
                result.pop(code, None)
        return result

    async def enrich_indicators(self, quotes: list[Quote], max_concurrency: int = 5, as_of: str = "") -> dict[str, str]:
        """Fetch bounded daily history with one shared client and worker set."""
        if not quotes:
            return {}
        # The runtime gate is global to this provider; the local worker count
        # also bounds task creation when a caller passes a 300-symbol target.
        worker_count = max(1, min(int(max_concurrency), self.http.max_concurrency, 20))
        results: dict[str, str] = {}
        queue: asyncio.Queue[Quote] = asyncio.Queue()
        for quote in quotes:
            queue.put_nowait(quote)

        async def enrich(quote: Quote, client: httpx.AsyncClient) -> None:
            cache_key = f"{quote.code}:{as_of or 'latest'}"
            cached = self._indicator_cache.get(cache_key)
            if cached and datetime.now(CHINA_TZ) - cached[0] < timedelta(minutes=15):
                values = cached[1]
                quote.rsi6, quote.ma5, quote.ma10, quote.ma20, quote.volume_ratio = (values["rsi6"], values["ma5"], values["ma10"], values["ma20"], values["volume_ratio"])
                quote.atr14, quote.support20, quote.resistance20, quote.volatility20, quote.history_days = (values.get("atr14"), values.get("support20"), values.get("resistance20"), values.get("volatility20"), int(values.get("history_days") or 0))
                quote.momentum5, quote.momentum20 = values.get("momentum5"), values.get("momentum20")
                results[quote.code] = "memory_cache" if quote.history_days >= 20 and quote.atr14 is not None else "failed"
                return
            self.history_bars.pop(quote.code, None)
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
                "end": str(as_of or "20500101").replace("-", ""),
                "lmt": "60",
            }
            try:
                async with self.http.slot() as shared_client:
                    response = await shared_client.get(url, params=params)
                    response.raise_for_status()
                    payload = response.json()
                klines = ((payload.get("data") or {}).get("klines") or [])
                parsed = []
                cutoff = str(as_of or "").replace("-", "")
                for row in klines:
                    fields = str(row).split(",")
                    if len(fields) <= 6:
                        continue
                    row_date = fields[0].replace("-", "").strip()
                    if cutoff and len(row_date) == 8 and row_date > cutoff:
                        continue
                    try:
                        parsed.append((row_date, float(fields[1]), float(fields[3]), float(fields[4]), float(fields[2]), float(fields[5]), float(fields[6]) if len(fields) > 6 else 0.0))
                    except (TypeError, ValueError):
                        continue
                history = [{"trade_date": f"{item[0][:4]}-{item[0][4:6]}-{item[0][6:8]}", "open": item[1], "high": item[2], "low": item[3], "close": item[4], "volume": item[5], "amount": item[6]} for item in parsed]
                self.history_bars[quote.code] = history
                values = calculate_daily_indicators(history)
                if apply_daily_indicators(quote, history):
                    self._indicator_cache[cache_key] = (datetime.now(CHINA_TZ), values)
                    results[quote.code] = "network"
                else:
                    results[quote.code] = "failed"
            except (httpx.HTTPError, ValueError, TypeError, KeyError, IndexError, AttributeError):
                results[quote.code] = "failed"

        async def worker() -> None:
            while True:
                try:
                    quote = queue.get_nowait()
                except asyncio.QueueEmpty:
                    return
                try:
                    await enrich(quote, await self.http.client())
                finally:
                    queue.task_done()

        await asyncio.gather(*(worker() for _ in range(min(worker_count, len(quotes)))))
        # Bound caches so a long-running process does not retain every symbol
        # ever seen by a custom universe.
        if len(self._indicator_cache) > 1200:
            for key in list(self._indicator_cache)[:-1000]:
                self._indicator_cache.pop(key, None)
        if len(self.history_bars) > 1200:
            for key in list(self.history_bars)[:-1000]:
                self.history_bars.pop(key, None)
        return {quote.code: results.get(quote.code, "failed") for quote in quotes}


class RssNewsProvider:
    def __init__(self, url: str, timeout: float = 10, http_runtime: HttpRuntime | None = None):
        self.url, self.timeout = url.strip(), timeout
        self.http = http_runtime or HttpRuntime(timeout, 4)

    async def close(self) -> None:
        await self.http.close()

    async def fetch(self) -> list[NewsItem]:
        if not self.url:
            return []
        async with self.http.slot() as client:
            response = await client.get(self.url, follow_redirects=True)
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
    def __init__(self, base_url: str, api_key: str, model: str, timeout: float = 30, min_interval: float = 10, daily_limit: int = 100, http_runtime: HttpRuntime | None = None):
        self.base_url, self.api_key, self.model, self.timeout = base_url.rstrip("/"), api_key, model, timeout
        self.min_interval, self.daily_limit = max(0, min_interval), max(1, daily_limit)
        self._annotation_times: list[datetime] = []
        self.http = http_runtime or HttpRuntime(timeout, 2)

    async def close(self) -> None:
        await self.http.close()

    async def summarize(self, items: list[NewsItem]) -> str:
        if not items or not self.api_key:
            return ""
        content = "\n".join(f"- {item.title}\n  {item.summary[:500]}" for item in items[:10])
        payload = {"model": self.model, "temperature": 0.1, "messages": [
            {"role": "system", "content": "你是A股资讯助手。只根据提供的新闻，简洁输出事件、涉及公司/行业、可能影响和可信度；不要给出买卖指令。"},
            {"role": "user", "content": content},
        ]}
        async with self.http.slot() as client:
            response = await client.post(self.base_url + "/chat/completions", json=payload, headers={"Authorization": "Bearer " + self.api_key})
            response.raise_for_status()
            data = response.json()
        return str(data["choices"][0]["message"]["content"]).strip()

    async def annotate_candidates(self, candidates: list[Candidate], max_tokens: int = 800) -> dict[str, dict]:
        if not candidates or not self.api_key:
            return {}
        now = datetime.now(timezone.utc)
        self._annotation_times = [value for value in self._annotation_times if (now - value).total_seconds() < 86400]
        if len(self._annotation_times) >= self.daily_limit:
            return {}
        if self._annotation_times and (now - self._annotation_times[-1]).total_seconds() < self.min_interval:
            return {}
        self._annotation_times.append(now)
        items = []
        for candidate in candidates[:20]:
            quote = candidate.quote
            items.append({
                "code": quote.code,
                "name": quote.name[:64],
                "price": quote.price,
                "pct_change": quote.pct_change,
                "amount": quote.amount,
                "volume": quote.volume,
                "score": candidate.score,
                "reasons": candidate.reasons[:6],
                "rsi6": quote.rsi6,
                "ma5": quote.ma5,
                "ma10": quote.ma10,
                "ma20": quote.ma20,
                "volume_ratio": quote.volume_ratio,
                "fetched_at": quote.fetched_at.isoformat(),
                "factor_overlay": ({key: getattr(candidate.factor_overlay, key) for key in candidate.factor_overlay.__dataclass_fields__} if candidate.factor_overlay else None),
            })
        allowed = {item["code"] for item in items}
        payload = {
            "model": self.model,
            "temperature": 0.1,
            "max_tokens": max(200, min(int(max_tokens), 2000)),
            "response_format": {"type": "json_object"},
            "messages": [
                {"role": "system", "content": "你是A股盘中信号解释助手。只能依据输入JSON，不得补造行情或新闻。只输出JSON，不给出买入、卖出、目标价或仓位建议。每项必须引用输入中的理由或指标作为evidence。"},
                {"role": "user", "content": json.dumps({"schema_version": "1", "items": items}, ensure_ascii=False)},
            ],
        }
        try:
            async with self.http.slot() as client:
                response = await client.post(self.base_url + "/chat/completions", json=payload, headers={"Authorization": "Bearer " + self.api_key})
                response.raise_for_status()
                content = response.json()["choices"][0]["message"]["content"]
            content = str(content).strip()
            if content.startswith("```"):
                content = re.sub(r"^```(?:json)?\s*|\s*```$", "", content, flags=re.I | re.S).strip()
            data = json.loads(content)
            raw_items = data.get("items") if isinstance(data, dict) and set(data) == {"items"} else None
            if not isinstance(raw_items, list):
                return {}
            result: dict[str, dict] = {}
            for item in raw_items:
                if not isinstance(item, dict):
                    return {}
                required = {"code", "risk_level", "summary", "evidence", "confidence"}
                if set(item) != required or not isinstance(item["code"], str) or not re.fullmatch(r"\d{6}", item["code"]):
                    return {}
                code = item["code"]
                risk = item["risk_level"]
                summary = item["summary"].strip() if isinstance(item["summary"], str) else ""
                evidence = item.get("evidence")
                confidence = item.get("confidence", 0.0)
                if code not in allowed or code in result or not isinstance(risk, str) or risk not in {"low", "medium", "high", "unknown"}:
                    return {}
                if not summary or len(summary) > 240 or not isinstance(evidence, list) or len(evidence) > 5:
                    return {}
                if not all(isinstance(value, str) and value.strip() and len(value) <= 120 for value in evidence):
                    return {}
                if isinstance(confidence, bool) or not isinstance(confidence, (int, float)):
                    return {}
                confidence = float(confidence)
                if not 0 <= confidence <= 1:
                    return {}
                safe_evidence = [re.sub(r"[\x00-\x1f\x7f]", " ", value).strip() for value in evidence[:5]]
                result[code] = {"risk_level": risk, "summary": summary, "evidence": safe_evidence, "confidence": confidence}
            return result
        except (httpx.HTTPError, ValueError, TypeError, KeyError, IndexError, json.JSONDecodeError):
            return {}


def news_fingerprint(item: NewsItem) -> str:
    return hashlib.sha256((item.title + "|" + item.link).encode("utf-8")).hexdigest()
