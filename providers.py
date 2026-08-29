from __future__ import annotations

import asyncio
import hashlib
import json
import math
import re
import xml.etree.ElementTree as ET
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Iterable

import httpx

from .core import CHINA_TZ, Candidate, NewsItem, Quote, calc_atr, calc_rsi, normalize_code, simple_moving_average


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


class SinaQuoteProvider:
    """Prototype provider; replace it with a licensed/stable source for production."""

    def __init__(self, timeout: float = 10, tushare_url: str = "", tushare_token: str = ""):
        self.timeout = timeout
        self.tushare_url = str(tushare_url or "").strip() or "https://api.tushare.pro"
        self.tushare_token = str(tushare_token or "").strip()
        self._indicator_cache: dict[str, tuple[datetime, dict[str, float | None]]] = {}
        self.history_bars: dict[str, list[dict[str, float | str]]] = {}
        self._last_tushare_date: str | None = None
        self._tushare_names: dict[str, str] = {}

    async def fetch_market_snapshot(self, daily_market_url: str = "", trade_date: str = "") -> list[Quote]:
        """Fetch one daily snapshot, preferring Tushare when a token is configured."""
        return (await self.fetch_market_snapshot_result(daily_market_url, trade_date)).quotes

    async def fetch_eastmoney_latest_trade_date(self) -> str | None:
        """Verify the snapshot date from the latest completed Shanghai index bar."""
        params = {"secid": "1.000001", "fields1": "f1,f2,f3,f4,f5,f6", "fields2": "f51,f52,f53,f54,f55", "klt": "101", "fqt": "0", "lmt": "2", "end": "20500101"}
        async with httpx.AsyncClient(timeout=self.timeout) as client:
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
        async with httpx.AsyncClient(timeout=self.timeout) as client:
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

    async def _apply_tushare_names(self, client: httpx.AsyncClient, quotes: list[Quote]) -> None:
        if not self._tushare_names:
            payload = {"api_name": "stock_basic", "token": self.tushare_token, "params": {"list_status": "L"}, "fields": "ts_code,name"}
            try:
                response = await client.post(self.tushare_url, json=payload)
                response.raise_for_status()
                data = response.json().get("data") or {}
                fields, items = list(data.get("fields") or []), data.get("items") or []
                self._tushare_names = {str(row["ts_code"]).split(".")[0]: str(row["name"]) for row in (dict(zip(fields, item)) for item in items) if row.get("ts_code") and row.get("name")}
            except (httpx.HTTPError, ValueError, TypeError, KeyError, IndexError):
                return
        for quote in quotes:
            quote.name = self._tushare_names.get(quote.code, quote.name)

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
        async with httpx.AsyncClient(timeout=self.timeout) as client:
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
        return Quote(code, str(row.get("f14") or code), price, prev_close, amount, pct_change, volume, source="eastmoney", provider_ts=datetime.now(CHINA_TZ), fetched_at=datetime.now(CHINA_TZ))

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
        async with httpx.AsyncClient(timeout=self.timeout, follow_redirects=True) as client:
            response = await client.get(url, params={"codes": ",".join(codes), "as_of": as_of})
            response.raise_for_status()
            payload = response.json()
        rows = payload.get("data", payload) if isinstance(payload, dict) else payload
        result = {}
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, dict) or not row.get("code"):
                continue
            result[str(row["code"])[-6:]] = row
        return result

    async def fetch_eastmoney_factors(self, codes: Iterable[str]) -> dict[str, dict]:
        """Best-effort public fields: industry, PE, PB and ROE."""
        result = {}
        async with httpx.AsyncClient(timeout=self.timeout, headers={"Referer": "https://quote.eastmoney.com/"}) as client:
            semaphore = asyncio.Semaphore(8)
            async def fetch_one(code):
                secid = ("1." if str(code).startswith(("6", "68", "9")) else "0.") + str(code)
                try:
                    async with semaphore:
                        response = await client.get("https://push2.eastmoney.com/api/qt/stock/get", params={"secid": secid, "fields": "f57,f58,f127,f162,f167,f173"})
                    response.raise_for_status()
                    data = (response.json().get("data") or {})
                    def finite(key):
                        try:
                            value = float(data.get(key))
                            return value if math.isfinite(value) else None
                        except (TypeError, ValueError):
                            return None
                    pe, pb = finite("f162"), finite("f167")
                    result[str(code)] = {"name": str(data.get("f58") or ""), "industry": str(data.get("f127") or ""), "pe": pe / 100 if pe is not None else None, "pb": pb / 100 if pb is not None else None, "roe": finite("f173"), "source": "eastmoney", "quality": "partial"}
                except (httpx.HTTPError, ValueError, TypeError, KeyError):
                    return
            await asyncio.gather(*(fetch_one(code) for code in list(codes)[:300]))
        return result

    async def fetch_tushare_factors(self, codes: Iterable[str], trade_date: str = "") -> dict[str, dict]:
        """Optional point-in-time daily and financial factors; permission failures degrade safely."""
        if not self.tushare_token:
            return {}
        values = [normalize_code(c) for c in codes]
        as_of = str(trade_date or datetime.now(CHINA_TZ).date().isoformat()).replace("-", "")
        async def call(client, api_name: str, params: dict, fields: str) -> list[dict]:
            payload = {"api_name": api_name, "token": self.tushare_token, "params": params, "fields": fields}
            response = await client.post(self.tushare_url, json=payload)
            response.raise_for_status()
            body = response.json()
            if not isinstance(body, dict) or int(body.get("code") or 0) != 0:
                return []
            data = body.get("data") or {}
            names = list(data.get("fields") or [])
            return [dict(zip(names, item)) for item in (data.get("items") or []) if isinstance(item, list)]
        async with httpx.AsyncClient(timeout=self.timeout) as client:
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
                rows_by_api = (indicator, income, cashflow)
                visible = [row for rows in rows_by_api for row in rows if str(row.get("ann_date") or "").replace("-", "") <= as_of]
                if not visible:
                    return
                latest = max(visible, key=lambda row: str(row.get("ann_date") or ""))
                target = result.setdefault(code, {"source": "tushare_financial", "quality": "partial", "as_of": self._normalize_trade_date(as_of)})
                for row in indicator:
                    if str(row.get("ann_date") or "").replace("-", "") <= as_of:
                        target.update({"roe": row.get("roe"), "ann_date": row.get("ann_date"), "report_period": row.get("end_date")})
                        break
                for row in income:
                    if str(row.get("ann_date") or "").replace("-", "") <= as_of:
                        target["profit_growth"] = row.get("revenue_yoy")
                        target["ann_date"] = target.get("ann_date") or row.get("ann_date")
                        target["report_period"] = target.get("report_period") or row.get("end_date")
                        break
                for row in cashflow:
                    if str(row.get("ann_date") or "").replace("-", "") <= as_of:
                        target["cash_quality"] = row.get("n_cashflow_act")
                        target["ann_date"] = target.get("ann_date") or row.get("ann_date")
                        target["report_period"] = target.get("report_period") or row.get("end_date")
                        break
                target["source"] = "tushare+financial"
                target["quality"] = "partial"
            await asyncio.gather(*(financial(code) for code in values[:50]))
        for code in list(result):
            row = result[code]
            # Financial data has a publication date; it is never silently treated as same-day data.
            if row.get("ann_date") and str(row["ann_date"]).replace("-", "") > as_of:
                result.pop(code, None)
        return result

    async def enrich_indicators(self, quotes: list[Quote], max_concurrency: int = 5, as_of: str = "") -> None:
        """Fetch a short adjusted daily history for the small candidate set."""
        if not quotes:
            return
        semaphore = asyncio.Semaphore(max(1, max_concurrency))

        async def enrich(quote: Quote) -> None:
            cache_key = f"{quote.code}:{as_of or 'latest'}"
            cached = self._indicator_cache.get(cache_key)
            if cached and datetime.now(CHINA_TZ) - cached[0] < timedelta(minutes=15):
                values = cached[1]
                quote.rsi6, quote.ma5, quote.ma10, quote.ma20, quote.volume_ratio = (values["rsi6"], values["ma5"], values["ma10"], values["ma20"], values["volume_ratio"])
                quote.atr14, quote.support20, quote.resistance20, quote.volatility20, quote.history_days = (values.get("atr14"), values.get("support20"), values.get("resistance20"), values.get("volatility20"), int(values.get("history_days") or 0))
                quote.momentum5, quote.momentum20 = values.get("momentum5"), values.get("momentum20")
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
                "end": str(as_of or "20500101").replace("-", ""),
                "lmt": "60",
            }
            try:
                async with semaphore:
                    async with httpx.AsyncClient(timeout=self.timeout) as client:
                        response = await client.get(url, params=params)
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
                closes = [item[4] for item in parsed]
                highs = [item[2] for item in parsed]
                lows = [item[3] for item in parsed]
                volumes = [item[5] for item in parsed]
                self.history_bars[quote.code] = [{"trade_date": f"{item[0][:4]}-{item[0][4:6]}-{item[0][6:8]}", "open": item[1], "high": item[2], "low": item[3], "close": item[4], "volume": item[5], "amount": item[6]} for item in parsed]
                if len(closes) < 20:
                    return
                returns = [(closes[i] - closes[i - 1]) / closes[i - 1] for i in range(1, len(closes)) if closes[i - 1] > 0]
                volatility = (sum((value - sum(returns[-20:]) / len(returns[-20:])) ** 2 for value in returns[-20:]) / len(returns[-20:])) ** 0.5 if len(returns) >= 5 else None
                values = {
                    "rsi6": calc_rsi(closes, 6),
                    "ma5": simple_moving_average(closes, 5),
                    "ma10": simple_moving_average(closes, 10),
                    "ma20": simple_moving_average(closes, 20),
                    "volume_ratio": (volumes[-1] / (sum(volumes[-6:-1]) / 5)) if len(volumes) >= 6 and sum(volumes[-6:-1]) > 0 else None,
                    "atr14": calc_atr(highs, lows, closes, 14),
                    "support20": min(lows[-20:]) if len(lows) >= 20 else None,
                    "resistance20": max(highs[-20:]) if len(highs) >= 20 else None,
                    "volatility20": volatility,
                    "history_days": len(closes),
                    "momentum5": ((closes[-1] / closes[-6] - 1) * 100) if len(closes) >= 6 and closes[-6] > 0 else None,
                    "momentum20": ((closes[-1] / closes[-21] - 1) * 100) if len(closes) >= 21 and closes[-21] > 0 else None,
                }
                self._indicator_cache[cache_key] = (datetime.now(CHINA_TZ), values)
                quote.rsi6, quote.ma5, quote.ma10, quote.ma20, quote.volume_ratio = (values["rsi6"], values["ma5"], values["ma10"], values["ma20"], values["volume_ratio"])
                quote.atr14, quote.support20, quote.resistance20, quote.volatility20, quote.history_days = (values["atr14"], values["support20"], values["resistance20"], values["volatility20"], values["history_days"])
                quote.momentum5, quote.momentum20 = values.get("momentum5"), values.get("momentum20")
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
    def __init__(self, base_url: str, api_key: str, model: str, timeout: float = 30, min_interval: float = 10, daily_limit: int = 100):
        self.base_url, self.api_key, self.model, self.timeout = base_url.rstrip("/"), api_key, model, timeout
        self.min_interval, self.daily_limit = max(0, min_interval), max(1, daily_limit)
        self._annotation_times: list[datetime] = []

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
            async with httpx.AsyncClient(timeout=min(self.timeout, 15)) as client:
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
