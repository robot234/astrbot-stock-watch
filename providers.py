from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import math
import random
import re
import time
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, time as datetime_time, timedelta, timezone
from typing import Iterable

import httpx

from .core import CHINA_TZ, Candidate, NewsItem, Quote, _finite_positive, _normalize_plan_date, _source_is_trusted, apply_daily_indicators, calculate_daily_indicators, normalize_code


DEFAULT_TUSHARE_MIN_SNAPSHOT_SIZE = 4000
DEFAULT_TUSHARE_REQUIRE_UNIVERSE_EVIDENCE = True


def _sina_symbol(code: str) -> str:
    value = normalize_code(code)
    if value.startswith(("4", "8", "920")):
        return "bj" + value
    return ("sh" if value.startswith(("6", "68", "9")) else "sz") + value


@dataclass(slots=True)
class MarketSnapshotResult:
    quotes: list[Quote]
    trade_date: str | None
    source: str = ""
    quality: str = "unknown"
    fetched_at: datetime = field(default_factory=lambda: datetime.now(CHINA_TZ))


@dataclass(slots=True)
class EastmoneyFallbackResult:
    """An in-memory, date-verified Eastmoney preview result.

    This result is deliberately separate from ``MarketSnapshotResult`` so a
    transient fallback cannot be mistaken for a cacheable complete snapshot.
    Its bars are fetched from the same Eastmoney endpoint family as the
    quotes and are never handed to ``StockStore`` by the provider.
    """

    quotes: list[Quote]
    trade_date: str | None
    requested_date: str | None = None
    source: str = "eastmoney"
    quality: str = "degraded"
    complete: bool = False
    diagnostics: dict[str, object] = field(default_factory=dict)
    fetched_at: datetime = field(default_factory=lambda: datetime.now(CHINA_TZ))

    @property
    def actual_date(self) -> str | None:
        """Compatibility spelling used by screen coordinators."""
        return self.trade_date

    @property
    def actual_trade_date(self) -> str | None:
        return self.trade_date

    @property
    def data_mode(self) -> str:
        return "eastmoney_transient"


# Public aliases keep integrations readable while allowing older callers to
# discover the result under either the provider or fallback terminology.
EastmoneySnapshotResult = EastmoneyFallbackResult


@dataclass(slots=True)
class BulkDailyResult:
    """Result of one immutable Tushare raw batch build."""

    quotes: list[Quote]
    bars: dict[str, list[dict]]
    trade_date: str | None
    batch_id: str | None = None
    source: str = "tushare"
    quality: str = "unknown"
    complete: bool = False
    diagnostics: dict[str, object] = field(default_factory=dict)
    fetched_at: datetime = field(default_factory=lambda: datetime.now(CHINA_TZ))


class TushareBulkError(RuntimeError):
    """A Tushare bulk load failed without changing the active generation."""


class TushareCircuitOpen(TushareBulkError):
    """The provider breaker is cooling down after repeated exhausted failures."""


class TushareRateLimitError(TushareBulkError):
    """A request remained rate limited after its one delayed retry."""


class TushareRequestGateway:
    """Shared, cancellable and persisted gateway for Tushare requests.

    The gateway owns rate buckets and response caching; callers only provide
    an already-open HTTP client and an API payload.  Reservation writes are
    completed before any wait or network operation, so a blocked API cannot
    hold the SQLite write lock and cannot affect another API bucket.
    """

    DEFAULT_BUCKETS = {
        "trade_cal": (1, 60.0),
        "stock_basic": (1, 60.0),
        "daily": (30, 60.0),
    }

    def __init__(
        self,
        token: str = "",
        url: str = "https://api.tushare.pro",
        *,
        http_runtime: HttpRuntime | None = None,
        storage=None,
        api_limits: dict | None = None,
        bucket_limits: dict | None = None,
        clock=None,
        sleeper=None,
        retry_attempts: int = 3,
        rate_limit_block_seconds: float = 65.0,
        enforce_rate_limits: bool | None = None,
    ):
        self.token = str(token or "").strip()
        self.url = str(url or "").strip() or "https://api.tushare.pro"
        self.http = http_runtime or HttpRuntime(10, 8)
        self.storage = storage
        self.clock = clock or time.time
        self.sleeper = sleeper or asyncio.sleep
        self.retry_attempts = max(1, min(int(retry_attempts), 3))
        self.rate_limit_block_seconds = max(65.0, float(rate_limit_block_seconds))
        self.enforce_rate_limits = bool(storage) if enforce_rate_limits is None else bool(enforce_rate_limits)
        configured = dict(self.DEFAULT_BUCKETS)
        for source in (api_limits, bucket_limits):
            if not isinstance(source, dict):
                continue
            for key, value in source.items():
                name = str(key or "").strip().lower()
                if not name:
                    continue
                if isinstance(value, (list, tuple)) and len(value) >= 2:
                    limit, window = value[0], value[1]
                elif isinstance(value, dict):
                    limit, window = value.get("limit", value.get("requests", 1)), value.get("window_seconds", value.get("window", 60))
                else:
                    limit, window = value, 60
                try:
                    configured[name] = (max(1, int(limit)), max(1.0, float(window)))
                except (TypeError, ValueError, OverflowError):
                    continue
        self.api_limits = configured
        self._local_states: dict[str, dict] = {}
        self._local_cache: dict[tuple[str, str], dict] = {}

    def _now(self) -> float:
        if callable(self.clock):
            value = self.clock()
        elif hasattr(self.clock, "time"):
            value = self.clock.time()
        else:
            value = self.clock
        value = float(value)
        if not math.isfinite(value) or value < 0:
            raise ValueError("Tushare gateway clock value is invalid")
        return value

    async def _sleep(self, delay: float) -> None:
        result = self.sleeper(max(0.0, float(delay)))
        if inspect.isawaitable(result):
            await result

    @classmethod
    def api_name_for(cls, payload: dict, api_name: str | None = None) -> str:
        value = str(api_name or (payload or {}).get("api_name") or "daily").strip().lower()
        return value or "daily"

    @staticmethod
    def request_digest(payload: dict) -> str:
        safe = dict(payload or {})
        # The credential is intentionally excluded from cache identity.  It
        # is neither logged nor needed to distinguish the API request shape.
        safe.pop("token", None)
        text = json.dumps(safe, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    @staticmethod
    def _response_digest(body: dict) -> str:
        text = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
        return hashlib.sha256(text.encode("utf-8")).hexdigest()

    def _bucket(self, api_name: str) -> tuple[int, float]:
        value = self.api_limits.get(api_name, (1, 60.0))
        return max(1, int(value[0])), max(1.0, float(value[1]))

    def _local_state(self, api_name: str) -> dict:
        limit, window = self._bucket(api_name)
        state = self._local_states.setdefault(
            api_name,
            {
                "api_name": api_name,
                "bucket_limit": limit,
                "window_seconds": window,
                "window_started_at": 0.0,
                "request_count": 0,
                "blocked_until": 0.0,
                "retry_after": 0.0,
                "rate_limit_failures": 0,
                "failure_streak": 0,
                "circuit_open_until": 0.0,
                "last_error": "",
            },
        )
        return state

    def _state(self, api_name: str) -> dict:
        limit, window = self._bucket(api_name)
        if self.storage is not None and callable(getattr(self.storage, "provider_api_state", None)):
            return dict(self.storage.provider_api_state(api_name, bucket_limit=limit, window_seconds=int(window)))
        return dict(self._local_state(api_name))

    def _check_open(self, api_name: str, now: float) -> None:
        state = self._state(api_name)
        until = max(float(state.get("blocked_until") or 0), float(state.get("circuit_open_until") or 0))
        if float(state.get("circuit_open_until") or 0) > now:
            raise TushareCircuitOpen(f"Tushare {api_name} gateway circuit is open")
        # A rate block is handled by _reserve.  A persisted circuit remains a
        # hard failure even if an older blocked_until has elapsed.
        if until > now and float(state.get("circuit_open_until") or 0) <= now:
            return

    def _reserve_once(self, api_name: str, now: float) -> dict:
        limit, window = self._bucket(api_name)
        if self.storage is not None and callable(getattr(self.storage, "reserve_provider_api_request", None)):
            return dict(self.storage.reserve_provider_api_request(api_name, bucket_limit=limit, window_seconds=int(window), now=now))
        state = self._local_state(api_name)
        blocked = max(float(state.get("blocked_until") or 0), float(state.get("circuit_open_until") or 0))
        if float(state.get("circuit_open_until") or 0) > now:
            raise TushareCircuitOpen(f"Tushare {api_name} gateway circuit is open")
        if blocked > now:
            return {**state, "allowed": False, "wait_seconds": blocked - now}
        started = float(state.get("window_started_at") or 0)
        count = int(state.get("request_count") or 0)
        if (started <= 0 and count <= 0) or now - started >= window:
            started, count = now, 0
        if count >= limit:
            return {**state, "allowed": False, "wait_seconds": max(0.0, started + window - now)}
        state.update({"window_started_at": started, "request_count": count + 1})
        return {**state, "allowed": True, "wait_seconds": 0.0}

    async def _reserve(self, api_name: str) -> None:
        if not self.enforce_rate_limits:
            return
        while True:
            now = self._now()
            self._check_open(api_name, now)
            state = self._reserve_once(api_name, now)
            if state.get("allowed"):
                return
            wait = max(0.0, float(state.get("wait_seconds") or 0.0))
            # Never sleep while a storage transaction is open.  Cancellation
            # deliberately propagates through the await unchanged.
            await self._sleep(wait)

    def _update_state(self, api_name: str, **kwargs) -> dict:
        now = kwargs.pop("now", None)
        if self.storage is not None and callable(getattr(self.storage, "update_provider_api_state", None)):
            return dict(self.storage.update_provider_api_state(api_name, now=self._now() if now is None else now, **kwargs))
        state = self._local_state(api_name)
        current = self._now() if now is None else float(now)
        if kwargs.get("success"):
            state.update({"failure_streak": 0, "rate_limit_failures": 0, "blocked_until": 0.0, "retry_after": 0.0, "circuit_open_until": 0.0, "last_error": ""})
        if kwargs.get("rate_limited"):
            state["rate_limit_failures"] = int(state.get("rate_limit_failures") or 0) + 1
            state["failure_streak"] = int(state.get("failure_streak") or 0) + 1
            until = float(kwargs.get("blocked_until") or current + self.rate_limit_block_seconds)
            state["blocked_until"] = max(float(state.get("blocked_until") or 0), until)
            state["retry_after"] = state["blocked_until"]
            if kwargs.get("open_circuit"):
                state["circuit_open_until"] = max(float(state.get("circuit_open_until") or 0), current + self.rate_limit_block_seconds)
        if kwargs.get("error"):
            state["last_error"] = str(kwargs["error"])[:240]
        return dict(state)

    def _cache_get(self, api_name: str, digest: str, now: float, cache_key: str | None = None) -> dict | None:
        if self.storage is not None and callable(getattr(self.storage, "get_provider_cache", None)):
            try:
                row = self.storage.get_provider_cache(api_name, digest, now=now, cache_key=cache_key)
            except TypeError:
                row = self.storage.get_provider_cache(api_name, cache_key or digest, now=now)
            if row and isinstance(row.get("body"), dict):
                return dict(row["body"])
            return None
        row = self._local_cache.get((api_name, cache_key or digest))
        if row and float(row.get("expires_at") or 0) > now:
            return dict(row["body"])
        return None

    def _cache_put(self, api_name: str, digest: str, payload: dict, body: dict, ttl: float, now: float, cache_key: str | None = None) -> None:
        if ttl <= 0:
            return
        if self.storage is not None and callable(getattr(self.storage, "save_provider_cache", None)):
            try:
                self.storage.save_provider_cache(api_name, digest, body, ttl_seconds=ttl, payload_digest=digest, now=now, cache_key=cache_key)
            except TypeError:
                self.storage.save_provider_cache(api_name, cache_key or digest, body, ttl_seconds=ttl, payload_digest=digest, now=now)
            return
        self._local_cache[(api_name, cache_key or digest)] = {"body": dict(body), "expires_at": now + ttl}

    @staticmethod
    def _rate_limited(response=None, body=None) -> bool:
        try:
            if int(getattr(response, "status_code", 0) or 0) == 429:
                return True
        except (TypeError, ValueError, OverflowError):
            pass
        try:
            return int((body or {}).get("code")) == 40203
        except (TypeError, ValueError, OverflowError, AttributeError):
            return False

    @staticmethod
    def _response_error(response, status: int) -> httpx.HTTPStatusError:
        try:
            request = getattr(response, "request", None)
        except (AttributeError, RuntimeError):
            request = None
        request = request or httpx.Request("POST", "https://api.tushare.pro")
        return httpx.HTTPStatusError(f"Tushare HTTP {status}", request=request, response=response)

    async def _send_once(self, client, payload: dict):
        response = client.post(self.url, json=payload)
        return await response if inspect.isawaitable(response) else response

    async def request_json(
        self,
        client=None,
        payload: dict | None = None,
        *,
        api_name: str | None = None,
        cache_ttl: float = 0,
        cache_key: str | None = None,
    ) -> dict:
        if payload is None and isinstance(client, dict):
            payload, client = client, None
        payload = dict(payload or {})
        name = self.api_name_for(payload, api_name)
        digest = self.request_digest({**payload, "api_name": name})
        now = self._now()
        stable_cache_key = str(cache_key) if cache_key is not None else None
        if cache_ttl and (cached := self._cache_get(name, digest, now, stable_cache_key)) is not None:
            return cached

        async def send():
            if client is not None:
                return await self._send_once(client, payload)
            async with self.http.slot() as shared:
                return await self._send_once(shared, payload)

        if self.enforce_rate_limits:
            await self._reserve(name)
        rate_retry = False
        attempts = 0
        while True:
            response = await send()
            try:
                status = int(getattr(response, "status_code", 200) or 200)
            except (TypeError, ValueError, OverflowError):
                status = 200
            body = None
            if status != 429:
                try:
                    body = response.json()
                except (ValueError, TypeError, AttributeError):
                    body = None
            if self._rate_limited(response, body):
                now = self._now()
                state = self._state(name)
                previous = int(state.get("rate_limit_failures") or 0)
                open_circuit = previous >= 1 or rate_retry
                until = now + self.rate_limit_block_seconds
                self._update_state(name, now=now, blocked_until=until, rate_limited=True, open_circuit=open_circuit, error="rate limited")
                if rate_retry:
                    raise TushareRateLimitError(f"Tushare {name} rate limit persisted")
                rate_retry = True
                await self._sleep(self.rate_limit_block_seconds)
                # The delayed retry belongs to this request.  It is sent
                # after the block and does not hold a reservation transaction.
                continue
            if status in {408, 425} or 500 <= status <= 599:
                error = self._response_error(response, status)
                attempts += 1
                if attempts < self.retry_attempts:
                    await self._sleep(min(8.0, 0.25 * (2 ** (attempts - 1))))
                    if self.enforce_rate_limits:
                        await self._reserve(name)
                    continue
                self._update_state(name, error=f"HTTP {status}")
                raise error
            if hasattr(response, "raise_for_status"):
                response.raise_for_status()
            if not isinstance(body, dict):
                self._update_state(name, error="invalid response")
                raise ValueError("Tushare response is not an object")
            try:
                code = int(body.get("code") if body.get("code") is not None else 0)
            except (TypeError, ValueError, OverflowError):
                code = -1
            if code != 0:
                self._update_state(name, error="Tushare returned an error")
                raise TushareBulkError("Tushare returned an error")
            self._update_state(name, now=self._now(), success=True)
            self._cache_put(name, digest, payload, body, float(cache_ttl or 0), self._now(), stable_cache_key)
            return body

    async def request_api(self, api_name: str, payload: dict, *, client=None, cache_ttl: float = 0, cache_key: str | None = None) -> dict:
        return await self.request_json(client, payload, api_name=api_name, cache_ttl=cache_ttl, cache_key=cache_key)

    async def request(self, *args, **kwargs) -> dict:
        """Compatibility wrapper accepting either (client, payload) or
        (api_name, payload, client=...)."""
        if args and isinstance(args[0], str):
            api_name = args[0]
            payload = args[1] if len(args) > 1 else kwargs.pop("payload", {})
            return await self.request_api(api_name, payload, **kwargs)
        return await self.request_json(*args, **kwargs)

    post_json = request_json
    call = request_api


def canonical_tushare_code(value) -> tuple[str, str] | None:
    """Return (numeric code, uppercase ``ts_code``) with exchange validation."""
    text = str(value or "").strip().upper()
    match = re.fullmatch(r"(\d{6})(?:\.([A-Z]{2}))?", text)
    if not match:
        return None
    code, suffix = match.groups()
    # 920xxx is the newer Beijing Stock Exchange range; the older 900xxx
    # Shanghai B-share range still belongs to SH.
    expected = "BJ" if code.startswith(("4", "8", "920")) else "SH" if code.startswith(("6", "68", "9")) else "SZ"
    if suffix and suffix != expected:
        return None
    suffix = suffix or expected
    return code, f"{code}.{suffix}"


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


class TushareBulkDailyProvider:
    """Bulk, validated and retry-bounded Tushare daily-data provider."""

    def __init__(self, token: str, url: str = "https://api.tushare.pro", *, http_runtime: HttpRuntime | None = None, storage=None, gateway: TushareRequestGateway | None = None, page_size: int = 6000, retry_attempts: int = 3, bj_calendar_policy: str = "require_bse", dataset_key: str = "tushare_daily", min_snapshot_size: int = DEFAULT_TUSHARE_MIN_SNAPSHOT_SIZE, daily_snapshot_min_size: int | None = None, min_overall_coverage: float = 0.97, min_market_coverage: float = 0.95, min_market_median_ratio: float = 0.95, universe_version: str = "", universe_counts=None, universe_evidence=None, require_universe_evidence: bool = DEFAULT_TUSHARE_REQUIRE_UNIVERSE_EVIDENCE, raw_publish_enabled: bool = True, session_count: int = 120):
        self.token = str(token or "").strip()
        self.url = str(url or "").strip() or "https://api.tushare.pro"
        self.http = http_runtime or HttpRuntime(10, 8)
        self.storage = storage
        # Tushare's documented maximum is 6000.  Clamp integrations that
        # still pass the old 10000 default rather than issuing oversized
        # requests that can be silently truncated by the server.
        self.page_size = max(1, min(int(page_size), 6000))
        self.retry_attempts = max(1, min(int(retry_attempts), 3))
        policy = str(bj_calendar_policy or "require_bse").strip().lower()
        self.bj_calendar_policy = {"strict": "require_bse", "required": "require_bse", "fallback": "sse_fallback"}.get(policy, policy)
        if self.bj_calendar_policy not in {"require_bse", "sse_fallback", "exclude"}:
            self.bj_calendar_policy = "require_bse"
        self.dataset_key = str(dataset_key or "tushare_daily").strip() or "tushare_daily"
        self.min_snapshot_size = max(1, int(daily_snapshot_min_size if daily_snapshot_min_size is not None else min_snapshot_size))
        self.min_overall_coverage = float(min_overall_coverage)
        self.min_market_coverage = float(min_market_coverage)
        self.min_market_median_ratio = float(min_market_median_ratio)
        if not all(math.isfinite(value) and 0 <= value <= 1 for value in (self.min_overall_coverage, self.min_market_coverage, self.min_market_median_ratio)):
            raise ValueError("invalid Tushare raw coverage thresholds")
        self._configured_universe_version = str(universe_version or "").strip()[:160]
        self.universe_version = self._configured_universe_version
        self.universe_counts = universe_counts if universe_counts is not None else universe_evidence
        self._configured_universe_evidence: dict | None = None
        self.require_universe_evidence = True
        self._universe_evidence_cache: dict[str, dict] = {}
        if self.universe_counts not in (None, "", {}):
            # Validate once at construction so a malformed operator setting
            # cannot silently weaken a later batch's coverage gate.
            normalizer = getattr(storage, "_normalize_universe_counts", None) if storage is not None else None
            if callable(normalizer):
                self.universe_counts = normalizer(self.universe_counts)
            else:
                # The no-store provider is still used by integrations and
                # tests; it must apply the same strict evidence contract as
                # the built-in SQLite-backed path.
                from .storage import StockStore

                self.universe_counts = StockStore._normalize_universe_counts(self.universe_counts)
            self._configured_universe_evidence = dict(self.universe_counts)
        self._failure_streaks = {"calendar": 0, "daily": 0, "universe": 0}
        self._breaker_open_untils: dict[str, datetime | None] = {"calendar": None, "daily": None, "universe": None}
        # Keep the original daily aliases for integrations that inspected the
        # prototype provider directly.
        self._failure_streak = 0
        self._breaker_open_until: datetime | None = None
        self._allowed_bj_dates: set[str] = set()
        self._last_stock_basic_status_metadata: dict[str, dict] = {}
        self._last_completed_calendar_dates: list[str] = []
        # Calendar evidence is normalized to the single SSE session source;
        # require_bse remains a raw market-policy compatibility label but no
        # longer triggers an extra BSE request.
        self._bj_calendar_available = True
        self._calendar_evidence_policy = "sse_fallback"
        self.last_diagnostics: dict[str, object] = {}
        self.raw_publish_enabled = bool(raw_publish_enabled)
        self.session_count = max(1, min(int(session_count), 366))
        self.gateway = gateway or TushareRequestGateway(
            self.token,
            self.url,
            http_runtime=self.http,
            storage=self.storage,
            retry_attempts=self.retry_attempts,
            # Lightweight injected test runtimes intentionally model the
            # transport only; production HttpRuntime gets the persisted
            # bucket enforcement.  Callers that need the policy with a fake
            # transport can inject a gateway explicitly.
            enforce_rate_limits=bool(self.storage) and isinstance(self.http, HttpRuntime),
        )

    @property
    def breaker_open_until(self) -> datetime | None:
        return self._breaker_open_untils.get("daily")

    @property
    def breaker_open(self) -> bool:
        until = self._breaker_open_untils.get("daily")
        return until is not None and datetime.now(timezone.utc) < until

    @staticmethod
    def _operation_name(operation: str = "daily") -> str:
        value = str(operation or "").strip().lower()
        return value if value in {"calendar", "universe", "daily"} else "daily"

    def _check_breaker(self, operation: str = "daily") -> None:
        operation = self._operation_name(operation)
        until = self._breaker_open_untils.get(operation)
        if until is None:
            return
        if datetime.now(timezone.utc) >= until:
            self._breaker_open_untils[operation] = None
            self._failure_streaks[operation] = 0
            if operation == "daily":
                self._breaker_open_until = None
                self._failure_streak = 0
            return
        raise TushareCircuitOpen(f"Tushare {operation} provider breaker is cooling down")

    def _record_success(self, operation: str = "daily") -> None:
        operation = self._operation_name(operation)
        self._failure_streaks[operation] = 0
        self._breaker_open_untils[operation] = None
        if operation == "daily":
            self._failure_streak = 0
            self._breaker_open_until = None

    def _record_exhausted_failure(self, operation: str = "daily") -> None:
        operation = self._operation_name(operation)
        self._failure_streaks[operation] += 1
        if operation == "daily":
            self._failure_streak = self._failure_streaks[operation]
        if self._failure_streaks[operation] >= 3:
            cooldown_minutes = min(60, 10 * (2 ** (self._failure_streaks[operation] - 3)))
            until = datetime.now(timezone.utc) + timedelta(minutes=cooldown_minutes)
            self._breaker_open_untils[operation] = until
            if operation == "daily":
                self._breaker_open_until = until

    @staticmethod
    def _operation_for_payload(payload: dict) -> str:
        api_name = str(payload.get("api_name") or "").strip().lower()
        if api_name == "trade_cal":
            return "calendar"
        if api_name == "stock_basic":
            return "universe"
        return "daily"

    @staticmethod
    def _retryable_status(status: int) -> bool:
        return int(status) in {408, 425, 429} or 500 <= int(status) <= 599

    async def _post_json(self, client, payload: dict) -> dict:
        operation = self._operation_for_payload(payload)
        self._check_breaker(operation)
        try:
            body = await self.gateway.request_json(client, payload, api_name={"calendar": "trade_cal", "universe": "stock_basic"}.get(operation, "daily"))
        except TushareCircuitOpen:
            self._record_exhausted_failure(operation)
            raise
        except TushareRateLimitError:
            # A rate limit is a network availability condition, never a
            # malformed-history condition.  The caller maps it separately.
            self._record_exhausted_failure(operation)
            raise
        except (httpx.RequestError, asyncio.TimeoutError, OSError, httpx.HTTPStatusError):
            self._record_exhausted_failure(operation)
            raise
        except (ValueError, TypeError, KeyError, TushareBulkError):
            raise
        self._record_success(operation)
        return body

    @staticmethod
    def _date(value: str) -> str | None:
        text = str(value or "").strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            digits = text.replace("-", "")
        elif re.fullmatch(r"\d{8}", text):
            digits = text
        else:
            return None
        try:
            return datetime.strptime(digits, "%Y%m%d").strftime("%Y-%m-%d")
        except (TypeError, ValueError, OverflowError):
            return None

    @staticmethod
    def _is_open(value) -> bool:
        return value in (1, True, "1", "true", "True", "TRUE")

    @staticmethod
    def _completed_end_date(end_date: str) -> str:
        """Return the latest date that can represent a completed session."""
        today = datetime.now(CHINA_TZ).date()
        requested = datetime.strptime(end_date, "%Y-%m-%d").date()
        if requested > today:
            raise ValueError("Tushare end_date cannot be in the future")
        # A same-day daily bar is provisional until the mainland close.  The
        # scheduler normally runs after 15:00, while manual commands may run
        # during the session and must use the prior completed session.
        if requested == today and datetime.now(CHINA_TZ).time() < datetime_time(15, 0):
            requested -= timedelta(days=1)
        return requested.isoformat()

    async def _calendar_exchange(self, client, exchange: str, start_date: str, end_date: str) -> set[str]:
        limit = 1000
        result: set[str] = set()
        seen_dates: set[str] = set()
        for page_no in range(2000):
            payload = {
                "api_name": "trade_cal",
                "token": self.token,
                "params": {"exchange": exchange, "start_date": start_date.replace("-", ""), "end_date": end_date.replace("-", ""), "is_open": 1, "limit": limit, "offset": page_no * limit},
                "fields": "cal_date,is_open",
            }
            # Calendar responses are immutable for a completed date range and
            # are shared by all callers/restarts through the provider cache.
            body = await self.gateway.request_json(client, payload, api_name="trade_cal", cache_ttl=86400)
            data = body.get("data")
            if not isinstance(data, dict):
                raise ValueError("Tushare trade_cal data is invalid")
            fields = data.get("fields")
            items = data.get("items")
            if not isinstance(fields, list) or not isinstance(items, list) or "cal_date" not in fields or "is_open" not in fields:
                raise ValueError("Tushare trade_cal fields are invalid")
            if len(items) > limit:
                raise ValueError("Tushare trade_cal page exceeds requested limit")
            positions = {str(name): index for index, name in enumerate(fields)}
            for values in items:
                if not isinstance(values, list) or len(values) != len(fields):
                    raise ValueError("Tushare trade_cal row is invalid")
                date = self._date(values[positions["cal_date"]])
                if not date:
                    raise ValueError("Tushare trade_cal contains an invalid date")
                if date in seen_dates:
                    raise ValueError("Tushare trade_cal contains duplicate dates across pages")
                seen_dates.add(date)
                if date > end_date:
                    raise ValueError("Tushare trade_cal contains a future date")
                if date < start_date:
                    raise ValueError("Tushare trade_cal contains a date outside the requested range")
                if self._is_open(values[positions["is_open"]]):
                    result.add(date)
            if len(items) < limit:
                self._record_success("calendar")
                return result
        raise TushareBulkError("Tushare trade_cal pagination exceeded max_pages")

    async def fetch_completed_trade_dates(
        self,
        end_date: str,
        lookback_days: int | None = None,
        *,
        session_count: int | None = None,
    ) -> list[str]:
        end = self._date(end_date)
        if not end:
            raise ValueError("end_date must be YYYY-MM-DD")
        end = self._completed_end_date(end)
        end_value = datetime.strptime(end, "%Y-%m-%d").date()
        if lookback_days is not None and session_count is not None:
            raise ValueError("provide only one of session_count and lookback_days")
        requested_sessions = self.session_count if session_count is None and lookback_days is None else max(1, min(int(session_count if session_count is not None else lookback_days), 366))
        # A session count needs a natural-day envelope large enough for
        # weekends and holidays.  Do not cap the envelope at one calendar
        # year: 366 requested sessions can span well over 366 natural days.
        natural_days = max(requested_sessions, requested_sessions * 3 + 30)
        start = (end_value - timedelta(days=natural_days)).strftime("%Y-%m-%d")
        async with self.http.slot() as client:
            # SSE is the canonical session calendar.  SZSE/BSE probes made
            # the same logical refresh consume three rate slots and could
            # leave a valid BJ universe looking incomplete.  BJ handling is
            # represented as sse_fallback evidence instead.
            sse = await self._calendar_exchange(client, "SSE", start, end)
            szse = set()
            bse = set()
            self._bj_calendar_available = True
        # A date is a completed A-share session when either main exchange is
        # open.  A required BSE calendar is evidence, not an optional filter:
        # silently dropping BJ rows would make the universe contract false.
        dates = sse | szse
        self._allowed_bj_dates = set(dates)
        self._calendar_evidence_policy = "sse_fallback"
        completed = sorted(dates, reverse=True)
        self._last_completed_calendar_dates = list(completed)
        if len(completed) < requested_sessions:
            raise TushareBulkError(
                f"Tushare trade calendar returned only {len(completed)} completed sessions; {requested_sessions} required"
            )
        return completed[:requested_sessions]

    fetch_trade_dates = fetch_completed_trade_dates

    @staticmethod
    def _number(value, field: str, *, positive: bool = False, nonnegative: bool = False) -> float:
        if isinstance(value, bool) or value is None or str(value).strip() == "":
            raise ValueError(f"Tushare row missing {field}")
        try:
            value = float(value)
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError(f"Tushare row has invalid {field}") from exc
        if not math.isfinite(value) or (positive and value <= 0) or (nonnegative and value < 0):
            raise ValueError(f"Tushare row has invalid {field}")
        return value

    def _parse_daily_rows(self, body: dict, trade_date: str) -> list[dict]:
        data = body.get("data")
        if not isinstance(data, dict):
            raise ValueError("Tushare daily data is invalid")
        fields = data.get("fields")
        items = data.get("items")
        required = {"ts_code", "trade_date", "open", "high", "low", "close", "pre_close", "pct_chg", "vol", "amount"}
        if not isinstance(fields, list) or not isinstance(items, list) or not required.issubset(set(fields)):
            raise ValueError("Tushare daily fields are incomplete")
        positions = {str(name): index for index, name in enumerate(fields)}
        expected = self._date(trade_date)
        if not expected:
            raise ValueError("invalid requested trade date")
        result: list[dict] = []
        seen: set[str] = set()
        for values in items:
            if not isinstance(values, list) or len(values) != len(fields):
                raise ValueError("Tushare daily row is malformed")
            raw_code = values[positions["ts_code"]]
            code_info = canonical_tushare_code(raw_code)
            if not code_info:
                raise ValueError("Tushare row has invalid ts_code")
            code, ts_code = code_info
            if code in seen:
                raise ValueError("Tushare daily page contains duplicate ts_code")
            seen.add(code)
            row_date = self._date(values[positions["trade_date"]])
            if row_date != expected:
                raise ValueError("Tushare daily row has mixed trade_date")
            open_value = self._number(values[positions["open"]], "open", positive=True)
            high = self._number(values[positions["high"]], "high", positive=True)
            low = self._number(values[positions["low"]], "low", positive=True)
            close = self._number(values[positions["close"]], "close", positive=True)
            pre_close = self._number(values[positions["pre_close"]], "pre_close", positive=True)
            pct_change = self._number(values[positions["pct_chg"]], "pct_chg")
            volume = self._number(values[positions["vol"]], "vol", nonnegative=True)
            amount = self._number(values[positions["amount"]], "amount", nonnegative=True) * 1000.0
            if high < max(open_value, close) or low > min(open_value, close) or high < low:
                raise ValueError("Tushare row violates OHLC bounds")
            expected_pct = (close / pre_close - 1) * 100
            if abs(pct_change - expected_pct) > 0.35:
                raise ValueError("Tushare row pct_chg does not match close/pre_close")
            result.append({
                "trade_date": expected,
                "code": code,
                "ts_code": ts_code,
                "name": "",
                "open": open_value,
                "high": high,
                "low": low,
                "close": close,
                "pre_close": pre_close,
                "pct_change": pct_change,
                "volume": volume,
                "amount": amount,
                "source": "tushare",
                "basis": "unadjusted",
            })
        return result

    def _coverage_metrics(
        self,
        all_rows: dict[str, list[dict]],
        *,
        requested_date: str = "",
        actual_trade_date: str = "",
    ) -> dict:
        """Measure every partition against independent universe evidence."""
        daily_counts: dict[str, dict[str, int]] = {}
        for trade_date, rows in all_rows.items():
            counts: dict[str, int] = {"total": len(rows)}
            for row in rows:
                code_info = canonical_tushare_code(row.get("ts_code") or row.get("code"))
                if not code_info:
                    continue
                market = code_info[1].rsplit(".", 1)[-1]
                counts[market] = counts.get(market, 0) + 1
            daily_counts[str(trade_date)] = counts
        dates = sorted(daily_counts)
        requested = requested_date or (dates[-1] if dates else "")
        actual = actual_trade_date or (dates[-1] if dates else "")
        calculator = getattr(self.storage, "_raw_coverage_metrics", None) if self.storage is not None else None
        if not callable(calculator):
            from .storage import StockStore

            calculator = StockStore._raw_coverage_metrics
        return calculator(
            daily_counts,
            min_row_count=self.min_snapshot_size,
            min_overall_coverage=self.min_overall_coverage,
            min_market_coverage=self.min_market_coverage,
            min_market_median_ratio=self.min_market_median_ratio,
            universe_version=self.universe_version,
            universe_counts=self.universe_counts,
            require_universe_evidence=True,
            requested_date=requested,
            actual_trade_date=actual,
            bj_calendar_policy=self.bj_calendar_policy,
        )

    async def fetch_daily_page(self, trade_date: str, *, offset: int = 0, page_size: int | None = None, client=None) -> list[dict]:
        expected = self._date(trade_date)
        if not expected:
            raise ValueError("trade_date must be YYYY-MM-DD")
        size = max(1, min(int(page_size or self.page_size), 6000))
        payload = {
            "api_name": "daily",
            "token": self.token,
            "params": {"trade_date": expected.replace("-", ""), "limit": size, "offset": max(0, int(offset))},
            "fields": "ts_code,trade_date,open,high,low,close,pre_close,pct_chg,vol,amount",
        }
        if client is None:
            async with self.http.slot() as shared:
                body = await self._post_json(shared, payload)
        else:
            body = await self._post_json(client, payload)
        return self._parse_daily_rows(body, expected)

    _fetch_daily_page = fetch_daily_page

    async def fetch_daily_date(self, trade_date: str, *, client=None, page_size: int | None = None, max_pages: int = 1000) -> list[dict]:
        """Fetch one date until Tushare returns an empty page."""
        rows: list[dict] = []
        seen: set[str] = set()
        size = max(1, min(int(page_size or self.page_size), 6000))
        for page in range(max(1, min(int(max_pages), 2000))):
            page_rows = await self.fetch_daily_page(trade_date, offset=page * size, page_size=size, client=client)
            if not page_rows:
                return rows
            for row in page_rows:
                if row["code"] in seen:
                    raise ValueError("Tushare daily result contains duplicate ts_code")
                seen.add(row["code"])
            rows.extend(page_rows)
        raise TushareBulkError("Tushare daily pagination exceeded max_pages")

    _fetch_tushare_date = fetch_daily_date

    async def _iter_pages(self, fetch_page, *, limit: int, max_pages: int = 1000, start_page: int = 0):
        """Yield pages using the unfiltered server length as the stop signal."""
        size = max(1, min(int(limit), 6000))
        page_limit = max(1, min(int(max_pages), 2000))
        try:
            first_page = max(0, int(start_page))
        except (TypeError, ValueError, OverflowError):
            first_page = 0
        for page_no in range(first_page, first_page + page_limit):
            page = await fetch_page(page_no * size, size)
            if not page:
                return
            short = len(page) < size
            yield page_no, page
            if short:
                return
        raise TushareBulkError("Tushare pagination exceeded max_pages")

    paginate_pages = _iter_pages

    def _page_filter_policy(self, trade_date: str) -> str:
        """Return the stable projection policy committed with each page."""
        if self.bj_calendar_policy == "exclude":
            return "exclude_bj"
        if self.bj_calendar_policy == "require_bse" and (
            not getattr(self, "_bj_calendar_available", False)
            or trade_date not in getattr(self, "_allowed_bj_dates", set())
        ):
            return "exclude_bj"
        return "include_bj"

    def _begin_raw_batch(self, requested: str, dates: list[str], evidence: dict, *, dataset_key: str | None = None) -> tuple[str | None, dict]:
        """Get or create a restartable raw batch for one exact window."""
        if self.storage is None:
            return None, {}
        values = {
            "dataset_key": str(dataset_key or self.dataset_key),
            "provider": "tushare",
            "expected_days": len(dates),
            "expected_trade_dates": dates,
            "page_size": self.page_size,
            "min_row_count": self.min_snapshot_size,
            "min_overall_coverage": self.min_overall_coverage,
            "min_market_coverage": self.min_market_coverage,
            "min_market_median_ratio": self.min_market_median_ratio,
            "universe_version": str(evidence.get("universe_version") or self.universe_version),
            "universe_counts": evidence,
            "bj_calendar_policy": self.bj_calendar_policy,
        }
        get_or_create = getattr(self.storage, "get_or_create_raw_batch", None)
        if callable(get_or_create):
            row = get_or_create(requested, **values)
            if isinstance(row, dict):
                identifier = row.get("batch_id")
                if identifier:
                    return str(identifier), row
            if row:
                return str(row), {"batch_id": str(row), "page_size": self.page_size}
            raise RuntimeError("raw batch lookup returned no batch")
        create = getattr(self.storage, "create_raw_batch", None)
        if not callable(create):
            raise RuntimeError("raw batch storage is unavailable")
        try:
            identifier = create(requested, **values)
        except TypeError:
            # Keep integrations with the v0.13 storage shim usable; the
            # compatibility path cannot resume pages but still publishes.
            values.pop("expected_trade_dates", None)
            values.pop("page_size", None)
            identifier = create(requested, **values)
        return str(identifier), {"batch_id": str(identifier), "page_size": self.page_size}

    def _staged_pages(self, batch_id: str, trade_date: str, *, page_size: int, filter_policy: str = "include_all") -> tuple[list[dict], int, bool]:
        """Return validated staged rows, next server page, and completion."""
        if self.storage is None:
            return [], 0, False
        loader = getattr(self.storage, "raw_batch_partitions", None) or getattr(self.storage, "staged_raw_partitions", None)
        if not callable(loader):
            return [], 0, False
        try:
            pages = loader(batch_id, trade_date, include_rows=True)
        except TypeError:
            pages = loader(batch_id, trade_date)
        pages = list(pages or [])
        pages.sort(key=lambda item: int(item.get("partition_no", 0)))
        expected_policy = str(filter_policy or "include_all").strip().lower()
        rows: list[dict] = []
        next_page = 0
        complete = False
        seen_codes: set[str] = set()
        for item in pages:
            if not isinstance(item, dict):
                raise TushareBulkError("staged raw partition metadata is invalid")
            try:
                page_no = int(item.get("partition_no"))
                count = int(item.get("row_count"))
                server_count = int(item.get("server_row_count", count))
            except (TypeError, ValueError, OverflowError) as exc:
                raise TushareBulkError("staged raw partition metadata is invalid") from exc
            if page_no < 0 or count < 0 or server_count < 0:
                raise TushareBulkError("staged raw partition metadata is invalid")
            if str(item.get("filter_policy") or "include_all").strip().lower() != expected_policy:
                raise TushareBulkError("staged raw page filter policy mismatch")
            if page_no < next_page:
                raise TushareBulkError("staged raw partition numbers are not ordered")
            if page_no > next_page:
                # A gap is possible when a market-policy filter removed a
                # whole server page.  Reuse remains safe after the gap only
                # when the caller probes that page again.
                break
            page_rows = list(item.get("rows") or [])
            if len(page_rows) != count:
                raise TushareBulkError("staged raw partition row count mismatch")
            for row in page_rows:
                code = str(row.get("code") or "")
                if code in seen_codes:
                    raise TushareBulkError("staged raw date contains duplicate codes")
                seen_codes.add(code)
            rows.extend(page_rows)
            next_page = page_no + 1
            # Completion is based on the unfiltered server page, not on the
            # filtered projection persisted in the bar partition.
            if server_count >= 0 and bool(item.get("server_terminal")):
                complete = True
                break
        return rows, next_page, complete

    @staticmethod
    def _canonical_evidence_date(value) -> str | None:
        text = str(value or "").strip()
        if not text:
            return None
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text) or re.fullmatch(r"\d{8}", text):
            return TushareBulkDailyProvider._date(text)
        return None

    def _normalize_evidence(self, value) -> dict:
        from .storage import StockStore

        return StockStore._normalize_universe_counts(value)

    @staticmethod
    def _normalize_evidence_status_rows(status: str, rows) -> list[dict]:
        from .storage import StockStore

        return StockStore._normalize_universe_status_rows(status, rows)

    def _validate_evidence(self, evidence: dict, trade_date: str) -> dict:
        from .storage import StockStore

        errors = StockStore._validate_universe_evidence(
            evidence,
            requested_date=trade_date,
            actual_trade_date=trade_date,
            bj_calendar_policy=self.bj_calendar_policy,
            universe_version=self._configured_universe_version,
        )
        if errors:
            raise TushareBulkError("; ".join(dict.fromkeys(errors)))
        return evidence

    async def _fetch_stock_basic_status(self, status: str, *, client=None, max_pages: int = 1000) -> list[dict]:
        status = str(status or "").strip().upper()
        if status not in {"L", "D", "P"}:
            raise ValueError("invalid Tushare list status")
        rows: list[dict] = []
        seen_codes: set[str] = set()
        size = max(1, min(int(self.page_size), 6000))
        request_digests: list[str] = []
        response_digests: list[str] = []
        required = {"ts_code", "list_status", "list_date", "delist_date"}

        def remember_metadata() -> None:
            metadata = {
                "status": status,
                "request_digest": hashlib.sha256(json.dumps(request_digests, separators=(",", ":")).encode("utf-8")).hexdigest(),
                "payload_digest": hashlib.sha256(json.dumps({"status": status, "pages": len(request_digests)}, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest(),
                "response_digest": hashlib.sha256(json.dumps(response_digests, separators=(",", ":")).encode("utf-8")).hexdigest(),
            }
            all_metadata = getattr(self, "_last_stock_basic_status_metadata", {})
            if not isinstance(all_metadata, dict):
                all_metadata = {}
            all_metadata[status] = metadata
            self._last_stock_basic_status_metadata = all_metadata

        for page_no in range(max(1, min(int(max_pages), 2000))):
            payload = {
                "api_name": "stock_basic",
                "token": self.token,
                "params": {"list_status": status, "limit": size, "offset": page_no * size},
                "fields": "ts_code,list_status,list_date,delist_date",
            }
            request_digest = getattr(self.gateway, "request_digest", None)
            if callable(request_digest):
                request_digests.append(str(request_digest(payload)))
            else:
                request_text = json.dumps({key: value for key, value in payload.items() if key != "token"}, sort_keys=True, separators=(",", ":"))
                request_digests.append(hashlib.sha256(request_text.encode("utf-8")).hexdigest())
            if client is None:
                async with self.http.slot() as shared:
                    body = await self.gateway.request_json(shared, payload, api_name="stock_basic", cache_ttl=86400)
            else:
                body = await self.gateway.request_json(client, payload, api_name="stock_basic", cache_ttl=86400)
            response_digest = getattr(self.gateway, "_response_digest", None)
            if callable(response_digest):
                response_digests.append(str(response_digest(body)))
            else:
                response_text = json.dumps(body, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
                response_digests.append(hashlib.sha256(response_text.encode("utf-8")).hexdigest())
            data = body.get("data")
            if not isinstance(data, dict):
                raise ValueError("Tushare stock_basic data is invalid")
            fields = data.get("fields")
            items = data.get("items")
            if not isinstance(fields, list) or not isinstance(items, list) or not required.issubset(set(fields)):
                raise ValueError("Tushare stock_basic fields are incomplete")
            positions = {str(name): index for index, name in enumerate(fields)}
            if not items:
                remember_metadata()
                return rows
            for values in items:
                if not isinstance(values, list) or len(values) != len(fields):
                    raise ValueError("Tushare stock_basic row is malformed")
                code_info = canonical_tushare_code(values[positions["ts_code"]])
                if not code_info:
                    raise ValueError("Tushare stock_basic contains an unknown market code")
                row_status = str(values[positions["list_status"]] or "").strip().upper()
                if row_status != status or row_status not in {"L", "D", "P"}:
                    raise ValueError("Tushare stock_basic contains an invalid list status")
                list_date = self._canonical_evidence_date(values[positions["list_date"]])
                delist_raw = values[positions["delist_date"]]
                delist_date = self._canonical_evidence_date(delist_raw) if str(delist_raw or "").strip() else ""
                if not list_date or (str(delist_raw or "").strip() and not delist_date):
                    raise ValueError("Tushare stock_basic contains an invalid listing date")
                if delist_date and delist_date < list_date:
                    raise ValueError("Tushare stock_basic contains an invalid delisting range")
                if code_info[0] in seen_codes:
                    raise ValueError("Tushare stock_basic contains duplicate codes across pages")
                seen_codes.add(code_info[0])
                rows.append({
                    "code": code_info[0],
                    "ts_code": code_info[1],
                    "market": code_info[1].rsplit(".", 1)[-1],
                    "list_status": row_status,
                    "list_date": list_date,
                    "delist_date": delist_date,
                })
            if len(items) < size:
                remember_metadata()
                return rows
        raise TushareBulkError("Tushare stock_basic pagination exceeded max_pages")

    async def fetch_universe_evidence(self, trade_date: str, *, client=None, max_pages: int = 1000) -> dict:
        """Build independent eligible-universe evidence for one trade date.

        When a store is available, the L/D/P responses form a durable staging
        cycle.  A restart first reuses a valid active snapshot, then valid
        staged status rows, and only requests the missing or rejected status.
        """
        effective = self._date(trade_date)
        if not effective:
            raise ValueError("universe evidence trade_date must be YYYY-MM-DD")

        # The durable path is authoritative.  Memory is only a compatibility
        # optimization for integrations that do not provide a store.
        if self.storage is not None:
            active_loader = getattr(self.storage, "active_universe_evidence", None) or getattr(self.storage, "get_active_universe_evidence", None)
            if callable(active_loader):
                active = active_loader(
                    effective,
                    provider="tushare",
                    bj_calendar_policy=self.bj_calendar_policy,
                    universe_version=self._configured_universe_version,
                )
                if isinstance(active, dict) and isinstance(active.get("evidence"), dict):
                    evidence = self._validate_evidence(dict(active["evidence"]), effective)
                    self._universe_evidence_cache[effective] = dict(evidence)
                    self.universe_version = str(evidence.get("universe_version") or self.universe_version)
                    self.universe_counts = evidence
                    return evidence
        else:
            cached = self._universe_evidence_cache.get(effective)
            if cached is not None:
                return self._validate_evidence(dict(cached), effective)

        configured = self._configured_universe_evidence
        if configured is not None:
            evidence = self._normalize_evidence(configured)
            evidence = self._validate_evidence(evidence, effective)
            self._universe_evidence_cache[effective] = dict(evidence)
            self.universe_version = str(evidence.get("universe_version") or self.universe_version)
            self.universe_counts = evidence
            return evidence
        if not self.token:
            raise TushareBulkError("Tushare token is required for universe evidence")
        if not self._bj_calendar_available:
            raise TushareBulkError("Tushare session calendar evidence is unavailable for universe date")

        batch_id: str | None = None
        batch: dict = {}
        if self.storage is not None:
            get_batch = getattr(self.storage, "get_or_create_universe_evidence_batch", None) or getattr(self.storage, "begin_universe_evidence_batch", None)
            if not callable(get_batch):
                raise RuntimeError("universe evidence storage is unavailable")
            batch = dict(get_batch(
                effective,
                provider="tushare",
                bj_calendar_policy=self.bj_calendar_policy,
                universe_version=self._configured_universe_version,
            ) or {})
            batch_id = str(batch.get("evidence_batch_id") or "") or None

        try:
            status_records: dict[str, dict] = {}
            if batch_id is not None:
                loader = getattr(self.storage, "universe_evidence_status_records", None) or getattr(self.storage, "load_universe_evidence_status_records", None)
                if callable(loader):
                    status_records = dict(loader(batch_id) or {})
                else:
                    loaded = self.storage.universe_evidence_statuses(batch_id, with_metadata=True)
                    status_records = dict(loaded or {})

            all_rows: dict[str, dict] = {}
            for record in status_records.values():
                for row in (record.get("rows") if isinstance(record, dict) else []) or []:
                    previous = all_rows.get(row["code"])
                    if previous is not None:
                        raise ValueError("Tushare stock_basic contains duplicate status rows")
                    all_rows[row["code"]] = dict(row)

            missing_statuses = [status for status in ("L", "D", "P") if status not in status_records]

            async def load_status(status: str, shared_client=None) -> None:
                if isinstance(getattr(self, "_last_stock_basic_status_metadata", None), dict):
                    self._last_stock_basic_status_metadata.pop(status, None)
                try:
                    values = await self._fetch_stock_basic_status(status, client=shared_client, max_pages=max_pages)
                except TypeError as exc:
                    # Older integrations monkeypatch the original one-argument
                    # helper; keep that adapter working during recovery.
                    try:
                        values = await self._fetch_stock_basic_status(status)
                    except TypeError:
                        raise exc
                if not isinstance(values, list):
                    raise ValueError("Tushare stock_basic status response is invalid")
                for row in values:
                    if not isinstance(row, dict) or str(row.get("code") or "") in all_rows:
                        raise ValueError("Tushare stock_basic contains duplicate status rows")
                    all_rows[str(row["code"])] = dict(row)
                if batch_id is not None:
                    metadata = getattr(self, "_last_stock_basic_status_metadata", {})
                    metadata = metadata.get(status, {}) if isinstance(metadata, dict) else {}
                    stage = getattr(self.storage, "stage_universe_evidence_status", None) or getattr(self.storage, "stage_evidence_status", None)
                    if not callable(stage):
                        raise RuntimeError("universe evidence status storage is unavailable")
                    stage_kwargs = {
                        "request_digest": str(metadata.get("request_digest") or ""),
                        "payload_digest": str(metadata.get("payload_digest") or ""),
                        "response_digest": str(metadata.get("response_digest") or ""),
                    }
                    try:
                        stage(batch_id, status, values, **stage_kwargs)
                    except TypeError as exc:
                        # Keep the adapter usable with a pre-v14 storage
                        # shim that has not added response_digest yet.
                        stage_kwargs.pop("response_digest", None)
                        try:
                            stage(batch_id, status, values, **stage_kwargs)
                        except TypeError:
                            raise exc
                    status_records[status] = {"rows": self._normalize_evidence_status_rows(status, values)}

            if missing_statuses:
                if client is None:
                    async with self.http.slot() as shared:
                        for status in missing_statuses:
                            await load_status(status, shared)
                else:
                    for status in missing_statuses:
                        await load_status(status, client)

            if not all_rows:
                raise TushareBulkError("Tushare stock_basic returned no universe rows")

            eligible = []
            for row in all_rows.values():
                if row["list_date"] > effective:
                    continue
                if row["delist_date"] and effective > row["delist_date"]:
                    continue
                if row["list_status"] == "D" and not row["delist_date"]:
                    raise ValueError("Tushare delisted row has no delist_date")
                if self.bj_calendar_policy == "exclude" and row["market"] == "BJ":
                    continue
                eligible.append(row)
            if not eligible:
                raise TushareBulkError("Tushare stock_basic has no eligible universe rows")

            markets: dict[str, int] = {}
            status_counts: dict[str, int] = {}
            for row in eligible:
                markets[row["market"]] = markets.get(row["market"], 0) + 1
                status_counts[row["list_status"]] = status_counts.get(row["list_status"], 0) + 1
            eligible_markets = sorted(markets)
            required_markets = {"SH", "SZ"}
            if self.bj_calendar_policy in {"require_bse", "sse_fallback"}:
                required_markets.add("BJ")
            if set(eligible_markets) != required_markets:
                raise TushareBulkError("universe evidence market set does not match BJ policy")
            memberships = [
                {
                    "code": row["code"],
                    "ts_code": row["ts_code"],
                    "market": row["market"],
                    "list_status": row["list_status"],
                    "list_date": self._date(row["list_date"]) or str(row["list_date"] or ""),
                    "delist_date": self._date(row["delist_date"]) if row["delist_date"] else "",
                }
                for row in sorted(eligible, key=lambda item: (item["code"], item["list_status"]))
            ]
            membership_text = json.dumps(memberships, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            market_text = json.dumps(dict(sorted(markets.items())), sort_keys=True, separators=(",", ":"))
            status_digests = {}
            for status in ("L", "D", "P"):
                status_rows = [item for item in memberships if item["list_status"] == status]
                status_digests[status] = hashlib.sha256(json.dumps(status_rows, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
            evidence = {
                "evidence_version": 2,
                "method": "stock_basic",
                "source": "tushare",
                "universe_version": self._configured_universe_version or f"tushare-stock-basic-v2:{effective}",
                "effective_date": effective,
                "target_session": effective,
                "valid_from": effective,
                "total": len(eligible),
                "markets": dict(sorted(markets.items())),
                "eligible_markets": eligible_markets,
                "bj_calendar_policy": self.bj_calendar_policy,
                "calendar_policy": "sse_fallback",
                "status_counts": dict(sorted(status_counts.items())),
                "memberships": memberships,
                "list_status_membership": memberships,
                "status_digests": status_digests,
                "membership_digest": hashlib.sha256(membership_text.encode("utf-8")).hexdigest(),
                "market_digest": hashlib.sha256(market_text.encode("utf-8")).hexdigest(),
                "exact_market_digest": hashlib.sha256(market_text.encode("utf-8")).hexdigest(),
                "suspension_method": "not_available_ratios_only",
                "suspension_evidence": False,
            }
            from .storage import StockStore

            evidence["digest"] = StockStore._universe_evidence_digest(evidence)
            evidence = self._validate_evidence(evidence, effective)
            if batch_id is not None:
                activate = getattr(self.storage, "activate_universe_evidence", None) or getattr(self.storage, "publish_universe_evidence", None)
                if not callable(activate):
                    raise RuntimeError("universe evidence activation storage is unavailable")
                activated = activate(batch_id, evidence)
                if isinstance(activated, dict) and isinstance(activated.get("evidence"), dict):
                    evidence = self._validate_evidence(dict(activated["evidence"]), effective)
            self._universe_evidence_cache[effective] = dict(evidence)
            self.universe_version = evidence["universe_version"]
            self.universe_counts = evidence
            return evidence
        except asyncio.CancelledError:
            if batch_id is not None:
                fail = getattr(self.storage, "fail_universe_evidence_batch", None) or getattr(self.storage, "abort_universe_evidence_batch", None)
                if callable(fail):
                    fail(batch_id, "universe evidence fetch cancelled")
            raise
        except Exception as exc:
            if batch_id is not None:
                fail = getattr(self.storage, "fail_universe_evidence_batch", None) or getattr(self.storage, "abort_universe_evidence_batch", None)
                if callable(fail):
                    fail(batch_id, str(exc)[:500])
            raise

    async def fetch_bulk_daily_result(self, trade_date: str = "", *, lookback_days: int | None = None, session_count: int | None = None, max_pages: int = 1000) -> BulkDailyResult:
        requested = self._date(trade_date or datetime.now(CHINA_TZ).date().isoformat())
        if not requested:
            raise ValueError("trade_date must be YYYY-MM-DD")
        self._check_breaker()
        diagnostics: dict[str, object] = {
            "requested_date": requested,
            "network_failed": 0,
            "history_invalid": 0,
            "cache_basis_rejected": 0,
            "pages": 0,
            "dates_requested": 0,
            "dates_loaded": 0,
            "rows": 0,
            "bj_calendar_policy": self.bj_calendar_policy,
        }
        try:
            completed_end = self._completed_end_date(requested)
            target_sessions = self.session_count if session_count is None and lookback_days is None else max(1, min(int(session_count if session_count is not None else lookback_days), 366))
            # Keep the production path on the authoritative session-count
            # contract.  ``lookback_days`` remains accepted only at the
            # public compatibility boundary below.
            dates = await self.fetch_completed_trade_dates(requested, session_count=target_sessions)
        except (httpx.HTTPError, asyncio.TimeoutError, OSError, TushareCircuitOpen, TushareRateLimitError):
            diagnostics["network_failed"] = 1
            self.last_diagnostics = diagnostics
            return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
        except TushareBulkError as exc:
            # ``lookback_days`` is the pre-v0.13 compatibility argument.  A
            # legacy caller historically accepted a short calendar window;
            # keep that adapter local while the public calendar method remains
            # exact-count and fail-closed for new session-count callers.
            available = list(getattr(self, "_last_completed_calendar_dates", []) or [])
            if session_count is None and lookback_days is not None and "only" in str(exc).lower() and available:
                dates = available[:target_sessions]
            else:
                diagnostics["history_invalid"] = 1
                self.last_diagnostics = diagnostics
                return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
        except (ValueError, TypeError, KeyError, TushareBulkError):
            diagnostics["history_invalid"] = 1
            self.last_diagnostics = diagnostics
            return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)

        if any(self._date(value) != value or value > completed_end or value > requested for value in dates):
            diagnostics["history_invalid"] = 1
            self.last_diagnostics = diagnostics
            return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
        # Only the deprecated lookback compatibility path may synthesize the
        # requested date for a caller that supplied no calendar rows.  Exact
        # session-count callers must rely on calendar evidence.
        if not dates and completed_end == requested and session_count is None and lookback_days is not None:
            dates = [requested]
        target_sessions = self.session_count if session_count is None and lookback_days is None else max(1, min(int(session_count if session_count is not None else lookback_days), 366))
        exact_session_contract = session_count is not None or lookback_days is None
        if exact_session_contract and len(dates) < target_sessions:
            diagnostics["history_invalid"] = 1
            self.last_diagnostics = diagnostics
            return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
        dates = dates[:target_sessions]
        if not dates:
            diagnostics["history_invalid"] = 1
            self.last_diagnostics = diagnostics
            return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
        diagnostics["dates_requested"] = len(dates)
        try:
            universe_evidence = await self.fetch_universe_evidence(dates[0], max_pages=max_pages)
        except (httpx.HTTPError, asyncio.TimeoutError, OSError, TushareCircuitOpen, TushareRateLimitError):
            diagnostics["network_failed"] = 1
            self.last_diagnostics = diagnostics
            return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
        except (ValueError, TypeError, KeyError, TushareBulkError):
            diagnostics["history_invalid"] = 1
            self.last_diagnostics = diagnostics
            return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
        diagnostics.update({
            "universe_version": universe_evidence.get("universe_version"),
            "universe_digest": universe_evidence.get("digest"),
            "universe_effective_date": universe_evidence.get("effective_date"),
            "eligible_markets": universe_evidence.get("eligible_markets", []),
            "universe_method": universe_evidence.get("method"),
            "universe_source": universe_evidence.get("source"),
            "suspension_method": universe_evidence.get("suspension_method"),
        })
        all_rows: dict[str, list[dict]] = {}
        batch_id = None
        batch_record: dict = {}
        if self.storage is not None:
            try:
                # Publish only a complete calendar window.  A calendar session
                # with no daily rows is an incomplete batch, never a usable
                # generation.
                batch_id, batch_record = self._begin_raw_batch(requested, dates, universe_evidence)
            except (ValueError, TypeError, KeyError, RuntimeError):
                diagnostics["history_invalid"] = 1
                self.last_diagnostics = diagnostics
                return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)

        def fail_staging(error: str) -> None:
            if batch_id is None:
                return
            fail_batch = getattr(self.storage, "fail_raw_batch", None)
            if callable(fail_batch):
                try:
                    fail_batch(batch_id, error)
                except Exception:
                    # The original fetch/validation error is the useful
                    # diagnostic; cleanup failure must not mask it.
                    pass

        try:
            async with self.http.slot() as client:
                for date in dates:
                    stored_page_size = self.page_size
                    try:
                        stored_page_size = max(1, min(int(batch_record.get("page_size") or self.page_size), 6000))
                    except (TypeError, ValueError, OverflowError):
                        stored_page_size = self.page_size
                    filter_policy = self._page_filter_policy(date)
                    try:
                        rows, start_page, staged_complete = self._staged_pages(batch_id, date, page_size=stored_page_size, filter_policy=filter_policy) if batch_id else ([], 0, False)
                    except TushareBulkError as exc:
                        if "filter policy mismatch" not in str(exc).lower() or not batch_id:
                            raise
                        # A changed projection policy cannot safely overwrite
                        # immutable filtered partitions in place.  Clear only
                        # this restartable date and fetch it again, retaining
                        # all other dates and the active generation.
                        reset = getattr(self.storage, "reset_raw_batch_pages", None)
                        if not callable(reset):
                            raise
                        reset(batch_id, date, from_page=0)
                        rows, start_page, staged_complete = [], 0, False
                    if staged_complete:
                        all_rows[date] = rows
                        continue
                    size = stored_page_size
                    async def load_page(offset, page_size):
                        return await self.fetch_daily_page(date, offset=offset, page_size=page_size, client=client)
                    remaining_pages = max(1, int(max_pages) - start_page)
                    async for page_no, raw_page in self._iter_pages(load_page, limit=size, max_pages=remaining_pages, start_page=start_page):
                        diagnostics["pages"] = int(diagnostics["pages"]) + 1
                        # Stop decisions happen inside _iter_pages before any
                        # market-policy filtering; a filtered short page must
                        # never hide an exact-limit server page.
                        page = raw_page if filter_policy != "exclude_bj" else [row for row in raw_page if not row["ts_code"].endswith(".BJ")]
                        if page:
                            rows.extend(page)
                            if batch_id is not None:
                                self.storage.stage_raw_partition(
                                    batch_id, date, page, partition_no=page_no,
                                    source="tushare", basis="unadjusted",
                                    server_rows=raw_page,
                                    server_row_count=len(raw_page),
                                    server_terminal=len(raw_page) < size,
                                    filter_policy=filter_policy,
                                )
                        elif batch_id is not None:
                            stage_page = getattr(self.storage, "stage_raw_page_metadata", None)
                            if not callable(stage_page):
                                raise RuntimeError("raw page metadata storage is unavailable")
                            stage_page(
                                batch_id,
                                date,
                                partition_no=page_no,
                                server_rows=raw_page,
                                server_row_count=len(raw_page),
                                server_terminal=len(raw_page) < size,
                                filter_policy=filter_policy,
                                filtered_rows=[],
                                source="tushare",
                            )
                    if not rows:
                        raise TushareBulkError("Tushare daily returned no rows for a completed session")
                    seen = set()
                    for row in rows:
                        if row["code"] in seen:
                            raise ValueError("Tushare daily result contains duplicate ts_code")
                        seen.add(row["code"])
                    all_rows[date] = rows
        except asyncio.CancelledError:
            fail_staging("raw fetch cancelled")
            raise
        except (httpx.HTTPError, asyncio.TimeoutError, OSError, TushareCircuitOpen, TushareRateLimitError):
            diagnostics["network_failed"] = 1
            fail_staging("network failure while fetching raw partitions")
            self.last_diagnostics = diagnostics
            return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
        except (ValueError, TypeError, KeyError, TushareBulkError):
            diagnostics["history_invalid"] = 1
            fail_staging("raw partition validation or pagination failed")
            self.last_diagnostics = diagnostics
            return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)

        if not all_rows:
            fail_staging("raw endpoint returned no usable rows")
            self.last_diagnostics = diagnostics
            return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
        loaded_dates = sorted(all_rows, reverse=True)
        diagnostics["dates_loaded"] = len(loaded_dates)
        diagnostics["rows"] = sum(len(rows) for rows in all_rows.values())
        coverage = self._coverage_metrics(all_rows, requested_date=requested, actual_trade_date=loaded_dates[0])
        diagnostics.update({
            "coverage_version": coverage.get("coverage_version", 1),
            "universe_version": coverage.get("universe_version", self.universe_version),
            "overall_coverage": coverage.get("overall_coverage", 0.0),
            "market_coverage": coverage.get("market_coverage", {}),
            "window_median": coverage.get("window_median", 0.0),
            "market_medians": coverage.get("market_medians", {}),
            "daily_counts": coverage.get("daily_counts", {}),
            "coverage_ok": bool(coverage.get("coverage_ok")),
            "coverage_errors": coverage.get("errors", []),
            "coverage_universe_evidence": coverage.get("universe_evidence"),
            "coverage_universe_digest": coverage.get("universe_digest"),
            "coverage_eligible_markets": coverage.get("eligible_markets", []),
        })
        if not coverage.get("coverage_ok"):
            diagnostics["history_invalid"] = int(diagnostics.get("history_invalid", 0) or 0) + 1
            fail_staging("raw batch coverage below configured floor")
            self.last_diagnostics = diagnostics
            return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
        complete = False
        if self.storage is not None:
            try:
                published = self.storage.publish_raw_batch(
                    batch_id,
                    expected_trade_dates=dates,
                    actual_trade_date=loaded_dates[0],
                    quality="good",
                    source="tushare",
                    shadow=not self.raw_publish_enabled,
                )
                complete = isinstance(published, dict) and str(published.get("status") or "") == "published"
                if not complete:
                    raise RuntimeError("raw batch publication did not return published status")
                diagnostics["batch_id"] = batch_id
                diagnostics["dataset_id"] = published.get("dataset_id")
                diagnostics["generation"] = published.get("generation")
                diagnostics["shadow"] = bool(published.get("shadow"))
            except Exception:
                diagnostics["history_invalid"] = 1
                fail_staging("raw batch publication failed")
                self.last_diagnostics = diagnostics
                return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
        latest = loaded_dates[0]
        bars: dict[str, list[dict]] = {}
        for date in sorted(loaded_dates):
            for row in all_rows[date]:
                bars.setdefault(row["code"], []).append(row)
        quotes = []
        now = datetime.now(CHINA_TZ)
        for row in all_rows[latest]:
            quotes.append(Quote(row["code"], row.get("name") or row["code"], row["close"], row["pre_close"], row["amount"], row["pct_change"], row["volume"], source="tushare", provider_ts=now, fetched_at=now, indicator_last_date=latest, indicator_last_close=row["close"], indicator_price_basis="unadjusted", indicator_source="tushare"))
        # A daily success is recorded only after every partition and the full
        # coverage/manifest checks have passed.
        self._record_success("daily")
        diagnostics["complete"] = complete
        self.last_diagnostics = diagnostics
        return BulkDailyResult(quotes, bars, latest, batch_id, quality="good" if complete or self.storage is None else "partial", complete=complete or self.storage is None, diagnostics=diagnostics)

    async def fetch_bulk_daily(self, trade_date: str = "", **kwargs) -> BulkDailyResult:
        return await self.fetch_bulk_daily_result(trade_date, **kwargs)

    async def fetch_completed_trade_dates_range(self, start_date: str, end_date: str) -> list[str]:
        """Return completed calendar sessions strictly inside a date range."""
        start = self._date(start_date)
        end = self._date(end_date)
        if not start or not end or start > end:
            raise ValueError("invalid Tushare calendar range")
        effective_end = self._completed_end_date(end)
        if effective_end < start:
            return []
        async with self.http.slot() as client:
            sse = await self._calendar_exchange(client, "SSE", start, effective_end)
        dates = sorted((sse) - {start}, reverse=True)
        self._allowed_bj_dates = set(dates)
        self._calendar_evidence_policy = "sse_fallback"
        return [value for value in dates if value > start]

    async def fetch_evaluation_daily_result(
        self,
        as_of: str,
        horizon: int = 5,
        *,
        codes=None,
        max_pages: int = 1000,
    ) -> BulkDailyResult:
        """Fetch future completed days into an evaluation-only raw dataset."""
        baseline = self._date(as_of)
        if not baseline:
            raise ValueError("as_of must be YYYY-MM-DD")
        baseline_date = datetime.strptime(baseline, "%Y-%m-%d").date()
        today = datetime.now(CHINA_TZ).date()
        if baseline_date > today:
            raise ValueError("as_of cannot be in the future")
        try:
            horizon = max(1, min(int(horizon), 20))
        except (TypeError, ValueError, OverflowError) as exc:
            raise ValueError("horizon must be a positive integer") from exc
        requested_end = datetime.strptime(baseline, "%Y-%m-%d").date() + timedelta(days=horizon * 3 + 14)
        today = datetime.now(CHINA_TZ).date()
        end_date = min(requested_end, today).isoformat()
        completed_cutoff = self._completed_end_date(end_date)
        diagnostics: dict[str, object] = {
            "evaluation": True,
            "as_of": baseline,
            "requested_date": end_date,
            "network_failed": 0,
            "history_invalid": 0,
            "cache_basis_rejected": 0,
            "universe_evidence_required": True,
        }
        evaluation_key = self.dataset_key if self.dataset_key.endswith("_evaluation") else f"{self.dataset_key}_evaluation"
        diagnostics["dataset_key"] = evaluation_key

        def add_universe_diagnostics(evidence: dict) -> None:
            diagnostics.update({
                "universe_version": evidence.get("universe_version"),
                "universe_digest": evidence.get("digest"),
                "universe_effective_date": evidence.get("effective_date"),
                "eligible_markets": evidence.get("eligible_markets", []),
                "universe_method": evidence.get("method"),
                "universe_source": evidence.get("source"),
                "suspension_method": evidence.get("suspension_method"),
                "bj_calendar_policy": evidence.get("bj_calendar_policy", self.bj_calendar_policy),
            })

        def cached_result() -> BulkDailyResult:
            if self.storage is None:
                return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
            try:
                active = self.storage.active_raw_batch(evaluation_key, as_of=completed_cutoff, max_stale_trading_days=366)
                if not active:
                    return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
                try:
                    add_universe_diagnostics(self._normalize_evidence(active.get("universe_counts_json") or {}))
                except (TypeError, ValueError, OverflowError):
                    return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)
                loaded = self.storage.raw_batch_bars(active.get("active_batch_id") or active.get("batch_id"), codes, after=baseline)
                # A cached evaluation generation is usable only for completed
                # rows strictly after this baseline and no later than the
                # completed endpoint used for this request.  Never let a
                # screening batch or a provisional/current row satisfy the
                # horizon by accident.
                valid_rows: dict[str, list[dict]] = {}
                for code, rows in (loaded or {}).items():
                    for row in rows or []:
                        value = self._date(row.get("trade_date"))
                        if not value or value <= baseline or value > completed_cutoff or value > today.isoformat():
                            continue
                        valid_rows.setdefault(str(code), []).append(row)
                loaded = valid_rows
                dates = sorted({str(row.get("trade_date")) for rows in loaded.values() for row in rows if row.get("trade_date")}, reverse=True)
                if len(dates) < horizon:
                    return BulkDailyResult([], {}, None, None, quality="partial", complete=False, diagnostics=diagnostics)
                diagnostics.update({"raw_cache": True, "batch_id": active.get("active_batch_id"), "dataset_id": active.get("dataset_id"), "generation": active.get("generation"), "trade_dates": dates[:horizon]})
                latest = dates[0]
                return BulkDailyResult([], loaded, latest, active.get("active_batch_id"), source="tushare", quality="good", complete=True, diagnostics=diagnostics)
            except (RuntimeError, ValueError, TypeError, KeyError):
                return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)

        try:
            completed_end = self._completed_end_date(end_date)
            dates = await self.fetch_completed_trade_dates_range(baseline, completed_end)
            dates = [value for value in dates if baseline < value <= completed_end and value <= today.isoformat()]
            dates = dates[: max(horizon, min(horizon * 3, 60))]
            if len(dates) < horizon:
                diagnostics["history_invalid"] = 1
                return cached_result()
        except (httpx.HTTPError, asyncio.TimeoutError, OSError, TushareCircuitOpen, TushareRateLimitError):
            diagnostics["network_failed"] = 1
            return cached_result()
        except (ValueError, TypeError, KeyError, TushareBulkError):
            diagnostics["history_invalid"] = 1
            return cached_result()

        try:
            universe_evidence = await self.fetch_universe_evidence(dates[0], max_pages=max_pages)
        except (httpx.HTTPError, asyncio.TimeoutError, OSError, TushareCircuitOpen):
            diagnostics["network_failed"] = 1
            return cached_result()
        except (ValueError, TypeError, KeyError, TushareBulkError):
            diagnostics["history_invalid"] = 1
            return cached_result()
        add_universe_diagnostics(universe_evidence)

        batch_id = None
        batch_record: dict = {}
        if self.storage is not None:
            try:
                batch_id, batch_record = self._begin_raw_batch(dates[0], dates, universe_evidence, dataset_key=evaluation_key)
            except (ValueError, TypeError, KeyError, RuntimeError):
                diagnostics["history_invalid"] = 1
                return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics=diagnostics)

        def fail_staging(error: str) -> None:
            if batch_id is None:
                return
            fail_batch = getattr(self.storage, "fail_raw_batch", None)
            if callable(fail_batch):
                try:
                    fail_batch(batch_id, error)
                except Exception:
                    pass

        all_rows: dict[str, list[dict]] = {}
        try:
            async with self.http.slot() as client:
                for trade_date in dates:
                    stored_page_size = self.page_size
                    try:
                        stored_page_size = max(1, min(int(batch_record.get("page_size") or self.page_size), 6000))
                    except (TypeError, ValueError, OverflowError):
                        stored_page_size = self.page_size
                    filter_policy = self._page_filter_policy(trade_date)
                    try:
                        rows, start_page, staged_complete = self._staged_pages(batch_id, trade_date, page_size=stored_page_size, filter_policy=filter_policy) if batch_id else ([], 0, False)
                    except TushareBulkError as exc:
                        if "filter policy mismatch" not in str(exc).lower() or not batch_id:
                            raise
                        reset = getattr(self.storage, "reset_raw_batch_pages", None)
                        if not callable(reset):
                            raise
                        reset(batch_id, trade_date, from_page=0)
                        rows, start_page, staged_complete = [], 0, False
                    if staged_complete:
                        all_rows[trade_date] = rows
                        continue
                    size = stored_page_size
                    async def load_page(offset, page_size):
                        return await self.fetch_daily_page(trade_date, offset=offset, page_size=page_size, client=client)
                    remaining_pages = max(1, int(max_pages) - start_page)
                    async for page_no, raw_page in self._iter_pages(load_page, limit=size, max_pages=remaining_pages, start_page=start_page):
                        # Apply BJ filtering only after the raw page length has
                        # been observed by _iter_pages.
                        page = raw_page if filter_policy != "exclude_bj" else [row for row in raw_page if not str(row.get("ts_code") or "").endswith(".BJ")]
                        rows.extend(page)
                        if page and batch_id is not None:
                            self.storage.stage_raw_partition(
                                batch_id,
                                trade_date,
                                page,
                                partition_no=page_no,
                                source="tushare",
                                basis="unadjusted",
                                server_rows=raw_page,
                                server_row_count=len(raw_page),
                                server_terminal=len(raw_page) < size,
                                filter_policy=filter_policy,
                            )
                        elif batch_id is not None:
                            stage_page = getattr(self.storage, "stage_raw_page_metadata", None)
                            if not callable(stage_page):
                                raise RuntimeError("raw page metadata storage is unavailable")
                            stage_page(
                                batch_id,
                                trade_date,
                                partition_no=page_no,
                                server_rows=raw_page,
                                server_row_count=len(raw_page),
                                server_terminal=len(raw_page) < size,
                                filter_policy=filter_policy,
                                filtered_rows=[],
                                source="tushare",
                            )
                    if not rows:
                        raise TushareBulkError("Tushare evaluation daily returned no rows")
                    if len({row["code"] for row in rows}) != len(rows):
                        raise ValueError("Tushare evaluation daily contains duplicate codes")
                    all_rows[trade_date] = rows
        except asyncio.CancelledError:
            fail_staging("evaluation raw fetch cancelled")
            raise
        except (httpx.HTTPError, asyncio.TimeoutError, OSError, TushareCircuitOpen):
            diagnostics["network_failed"] = 1
            fail_staging("network failure while fetching evaluation partitions")
            return cached_result()
        except (ValueError, TypeError, KeyError, TushareBulkError):
            diagnostics["history_invalid"] = 1
            fail_staging("evaluation partition validation failed")
            return cached_result()

        coverage = self._coverage_metrics(all_rows, requested_date=dates[0], actual_trade_date=dates[0])
        diagnostics.update({
            "coverage_version": coverage.get("coverage_version", 1),
            "coverage_ok": bool(coverage.get("coverage_ok")),
            "coverage_errors": coverage.get("errors", []),
            "coverage_universe_evidence": coverage.get("universe_evidence"),
            "coverage_universe_digest": coverage.get("universe_digest"),
            "coverage_eligible_markets": coverage.get("eligible_markets", []),
            "coverage_universe_version": coverage.get("universe_version"),
            "coverage_bj_calendar_policy": coverage.get("bj_calendar_policy"),
            "coverage_suspension_method": coverage.get("suspension_method"),
        })
        if not coverage.get("coverage_ok"):
            diagnostics["history_invalid"] = 1
            fail_staging("evaluation raw coverage below configured floor")
            return cached_result()
        published = None
        if self.storage is not None:
            try:
                published = self.storage.publish_raw_batch(
                    batch_id,
                    expected_trade_dates=dates,
                    actual_trade_date=dates[0],
                    quality="good",
                    source="tushare",
                    shadow=not self.raw_publish_enabled,
                )
                if not isinstance(published, dict) or str(published.get("status") or "") != "published":
                    raise RuntimeError("evaluation raw batch publication did not return published status")
            except Exception:
                diagnostics["history_invalid"] = 1
                fail_staging("evaluation raw batch publication failed")
                return cached_result()
        selected = {str(code).split(".", 1)[0] for code in (codes or [])}
        bars: dict[str, list[dict]] = {}
        for trade_date in sorted(all_rows):
            for row in all_rows[trade_date]:
                if selected and row["code"] not in selected:
                    continue
                bars.setdefault(row["code"], []).append(row)
        diagnostics.update({"batch_id": batch_id, "dataset_id": published.get("dataset_id") if published else None, "generation": published.get("generation") if published else None, "trade_dates": sorted(all_rows, reverse=True), "complete": True, "shadow": bool(published and published.get("shadow"))})
        self._record_success("daily")
        return BulkDailyResult([], bars, dates[0], batch_id, source="tushare", quality="good", complete=True, diagnostics=diagnostics)

    async def fetch_evaluation_daily(self, as_of: str, horizon: int = 5, **kwargs) -> BulkDailyResult:
        return await self.fetch_evaluation_daily_result(as_of, horizon, **kwargs)

    fetch_daily_batch = fetch_bulk_daily_result
    fetch_bulk_history = fetch_bulk_daily_result
class SinaQuoteProvider:
    """Prototype provider; replace it with a licensed/stable source for production."""

    def __init__(self, timeout: float = 10, tushare_url: str = "", tushare_token: str = "", max_concurrency: int = 8, http_runtime: HttpRuntime | None = None, symbol_store=None, *, gateway: TushareRequestGateway | None = None, bulk_page_size: int = 6000, bulk_retry_attempts: int = 3, bj_calendar_policy: str = "require_bse", raw_dataset_key: str = "tushare_daily", min_snapshot_size: int = DEFAULT_TUSHARE_MIN_SNAPSHOT_SIZE, daily_snapshot_min_size: int | None = None, min_overall_coverage: float = 0.97, min_market_coverage: float = 0.95, min_market_median_ratio: float = 0.95, universe_version: str = "", universe_counts=None, universe_evidence=None, require_universe_evidence: bool = DEFAULT_TUSHARE_REQUIRE_UNIVERSE_EVIDENCE, raw_publish_enabled: bool = True, session_count: int = 120):
        self.timeout = timeout
        self.tushare_url = str(tushare_url or "").strip() or "https://api.tushare.pro"
        self.tushare_token = str(tushare_token or "").strip()
        self.http = http_runtime or HttpRuntime(timeout, max_concurrency)
        self._indicator_cache: dict[str, tuple[datetime, dict[str, float | None]]] = {}
        self.history_bars: dict[str, list[dict[str, float | str]]] = {}
        self.last_diagnostics: dict[str, object] = {}
        self._last_tushare_date: str | None = None
        self._tushare_names: dict[str, str] = {}
        self.symbol_store = symbol_store
        self.gateway = gateway or TushareRequestGateway(
            self.tushare_token,
            self.tushare_url,
            http_runtime=self.http,
            storage=symbol_store if hasattr(symbol_store, "provider_api_state") else None,
            retry_attempts=bulk_retry_attempts,
            enforce_rate_limits=bool(symbol_store) and isinstance(self.http, HttpRuntime),
        )
        self.bulk_provider = TushareBulkDailyProvider(
            self.tushare_token,
            self.tushare_url,
            http_runtime=self.http,
            storage=symbol_store if hasattr(symbol_store, "create_raw_batch") else None,
            page_size=bulk_page_size,
            retry_attempts=bulk_retry_attempts,
            bj_calendar_policy=bj_calendar_policy,
            dataset_key=raw_dataset_key,
            min_snapshot_size=min_snapshot_size,
            daily_snapshot_min_size=daily_snapshot_min_size,
            min_overall_coverage=min_overall_coverage,
            min_market_coverage=min_market_coverage,
            min_market_median_ratio=min_market_median_ratio,
            universe_version=universe_version,
            universe_counts=universe_counts,
            universe_evidence=universe_evidence,
            require_universe_evidence=require_universe_evidence,
            gateway=self.gateway,
            raw_publish_enabled=raw_publish_enabled,
            session_count=session_count,
        )
        self.evaluation_provider = TushareBulkDailyProvider(
            self.tushare_token,
            self.tushare_url,
            http_runtime=self.http,
            storage=symbol_store if hasattr(symbol_store, "create_raw_batch") else None,
            page_size=bulk_page_size,
            retry_attempts=bulk_retry_attempts,
            bj_calendar_policy=bj_calendar_policy,
            dataset_key=f"{raw_dataset_key}_evaluation",
            min_snapshot_size=min_snapshot_size,
            daily_snapshot_min_size=daily_snapshot_min_size,
            min_overall_coverage=min_overall_coverage,
            min_market_coverage=min_market_coverage,
            min_market_median_ratio=min_market_median_ratio,
            universe_version=universe_version,
            universe_counts=universe_counts,
            universe_evidence=universe_evidence,
            require_universe_evidence=require_universe_evidence,
            gateway=self.gateway,
            raw_publish_enabled=raw_publish_enabled,
            session_count=session_count,
        )
        # Current industry labels are display annotations, not historical
        # factor snapshots.  Cache both successful lookups and short-lived
        # failures so a large candidate report cannot hammer the endpoint.
        self._industry_cache: dict[str, tuple[datetime, str, bool]] = {}
        # Futures are loop-bound.  The loop identity is checked before a
        # caller awaits one so providers remain safe across short-lived test
        # loops and AstrBot's long-lived event loop.
        self._industry_inflight: dict[str, asyncio.Future] = {}

    def _remember_symbol(self, code: str, name: str, source: str) -> None:
        if not self.symbol_store:
            return
        try:
            self.symbol_store.upsert_stock_symbol(code, name, source)
        except Exception:
            # A lookup cache must never turn an otherwise usable quote into a
            # failed provider response.
            pass

    @staticmethod
    def _clear_indicator_state(quote: Quote) -> None:
        quote.rsi6 = quote.ma5 = quote.ma10 = quote.ma20 = quote.volume_ratio = None
        quote.atr14 = quote.support20 = quote.resistance20 = quote.volatility20 = None
        quote.momentum5 = quote.momentum20 = None
        quote.history_days = 0
        quote.indicator_last_date = ""
        quote.indicator_last_close = None
        quote.indicator_price_basis = "unknown"
        quote.indicator_source = ""

    async def close(self) -> None:
        await self.http.close()

    async def fetch_market_snapshot(self, daily_market_url: str = "", trade_date: str = "") -> list[Quote]:
        """Fetch one daily snapshot, preferring Tushare when a token is configured."""
        return (await self.fetch_market_snapshot_result(daily_market_url, trade_date)).quotes

    async def fetch_bulk_daily_result(self, trade_date: str = "", **kwargs) -> BulkDailyResult:
        """Expose the immutable Tushare bulk loader to the screen coordinator."""
        if not self.tushare_token:
            return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics={"network_failed": 0})
        result = await self.bulk_provider.fetch_bulk_daily_result(trade_date, **kwargs)
        self.last_diagnostics = dict(result.diagnostics or {}) if isinstance(result, BulkDailyResult) else {}
        return result

    async def fetch_bulk_daily(self, trade_date: str = "", **kwargs) -> BulkDailyResult:
        return await self.fetch_bulk_daily_result(trade_date, **kwargs)

    async def fetch_evaluation_daily_result(self, as_of: str, horizon: int = 5, **kwargs) -> BulkDailyResult:
        if not self.tushare_token:
            return BulkDailyResult([], {}, None, None, quality="unknown", complete=False, diagnostics={"evaluation": True, "network_failed": 0, "history_invalid": 0})
        return await self.evaluation_provider.fetch_evaluation_daily_result(as_of, horizon, **kwargs)

    async def fetch_evaluation_daily(self, as_of: str, horizon: int = 5, **kwargs) -> BulkDailyResult:
        return await self.fetch_evaluation_daily_result(as_of, horizon, **kwargs)

    async def fetch_eastmoney_latest_trade_date(self) -> str | None:
        """Verify the snapshot date from the latest completed Shanghai index bar."""
        params = {"secid": "1.000001", "fields1": "f1,f2,f3,f4,f5,f6", "fields2": "f51,f52,f53,f54,f55", "klt": "101", "fqt": "0", "lmt": "2", "end": "20500101"}
        async with self.http.slot() as client:
            response = await client.get("https://push2his.eastmoney.com/api/qt/stock/kline/get", params=params)
            response.raise_for_status()
            payload = response.json()
            if not isinstance(payload, dict):
                raise ValueError("Eastmoney index response is not an object")
            data = payload.get("data") or {}
            if not isinstance(data, dict):
                raise ValueError("Eastmoney index data is not an object")
            rows = data.get("klines") or []
        if not rows:
            return None
        dates = []
        for row in rows:
            value = str(row or "").split(",", 1)[0].strip()
            canonical = self._canonical_eastmoney_date(value)
            if canonical:
                dates.append(canonical)
        return max(dates) if dates else None

    @staticmethod
    def _canonical_eastmoney_date(value: str, *, allow_empty: bool = False) -> str | None:
        text = str(value or "").strip().replace("-", "")
        if not text:
            return "" if allow_empty else None
        if not re.fullmatch(r"\d{8}", text):
            return None
        try:
            parsed = datetime.strptime(text, "%Y%m%d").date()
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed.isoformat()

    async def fetch_eastmoney_fallback_result(
        self,
        trade_date: str = "",
        daily_market_url: str = "",
    ) -> EastmoneyFallbackResult:
        """Fetch an EM-only snapshot with a stable, verified actual date.

        The token is intentionally ignored.  The latest Shanghai index bar is
        observed before and after the snapshot.  A single extra observation
        is allowed when the two observations race; an unstable or missing
        date is returned as unusable rather than guessed from a weekday.
        """
        requested = self._canonical_eastmoney_date(
            trade_date or datetime.now(CHINA_TZ).date().isoformat()
        )
        if not requested:
            raise ValueError("Eastmoney fallback requested date is invalid")

        diagnostics: dict[str, object] = {
            "date_verified": False,
            "date_observations": 0,
            "date_race": False,
            "snapshot_source": "eastmoney",
        }

        async def observe() -> str | None:
            diagnostics["date_observations"] = int(diagnostics.get("date_observations", 0) or 0) + 1
            value = await self.fetch_eastmoney_latest_trade_date()
            return self._canonical_eastmoney_date(value)

        before = await observe()
        quotes = await self._fetch_eastmoney_snapshot(daily_market_url)
        after = await observe()
        actual = before if before and before == after else None
        if before != after:
            diagnostics["date_race"] = True
            # One bounded re-observation is enough to distinguish a moving
            # index from a transient HTTP/JSON miss without looping forever.
            retry = await observe()
            if retry and retry == after:
                actual = retry

        if actual and actual > requested:
            diagnostics["future_date"] = True
            diagnostics["date_error"] = "verified actual date is after requested date"
            return EastmoneyFallbackResult(
                [], None, requested, "eastmoney", "unknown", False, diagnostics
            )
        if not actual:
            diagnostics["date_unverified"] = True
            diagnostics["degraded_unavailable"] = True
            return EastmoneyFallbackResult(
                [], None, requested, "eastmoney", "unknown", False, diagnostics
            )
        diagnostics.update({"date_verified": True, "actual_trade_date": actual})
        for quote in quotes:
            # Keep the provider provenance explicit.  The main coordinator
            # may relabel the in-memory quote as ``eastmoney_fallback`` for
            # the preview report without changing its history source.
            quote.source = "eastmoney"
        return EastmoneyFallbackResult(
            quotes,
            actual,
            requested,
            "eastmoney",
            "degraded",
            False,
            diagnostics,
        )

    async def fetch_eastmoney_snapshot_result(
        self,
        trade_date: str = "",
        daily_market_url: str = "",
    ) -> EastmoneyFallbackResult:
        """Public alias for the EM-only transient snapshot contract."""
        # Accept the historical URL-first shape when an integration passes a
        # custom endpoint positionally.
        if str(trade_date or "").strip().lower().startswith(("http://", "https://")):
            old_url = str(trade_date)
            trade_date, daily_market_url = (str(daily_market_url), old_url) if daily_market_url else ("", old_url)
        return await self.fetch_eastmoney_fallback_result(trade_date, daily_market_url)

    fetch_eastmoney_preview_result = fetch_eastmoney_fallback_result
    fetch_eastmoney_fallback_snapshot_result = fetch_eastmoney_fallback_result
    fetch_eastmoney_transient_snapshot_result = fetch_eastmoney_fallback_result

    async def fetch_market_snapshot_result(self, daily_market_url: str = "", trade_date: str = "") -> MarketSnapshotResult:
        """Fetch a snapshot and report the actual trading date represented by the data."""
        if self.tushare_token:
            try:
                if self.bulk_provider.storage is not None:
                    bulk = await self.bulk_provider.fetch_bulk_daily_result(
                        trade_date,
                        session_count=self.bulk_provider.session_count,
                    )
                    return MarketSnapshotResult(bulk.quotes, bulk.trade_date, "tushare", bulk.quality, bulk.fetched_at)
                return await self._fetch_tushare_snapshot_result(trade_date)
            except (httpx.HTTPError, ValueError, TypeError, KeyError, RuntimeError):
                # With a configured raw store, do not silently switch the
                # screening basis to another provider.  The coordinator may
                # use a fresh active raw generation or report unknown.
                if self.bulk_provider.storage is not None:
                    return MarketSnapshotResult([], None, "tushare", "unknown")
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
        # The no-store compatibility path still shares the bulk provider's
        # breaker.  Keep its state in sync with the validated daily result.
        self.bulk_provider._check_breaker("daily")
        async with self.http.slot() as client:
            self._last_tushare_date = None
            quotes = await self._fetch_tushare_daily(client, requested)
            if quotes:
                await self._apply_tushare_names(client, quotes)
                self.bulk_provider._record_success("daily")
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
                    self.bulk_provider._record_success("daily")
                    return MarketSnapshotResult(quotes, self._last_tushare_date or self._normalize_trade_date(date_value), "tushare", "good")
        return MarketSnapshotResult([], None, "tushare", "unknown")

    async def _fetch_tushare_daily(self, client: httpx.AsyncClient, date_value: str) -> list[Quote]:
        payload = {
            "api_name": "daily",
            "token": self.tushare_token,
            "params": {"trade_date": date_value, "limit": 6000},
            "fields": "ts_code,trade_date,close,pre_close,pct_chg,vol,amount",
        }
        body = await self.bulk_provider._post_json(client, payload)
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
            result.append(
                Quote(
                    code,
                    code,
                    price,
                    prev_close,
                    amount,
                    pct_change,
                    volume,
                    source="tushare",
                    provider_ts=datetime.now(CHINA_TZ),
                    fetched_at=datetime.now(CHINA_TZ),
                    indicator_last_date=self._normalize_trade_date(row_date),
                    indicator_last_close=price,
                    indicator_price_basis="unadjusted",
                    indicator_source="tushare",
                )
            )
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
                    body = await self.gateway.request_json(client, payload, api_name="stock_basic", cache_ttl=86400)
                    if not isinstance(body, dict) or int(body.get("code") or 0) != 0:
                        continue
                    data = body.get("data") or {}
                    fields, items = list(data.get("fields") or []), data.get("items") or []
                    for row in (dict(zip(fields, item)) for item in items if isinstance(item, list)):
                        code, name = str(row.get("ts_code") or "").split(".")[0], str(row.get("name") or "").strip()
                        if len(code) == 6 and name:
                            self._tushare_names[code] = name
                except (httpx.HTTPError, ValueError, TypeError, KeyError, IndexError, TushareBulkError):
                    continue
        updated = 0
        for quote in quotes:
            name = self._tushare_names.get(quote.code)
            if name:
                if quote.name != name:
                    quote.name = name
                    updated += 1
                self._remember_symbol(quote.code, name, "tushare")
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
        body = await self.gateway.request_json(client, payload, api_name="trade_cal", cache_ttl=86400)
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
            body = await self.gateway.request_json(client, payload, api_name="trade_cal", cache_ttl=86400)
            data = body.get("data") or {}; fields = list(data.get("fields") or [])
            for values in data.get("items") or []:
                row = dict(zip(fields, values))
                if str(row.get("cal_date") or "").replace("-", "") == value:
                    self.bulk_provider._record_success("calendar")
                    return str(row.get("is_open", "0")) in {"1", "True", "true"}
        # A valid response without the requested row is not a success for the
        # calendar operation; retain the breaker state and let callers use the
        # explicit unknown path.
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
                        payload = response.json()
                        if not isinstance(payload, dict):
                            raise ValueError("Eastmoney snapshot response is not an object")
                        data = payload.get("data") or {}
                        if not isinstance(data, dict):
                            raise ValueError("Eastmoney snapshot data is not an object")
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
        if not isinstance(row, dict):
            return None
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
            code = normalize_code(symbol[2:])
            name = fields[0].strip() or code
            result.append(Quote(code, name, price, prev_close, amount, pct, volume, source="sina", provider_ts=quote_time, fetched_at=quote_time))
            if name and name != code:
                self._remember_symbol(code, name, "sina")
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

    @staticmethod
    def _parse_eastmoney_daily_history(payload: dict, as_of: str = "", limit: int = 60) -> list[dict]:
        """Extract validated unadjusted EM daily bars from one response."""
        cutoff = SinaQuoteProvider._canonical_eastmoney_date(as_of, allow_empty=True)
        if cutoff is None:
            raise ValueError("Eastmoney history cutoff date is invalid")
        rows = ((payload.get("data") or {}).get("klines") or []) if isinstance(payload, dict) else []
        parsed: list[dict] = []
        seen: set[str] = set()
        for raw in rows:
            fields = str(raw or "").split(",")
            if len(fields) <= 6:
                continue
            row_date = SinaQuoteProvider._canonical_eastmoney_date(fields[0])
            if not row_date or (cutoff and row_date > cutoff) or row_date in seen:
                continue
            try:
                values = {
                    "open": float(fields[1]),
                    "close": float(fields[2]),
                    "high": float(fields[3]),
                    "low": float(fields[4]),
                    "volume": float(fields[5]),
                    "amount": float(fields[6]) if len(fields) > 6 else 0.0,
                }
            except (TypeError, ValueError, OverflowError):
                continue
            if (
                not all(math.isfinite(value) for value in values.values())
                or any(values[key] <= 0 for key in ("open", "close", "high", "low"))
                or values["volume"] < 0
                or values["amount"] < 0
                or values["high"] < max(values["open"], values["close"])
                or values["low"] > min(values["open"], values["close"])
                or values["high"] < values["low"]
            ):
                continue
            parsed.append({
                "trade_date": row_date,
                **values,
                "price_basis": "unadjusted",
                "source": "eastmoney",
            })
            seen.add(row_date)
        parsed.sort(key=lambda row: row["trade_date"])
        return parsed[-max(1, min(int(limit), 1000)):]

    async def fetch_eastmoney_daily_history(
        self,
        code: str,
        as_of: str = "",
        *,
        limit: int = 60,
        client=None,
    ) -> list[dict]:
        """Fetch one EM-only, unadjusted daily history up to ``as_of``.

        This is the public history primitive used by ``enrich_indicators``;
        the transient fallback therefore cannot accidentally gain a second
        parser or a Tushare/legacy history path.
        """
        normalized = normalize_code(code)
        if not re.fullmatch(r"\d{6}", normalized):
            raise ValueError("Eastmoney history code is invalid")
        cutoff = self._canonical_eastmoney_date(
            as_of or datetime.now(CHINA_TZ).date().isoformat()
        )
        if not cutoff:
            raise ValueError("Eastmoney history date is invalid")
        params = {
            "secid": self._eastmoney_secid(normalized),
            "ut": "fa5fd1943c7b386f172d6893dbfba10b",
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",
            "fqt": "0",
            "beg": "0",
            "end": cutoff.replace("-", ""),
            "lmt": str(max(1, min(int(limit), 1000))),
        }
        async def request(shared_client):
            response = await shared_client.get(
                "https://push2his.eastmoney.com/api/qt/stock/kline/get",
                params=params,
            )
            response.raise_for_status()
            return response.json()
        if client is None:
            async with self.http.slot() as shared_client:
                payload = await request(shared_client)
        else:
            payload = await request(client)
        return self._parse_eastmoney_daily_history(payload, cutoff, limit)

    fetch_eastmoney_history = fetch_eastmoney_daily_history

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
            if (
                cached
                and datetime.now(CHINA_TZ) - cached[0] < timedelta(minutes=15)
                and str(cached[1].get("indicator_price_basis") or "unknown").lower() == "unadjusted"
                and bool(_normalize_plan_date(cached[1].get("indicator_last_date")))
                and _finite_positive(cached[1].get("indicator_last_close")) is not None
                and _finite_positive(cached[1].get("atr14")) is not None
                and int(cached[1].get("history_days") or 0) >= 20
                and _source_is_trusted(cached[1].get("indicator_source"))
            ):
                values = cached[1]
                quote.rsi6, quote.ma5, quote.ma10, quote.ma20, quote.volume_ratio = (values["rsi6"], values["ma5"], values["ma10"], values["ma20"], values["volume_ratio"])
                quote.atr14, quote.support20, quote.resistance20, quote.volatility20, quote.history_days = (values.get("atr14"), values.get("support20"), values.get("resistance20"), values.get("volatility20"), int(values.get("history_days") or 0))
                quote.momentum5, quote.momentum20 = values.get("momentum5"), values.get("momentum20")
                quote.indicator_last_date = str(values.get("indicator_last_date") or "")
                quote.indicator_last_close = values.get("indicator_last_close")
                quote.indicator_price_basis = str(values.get("indicator_price_basis") or "unknown")
                quote.indicator_source = str(values.get("indicator_source") or "")
                results[quote.code] = "memory_cache"
                return
            # A rejected/expired cache must not leave a caller's Quote carrying
            # stale indicators if the replacement request fails.
            self._clear_indicator_state(quote)
            self.history_bars.pop(quote.code, None)
            try:
                history = await self.fetch_eastmoney_daily_history(
                    quote.code, as_of, limit=60, client=client
                )
                self.history_bars[quote.code] = history
                values = calculate_daily_indicators(history)
                quote.indicator_last_date = str(values.get("indicator_last_date") or "")
                quote.indicator_last_close = values.get("indicator_last_close")
                quote.indicator_price_basis = str(values.get("indicator_price_basis") or "unknown")
                quote.indicator_source = str(values.get("indicator_source") or "")
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
