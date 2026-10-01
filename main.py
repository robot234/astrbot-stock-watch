from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import math
import re
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from types import MappingProxyType
from typing import Mapping

import httpx
from astrbot.api import logger
from astrbot.api.event import AstrMessageEvent, MessageChain, filter
from astrbot.api.message_components import Plain
from astrbot.api.star import Context, Star, register
from astrbot.core.utils.astrbot_path import get_astrbot_data_path

from .core import CHINA_TZ, Candidate, FactorOverlay, MinuteBarAggregator, PricePlan, Quote, apply_daily_indicators, assess_market_context, build_price_plan, candidate_rank_key, format_candidate, format_compact_candidate, format_stored_compact_candidate, in_trading_session, is_screenable, is_tradable, normalize_code, parse_codes, price_plan_is_validated, risk_label, review_risk, score_quote
from .factors import fundamental_score, industry_strength, market_adjustment
from .intraday_artifact import publish as publish_intraday_artifact
from .delivery_capabilities import delivery_capabilities
from . import market_comparison as market_comparison_evidence
from .data_evidence import EvidenceStore, OfficialEvidenceAdapter, RiskEvidenceService, OFFICIAL_HOSTS, safe_factor_row, factor_capture
from .providers import BulkDailyResult, EastmoneyFallbackResult, HttpRuntime, OpenAICompatibleClient, RssNewsProvider, SinaQuoteProvider, TUSHARE_DAILY_CALENDAR_EVIDENCE_VERSION, TUSHARE_DAILY_CALENDAR_POLICY_VERSION, TUSHARE_DAILY_CALENDAR_RULE, TUSHARE_DAILY_CALENDAR_SOURCE, TUSHARE_DAILY_CALENDAR_SYMBOLS, TushareBulkError, TushareCalendarEndpointError, TushareCalendarError, TushareCircuitOpen, TushareCoverageError, TushareHistoryError, TushareNetworkError, TushareNotPublishedError, TusharePermissionError, TushareProviderUnknownError, TusharePublishError, TushareRateLimitError, TushareRequestGateway, completed_session_cutoff, news_fingerprint, tushare_daily_calendar_digest, tushare_daily_calendar_policy_fingerprint
from .storage import SnapshotLeaseCapabilityError, SnapshotLeaseError, SnapshotLeaseLostError, StockStore
from .research_selector import radar_crossed, select_pools
from .research_risk import evidence_label, load_evidence
from . import formal_source_policy

PLUGIN_NAME = "astrbot_stock_watch"

_TUSHARE_FALLBACK_DIAGNOSTICS = {
    "network_failed": "network",
    "network": "network",
    "timeout": "timeout",
    "circuit_open": "breaker",
    "breaker_open": "breaker",
    "rate_limited": "rate_limit",
    "rate_limit_failures": "rate_limit",
    "not_published": "not_published",
    "calendar_unavailable": "calendar",
    "publish_failed": "publish",
    "coverage_failed": "coverage",
    "history_invalid": "history_invalid",
}

_TUSHARE_FALLBACK_FAILURE_KINDS = {
    "network": "network",
    "network_failed": "network",
    "timeout": "timeout",
    "breaker": "breaker",
    "breaker_open": "breaker",
    "rate_limit": "rate_limit",
    "rate_limited": "rate_limit",
    "not_published": "not_published",
    "calendar": "calendar",
    "calendar_unavailable": "calendar",
    "publish": "publish",
    "publish_failed": "publish",
    "coverage": "coverage",
    "coverage_failed": "coverage",
    "history_invalid": "history_invalid",
    "history": "history_invalid",
}


@dataclass(frozen=True, slots=True)
class ScreenScoreResult:
    """One invocation's candidates and immutable diagnostic snapshot."""

    candidates: tuple[Candidate, ...]
    diagnostics: Mapping[str, object]

    @classmethod
    def build(cls, candidates, diagnostics: Mapping[str, object] | None = None) -> "ScreenScoreResult":
        return cls(tuple(candidates or ()), MappingProxyType(dict(diagnostics or {})))


@dataclass(frozen=True, slots=True)
class AutomaticDeliverySendResult:
    """Outcome of one no-fallback automatic delivery attempt."""

    state: str
    error: str = ""
    attempted_chunks: int = 0
    accepted_chunks: int = 0


@register(PLUGIN_NAME, "DIO", "A股收盘选股与自选股监听", "0.13.3")
class Main(Star):
    # Calendar calls are immutable for a completed date/range.  Keep one
    # in-flight future per provider identity and event loop so concurrent
    # Main instances do not create duplicate upstream requests.
    _calendar_inflight: dict[tuple, dict] = {}

    def __init__(self, context: Context, config=None, **kwargs):
        super().__init__(context, config=config)
        self.context, self.config = context, config or {}
        data_dir = Path(get_astrbot_data_path()) / "plugin_data" / PLUGIN_NAME
        self.store = StockStore(data_dir / "stock_watch.sqlite3")
        configured_artifact = str(self.config.get("intraday_artifact_path", "") or "").strip()
        self.intraday_artifact_path = Path(configured_artifact) if configured_artifact else data_dir / "intraday_quotes.json"
        timeout = self._float("request_timeout", 10, 3, 60)
        self.http = HttpRuntime(timeout, self._int("max_concurrency", 8, 1, 64))
        self.raw_dataset_key = str(self.config.get("tushare_raw_dataset_key", "tushare_daily")).strip() or "tushare_daily"
        # Normalize the deprecated natural-day setting once at the boundary;
        # every production bulk request below uses the session-count contract.
        self.raw_session_count = self._session_count_from_config(self.config)
        # Keep the historical attribute as a compatibility alias for
        # integrations that still inspect raw_lookback_days.
        self.raw_lookback_days = self.raw_session_count
        self.raw_max_stale_trading_days = self._int("tushare_raw_max_stale_trading_days", 2, 0, 10)
        self.raw_chunk_size = self._int("tushare_raw_chunk_size", 500, 50, 2000)
        self.raw_min_snapshot_size = self._int("daily_snapshot_min_size", 4000, 1, 10000)
        self.raw_min_overall_coverage = self._float("tushare_raw_min_overall_coverage", 0.97, 0.0, 1.0)
        self.raw_min_market_coverage = self._float("tushare_raw_min_market_coverage", 0.95, 0.0, 1.0)
        self.raw_min_market_median_ratio = self._float("tushare_raw_min_market_median_ratio", 0.95, 0.0, 1.0)
        self.raw_publish_enabled = self._bool("tushare_raw_publish_enabled", True)
        self.raw_universe_version = str(self.config.get("tushare_raw_universe_version", "")).strip()[:160]
        self.raw_universe_counts = self.config.get("tushare_raw_universe_counts")
        if self.raw_universe_counts in (None, ""):
            self.raw_universe_counts = self.config.get("tushare_raw_universe_evidence")
        # Independent universe evidence is part of the raw integrity
        # contract.  Keep the legacy setting readable, but never let an
        # explicit false value weaken the fail-closed path.
        self.raw_require_universe_evidence = True
        self.quotes = SinaQuoteProvider(
            timeout,
            str(self.config.get("tushare_url", "")),
            str(self.config.get("tushare_token", "")),
            self._int("max_concurrency", 8, 1, 64),
            self.http,
            self.store,
            bulk_page_size=self._int("tushare_bulk_page_size", 6000, 1, 10000),
            bulk_retry_attempts=self._int("tushare_retry_attempts", 3, 1, 3),
            bj_calendar_policy=str(self.config.get("tushare_bj_calendar_policy", "require_bse")),
            raw_dataset_key=self.raw_dataset_key,
            min_snapshot_size=self.raw_min_snapshot_size,
            min_overall_coverage=self.raw_min_overall_coverage,
            min_market_coverage=self.raw_min_market_coverage,
            min_market_median_ratio=self.raw_min_market_median_ratio,
            universe_version=self.raw_universe_version,
            universe_counts=self.raw_universe_counts,
            require_universe_evidence=self.raw_require_universe_evidence,
            raw_publish_enabled=self.raw_publish_enabled,
            session_count=self.raw_session_count,
            universe_statuses=str(self.config.get("tushare_universe_statuses", "L,D,P")),
            realtime_backup_mode=str(self.config.get("realtime_backup_mode", "disabled")),
            realtime_backup_min_interval_seconds=self._float("realtime_backup_min_interval_seconds", 60, 5, 3600),
        )
        self.news = RssNewsProvider(str(self.config.get("news_rss_url", "")), timeout, self.http)
        self.risk_evidence = None
        if self._bool("official_evidence_enabled", False):
            try:
                documents = json.loads(str(self.config.get("official_evidence_documents", "[]")))
                if not isinstance(documents, list):
                    raise ValueError("document descriptors must be a list")
                hosts = tuple(h.strip() for h in str(self.config.get("official_evidence_trusted_hosts", ",".join(OFFICIAL_HOSTS))).split(",") if h.strip())
                adapter = OfficialEvidenceAdapter(self.http,
                    search_url=str(self.config.get("official_evidence_search_url", "")),
                    documents=documents, trusted_hosts=hosts)
                self.risk_evidence = RiskEvidenceService(EvidenceStore(self.store.path), adapter,
                    cache_seconds=self._int("official_evidence_cache_seconds", 3600, 60, 86400))
            except (ValueError, TypeError, sqlite3.Error):
                logger.warning("[%s] 公告证据配置不可用，风险状态保持未知", PLUGIN_NAME)
        llm_timeout = self._float("llm_timeout_seconds", 120, 5, 300)
        self.llm = OpenAICompatibleClient(
            str(self.config.get("llm_base_url", "https://api.openai.com/v1")),
            str(self.config.get("llm_api_key", "")),
            str(self.config.get("llm_model", "gpt-4o-mini")),
            llm_timeout,
            self._float("llm_min_interval_seconds", 10, 0, 3600),
            self._int("llm_daily_request_limit", 100, 1, 10000),
            HttpRuntime(llm_timeout, 1),
        )
        self.tasks: list[asyncio.Task] = []
        self.last_daily_scan: str | None = None
        self._daily_snapshot_lock = asyncio.Lock()
        self._snapshot_fallback_owner: str | None = None
        self._authorized_fallback_owner: str | None = None
        self._daily_date_alias: dict[str, str] = {}
        self._daily_retry_after: datetime | None = None
        self._annotation_task: asyncio.Task | None = None
        self._intraday_market_refresh_active = False
        self._annotation_cache: dict[str, tuple[datetime, dict]] = {}
        self._last_annotation_at: datetime | None = None
        self._terminated = False
        self.minute_bars = MinuteBarAggregator(self._int("minute_bar_history", 120, 10, 2000))
        self._intraday_date: str | None = None
        self._minute_restore_pending = False
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
            "selected_targets": 0,
            "candidate_targets": 0,
            "expired_candidates": 0,
            "focus_targets": 0,
            "dropped_targets": 0,
            "triggered_events": 0,
            "last_state": "not_started",
            "last_invocation_id": None,
            "last_nontrigger_reasons": {},
        }
        self._source_health = {
            "sina": {
                "batches": 0,
                "successes": 0,
                "failures": 0,
                "last_success_at": None,
                "last_error_at": None,
            },
            "tushare_rt_k": {
                "attempts": 0,
                "successes": 0,
                "failures": 0,
                "skipped": 0,
                "last_status": "disabled",
                "last_error": "",
                "last_attempt_at": None,
                "selected_source": "sina",
            }
        }
        self._last_screen_diagnostics: dict[str, object] = {}
        self._raw_screen_provenance: dict[str, object] = {}
        self._screen_sequence = 0
        self._last_screen_report_claimed = True
        self._automatic_delivery_owner = "automatic-close:" + uuid.uuid4().hex
        self._last_automatic_unknown_count = 0
        self._daily_acceptance_owner = "daily-acceptance:" + uuid.uuid4().hex
        self._last_daily_acceptance_unknown_count = 0
        self._intraday_delivery_owner = "intraday:" + uuid.uuid4().hex
        self._last_intraday_unknown_count = 0

    async def _store_call(self, method, /, *args, **kwargs):
        """Keep synchronous SQLite work off AstrBot's shared event loop."""
        return await asyncio.to_thread(method, *args, **kwargs)

    @staticmethod
    async def _await_result(value):
        """Keep lightweight test/integration hook replacements compatible."""
        return await value if inspect.isawaitable(value) else value

    async def _read_fresh_raw_history_async(self, codes, as_of: str, **kwargs):
        return await self._store_call(self._read_fresh_raw_history, codes, as_of, **kwargs)

    async def _fresh_raw_snapshot_async(self, requested_date: str, **kwargs):
        return await self._store_call(self._fresh_raw_snapshot, requested_date, **kwargs)

    async def _enrich_daily_snapshot_risk_fields(
        self,
        trade_date: str,
        quotes,
        *,
        persist: bool = False,
        collect_evidence: bool = False,
    ) -> list:
        """Fill only unknown daily risk tuples before persistence or scoring.

        The provider owns the bounded Eastmoney companion requests and accepts
        no inferred price or board state.  A missing provider hook, failed
        request, or ambiguous companion row deliberately leaves the tuple
        unchanged (and therefore blocks ``is_screenable``).
        """
        rows = [formal_source_policy.mask_quote_negatives(quote) for quote in (quotes or [])]
        targets = [
            quote for quote in rows
            if any(getattr(quote, field, None) is None for field in ("suspended", "limit_up", "limit_down", "st"))
        ]
        enrich = getattr(getattr(self, "quotes", None), "enrich_daily_risk_fields", None)
        if not targets or not callable(enrich):
            return (rows, []) if collect_evidence else rows
        before = {
            id(quote): tuple(getattr(quote, field, None) for field in ("suspended", "limit_up", "limit_down", "st"))
            for quote in targets
        }
        try:
            kwargs = {}
            if collect_evidence:
                try:
                    inspect.signature(enrich).bind(targets, collect_observations=True)
                    kwargs["collect_observations"] = True
                except (TypeError, ValueError):
                    pass
            outcome = enrich(targets, **kwargs)
            if inspect.isawaitable(outcome):
                outcome = await outcome
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("[%s] 日快照风险字段补全失败，保留未知状态", PLUGIN_NAME)
            return (rows, []) if collect_evidence else rows
        if isinstance(outcome, Mapping) and int(outcome.get("complete", 0) or 0) < len(targets):
            logger.warning(
                "[%s] 日快照风险证据不足：requested=%s batches=%s transport_failed=%s invalid_response=%s matched_rows=%s complete=%s",
                PLUGIN_NAME, outcome.get("requested", len(targets)), outcome.get("batches", 0),
                outcome.get("transport_failed", 0), outcome.get("invalid_response", 0),
                outcome.get("matched_rows", 0), outcome.get("complete", 0),
            )
        changed = any(
            before[id(quote)] != tuple(getattr(quote, field, None) for field in ("suspended", "limit_up", "limit_down", "st"))
            for quote in targets
        )
        if changed and persist and trade_date:
            save_daily_quotes = getattr(self.store, "save_daily_quotes", None)
            if callable(save_daily_quotes):
                try:
                    await self._store_call(
                        save_daily_quotes,
                        trade_date,
                        rows,
                        self._int("daily_cache_keep_days", 180, 7, 730),
                    )
                except Exception:
                    # The in-memory quote is still explicit.  Do not overwrite
                    # it with an invented cache result when SQLite is unavailable.
                    logger.warning("[%s] 日快照风险字段缓存回写失败", PLUGIN_NAME)
        observations = outcome.get("observations", []) if isinstance(outcome, Mapping) else []
        return (rows, list(observations) if isinstance(observations, list) else []) if collect_evidence else rows

    async def _apply_cached_daily_risk_evidence(self, trade_date: str, batch_id: str, quotes,
                                                *, decision_at: datetime | None = None) -> list:
        """Use only persisted, batch-bound risk facts; this path never fetches."""
        rows = [formal_source_policy.mask_quote_negatives(quote) for quote in (quotes or [])]
        loader = getattr(self.store, "daily_risk_evidence_for_quotes", None)
        if not callable(loader) or not batch_id or not rows:
            return rows
        cutoff = decision_at or datetime.now(timezone.utc)
        try:
            evidence = await self._store_call(loader, trade_date, batch_id, rows, cutoff)
        except (sqlite3.Error, ValueError, TypeError, OverflowError):
            logger.warning("[%s] 缓存风险证据不可读取，状态保持未知", PLUGIN_NAME)
            return rows
        for quote in rows:
            fields = evidence.get(str(quote.code), {})
            for field in ("suspended", "limit_up", "limit_down", "st"):
                value = fields.get(field)
                if getattr(quote, field, None) is None and isinstance(value, bool):
                    setattr(quote, field, value)
        return rows

    async def _collect_automatic_close_risk_evidence(self, trade_date: str, batch_id: str, quotes) -> list:
        """Collect later facts for this attempt, then decide only from SQLite.

        The cache-only snapshot reader never calls this method. Failed or
        conflicting source observations leave their fields unknown.
        """
        rows = list(quotes or [])
        recorder = getattr(self.store, "record_daily_risk_observations", None)
        loader = getattr(self.store, "daily_risk_evidence_for_quotes", None)
        if not rows or not batch_id or not callable(recorder) or not callable(loader):
            return rows
        from . import risk_qualification as risk

        path = self.store.path.parent / "research_risk_evidence" / (trade_date + ".json")
        try:
            sidecar = await self._store_call(risk.read_research_sidecar, path,
                                              trade_date=trade_date, batch_id=batch_id)
            await self._store_call(recorder, sidecar)
        except (OSError, sqlite3.Error, ValueError, TypeError, KeyError):
            # The bounded sidecar is optional; the collection time remains
            # separate from the research freeze and from this decision.
            pass
        for quote in rows:
            quote.suspended = quote.limit_up = quote.limit_down = quote.st = None
        rows = await self._apply_cached_daily_risk_evidence(trade_date, batch_id, rows)
        targets = [quote for quote in rows if any(
            getattr(quote, field, None) is None for field in ("suspended", "limit_up", "limit_down", "st")
        )]
        if targets:
            _, observations = await self._enrich_daily_snapshot_risk_fields(
                trade_date, targets, collect_evidence=True,
            )
            if observations:
                bundle = {"trade_date": trade_date, "batch_id": batch_id,
                          "observations": [{**item, "trade_date": trade_date,
                                            "batch_id": batch_id, "source": "eastmoney:companion"}
                                           for item in observations if isinstance(item, dict)]}
                try:
                    await self._store_call(recorder, bundle)
                except (sqlite3.Error, ValueError, TypeError, RuntimeError):
                    logger.warning("[%s] 自动收盘风险证据未能持久化，按未知处理", PLUGIN_NAME)
        for quote in rows:
            quote.suspended = quote.limit_up = quote.limit_down = quote.st = None
        return await self._apply_cached_daily_risk_evidence(trade_date, batch_id, rows)

    @staticmethod
    def _price_plan_from_payload(payload: str) -> PricePlan | None:
        try:
            raw = json.loads(payload or "{}")
            fields = PricePlan.__dataclass_fields__
            if not isinstance(raw, dict) or not raw.get("quality"):
                return None
            # Do not pass absent v0.12 fields as explicit None: dataclass
            # defaults preserve legacy JSON as unvalidated/hidden.
            values = {key: raw[key] for key in fields if key in raw}
            if "evidence" in values and not isinstance(values["evidence"], list):
                values["evidence"] = []
            if "provenance" in values and not isinstance(values["provenance"], dict):
                values["provenance"] = {}
            if "validated" in values:
                values["validated"] = values["validated"] is True
            return PricePlan(**values)
        except (TypeError, ValueError, KeyError):
            return None

    @staticmethod
    def _price_state_for_plan(quote, plan: PricePlan | None) -> str:
        """Evaluate the immutable closing plan against a fresh intraday quote."""
        if not price_plan_is_validated(plan) or quote.price <= 0:
            return "unknown"
        if plan.invalidation is not None and quote.price <= plan.invalidation:
            return "invalidated"
        if plan.sell_low is not None and quote.price >= plan.sell_low:
            return "near_sell"
        if plan.confirmation is not None and quote.price >= plan.confirmation:
            return "confirmed"
        if plan.attention_low is not None and plan.attention_high is not None and plan.attention_low <= quote.price <= plan.attention_high:
            return "in_attention"
        return "between"

    def _configured_intraday_focus_codes(self) -> list[str]:
        return parse_codes(str(self.config.get("intraday_focus_codes", "") or ""))[
            : self._int("intraday_focus_limit", 200, 10, 500)
        ]

    @staticmethod
    def _intraday_plan_version(stored: Mapping[str, object] | None) -> str:
        if not stored:
            return "watch-v1"
        run_id = str(stored.get("run_id") or "candidate")
        payload = str(stored.get("price_plan") or "")
        digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]
        return f"{run_id}:{digest}"

    @classmethod
    def _intraday_close_regime(cls, stored: Mapping[str, object] | None) -> str:
        """Return the immutable close-time regime, never a live market claim."""
        payload = cls._stored_factor_payload(dict(stored or {}))
        value = str(payload.get("market_regime") or "unknown").strip().lower()
        return value if value in {"risk_on", "neutral", "risk_off"} else "unknown"

    @classmethod
    def _intraday_market_regime(cls, stored: Mapping[str, object] | None) -> str:
        """Compatibility spelling for callers that need the close-time label."""
        return cls._intraday_close_regime(stored)

    @staticmethod
    def _intraday_live_market_regime() -> tuple[str, str]:
        """No current whole-market evidence interface is available yet.

        A candidate's previous close regime is deliberately not substituted
        here. Opportunity notifications remain fail-closed until a provider
        can prove a current, sufficiently broad market sample.
        """
        return "unknown", "live_market_evidence_unavailable"

    @staticmethod
    def _intraday_timestamp(value: object) -> datetime | None:
        if not isinstance(value, datetime):
            return None
        return value.astimezone(CHINA_TZ) if value.tzinfo else value.replace(tzinfo=CHINA_TZ)

    @classmethod
    def _quote_source_timestamp(cls, quote) -> datetime | None:
        provider_time = getattr(quote, "provider_ts", None)
        return cls._intraday_timestamp(provider_time if provider_time is not None else getattr(quote, "fetched_at", None))

    def _intraday_market_state_is_fresh(self, state: Mapping[str, object] | None, now: datetime) -> bool:
        if not isinstance(state, Mapping) or str(state.get("quality") or "") != "good":
            return False
        if str(state.get("source") or "") != "sina":
            return False
        oldest = self._intraday_delivery_time(state.get("quote_timestamp_min"))
        latest = self._intraday_delivery_time(state.get("quote_timestamp_max"))
        if oldest is None or latest is None or oldest > latest:
            return False
        if oldest.date() != now.date() or latest.date() != now.date():
            return False
        age = (now - oldest).total_seconds()
        maximum = self._int("intraday_market_max_age_seconds", 150, 30, 900)
        maximum_skew = self._int("intraday_market_max_timestamp_skew_seconds", 90, 1, 900)
        allowed_future_skew = self._int("quote_clock_skew_seconds", 120, 0, 900)
        try:
            sample = int(state.get("sample_size") or 0)
            expected = int(state.get("expected_size") or 0)
            coverage = float(state.get("coverage") or 0)
        except (TypeError, ValueError, OverflowError):
            return False
        minimum = self._int("market_min_snapshot_size", 4000, 1000, 10000)
        coverage_floor = self._float("intraday_market_min_coverage", 0.95, 0.50, 1.0)
        return (
            -allowed_future_skew <= age <= maximum
            and (latest - now).total_seconds() <= allowed_future_skew
            and (latest - oldest).total_seconds() <= maximum_skew
            and sample >= minimum and expected >= sample and coverage >= coverage_floor
        )

    @staticmethod
    def _intraday_market_evaluation_clock(_started_at: datetime) -> datetime:
        """Return the post-fetch clock used to evaluate quote timestamps."""
        return datetime.now(CHINA_TZ)

    def _intraday_market_candidate_regime(
        self,
        breadth: float,
        median_return: float,
        previous: str = "unknown",
    ) -> str:
        """Classify one verified cross-section with explicit hysteresis bands."""
        strong_breadth = self._float("intraday_market_strong_breadth", 0.60, 0.50, 0.95)
        weak_breadth = self._float("intraday_market_weak_breadth", 0.40, 0.05, 0.50)
        risk_breadth = self._float("intraday_market_risk_off_breadth", 0.30, 0.01, weak_breadth)
        strong_median = self._float("intraday_market_strong_median_pct", 0.30, -10, 10)
        weak_median = self._float("intraday_market_weak_median_pct", -0.20, -10, 10)
        risk_median = self._float("intraday_market_risk_off_median_pct", -0.70, -10, weak_median)
        # Staying bands prevent harmless one-snapshot edge oscillation from
        # changing delivery rules.  A transition still needs confirmation.
        if previous == "risk_off" and breadth <= risk_breadth + 0.04 and median_return <= risk_median + 0.20:
            return "risk_off"
        if breadth <= risk_breadth and median_return <= risk_median:
            return "risk_off"
        if previous == "strong" and breadth >= strong_breadth - 0.04 and median_return >= strong_median - 0.15:
            return "strong"
        if breadth <= weak_breadth and median_return <= weak_median:
            return "weak"
        if breadth >= strong_breadth and median_return >= strong_median:
            return "strong"
        if previous == "weak" and breadth <= weak_breadth + 0.04 and median_return <= weak_median + 0.15:
            return "weak"
        return "neutral"

    async def _resolve_intraday_market_context(self, now: datetime) -> dict[str, object]:
        """Load or collect a verified whole-market intraday cross-section.

        The expected universe is the active raw generation's membership only.
        It never supplies current prices.  The live quotes are independently
        obtained through the existing Sina provider and must carry coherent
        exchange timestamps before they can affect opportunities.
        """
        # This flag is an in-flight guard only. Acquire it before the first
        # await, return unknown to re-entrant callers, and always release it.
        if getattr(self, "_intraday_market_refresh_active", False):
            return {"regime": "unknown", "quality": "unknown", "reason": "intraday_market_refresh_pending", "source": ""}
        self._intraday_market_refresh_active = True
        try:
            loader = getattr(self.store, "intraday_market_regime_state", None)
            saved = await self._store_call(loader) if callable(loader) else None
            if self._intraday_market_state_is_fresh(saved, now):
                refreshed_at = self._intraday_delivery_time(saved.get("source_timestamp"))
                refresh = self._int("intraday_market_refresh_seconds", 120, 30, 900)
                if refreshed_at is not None and (now - refreshed_at).total_seconds() <= refresh:
                    return dict(saved)
            # Existing integrations/tests can deliberately override the former
            # fail-closed hook.  Keep that narrow compatibility seam without
            # allowing the default stub to replace real collection.
            hook = getattr(self, "_intraday_live_market_regime")
            if hook is not Main._intraday_live_market_regime:
                regime, reason = hook()
                return {"regime": str(regime), "quality": "unknown", "reason": str(reason), "source": "compatibility_hook"}

            universe_loader = getattr(self.store, "active_raw_universe_codes", None)
            if not callable(universe_loader):
                return {"regime": "unknown", "quality": "unknown", "reason": "active_raw_universe_unavailable", "source": ""}
            codes, raw = await self._store_call(
                universe_loader,
                self._raw_dataset(),
                as_of=now.date().isoformat(),
                max_stale_trading_days=self._raw_stale_days(),
            )
            expected = list(dict.fromkeys(str(code) for code in codes if str(code)))
            if not expected:
                return await self._persist_intraday_market_unavailable("active_raw_universe_unavailable", now, raw=raw)
            fetcher = getattr(self.quotes, "fetch_intraday_market_snapshot", None)
            if not callable(fetcher):
                return await self._persist_intraday_market_unavailable("intraday_market_provider_unavailable", now, raw=raw, expected=len(expected))
            try:
                result = await fetcher(expected)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[%s] 盘中全市场截面获取失败", PLUGIN_NAME)
                return await self._persist_intraday_market_unavailable("intraday_market_provider_error", now, raw=raw, expected=len(expected))
            evaluated_at = self._intraday_market_evaluation_clock(now)
            evaluated_at = evaluated_at.astimezone(CHINA_TZ) if evaluated_at.tzinfo else evaluated_at.replace(tzinfo=CHINA_TZ)
            rows = list(getattr(result, "quotes", []) or [])
            source = str(getattr(result, "source", "") or "")
            failed_batches = int(getattr(result, "failed_batches", 0) or 0)
            expected_codes = set(expected)
            usable: list[Quote] = []
            timestamps: list[datetime] = []
            source_gap = False
            for quote in rows:
                timestamp = self._intraday_timestamp(getattr(quote, "provider_ts", None) or getattr(quote, "fetched_at", None))
                try:
                    valid = (
                        str(getattr(quote, "code", "")) in expected_codes
                        and str(getattr(quote, "source", "")) == source == "sina"
                        and math.isfinite(float(quote.price)) and float(quote.price) > 0
                        and math.isfinite(float(quote.pct_change))
                    )
                except (TypeError, ValueError, OverflowError):
                    valid = False
                if timestamp is None:
                    source_gap = True
                    continue
                if timestamp.date() != evaluated_at.date():
                    source_gap = True
                    continue
                if valid:
                    usable.append(quote)
                    timestamps.append(timestamp)
            by_code = {quote.code: quote for quote in usable}
            usable = list(by_code.values())
            minimum = self._int("market_min_snapshot_size", 4000, 1000, 10000)
            coverage_floor = self._float("intraday_market_min_coverage", 0.95, 0.50, 1.0)
            coverage = len(usable) / len(expected) if expected else 0.0
            reason = ""
            if source != "sina":
                reason = "intraday_market_source_unverified"
            elif failed_batches:
                reason = "intraday_market_source_gap"
            elif source_gap:
                reason = "intraday_market_timestamp_gap"
            elif len(usable) < minimum or coverage < coverage_floor:
                reason = "intraday_market_coverage_inadequate"
            elif not timestamps:
                reason = "intraday_market_timestamp_missing"
            else:
                earliest, latest = min(timestamps), max(timestamps)
                max_age = self._int("intraday_market_max_age_seconds", 150, 30, 900)
                max_skew = self._int("intraday_market_max_timestamp_skew_seconds", 90, 1, 900)
                allowed_future_skew = self._int("quote_clock_skew_seconds", 120, 0, 900)
                if (evaluated_at - earliest).total_seconds() < -allowed_future_skew:
                    reason = "intraday_market_future_timestamp"
                elif (evaluated_at - earliest).total_seconds() > max_age:
                    reason = "intraday_market_stale"
                elif (latest - evaluated_at).total_seconds() > allowed_future_skew:
                    reason = "intraday_market_future_timestamp"
                elif (latest - earliest).total_seconds() > max_skew:
                    reason = "intraday_market_mixed_timestamps"
            if reason:
                return await self._persist_intraday_market_unavailable(
                    reason, now, raw=raw, expected=len(expected), sample=len(usable), coverage=coverage,
                    source=source, timestamps=timestamps,
                )
            market = assess_market_context(usable)
            median = float(market.median_return) if market.median_return is not None else math.nan
            if not math.isfinite(median):
                return await self._persist_intraday_market_unavailable("intraday_market_median_unavailable", now, raw=raw, expected=len(expected), sample=len(usable), coverage=coverage, source=source, timestamps=timestamps)
            prior = dict(saved or {})
            prior_regime = str(prior.get("regime") or "unknown")
            candidate = self._intraday_market_candidate_regime(float(market.breadth), median, prior_regime)
            required = self._int("intraday_market_regime_confirmations", 2, 1, 10)
            pending = str(prior.get("pending_regime") or "unknown")
            pending_count = int(prior.get("pending_count") or 0)
            regime = prior_regime if prior_regime in {"strong", "neutral", "weak", "risk_off"} else "unknown"
            if candidate == regime:
                pending, pending_count = "unknown", 0
            elif candidate == pending:
                pending_count += 1
            else:
                pending, pending_count = candidate, 1
            if pending_count >= required:
                regime, pending, pending_count = candidate, "unknown", 0
            state = {
                "regime": regime,
                "pending_regime": pending,
                "pending_count": pending_count,
                "source": source,
                "source_timestamp": max(timestamps).astimezone(timezone.utc).isoformat(),
                "sample_size": market.sample_size,
                "expected_size": len(expected),
                "coverage": coverage,
                "breadth": market.breadth,
                "advancing": market.advancing,
                "declining": market.declining,
                "flat": market.flat,
                "median_return": median,
                "quote_timestamp_min": min(timestamps).astimezone(timezone.utc).isoformat(),
                "quote_timestamp_max": max(timestamps).astimezone(timezone.utc).isoformat(),
                "quality": "good",
                "reason": "confirmed" if regime != "unknown" else f"awaiting_regime_confirmation:{pending_count}/{required}",
                "raw_batch_id": str(raw.get("batch_id") or raw.get("active_batch_id") or ""),
                "raw_generation": raw.get("generation"),
                "updated_at": evaluated_at.astimezone(timezone.utc).isoformat(),
            }
            saver = getattr(self.store, "save_intraday_market_regime_state", None)
            return await self._store_call(saver, state) if callable(saver) else state

        finally:
            self._intraday_market_refresh_active = False

    async def _refresh_intraday_market_regime_state(self, now: datetime | None = None) -> dict[str, object]:
        """Refresh the durable regime state; the resolver owns the reentry guard."""
        current = now or datetime.now(CHINA_TZ)
        current = current.astimezone(CHINA_TZ) if current.tzinfo else current.replace(tzinfo=CHINA_TZ)
        return await self._resolve_intraday_market_context(current)

    async def _persist_intraday_market_unavailable(self, reason: str, now: datetime, **values) -> dict[str, object]:
        timestamps = [value for value in values.pop("timestamps", []) if isinstance(value, datetime)]
        raw = values.pop("raw", {}) if isinstance(values.get("raw", {}), Mapping) else {}
        state = {
            "regime": "unknown", "pending_regime": "unknown", "pending_count": 0,
            "source": str(values.get("source") or ""), "source_timestamp": "",
            "sample_size": int(values.get("sample") or 0), "expected_size": int(values.get("expected") or 0),
            "coverage": float(values.get("coverage") or 0), "breadth": None,
            "advancing": 0, "declining": 0, "flat": 0, "median_return": None,
            "quote_timestamp_min": min(timestamps).astimezone(timezone.utc).isoformat() if timestamps else "",
            "quote_timestamp_max": max(timestamps).astimezone(timezone.utc).isoformat() if timestamps else "",
            "quality": "unknown", "reason": str(reason),
            "raw_batch_id": str(raw.get("batch_id") or raw.get("active_batch_id") or ""),
            "raw_generation": raw.get("generation"), "updated_at": now.astimezone(timezone.utc).isoformat(),
        }
        saver = getattr(self.store, "save_intraday_market_regime_state", None)
        return await self._store_call(saver, state) if callable(saver) else state

    def _intraday_signal_specs(
        self,
        quote: Quote,
        target: Mapping[str, object],
        *,
        minute_signal: str = "",
        completed_bar_close: float | None = None,
        completed_bar_start: datetime | None = None,
        live_regime: str = "unknown",
        now: datetime | None = None,
    ) -> tuple[list[dict[str, object]], list[str]]:
        """Build fail-closed signal observations from one fresh quote."""
        current = now or datetime.now(CHINA_TZ)
        current = current.astimezone(CHINA_TZ) if current.tzinfo else current.replace(tzinfo=CHINA_TZ)
        reasons: list[str] = []
        try:
            price = float(quote.price)
        except (TypeError, ValueError, OverflowError):
            return [], ["invalid_price"]
        if not math.isfinite(price) or price <= 0:
            return [], ["invalid_price"]
        fetched = self._intraday_timestamp(quote.fetched_at)
        source_time = self._quote_source_timestamp(quote)
        if fetched is None or source_time is None:
            return [], ["missing_quote_timestamp"]
        max_age = max(30, self._int("quote_interval_seconds", 30, 10, 600) * 2)
        allowed_future_skew = self._int("quote_clock_skew_seconds", 120, 0, 900)
        if any(not -allowed_future_skew <= (current - value).total_seconds() <= max_age for value in (fetched, source_time)):
            return [], ["stale_quote"]
        if (quote.suspended is None or quote.limit_up is None
                or quote.limit_down is None or quote.st is None):
            return [], ["risk_state_unknown"]

        stored = target.get("stored") if isinstance(target.get("stored"), Mapping) else None
        plan = self._price_plan_from_payload(str(stored.get("price_plan") or "")) if stored else None
        plan_valid = price_plan_is_validated(plan)
        close_regime = self._intraday_close_regime(stored)
        regime = str(live_regime or "unknown").strip().lower()
        if regime not in {"strong", "neutral", "weak", "risk_off"}:
            regime = "unknown"
        hysteresis = self._float("intraday_hysteresis_pct", 0.3, 0.05, 5.0) / 100
        cooldown = self._int("intraday_cooldown_seconds", 1800, 30, 86400)
        gap = self._int("intraday_confirmation_max_gap_seconds", 90, 10, 900)
        base_required = self._int("intraday_confirmation_periods", 2, 1, 10)
        weak_extra = 1 if regime in {"weak", "risk_off"} else 0
        specs: list[dict[str, object]] = []

        hard_reasons: list[str] = []
        if quote.suspended is True:
            hard_reasons.append("suspended")
        if quote.limit_up is True or quote.limit_down is True:
            hard_reasons.append("price_limit")
        if quote.st is True:
            hard_reasons.append("st")
        minimum_amount = self._float("intraday_min_amount", 5_000_000, 0, 1_000_000_000_000)
        try:
            amount = float(quote.amount)
        except (TypeError, ValueError, OverflowError):
            amount = -1
        if not math.isfinite(amount) or amount < minimum_amount:
            hard_reasons.append("illiquid")
        stored_risk = str(stored.get("risk_level") or "") if stored else ""
        if stored_risk == "blocked":
            hard_reasons.append("stored_risk_blocked")
        specs.append({
            "signal": "risk_invalidated",
            "qualifies": bool(hard_reasons),
            "rearm_ready": not hard_reasons,
            "required": 1,
            "max_gap": gap,
            "cooldown": cooldown,
            "condition": "、".join(hard_reasons) if hard_reasons else "硬风险条件未触发",
            "evidence": hard_reasons or ["停牌/涨跌停/流动性/候选风险检查通过"],
            "risk": "blocked" if hard_reasons else "eligible",
        })
        if hard_reasons:
            return specs, hard_reasons
        if stored and stored_risk == "unknown":
            return specs, ["stored_risk_unknown"]

        if stored and not plan_valid:
            reasons.append("invalid_or_unverified_plan")
        opportunity_allowed = regime not in {"unknown", "risk_off"}
        if not opportunity_allowed:
            reasons.append("live_market_regime_unavailable")
        if plan_valid and plan is not None:
            attention = bool(plan.attention_low <= price <= plan.attention_high)
            specs.append({
                "signal": "attention_entry", "qualifies": attention and opportunity_allowed,
                "rearm_ready": price < plan.attention_low * (1 - hysteresis) or price > plan.attention_high * (1 + hysteresis),
                "required": base_required + weak_extra, "max_gap": gap, "cooldown": cooldown,
                "condition": f"现价进入风险区间 {plan.attention_low:.2f}-{plan.attention_high:.2f}",
                "evidence": [f"price={price:.2f}", f"attention={plan.attention_low:.2f}-{plan.attention_high:.2f}"],
                "risk": stored_risk or "eligible",
            })
            breakout_buffer = self._float("intraday_breakout_buffer_pct", 0.2, 0, 5.0)
            if regime in {"weak", "risk_off"}:
                breakout_buffer = max(breakout_buffer, self._float("intraday_weak_breakout_buffer_pct", 0.5, 0, 10.0))
            breakout_level = plan.confirmation * (1 + breakout_buffer / 100)
            # A quote well above the trigger is an observation, not an
            # executable entry. Keep the cap below the separate exit zone.
            chase_ceiling = min(plan.confirmation * 1.02, plan.sell_low)
            try:
                bar_close = float(completed_bar_close)
            except (TypeError, ValueError, OverflowError):
                bar_close = math.nan
            bar_start = self._intraday_timestamp(completed_bar_start)
            bar_age = (source_time - bar_start - timedelta(minutes=1)).total_seconds() if bar_start else math.nan
            completed_breakout = (math.isfinite(bar_close) and breakout_level <= bar_close <= chase_ceiling
                                  and math.isfinite(bar_age) and 0 <= bar_age <= gap)
            quote_in_breakout = breakout_level <= price <= chase_ceiling and opportunity_allowed and stored_risk == "eligible"
            awaiting_bar = quote_in_breakout and completed_bar_close is None and completed_bar_start is None
            specs.append({
                "signal": "confirmed_breakout", "qualifies": completed_breakout and quote_in_breakout,
                "awaiting_completed_bar": awaiting_bar,
                "observation_at": bar_start if completed_breakout else None,
                "rearm_ready": price < plan.confirmation * (1 - hysteresis),
                "required": base_required + weak_extra, "max_gap": gap, "cooldown": cooldown,
                "condition": f"现价位于突破观察区 {breakout_level:.2f}-{chase_ceiling:.2f}；尚需成交与T+1约束核验",
                "evidence": [f"price={price:.2f}", f"confirmation={plan.confirmation:.2f}", f"chase_ceiling={chase_ceiling:.2f}", f"completed_bar_close={bar_close:.2f}" if math.isfinite(bar_close) else "completed_bar_missing", f"completed_bar_start={bar_start.isoformat()}" if bar_start else "completed_bar_time_missing", f"regime={regime}"],
                "risk": stored_risk or "eligible",
            })
            specs.append({
                "signal": "invalidation_breach", "qualifies": price <= plan.invalidation,
                "rearm_ready": price > plan.invalidation * (1 + hysteresis),
                "required": 1, "max_gap": gap, "cooldown": cooldown,
                "condition": f"现价跌破失效位 {plan.invalidation:.2f}",
                "evidence": [f"price={price:.2f}", f"invalidation={plan.invalidation:.2f}"],
                "risk": "blocked",
            })
            specs.append({
                "signal": "reference_exit_zone", "qualifies": price >= plan.sell_low and opportunity_allowed,
                "rearm_ready": price < plan.sell_low * (1 - hysteresis),
                "required": base_required + weak_extra, "max_gap": gap, "cooldown": cooldown,
                "condition": f"现价进入参考退出区 {plan.sell_low:.2f}-{plan.sell_high:.2f}",
                "evidence": [f"price={price:.2f}", f"reference_exit={plan.sell_low:.2f}-{plan.sell_high:.2f}"],
                "risk": "watch_only",
            })

        volume_ratio = quote.volume_ratio
        try:
            volume_ratio = float(volume_ratio) if volume_ratio is not None else None
        except (TypeError, ValueError, OverflowError):
            volume_ratio = None
        volume_threshold = self._float("intraday_abnormal_volume_ratio", 2.0, 1.0, 20.0)
        move_floor = self._float("intraday_abnormal_volume_move_pct", 1.0, 0, 20.0)
        try:
            pct_change = float(quote.pct_change)
        except (TypeError, ValueError, OverflowError):
            pct_change = math.nan
        volume_qualifies = bool(
            volume_ratio is not None and math.isfinite(volume_ratio)
            and math.isfinite(pct_change) and volume_ratio >= volume_threshold and abs(pct_change) >= move_floor
        )
        specs.append({
            "signal": "abnormal_volume", "qualifies": volume_qualifies and opportunity_allowed,
            "rearm_ready": volume_ratio is not None and volume_ratio <= max(1.0, volume_threshold - 0.25),
            "required": base_required + weak_extra, "max_gap": gap, "cooldown": cooldown,
            "condition": f"量比达到 {volume_threshold:.2f} 且涨跌幅绝对值达到 {move_floor:.2f}%",
            "evidence": [
                f"volume_ratio={volume_ratio:.2f}" if volume_ratio is not None else "volume_ratio=missing",
                f"pct_change={pct_change:+.2f}%" if math.isfinite(pct_change) else "pct_change=missing",
            ],
            "risk": "watch_only",
        })
        specs.append({
            "signal": "rapid_move", "qualifies": bool(minute_signal) and opportunity_allowed, "rearm_ready": not minute_signal,
            "required": base_required + weak_extra, "max_gap": gap, "cooldown": cooldown,
            "condition": "分钟级连续变动与区间突破同时满足",
            "evidence": [self._clean_external_text(minute_signal, 240)] if minute_signal else ["minute_condition_false"],
            "risk": "watch_only",
        })
        cost = target.get("cost_price")
        try:
            cost = float(cost) if cost is not None else None
        except (TypeError, ValueError, OverflowError):
            cost = None
        if cost is not None and math.isfinite(cost) and cost > 0:
            change = (price - cost) / cost * 100
            profit = self._float("cost_profit_threshold_pct", 5.0, 0.1, 1000)
            loss = self._float("cost_risk_threshold_pct", 5.0, 0.1, 1000)
            specs.extend((
                {
                    "signal": "cost_profit", "qualifies": change >= profit and opportunity_allowed,
                    "rearm_ready": change < profit - max(0.1, hysteresis * 100),
                    "required": base_required, "max_gap": gap, "cooldown": cooldown,
                    "condition": f"相对记录成本达到 +{profit:.2f}% 观察阈值",
                    "evidence": [f"price={price:.2f}", f"cost={cost:.2f}", f"change={change:+.2f}%"],
                    "risk": "watch_only",
                },
                {
                    "signal": "cost_risk", "qualifies": change <= -loss,
                    "rearm_ready": change > -loss + max(0.1, hysteresis * 100),
                    "required": 1, "max_gap": gap, "cooldown": cooldown,
                    "condition": f"相对记录成本达到 -{loss:.2f}% 风险阈值",
                    "evidence": [f"price={price:.2f}", f"cost={cost:.2f}", f"change={change:+.2f}%"],
                    "risk": "blocked",
                },
            ))
        return specs, reasons

    @staticmethod
    def _intraday_event_key(origin: str, code: str, signal: str, plan_version: str, sequence: int) -> str:
        digest = hashlib.sha256(f"{origin}\0{code}\0{signal}\0{plan_version}\0{sequence}".encode("utf-8")).hexdigest()
        return f"intraday:{digest}"

    def _intraday_event_payload(
        self,
        quote: Quote | None,
        target: Mapping[str, object],
        spec: Mapping[str, object],
        *,
        invocation_id: str,
        now: datetime,
        live_regime: str = "unknown",
    ) -> str:
        stored = target.get("stored") if isinstance(target.get("stored"), Mapping) else None
        plan = self._price_plan_from_payload(str(stored.get("price_plan") or "")) if stored else None
        def level(value) -> str:
            try:
                number = float(value)
                return f"{number:.2f}" if math.isfinite(number) and number > 0 else "未确认"
            except (TypeError, ValueError, OverflowError):
                return "未确认"
        current = now.astimezone(CHINA_TZ) if now.tzinfo else now.replace(tzinfo=CHINA_TZ)
        quote_price = None
        fetched = None
        if quote is not None:
            try:
                candidate_price = float(quote.price)
                quote_price = candidate_price if math.isfinite(candidate_price) and candidate_price > 0 else None
            except (TypeError, ValueError, OverflowError):
                quote_price = None
            fetched = self._quote_source_timestamp(quote)
        if quote_price is not None and fetched is not None:
            price_text = f"现价：{quote_price:.2f}"
            freshness = f"行情新鲜度：{max(0.0, (current - fetched).total_seconds()):.0f}秒"
            label = quote.name or quote.code
            code = quote.code
        else:
            provenance = plan.provenance if isinstance(getattr(plan, "provenance", None), dict) else {}
            last_price = level(provenance.get("last_close") or getattr(plan, "reference_price", None))
            last_date = str(provenance.get("last_date") or provenance.get("actual_date") or "未记录")
            price_text = f"最近可核验价：{last_price}（收盘计划参考，日期 {last_date}）"
            freshness = "当前行情：未验证，不作为机会信号"
            label = str((stored or {}).get("name") or target.get("name") or target.get("code") or "标的")
            code = str((stored or {}).get("code") or target.get("code") or "未记录")
        provenance = "/".join(sorted(str(value) for value in target.get("provenance", []) if str(value))) or "未记录"
        close_regime = self._intraday_close_regime(stored)
        current_regime = str(live_regime or "unknown").strip().lower()
        current_label = self._market_label(current_regime) if current_regime in {"strong", "neutral", "weak", "risk_off"} else "未验证"
        evidence = "；".join(str(value) for value in spec.get("evidence", []) if str(value))[:500] or "无"
        return (
            f"盘中研究提醒：{label}（{code}）\n"
            f"时间：{current:%Y-%m-%d %H:%M:%S}；{price_text}；"
            f"信号：{spec.get('signal', 'unknown')}；条件：{spec.get('condition', '')}\n"
            f"证据：{evidence}\n"
            f"价位：风险区间 {level(getattr(plan, 'attention_low', None))}-{level(getattr(plan, 'attention_high', None))}；"
            f"建议买入价 {level(getattr(plan, 'confirmation', None))}；失效 {level(getattr(plan, 'invalidation', None))}\n"
            f"市场：当前 {current_label}；收盘候选环境 {self._market_label(close_regime)}；{freshness}；风险：{spec.get('risk', 'unknown')}\n"
            f"来源：{provenance}；计划版本：{target.get('plan_version', 'watch-v1')}；invocation：{invocation_id}\n"
            "仅供研究/模拟盘，不自动下单。"
        )

    def _build_intraday_targets(
        self,
        watch: Mapping[str, list[str]],
        watch_details: Mapping[str, Mapping[str, object]],
        subscriptions: set[str],
        stored_candidates: Mapping[str, Mapping[str, object]],
        focus_codes: list[str],
        research_pools: Mapping[str, object] | None = None,
    ) -> dict[str, dict[str, dict[str, object]]]:
        result: dict[str, dict[str, dict[str, object]]] = {}
        for origin in sorted(subscriptions):
            targets: dict[str, dict[str, object]] = {}
            for code in watch.get(origin, []):
                target = targets.setdefault(str(code), {"provenance": set(), "stored": None})
                target["provenance"].add("watchlist")
                target["cost_price"] = watch_details.get(origin, {}).get(str(code))
            for code, stored in stored_candidates.items():
                target = targets.setdefault(str(code), {"provenance": set(), "stored": None})
                target["provenance"].add("close_candidate")
                target["stored"] = stored
            for code in focus_codes:
                targets.setdefault(str(code), {"provenance": set(), "stored": None})["provenance"].add("configured_focus")
            for pool in ("primary", "radar"):
                for item in (research_pools or {}).get("picks", {}).get(pool, []):
                    code = str(item.get("code") or "")
                    if not code or code in stored_candidates or code in watch.get(origin, []):
                        continue
                    target = targets.setdefault(code, {"provenance": set(), "stored": None})
                    target["provenance"].add("research_" + pool)
                    target["research"] = item
                    target["research_pool"] = pool
                    target["research_run_id"] = str(research_pools.get("run_id") or "")
                    target["research_trade_date"] = str(research_pools.get("trade_date") or "")
            for code, target in targets.items():
                stored = target.get("stored") if isinstance(target.get("stored"), Mapping) else None
                target["code"] = str(code)
                if stored:
                    target["name"] = str(stored.get("name") or "")
                elif target.get("research"):
                    target["name"] = str(target["research"].get("name") or "")
                target["plan_version"] = self._intraday_plan_version(stored)
                if target.get("research"):
                    target["plan_version"] = str(target["research_run_id"]) + ":" + str(target["research_pool"])
                target["run_id"] = str(stored.get("run_id") or "") if stored else ""
                target["valid_until"] = str(stored.get("valid_until") or "") if stored else ""
            result[origin] = targets
        return result

    @staticmethod
    def _intraday_is_risk_signal(signal: str) -> bool:
        return str(signal) in {
            "risk_invalidated", "invalidation_breach", "cost_risk",
            "data_invalidated", "plan_expired", "market_regime_invalidated",
        }

    async def _emit_intraday_invalidation(
        self,
        origin: str,
        target: Mapping[str, object],
        *,
        signal: str,
        reason: str,
        invocation_id: str,
        now: datetime,
        quote: Quote | None = None,
    ) -> tuple[int, str]:
        code = str((quote.code if quote is not None else target.get("code")) or "").strip()
        if not code:
            return 0, "invalid_target"
        plan_version = str(target.get("plan_version") or "watch-v1")
        run_id = str(target.get("run_id") or f"watch:{now.astimezone(CHINA_TZ).date().isoformat()}")
        spec = {
            "signal": signal,
            "condition": reason,
            "evidence": [reason, "当前行情或环境未达到机会通知证据要求"],
            "risk": "blocked",
        }
        payload = self._intraday_event_payload(
            quote,
            target,
            spec,
            invocation_id=invocation_id,
            now=now,
            live_regime="unknown",
        )
        decision = await self._store_call(
            self.store.observe_and_enqueue_intraday_event,
            origin,
            code,
            signal,
            plan_version,
            qualifies=True,
            rearm_ready=False,
            required=1,
            max_gap_seconds=self._int("intraday_confirmation_max_gap_seconds", 90, 10, 900),
            cooldown_seconds=self._int("intraday_cooldown_seconds", 1800, 30, 86400),
            reason=reason,
            name=str((quote.name if quote is not None else target.get("name")) or code),
            run_id=run_id,
            invocation_id=invocation_id,
            payload=payload,
            quote_fetched_at=(self._quote_source_timestamp(quote).astimezone(timezone.utc).isoformat() if self._quote_source_timestamp(quote) else ""),
            candidate_valid_until=str(target.get("valid_until") or ""),
            risk_event=True,
            now=now,
        )
        if not decision.get("triggered"):
            return 0, f"{signal}:{decision.get('reason', 'not_triggered')}"
        outbox = decision.get("outbox") or {}
        event_key = str(decision.get("event_key") or outbox.get("event_key") or "")
        await self._store_call(
            self.store.save_risk_event,
            event_key,
            run_id,
            code,
            signal,
            "blocked",
            json.dumps({"reason": reason, "plan_version": plan_version, "quote_fetched_at": outbox.get("quote_fetched_at", "")}, ensure_ascii=False),
            now.astimezone(timezone.utc).isoformat(),
        )
        return 1, f"{signal}:{await self._dispatch_intraday_delivery(outbox)}"

    async def _invalidate_intraday_target_batch(
        self,
        targets: Mapping[str, Mapping[str, Mapping[str, object]]],
        reason: str,
        *,
        now: datetime,
    ) -> int:
        invalidator = getattr(self.store, "invalidate_intraday_target_states", None)
        if not callable(invalidator):
            return 0
        identities = [
            (origin, code, str(target.get("plan_version") or "watch-v1"))
            for origin, rows in targets.items()
            for code, target in rows.items()
        ]
        return await self._store_call(invalidator, identities, reason, now=now)

    async def _process_intraday_events(
        self,
        origin: str,
        quote: Quote,
        target: Mapping[str, object],
        *,
        minute_signal: str,
        completed_bar_close: float | None = None,
        completed_bar_start: datetime | None = None,
        invocation_id: str,
        now: datetime,
        live_regime: str = "unknown",
        market_context: Mapping[str, object] | None = None,
    ) -> tuple[int, list[str]]:
        specs, reasons = self._intraday_signal_specs(
            quote, target, minute_signal=minute_signal, completed_bar_close=completed_bar_close,
            completed_bar_start=completed_bar_start,
            live_regime=live_regime, now=now,
        )
        triggered = 0
        plan_version = str(target.get("plan_version") or "watch-v1")
        run_id = str(target.get("run_id") or f"watch:{now.astimezone(CHINA_TZ).date().isoformat()}")
        invalidator = getattr(self.store, "invalidate_intraday_observations", None)
        if callable(invalidator):
            await self._store_call(
                invalidator,
                origin,
                quote.code,
                plan_version,
                ";".join(reasons) or "signal_set_changed",
                keep_signals=[str(spec.get("signal") or "") for spec in specs],
                now=now,
            )
        if not specs:
            return 0, reasons
        # A fresh quote re-arms only data/expiry invalidations. It never turns
        # an unavailable live market environment into a claimed current regime.
        for signal in ("data_invalidated", "plan_expired"):
            await self._store_call(
                self.store.observe_intraday_signal,
                origin,
                quote.code,
                signal,
                plan_version,
                qualifies=False,
                rearm_ready=True,
                required=1,
                reason="fresh_quote_or_valid_plan_observed",
                now=now,
            )
        for spec in specs:
            if spec.get("awaiting_completed_bar"):
                reasons.append("confirmed_breakout:awaiting_completed_bar")
                continue
            payload = self._intraday_event_payload(
                quote,
                target,
                spec,
                invocation_id=invocation_id,
                now=now,
                live_regime=live_regime,
            )
            decision = await self._store_call(
                self.store.observe_and_enqueue_intraday_event,
                origin,
                quote.code,
                str(spec["signal"]),
                plan_version,
                qualifies=bool(spec.get("qualifies")),
                rearm_ready=bool(spec.get("rearm_ready")),
                required=int(spec.get("required") or 1),
                max_gap_seconds=float(spec.get("max_gap") or 90),
                cooldown_seconds=float(spec.get("cooldown") or 1800),
                reason=str(spec.get("condition") or "condition_false"),
                name=quote.name,
                run_id=run_id,
                invocation_id=invocation_id,
                payload=payload,
                quote_fetched_at=self._quote_source_timestamp(quote).astimezone(timezone.utc).isoformat(),
                candidate_valid_until=str(target.get("valid_until") or ""),
                market_regime=str((market_context or {}).get("regime") or live_regime),
                market_snapshot_at=str((market_context or {}).get("source_timestamp") or ""),
                risk_event=self._intraday_is_risk_signal(str(spec.get("signal") or "")),
                observation_at=spec.get("observation_at"),
                now=now,
            )
            if not decision.get("triggered"):
                reasons.append(f"{spec['signal']}:{decision.get('reason', 'not_triggered')}")
                continue
            outbox = decision.get("outbox") or {}
            event_key = str(decision.get("event_key") or outbox.get("event_key") or "")
            if self._intraday_is_risk_signal(str(spec["signal"])):
                await self._store_call(
                    self.store.save_risk_event,
                    event_key,
                    run_id,
                    quote.code,
                    str(spec["signal"]),
                    str(spec.get("risk") or "unknown"),
                    json.dumps({"price": quote.price, "condition": spec.get("condition"), "plan_version": plan_version}, ensure_ascii=False),
                    now.astimezone(timezone.utc).isoformat(),
                )
            outcome = await self._dispatch_intraday_delivery(outbox)
            reasons.append(f"{spec['signal']}:{outcome}")
            triggered += 1
        return triggered, reasons

    @staticmethod
    def _unadjusted_bars(bars) -> list[dict]:
        """Keep technical calculations on explicitly unadjusted rows only."""
        result = []
        for row in bars or []:
            if not isinstance(row, dict):
                continue
            if str(row.get("price_basis") or "unknown").strip().lower() != "unadjusted":
                continue
            result.append(row)
        return result

    def _daily_close_plan(self, quote, actual_date: str) -> PricePlan:
        plan = build_price_plan(
            quote,
            context="daily_close",
            actual_date=actual_date,
            tolerance_pct=self._float("price_plan_close_tolerance_pct", 1.0, 0.0, 20.0),
        )
        # Keep the exact raw generation beside the validated anchor so a
        # later replay can select the same immutable batch instead of relying
        # on whichever cache happens to be active then.
        if self._tushare_mode() and isinstance(plan.provenance, dict):
            raw = self._raw_screen_provenance if isinstance(self._raw_screen_provenance, dict) else {}
            for key in ("batch_id", "dataset_id", "generation"):
                if raw.get(key) is not None:
                    plan.provenance[key] = raw[key]
            if raw.get("batch_id") is not None:
                plan.provenance["raw_batch_id"] = raw["batch_id"]
            if raw.get("generation") is not None:
                plan.provenance["raw_generation"] = raw["generation"]
        if isinstance(plan.provenance, dict):
            factor = getattr(quote, "corporate_action_factor", None)
            evidence_text = str(getattr(quote, "corporate_action_evidence", "") or "").strip()
            try:
                factor = float(factor)
            except (TypeError, ValueError, OverflowError):
                factor = None
            code_info = StockStore._canonical_raw_code(getattr(quote, "code", ""))
            expected_evidence = f"tushare:adj_factor:{actual_date}:{code_info[1]}" if code_info else None
            captured = factor_capture({
                "adj_factor": factor, "source": "tushare_adj_factor", "evidence": evidence_text,
                "observed_at": getattr(quote, "corporate_action_observed_at", ""),
                "response_sha256": getattr(quote, "corporate_action_response_sha256", ""),
            }, getattr(quote, "code", ""), actual_date, self._factor_evidence_cutoff(actual_date))
            if captured and evidence_text == expected_evidence:
                plan.provenance["corporate_action_evidence"] = {
                    "comparable": True,
                    "factor": captured["factor"],
                    "source": "tushare_adj_factor",
                    "trade_date": actual_date,
                    "evidence": evidence_text,
                    "observed_at": captured["observed_at"],
                    "response_sha256": captured["response_sha256"],
                }
            # Keep explicit fixture/integration injection compatible, but do
            # not manufacture evidence from a provider name or declaration.
            raw = self._raw_screen_provenance if isinstance(self._raw_screen_provenance, dict) else {}
            evidence = raw.get("corporate_action_evidence")
            if "corporate_action_evidence" not in plan.provenance and isinstance(evidence, dict) and evidence.get("comparable") is True:
                try:
                    injected_factor = float(evidence.get("factor"))
                except (TypeError, ValueError, OverflowError):
                    injected_factor = 0.0
                if math.isfinite(injected_factor) and injected_factor > 0 and factor_capture(
                    {**evidence, "adj_factor": injected_factor}, getattr(quote, "code", ""), actual_date,
                    self._factor_evidence_cutoff(actual_date),
                ):
                    plan.provenance["corporate_action_evidence"] = dict(evidence)
        return plan

    async def _persist_recommendation_daily_bars(self, result, actual_date: str) -> int:
        code_reader = getattr(self.store, "recommendation_codes_for_outcomes", None)
        save_bars = getattr(self.store, "save_daily_bars", None)
        bars = self._bulk_value(result, "bars", {}) or {}
        if not callable(code_reader) or not callable(save_bars) or not isinstance(bars, dict):
            return 0
        codes = await self._store_call(code_reader)
        saved = 0
        for code in codes:
            rows = [
                row for row in bars.get(code, [])
                if isinstance(row, dict)
                and self._canonical_screen_date(row.get("trade_date")) == actual_date
                and row.get("corporate_action_factor") is not None
                and str(row.get("corporate_action_evidence") or "").strip()
            ]
            if rows:
                saved += int(await self._store_call(save_bars, code, rows, "tushare_adj_factor", "unadjusted") or 0)
        return saved

    async def _collect_market_comparison(self, result, actual_date: str, batch_id: str) -> dict:
        benchmark = str(self.config.get("market_comparison_benchmark", "") or "").strip().upper()
        identity = {"trade_date": actual_date, "benchmark_code": benchmark, "universe_ref": str(batch_id or "")}
        if not self._bool("market_comparison_enabled", False):
            return market_comparison_evidence.unknown("disabled", **identity)
        packet = {**identity, "status": "unknown", "reason": "benchmark_or_raw_batch_missing"}
        try:
            bars = self._bulk_value(result, "bars", {}) or {}
            dates = sorted({
                str(row.get("trade_date") or "") for values in bars.values() for row in values
                if isinstance(row, dict) and str(row.get("trade_date") or "") < actual_date
            })
            helper = getattr(getattr(self.quotes, "bulk_provider", None), "fetch_market_comparison", None)
            if benchmark and batch_id and dates and callable(helper):
                rows = self._bulk_value(result, "quotes", []) or []
                packet = await asyncio.wait_for(
                    helper(actual_date, dates[-1], benchmark, [q.code for q in rows], str(batch_id)),
                    timeout=30,
                )
        except (asyncio.TimeoutError, RuntimeError, ValueError, TypeError, AttributeError):
            packet = {**identity, "status": "unknown", "reason": "collection_unavailable"}
        packet.setdefault("available_at", market_comparison_evidence.utc_now())
        try:
            await self._store_call(self.store.save_market_comparison, packet)
        except (sqlite3.Error, RuntimeError, ValueError, TypeError, AttributeError):
            return market_comparison_evidence.unknown("persistence_failed", **identity)
        return market_comparison_evidence.evaluate(packet, as_of=market_comparison_evidence.utc_now())

    def _candidate_valid_until(self, actual_date: str) -> str | None:
        """Return a loose integrity bound; calendar open-count owns expiry."""
        try:
            value = str(actual_date or "")
            start = datetime.strptime(value, "%Y-%m-%d").date()
            if start.isoformat() != value:
                return None
            valid_days = self._int("candidate_plan_valid_days", 10, 1, 60)
        except (TypeError, ValueError, OverflowError):
            return None
        # This bound only limits corrupted/orphaned rows.  Do not derive it
        # from weekdays: verified calendar states and their open-count are the
        # sole business validity decision.
        safety_days = min(366, max(30, valid_days * 7))
        return f"{(start + timedelta(days=safety_days)).isoformat()}T23:59:59+08:00"

    @staticmethod
    def _trade_day_distance(start_date: str, end_date: str) -> int | None:
        try:
            start_value, end_value = str(start_date or ""), str(end_date or "")
            start = datetime.strptime(start_value, "%Y-%m-%d").date()
            end = datetime.strptime(end_value, "%Y-%m-%d").date()
            if start.isoformat() != start_value or end.isoformat() != end_value:
                return None
        except (TypeError, ValueError):
            return None
        if end < start:
            return -1
        count = 0
        cursor = start
        while cursor < end:
            cursor += timedelta(days=1)
            if cursor.weekday() < 5:
                count += 1
        return count

    def _candidate_is_valid(self, actual_date: str, today: str | None = None, valid_until: str | None = None) -> bool:
        """Require an uncorrupted expiry and a verified calendar path."""
        current = today or datetime.now(CHINA_TZ).date().isoformat()
        distance = self._trade_day_distance(actual_date, current)
        if distance is None or distance < 0:
            return False
        if not valid_until:
            return False
        try:
            expiry = datetime.fromisoformat(str(valid_until).replace("Z", "+00:00"))
            if expiry.tzinfo is None:
                return False
            current_date = datetime.strptime(current, "%Y-%m-%d").date()
            if current_date.isoformat() != current:
                return False
            expiry_date = expiry.astimezone(CHINA_TZ).date()
            actual = datetime.strptime(str(actual_date), "%Y-%m-%d").date()
            if actual.isoformat() != str(actual_date) or current_date > expiry_date or expiry_date < actual:
                return False
        except (TypeError, ValueError, OverflowError):
            return False
        states = self.store.calendar_states(actual_date, current)
        if states.get(actual_date) != "open":
            return False
        cursor = datetime.strptime(actual_date, "%Y-%m-%d").date()
        while cursor < current_date:
            cursor += timedelta(days=1)
            if states.get(cursor.isoformat()) not in {"open", "closed"}:
                return False
        verified_distance = sum(1 for value in states.values() if value == "open") - 1
        return 0 <= verified_distance <= self._int("candidate_plan_valid_days", 10, 1, 60)

    async def _candidate_is_valid_async(self, actual_date: str, today: str | None = None, valid_until: str | None = None) -> bool:
        return await self._store_call(self._candidate_is_valid, actual_date, today, valid_until)

    async def _snapshot_context(self, requested_date: str, actual_date: str, quotes: list | None = None) -> dict:
        meta = await self._store_call(self.store.snapshot_meta, actual_date) or {}
        def usable(value) -> str:
            text = str(value or "").strip()
            return "" if text.lower() in {"", "unknown", "未知", "none", "null"} else text
        sources = {usable(getattr(quote, "source", "")) for quote in (quotes or [])}
        sources.discard("")
        source = usable(meta.get("source")) or (next(iter(sources)) if len(sources) == 1 else "")
        quality = usable(meta.get("quality")).lower()
        valid_count = len({str(getattr(quote, "code", "")) for quote in (quotes or []) if str(getattr(quote, "code", "")).isdigit() and float(getattr(quote, "price", 0) or 0) > 0})
        if not source and valid_count:
            # Legacy snapshots did not preserve a provider label. They are still local cached data,
            # but must not be presented as a verified source such as Tushare.
            source, quality = "历史缓存", "cached"
        elif not source:
            source = "未记录"
        if quality not in {"good", "partial", "degraded", "cached"}:
            if source == "tushare" and valid_count >= self._int("daily_snapshot_min_size", 4000, 1000, 10000):
                quality = "good"
            elif source == "eastmoney":
                quality = "degraded"
            elif valid_count:
                quality = "partial"
            else:
                quality = "未记录"
        return {
            "requested_date": requested_date,
            "actual_date": actual_date,
            "source": source,
            "quality": quality,
            "complete": bool(meta.get("complete")) and quality == "good",
            "note": str(meta.get("note") or ""),
        }

    async def _fill_quote_names(self, quotes: list, as_of: str = "") -> int:
        missing = [quote.code for quote in quotes if not getattr(quote, "name", "") or quote.name == quote.code]
        if not missing:
            return 0
        updated = 0
        try:
            updated += await self.quotes.enrich_names(quotes)
        except Exception:
            logger.warning("[%s] Tushare 股票名称补全失败", PLUGIN_NAME)
        still_missing = [quote.code for quote in quotes if not getattr(quote, "name", "") or quote.name == quote.code]
        try:
            cached_names = await self._store_call(self.store.latest_quote_names, still_missing)
        except Exception:
            logger.warning("[%s] 本地股票名称缓存查询失败", PLUGIN_NAME)
            cached_names = {}
        for quote in quotes:
            name = cached_names.get(quote.code)
            if name and (not quote.name or quote.name == quote.code):
                quote.name = name
                updated += 1
        if updated and as_of:
            await self._store_call(
                self.store.save_daily_quotes,
                as_of,
                quotes,
                self._int("daily_cache_keep_days", 180, 7, 730),
            )
        return updated

    @staticmethod
    def _market_label(regime: str) -> str:
        return {"risk_on": "偏强", "strong": "强势", "neutral": "震荡", "weak": "偏弱", "risk_off": "风险偏弱"}.get(str(regime), "未记录")

    def _validate_report_diagnostics(
        self,
        diagnostics: Mapping[str, object] | None,
        *,
        requested_date: str,
        actual_date: str,
        quote_count: int,
        active_raw: Mapping[str, object] | None = None,
    ) -> Mapping[str, object]:
        """Fail closed for market statistics without discarding candidates."""
        values = dict(diagnostics or {})
        reasons: list[str] = []

        def integer(key: str, default: int = -1) -> int:
            try:
                return int(values.get(key, default))
            except (TypeError, ValueError, OverflowError):
                return default

        input_count = integer("input")
        tradable = integer("tradable")
        targets = integer("indicator_targets")
        indicator_total = sum(
            max(0, integer(key, 0))
            for key in (
                "indicator_raw_batch", "indicator_network", "indicator_memory_cache",
                "indicator_persistent_cache", "indicator_failed",
            )
        )
        if input_count != int(quote_count):
            reasons.append("input_mismatch")
        if not (0 <= tradable <= input_count):
            reasons.append("tradable_bounds")
        if not (0 <= targets <= tradable) or indicator_total != targets:
            reasons.append("indicator_counts")
        breadth = math.nan
        try:
            breadth = float(values.get("market_breadth"))
            if not math.isfinite(breadth) or not 0.0 <= breadth <= 1.0:
                reasons.append("breadth_bounds")
        except (TypeError, ValueError, OverflowError):
            reasons.append("breadth_missing")
        requested = self._canonical_screen_date(requested_date)
        actual = self._canonical_screen_date(actual_date)
        if not requested or not actual or actual > requested:
            reasons.append("date_bounds")
        if values.get("diagnostics_requested_date") not in (None, "", requested):
            reasons.append("requested_owner")
        if values.get("diagnostics_actual_date") not in (None, "", actual):
            reasons.append("actual_owner")
        if not str(values.get("diagnostics_invocation_id") or "").strip():
            reasons.append("invocation_owner")
        market_sample = integer("market_sample_size", input_count)
        market_minimum = self._int("market_min_snapshot_size", 4000, 1000, 10000)
        advancing = integer("market_advancing")
        declining = integer("market_declining")
        flat = integer("market_flat")
        if market_sample != input_count or market_sample < market_minimum:
            reasons.append("market_sample")
        if min(advancing, declining, flat) < 0 or advancing + declining + flat != market_sample:
            reasons.append("market_counts")
        elif market_sample and abs(breadth - advancing / market_sample) > 0.00011:
            reasons.append("breadth_counts")
        try:
            median_return = float(values.get("market_median_return"))
            if not math.isfinite(median_return):
                reasons.append("median_return")
        except (TypeError, ValueError, OverflowError):
            reasons.append("median_return")
        raw_batch_id = str(values.get("raw_batch_id") or "")
        raw_generation = integer("raw_generation", 0)
        if raw_batch_id or raw_generation:
            if str(values.get("diagnostics_raw_batch_id") or "") != raw_batch_id:
                reasons.append("raw_batch_owner")
            if integer("diagnostics_raw_generation", 0) != raw_generation:
                reasons.append("raw_generation_owner")
            active = dict(active_raw or {})
            active_id = str(active.get("batch_id") or active.get("active_batch_id") or "")
            try:
                active_generation = int(active.get("generation") or 0)
            except (TypeError, ValueError, OverflowError):
                active_generation = 0
            if not active_id or active_id != raw_batch_id or active_generation != raw_generation:
                reasons.append("raw_generation")
        values["market_stats_confirmed"] = not reasons
        values["market_stats_unconfirmed_reasons"] = tuple(reasons)
        return MappingProxyType(values)

    async def _report_diagnostics_for_send(
        self,
        diagnostics: Mapping[str, object] | None,
        *,
        requested_date: str,
        actual_date: str,
        quote_count: int,
    ) -> Mapping[str, object]:
        values = dict(diagnostics or {})
        active_raw = None
        if values.get("raw_batch_id") or values.get("raw_generation"):
            active_raw_batch = getattr(self.store, "active_raw_batch", None)
            if callable(active_raw_batch):
                active_raw = await self._store_call(
                    active_raw_batch,
                    self._raw_dataset(),
                    as_of=actual_date,
                    max_stale_trading_days=self._raw_stale_days(),
                )
        validated = self._validate_report_diagnostics(
            values,
            requested_date=requested_date,
            actual_date=actual_date,
            quote_count=quote_count,
            active_raw=active_raw,
        )
        if self._bool("market_comparison_enabled", False):
            benchmark = str(self.config.get("market_comparison_benchmark", "") or "").strip().upper()
            comparison = market_comparison_evidence.unknown("market_statistics_unverified")
            if validated.get("market_stats_confirmed"):
                try:
                    comparison = await self._store_call(
                        self.store.market_comparison_report, actual_date, benchmark, str(values.get("raw_batch_id") or ""),
                        as_of=market_comparison_evidence.utc_now(),
                    )
                except (sqlite3.Error, RuntimeError, ValueError, TypeError, AttributeError):
                    comparison = market_comparison_evidence.unknown("comparison_read_unavailable")
            return MappingProxyType({**validated, "market_comparison": comparison})
        return validated

    def _market_report_lines(
        self,
        requested_date: str,
        actual_date: str,
        quotes: list,
        candidates,
        snapshot: dict,
        diagnostics: Mapping[str, object] | None = None,
    ) -> list[str]:
        diagnostics = dict(diagnostics if diagnostics is not None else self._last_screen_diagnostics)
        shown = candidates[:self._int("report_candidate_limit", 10, 1, 30)]
        quality_code = str(snapshot.get("quality"))
        quality = "完整" if snapshot.get("complete") and quality_code == "good" else {"good": "未确认", "partial": "部分", "degraded": "降级", "cached": "缓存"}.get(quality_code, "未记录")
        if diagnostics.get("market_stats_confirmed", True):
            market_line = (
                f"市场：{self._market_label(diagnostics.get('market_regime', 'unknown'))}"
                f"｜涨/跌/平 {int(diagnostics.get('market_advancing', 0) or 0)}/"
                f"{int(diagnostics.get('market_declining', 0) or 0)}/"
                f"{int(diagnostics.get('market_flat', 0) or 0)}"
                f"｜上涨占比 {float(diagnostics.get('market_breadth', 0)):.1%}"
                f"｜中位涨跌 {float(diagnostics.get('market_median_return', 0)):+.2f}%"
            )
            market_line += f"｜候选 {len(candidates)} 只"
        else:
            market_line = f"市场：统计未确认｜候选 {len(candidates)} 只"
        lines = [
            f"全市场选股｜{actual_date}",
            f"行情：{snapshot.get('source', '未记录')} · {quality}｜共 {len(quotes)} 只",
            market_line,
        ]
        if self._bool("market_comparison_enabled", False):
            comparison = diagnostics.get("market_comparison") or market_comparison_evidence.unknown("evidence_missing")
            if not diagnostics.get("market_stats_confirmed"):
                comparison = market_comparison_evidence.unknown("market_statistics_unverified")
            lines.append(market_comparison_evidence.render(
                comparison))
        targets = int(diagnostics.get("indicator_targets", 0) or 0)
        if targets:
            lines.append(
                "指标：raw批次 {raw}｜网络 {network}｜短时缓存 {memory}｜历史缓存 {persistent}｜失败 {failed}".format(
                    raw=diagnostics.get("indicator_raw_batch", 0),
                    network=diagnostics.get("indicator_network", 0),
                    memory=diagnostics.get("indicator_memory_cache", 0),
                    persistent=diagnostics.get("indicator_persistent_cache", 0),
                    failed=diagnostics.get("indicator_failed", 0),
                )
            )
        if actual_date != requested_date:
            lines.append(f"注：请求 {requested_date}，当前使用最近交易日数据。")
        for index, item in enumerate(shown, 1):
            if index > 1:
                lines.append("")
            lines.extend(format_compact_candidate(item, index).splitlines())
        if len(candidates) > len(shown):
            lines.append(f"另有 {len(candidates) - len(shown)} 只候选，发送 /候选池 查看。")
        if not candidates:
            enriched = int(diagnostics.get("enriched", 0) or 0)
            if diagnostics.get("calendar_endpoint_unavailable") or diagnostics.get("failure_kind") == "calendar_endpoint_unavailable":
                lines.append(
                    "交易日历证据不可用：未能确认完整交易日窗口，本次尚未开始 Tushare daily 行情抓取；"
                    "请稍后重试，不要将本次结果解读为“没有候选”。"
                )
            elif diagnostics.get("history_unavailable"):
                lines.append(
                    "历史日线不可用：Tushare raw 批次缺失、过期或校验未通过，不能把本次结果解读为“没有候选”；"
                    "请稍后重试并确认 raw 批次已同步。"
                )
            elif targets and not enriched:
                lines.append(f"指标数据未能补齐：可交易 {diagnostics.get('tradable', 0)} 只，目标 {targets} 只均失败。本次不能解读为“没有候选”，请稍后重试或先执行 /股票同步。")
            elif targets and enriched < targets:
                lines.append(f"暂无达标候选：可交易 {diagnostics.get('tradable', 0)} 只，已补齐 {enriched}/{targets} 只，最高分 {diagnostics.get('max_score', 0)}。结论仅覆盖已补齐指标的股票。")
            else:
                lines.append(f"暂无达标候选：可交易 {diagnostics.get('tradable', 0)} 只，补齐指标 {enriched} 只，最高分 {diagnostics.get('max_score', 0)}。")
        lines.append("仅供研究/模拟盘，价位须结合公告、基本面和自身风险承受复核。")
        return lines

    def _minute_signal_text(self, quote, completed) -> str:
        if not completed or not self._bool("minute_trigger_enabled", False):
            return ""
        if (quote.suspended is None or quote.limit_up is None
                or quote.limit_down is None or quote.st is None):
            return ""
        if (quote.suspended is True or quote.limit_up is True
                or quote.limit_down is True or quote.st is True):
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
            asyncio.create_task(self._intraday_market_loop(), name=f"{PLUGIN_NAME}:market"),
            asyncio.create_task(self._news_loop(), name=f"{PLUGIN_NAME}:news"),
        ]
        logger.info("[%s] 已加载，研究/模拟盘模式=%s", PLUGIN_NAME, self._bool("paper_trading_only", True))

    async def terminate(self):
        if self._terminated:
            return
        self._terminated = True
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
        self._intraday_market_refresh_active = False
        try:
            await self.quotes.close()
            await self.news.close()
            await self.llm.close()
        except Exception:
            logger.debug("[%s] 共享行情 HTTP runtime 关闭时已结束", PLUGIN_NAME)

    def _int(self, key: str, default: int, minimum: int, maximum: int) -> int:
        try:
            return max(minimum, min(int(self.config.get(key, default)), maximum))
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _session_count_from_config(config) -> int:
        """Normalize the exact raw session count at the configuration boundary."""
        settings = config if isinstance(config, dict) else {}
        value = settings.get("tushare_raw_session_count")
        if value in (None, ""):
            value = settings.get("tushare_raw_lookback_days", 120)
        try:
            return max(1, min(int(value), 366))
        except (TypeError, ValueError, OverflowError):
            return 120

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

    def _resolve_stock_query(self, query: str) -> dict:
        """Resolve a command argument through the shared code/name registry."""
        raw = str(query or "").strip()
        codes = parse_codes(raw)
        if len(codes) == 1 and (raw == codes[0] or normalize_code(raw) == codes[0]):
            return {"status": "ok", "query": raw, "code": codes[0], "name": "", "matches": []}
        resolver = getattr(self.store, "resolve_stock_symbol", None) or getattr(self.store, "resolve_stock", None)
        if callable(resolver):
            try:
                return resolver(raw, 10)
            except TypeError:
                return resolver(raw)
        return {"status": "miss", "query": raw, "matches": []}

    @staticmethod
    def _parse_cost(value) -> float | None:
        raw = str(value or "").strip()
        if not raw or isinstance(value, bool) or not re.fullmatch(r"[+]?\d+(?:\.\d*)?(?:[eE][+-]?\d+)?|[+]?\.\d+(?:[eE][+-]?\d+)?", raw):
            return None
        try:
            parsed = float(raw)
        except (TypeError, ValueError, OverflowError):
            return None
        return parsed if math.isfinite(parsed) and parsed > 0 else None

    @staticmethod
    def _ambiguous_stock_text(result: dict) -> str:
        matches = result.get("matches") if isinstance(result, dict) else []
        labels = [f"{item.get('name') or '名称未知'}（{item.get('code') or '未知'}）" for item in matches[:10] if isinstance(item, dict)]
        return "股票名称有歧义，请改用代码或更完整名称：" + "、".join(labels)

    def _tushare_mode(self) -> bool:
        """A configured Tushare token selects the immutable raw-data path."""
        return bool(str(getattr(self.quotes, "tushare_token", "") or "").strip())

    def _raw_dataset(self) -> str:
        return str(getattr(self, "raw_dataset_key", self.config.get("tushare_raw_dataset_key", "tushare_daily"))).strip() or "tushare_daily"

    def _raw_stale_days(self) -> int:
        return int(getattr(self, "raw_max_stale_trading_days", self._int("tushare_raw_max_stale_trading_days", 2, 0, 10)))

    def _raw_chunk(self) -> int:
        return max(1, int(getattr(self, "raw_chunk_size", self._int("tushare_raw_chunk_size", 500, 50, 2000))))

    def _raw_lookback(self) -> int:
        return int(getattr(self, "raw_session_count", getattr(self, "raw_lookback_days", self._int("tushare_raw_lookback_days", 120, 1, 366))))

    @staticmethod
    def _bulk_value(result, key: str, default=None):
        if isinstance(result, dict):
            return result.get(key, default)
        return getattr(result, key, default)

    @classmethod
    def _bulk_result_is_typed(cls, result) -> bool:
        """Require the provider result contract before classifying an outcome."""
        if isinstance(result, dict):
            return all(key in result for key in ("quotes", "trade_date", "complete"))
        return all(hasattr(result, key) for key in ("quotes", "trade_date", "complete"))

    @staticmethod
    def _canonical_screen_date(value: str) -> str | None:
        text = str(value or "").strip()
        if re.fullmatch(r"\d{4}-\d{2}-\d{2}", text):
            digits = text.replace("-", "")
        elif re.fullmatch(r"\d{8}", text):
            digits = text
        else:
            return None
        try:
            return datetime.strptime(digits, "%Y%m%d").date().isoformat()
        except (TypeError, ValueError, OverflowError):
            return None

    def _raw_diagnostics(self, values: dict | None = None) -> dict[str, object]:
        diagnostics = dict(values or {})
        for key in ("network_failed", "history_invalid", "cache_basis_rejected"):
            try:
                diagnostics[key] = int(diagnostics.get(key, 0) or 0)
            except (TypeError, ValueError):
                diagnostics[key] = 0
        return diagnostics

    @staticmethod
    def _diagnostic_flag(value) -> bool:
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)):
            return bool(value)
        return str(value or "").strip().lower() in {"1", "true", "yes", "on", "y", "failed", "unavailable"}

    @classmethod
    def _shadow_only_diagnostics(cls, diagnostics) -> bool:
        values = diagnostics if isinstance(diagnostics, dict) else {}
        return (
            cls._diagnostic_flag(values.get("shadow"))
            or cls._diagnostic_flag(values.get("configured_shadow"))
            or cls._diagnostic_flag(values.get("shadow_only"))
            or str(values.get("data_mode") or "").strip().lower() in {"configured_shadow", "shadow_only"}
            or str(values.get("publication_mode") or "").strip().lower() == "shadow"
        )

    @classmethod
    def _tushare_result_fallback_reason(cls, result) -> str | None:
        """Return an explicit transient reason, never a catch-all fallback."""
        diagnostics = cls._bulk_value(result, "diagnostics", {})
        diagnostics = diagnostics if isinstance(diagnostics, dict) else {}
        # A returned result can carry a cancellation or internal-error marker
        # even when its payload is empty.  Those are not provider transients:
        # treating them as "not published" would silently switch data source.
        if any(cls._diagnostic_flag(diagnostics.get(key)) for key in (
            "cancelled", "canceled", "cancellation", "cancelled_error", "canceled_error",
            "unknown_error", "unexpected_error", "programming_error", "internal_error",
        )):
            return None
        if any(cls._diagnostic_flag(diagnostics.get(key)) for key in ("db_integrity", "storage_integrity", "database_error", "invalid_date", "future_date")):
            return None
        for key, reason in _TUSHARE_FALLBACK_DIAGNOSTICS.items():
            if cls._diagnostic_flag(diagnostics.get(key)):
                return reason
        for value in (diagnostics.get("failure_kind"), cls._bulk_value(result, "failure_kind", None)):
            key = str(value or "").strip().lower()
            if key in _TUSHARE_FALLBACK_FAILURE_KINDS:
                return _TUSHARE_FALLBACK_FAILURE_KINDS[key]
        return None

    @staticmethod
    def _tushare_exception_fallback_reason(exc: BaseException) -> str | None:
        """Classify only known transient Tushare failures for EM fallback."""
        if isinstance(exc, asyncio.CancelledError):
            raise exc
        if isinstance(exc, TushareCircuitOpen):
            return "breaker"
        if isinstance(exc, TushareRateLimitError):
            return "rate_limit"
        if isinstance(exc, TushareNetworkError):
            return "network"
        if isinstance(exc, TushareCalendarError):
            return "calendar"
        if isinstance(exc, TushareNotPublishedError):
            return "not_published"
        if isinstance(exc, TusharePublishError):
            return "publish"
        if isinstance(exc, TushareCoverageError):
            return "coverage"
        if isinstance(exc, TushareHistoryError):
            return "history_invalid"
        if isinstance(exc, (httpx.TimeoutException, asyncio.TimeoutError)):
            return "timeout"
        if isinstance(exc, httpx.HTTPStatusError):
            try:
                if int(exc.response.status_code) == 429:
                    return "rate_limit"
            except (AttributeError, TypeError, ValueError, OverflowError):
                pass
        if isinstance(exc, (httpx.HTTPError, asyncio.TimeoutError, OSError)):
            return "timeout" if isinstance(exc, asyncio.TimeoutError) else "network"
        return None

    @classmethod
    def _tushare_calendar_fallback_reason(cls, exc: BaseException) -> str | None:
        """Allow EM only for failures at the Tushare calendar boundary."""
        if isinstance(exc, TushareCalendarError):
            return None
        if isinstance(exc, TusharePermissionError):
            return "permission"
        if isinstance(exc, TushareProviderUnknownError):
            return "provider"
        if isinstance(exc, TushareNotPublishedError):
            return "provider"
        reason = cls._tushare_exception_fallback_reason(exc)
        return reason if reason in {"breaker", "rate_limit", "network", "timeout"} else None

    @staticmethod
    def _tushare_daily_calendar_fallback_reason(exc: BaseException) -> str | None:
        """Allow EM only for typed transport/provider failures in daily evidence."""
        if isinstance(exc, TushareCircuitOpen):
            return "breaker"
        if isinstance(exc, TushareRateLimitError):
            return "rate_limit"
        if isinstance(exc, TusharePermissionError):
            return "permission"
        if isinstance(exc, TushareProviderUnknownError) or isinstance(exc, TushareNotPublishedError):
            return "provider"
        if isinstance(exc, TushareNetworkError):
            return "network"
        if isinstance(exc, (httpx.TimeoutException, asyncio.TimeoutError)):
            return "timeout"
        if isinstance(exc, httpx.HTTPError) or isinstance(exc, OSError):
            return "network"
        return None

    def _record_tushare_failure_diagnostics(self, exc: BaseException, *, stage: str = "daily") -> dict:
        """Expose typed non-fallback failures without persisting provider text."""
        diagnostics = self._raw_diagnostics(self._last_screen_diagnostics)
        diagnostics["failure_stage"] = str(stage or "daily")[:32]
        if isinstance(exc, TushareCalendarEndpointError):
            diagnostics.update({
                "calendar_endpoint_unavailable": 1,
                "calendar_failure_kind": str(getattr(exc, "category", "network") or "network"),
                "network_failed": 1,
                "failure_kind": "calendar_endpoint_unavailable",
                "fallback_allowed": False,
            })
        elif isinstance(exc, TusharePermissionError):
            diagnostics.update({"permission_denied": 1, "failure_kind": "permission_denied", "fallback_allowed": False})
        elif isinstance(exc, TushareProviderUnknownError):
            diagnostics.update({"provider_unknown": 1, "failure_kind": "provider_unknown", "fallback_allowed": False})
        elif isinstance(exc, TushareCalendarError):
            diagnostics.update({"calendar_unavailable": 1, "failure_kind": "calendar", "fallback_allowed": False})
        elif isinstance(exc, SnapshotLeaseLostError):
            diagnostics.update({"lease_lost": 1, "failure_kind": "lease_lost", "fallback_allowed": False})
        elif isinstance(exc, SnapshotLeaseError):
            diagnostics.update({"lease_error": 1, "failure_kind": "lease_error", "fallback_allowed": False})
        self._last_screen_diagnostics = diagnostics
        return diagnostics

    def _mark_tushare_fallback(self, reason: str, diagnostics: dict | None = None) -> dict:
        values = dict(diagnostics or {})
        values["fallback_allowed"] = True
        values["fallback_reason"] = str(reason or "transient")
        values["data_mode"] = "tushare_transient_failure"
        owner = getattr(self, "_snapshot_fallback_owner", None)
        if owner:
            values["fallback_owner"] = str(owner)
            self._authorized_fallback_owner = str(owner)
        self._last_screen_diagnostics = self._raw_diagnostics(values)
        return self._last_screen_diagnostics

    async def _preview_fallback_needed(self, quotes, as_of: str, diagnostics: dict | None = None) -> bool:
        values = diagnostics if isinstance(diagnostics, dict) else {}
        if self._shadow_only_diagnostics(values) or not self._diagnostic_flag(values.get("fallback_allowed")):
            return False
        quotes = [formal_source_policy.mask_quote_negatives(quote) for quote in (quotes or [])]
        if not quotes:
            return True
        tradable = [quote for quote in quotes if is_screenable(
            quote,
            self._float("price_min", 2, 0.01, 100000),
            self._float("price_max", 80, 0.01, 100000),
        )]
        tradable.sort(key=lambda quote: (float(quote.amount or 0), str(quote.code)), reverse=True)
        enrich_targets = tradable[:self._int("deep_screen_limit", 300, 1, 1000)]
        if not enrich_targets:
            return False
        persistent, _provenance, _history_reason = await self._read_fresh_raw_history_async(
            [quote.code for quote in enrich_targets],
            self._canonical_screen_date(as_of) or as_of,
        )
        if not isinstance(persistent, dict) or not persistent:
            return True
        for quote in enrich_targets:
            bars, _rejection = self._raw_indicator_rows(persistent.get(quote.code, []), expected_code=quote.code)
            if apply_daily_indicators(quote, bars):
                return False
        return True

    def _read_fresh_raw_history(
        self,
        codes,
        as_of: str,
        *,
        max_stale_trading_days: int | None = None,
        latest_only: bool = False,
    ) -> tuple[dict[str, list[dict]], dict, str]:
        """Read one active raw generation, never a legacy history table.

        ``latest_only=True`` limits the read to the batch's ``actual_trade_date``
        (one day, ~5.5k rows) instead of the full ~120-day window.  The daily
        snapshot only needs the latest bar per symbol, so reading every
        historical bar just to discard it blocks the event loop for minutes.
        """
        stale_days = self._raw_stale_days() if max_stale_trading_days is None else max(0, int(max_stale_trading_days))
        raw_history = getattr(self.store, "raw_history", None)
        read_active = getattr(self.store, "read_active_raw_bars", None)
        active_lookup = getattr(self.store, "active_raw_batch", None)
        active = None
        if callable(active_lookup):
            try:
                active = active_lookup(
                    self._raw_dataset(),
                    as_of=as_of,
                    max_stale_trading_days=stale_days,
                )
            except TypeError:
                try:
                    active = active_lookup(self._raw_dataset(), as_of=as_of)
                except Exception:
                    active = None
            except Exception:
                active = None
        if active and active.get("fresh") is False:
            basis = str(active.get("basis") or active.get("dataset_basis") or "").strip().lower()
            return {}, active, "cache_basis_rejected" if basis and basis != "unadjusted" else "stale"
        after = ""
        if latest_only and active and str(active.get("actual_trade_date") or ""):
            try:
                after = (datetime.strptime(str(active["actual_trade_date"]), "%Y-%m-%d").date() - timedelta(days=1)).isoformat()
            except (TypeError, ValueError):
                after = ""
        try:
            if callable(raw_history):
                try:
                    if after:
                        value = raw_history(
                            codes,
                            as_of=as_of,
                            dataset_key=self._raw_dataset(),
                            max_stale_trading_days=stale_days,
                            after=after,
                        )
                    else:
                        value = raw_history(
                            codes,
                            as_of=as_of,
                            dataset_key=self._raw_dataset(),
                            max_stale_trading_days=stale_days,
                        )
                except TypeError:
                    value = raw_history(codes, as_of=as_of)
                if isinstance(value, tuple) and len(value) == 2:
                    bars, provenance = value
                else:
                    bars, provenance = value, {}
            elif callable(read_active):
                try:
                    if after:
                        bars = read_active(
                            codes,
                            as_of=as_of,
                            dataset_key=self._raw_dataset(),
                            max_stale_trading_days=stale_days,
                            after=after,
                        )
                    else:
                        bars = read_active(
                            codes,
                            as_of=as_of,
                            dataset_key=self._raw_dataset(),
                            max_stale_trading_days=stale_days,
                        )
                except TypeError:
                    bars = read_active(codes, as_of=as_of)
                provenance = {}
            else:
                return {}, active or {}, "unavailable"
        except (RuntimeError, ValueError, TypeError, KeyError):
            return {}, active or {}, "invalid"
        if not isinstance(bars, dict):
            return {}, provenance if isinstance(provenance, dict) else {}, "unavailable"
        factor_reader = getattr(self.store, "corporate_action_factors", None)
        if callable(factor_reader) and bars:
            factor_dates = {
                str(row.get("trade_date") or "")
                for rows in bars.values()
                for row in rows or []
                if isinstance(row, dict)
            }
            try:
                factors = factor_reader(bars.keys(), factor_dates)
            except (RuntimeError, ValueError, TypeError, KeyError):
                factors = {}
            if isinstance(factors, dict):
                for code, rows in bars.items():
                    for row in rows or []:
                        if not isinstance(row, dict):
                            continue
                        factor = factors.get(f"{normalize_code(str(code))}:{row.get('trade_date')}")
                        if factor and factor_capture(factor, normalize_code(str(code)), row.get("trade_date"),
                                                     self._factor_evidence_cutoff(str(row.get("trade_date") or ""))):
                            row["corporate_action_factor"] = factor.get("adj_factor")
                            row["corporate_action_evidence"] = factor.get("evidence")
                            row["corporate_action_observed_at"] = factor.get("observed_at")
                            row["corporate_action_response_sha256"] = factor.get("response_sha256")
        provenance = provenance if isinstance(provenance, dict) else {}
        if active:
            provenance = {**active, **provenance}
        provenance_basis = str(provenance.get("basis") or provenance.get("price_basis") or "").strip().lower()
        provenance_source = str(provenance.get("source") or "").strip().lower()
        if provenance_basis and provenance_basis != "unadjusted":
            return {}, provenance, "cache_basis_rejected"
        if provenance_source and provenance_source != "tushare":
            return {}, provenance, "invalid"
        if not bars:
            return {}, provenance, "unavailable"
        return bars, provenance, "ok"

    @staticmethod
    def _raw_indicator_rows(rows, *, expected_code: str = "") -> tuple[list[dict], str | None]:
        """Adapt raw rows to the indicator API while enforcing provenance."""
        expected = str(expected_code or "").strip().lower()
        if "." in expected:
            expected = expected.split(".", 1)[0]
        expected = normalize_code(expected)
        if expected and not re.fullmatch(r"\d{6}", expected):
            return [], "invalid"
        normalized: list[dict] = []
        for row in rows or []:
            if not isinstance(row, dict):
                return [], "invalid"
            basis = str(row.get("price_basis") or row.get("basis") or "").strip().lower()
            source = str(row.get("source") or "").strip().lower()
            if basis != "unadjusted":
                return [], "basis"
            if source != "tushare":
                return [], "source"
            trade_date = Main._canonical_screen_date(row.get("trade_date"))
            raw_code = str(row.get("code") or row.get("ts_code") or "").strip().lower()
            if "." in raw_code:
                raw_code = raw_code.split(".", 1)[0]
            code = normalize_code(raw_code)
            if not trade_date or not re.fullmatch(r"\d{6}", code) or (expected and code != expected):
                return [], "invalid"
            try:
                numeric = {
                    "open": float(row.get("open")),
                    "high": float(row.get("high")),
                    "low": float(row.get("low")),
                    "close": float(row.get("close")),
                    "pre_close": float(row.get("pre_close") if row.get("pre_close") is not None else row.get("prev_close")),
                    "pct_change": float(row.get("pct_change") if row.get("pct_change") is not None else row.get("pct_chg")),
                    "volume": float(row.get("volume") if row.get("volume") is not None else row.get("vol") or 0),
                    "amount": float(row.get("amount") or 0),
                }
            except (TypeError, ValueError, OverflowError):
                return [], "invalid"
            if (
                not all(math.isfinite(value) for value in numeric.values())
                or any(numeric[key] <= 0 for key in ("open", "high", "low", "close", "pre_close"))
                or numeric["volume"] < 0
                or numeric["amount"] < 0
                or numeric["high"] < max(numeric["open"], numeric["close"])
                or numeric["low"] > min(numeric["open"], numeric["close"])
                or numeric["high"] < numeric["low"]
                or abs(numeric["pct_change"] - (numeric["close"] / numeric["pre_close"] - 1) * 100) > 0.35
            ):
                return [], "invalid"
            item = dict(row)
            item["trade_date"] = trade_date
            item["code"] = code
            item["price_basis"] = "unadjusted"
            item["source"] = "tushare"
            normalized.append(item)
        return normalized, None

    @staticmethod
    def _quotes_from_raw_bars(bars: dict[str, list[dict]], requested_date: str = "") -> tuple[list, str | None]:
        requested = Main._canonical_screen_date(requested_date) if requested_date else ""
        latest_by_code: dict[str, dict] = {}
        for raw_code, rows in (bars or {}).items():
            for row in rows or []:
                if not isinstance(row, dict):
                    continue
                cleaned, rejection = Main._raw_indicator_rows([row], expected_code=raw_code)
                if rejection or not cleaned:
                    continue
                row = cleaned[0]
                trade_date = Main._canonical_screen_date(row.get("trade_date"))
                code = normalize_code(str(row.get("code") or ""))
                if not re.fullmatch(r"\d{6}", code):
                    continue
                if not trade_date or not code or (requested and trade_date > requested):
                    continue
                current = latest_by_code.get(code)
                current_date = Main._canonical_screen_date(current.get("trade_date")) if current else None
                if current is None or (current_date and current_date < trade_date):
                    latest_by_code[code] = row
        if not latest_by_code:
            return [], None
        actual_date = max(Main._canonical_screen_date(row.get("trade_date")) for row in latest_by_code.values())
        now = datetime.now(CHINA_TZ)
        quotes = []
        for code, row in sorted(latest_by_code.items()):
            if Main._canonical_screen_date(row.get("trade_date")) != actual_date:
                continue
            try:
                quote = Quote(
                    code,
                    str(row.get("name") or code),
                    float(row.get("close")),
                    float(row.get("pre_close") or row.get("prev_close") or 0),
                    float(row.get("amount") or 0),
                    float(row.get("pct_change") if row.get("pct_change") is not None else row.get("pct_chg") or 0),
                    float(row.get("volume") or row.get("vol") or 0),
                    source="tushare",
                    provider_ts=now,
                    fetched_at=now,
                    indicator_last_date=actual_date,
                    indicator_last_close=float(row.get("close")),
                    indicator_price_basis="unadjusted",
                    indicator_source="tushare",
                    corporate_action_factor=row.get("corporate_action_factor"),
                    corporate_action_evidence=str(row.get("corporate_action_evidence") or ""),
                    corporate_action_observed_at=str(row.get("corporate_action_observed_at") or ""),
                    corporate_action_response_sha256=str(row.get("corporate_action_response_sha256") or ""),
                )
            except (TypeError, ValueError, OverflowError):
                continue
            quotes.append(quote)
        return quotes, actual_date

    def _fresh_raw_snapshot(
        self,
        requested_date: str,
        *,
        cache_as_of: str | None = None,
        max_stale_trading_days: int | None = None,
    ) -> tuple[list, str | None, dict, str]:
        as_of = self._canonical_screen_date(cache_as_of or requested_date) or requested_date
        bars, provenance, state = self._read_fresh_raw_history(
            None,
            as_of,
            max_stale_trading_days=max_stale_trading_days,
            latest_only=True,
        )
        if state != "ok":
            return [], None, provenance, state
        quotes, actual_date = self._quotes_from_raw_bars(bars, as_of)
        return quotes, actual_date, provenance, "ok" if quotes else "unavailable"

    @staticmethod
    def _calendar_helper_call_form(helper, requested_date: str, session_count: int) -> tuple[tuple, dict]:
        """Select one compatible calendar call without probing by execution."""
        try:
            signature = inspect.signature(helper)
        except (TypeError, ValueError):
            # Some extension callables do not expose a signature.  A single
            # positional call is the only safe fallback: retrying after a
            # TypeError could execute an internal failure more than once.
            return (requested_date,), {}

        candidates = (
            ((requested_date,), {"session_count": session_count}),
            ((requested_date,), {"lookback_days": session_count}),
            ((requested_date,), {}),
        )
        for args, kwargs in candidates:
            try:
                signature.bind(*args, **kwargs)
            except TypeError:
                continue
            return args, kwargs
        # Preserve the callable's own missing-argument TypeError while still
        # making exactly one invocation.
        return (requested_date,), {}

    @staticmethod
    def _compatible_kwargs(helper, args: tuple, values: dict) -> dict:
        """Select optional keyword arguments without probing execution."""
        try:
            signature = inspect.signature(helper)
        except (TypeError, ValueError):
            return dict(values)
        selected: dict = {}
        for key, value in values.items():
            candidate = {**selected, key: value}
            try:
                signature.bind(*args, **candidate)
            except TypeError:
                continue
            selected = candidate
        return selected

    @staticmethod
    def _snapshot_lease_capability(store) -> dict:
        """Select the durable lease API before invoking any of its methods.

        A store with no lease surface at all is the explicit compatibility
        case for old test/integration adapters.  A partial surface, an
        uninspectable callable, or a missing atomic result writer is instead
        an unsafe durable configuration and must fail closed.
        """
        required_names = (
            "claim_snapshot_lease",
            "snapshot_lease_state",
            "renew_snapshot_lease",
            "release_snapshot_lease",
            "save_snapshot_request_owned",
        )
        atomic_names = (
            "save_tushare_snapshot_owned",
            "finalize_snapshot_request_owned",
            "persist_tushare_snapshot_owned",
            "save_snapshot_result_owned",
            "persist_snapshot_result_owned",
        )
        methods: dict[str, object] = {}
        present = False
        for name in (*required_names, *atomic_names):
            try:
                value = getattr(store, name, None)
            except Exception as exc:
                return {
                    "supported": False,
                    "legacy": False,
                    "reason": "uninspectable",
                    "error_type": type(exc).__name__,
                }
            if value is not None:
                present = True
            if callable(value):
                methods[name] = value
        if not present:
            return {"supported": False, "legacy": True, "reason": "absent"}

        missing = [name for name in required_names if name not in methods]
        atomic_name = next((name for name in atomic_names if name in methods), None)
        if missing or atomic_name is None:
            return {
                "supported": False,
                "legacy": False,
                "reason": "incomplete",
                "missing": [*missing, *([] if atomic_name else ["atomic_snapshot_result"])],
            }

        probes = (
            ("claim", methods["claim_snapshot_lease"], ("daily_snapshot:2026-08-28", "2026-08-28", "owner"), {"ttl_seconds": 30}),
            ("state", methods["snapshot_lease_state"], ("daily_snapshot:2026-08-28",), {}),
            ("renew", methods["renew_snapshot_lease"], ("daily_snapshot:2026-08-28", "owner", 1), {"ttl_seconds": 30}),
            ("release", methods["release_snapshot_lease"], ("daily_snapshot:2026-08-28", "owner", 1), {}),
            (
                "owned_save",
                methods["save_snapshot_request_owned"],
                ("daily_snapshot:2026-08-28", "2026-08-28", "owner", 1),
                {"state": "fetching", "attempts": 1, "source": "tushare", "quality": "unknown", "failure_kind": "", "calendar_evidence": {}, "provenance": {}},
            ),
            (
                "atomic",
                methods[atomic_name],
                ("daily_snapshot:2026-08-28", "2026-08-28", "owner", 1),
                {"quotes": [], "actual_trade_date": "2026-08-28", "source": "tushare", "quality": "good", "complete": True, "state": "complete", "attempts": 1, "terminal": True},
            ),
        )
        for label, method, args, kwargs in probes:
            try:
                signature = inspect.signature(method)
            except (TypeError, ValueError) as exc:
                return {
                    "supported": False,
                    "legacy": False,
                    "reason": "signature_unavailable",
                    "method": label,
                    "error_type": type(exc).__name__,
                }
            try:
                signature.bind(*args, **kwargs)
            except TypeError:
                return {
                    "supported": False,
                    "legacy": False,
                    "reason": "signature_incompatible",
                    "method": label,
                }
        return {
            "supported": True,
            "legacy": False,
            "reason": "supported",
            "claim": methods["claim_snapshot_lease"],
            "state": methods["snapshot_lease_state"],
            "renew": methods["renew_snapshot_lease"],
            "release": methods["release_snapshot_lease"],
            "owned_save": methods["save_snapshot_request_owned"],
            "atomic": methods[atomic_name],
            "atomic_name": atomic_name,
        }

    @staticmethod
    def _calendar_provider_identity(helper) -> int:
        target = getattr(helper, "__self__", None)
        gateway = getattr(target, "gateway", None)
        return id(gateway or target or helper)

    @classmethod
    async def _calendar_singleflight(cls, key: tuple, operation) :
        """Share one calendar operation while keeping cancellation isolated."""
        loop = asyncio.get_running_loop()
        inflight_key = (*key, loop)
        entry = cls._calendar_inflight.get(inflight_key)
        if entry is not None:
            future = entry.get("future")
            if entry.get("loop") is loop and isinstance(future, asyncio.Future) and not future.done():
                return await asyncio.shield(future)
            if cls._calendar_inflight.get(inflight_key) is entry:
                cls._calendar_inflight.pop(inflight_key, None)

        future = loop.create_future()
        entry = {"loop": loop, "future": future}
        cls._calendar_inflight[inflight_key] = entry

        async def perform() -> None:
            try:
                result = await operation()
                if not future.done():
                    future.set_result(result)
            except BaseException as exc:
                if not future.done():
                    future.set_exception(exc)
                    future.add_done_callback(lambda item: item.exception() if not item.cancelled() else None)
            finally:
                if cls._calendar_inflight.get(inflight_key) is entry:
                    cls._calendar_inflight.pop(inflight_key, None)

        try:
            entry["task"] = asyncio.create_task(perform(), name="astrbot_stock_watch:calendar")
        except BaseException:
            if cls._calendar_inflight.get(inflight_key) is entry:
                cls._calendar_inflight.pop(inflight_key, None)
            raise
        return await asyncio.shield(future)

    async def _resolve_tushare_completed_session(self, requested_date: str) -> tuple[str, dict]:
        """Resolve the date through Tushare, daily-symbol consensus, then EM."""
        helper = getattr(self.quotes, "fetch_completed_trade_dates", None)
        if not callable(helper):
            bulk_provider = getattr(self.quotes, "bulk_provider", None)
            helper = getattr(bulk_provider, "fetch_completed_trade_dates", None)
        daily_symbol_helper = getattr(self.quotes, "fetch_daily_symbol_calendar_evidence", None)
        session_count = int(getattr(self, "raw_session_count", 120))
        cutoff = completed_session_cutoff(requested_date)

        def normalize_dates(values, *, source: str, require_exact: bool = False) -> list[str]:
            if isinstance(values, str):
                values = [values]
            if not isinstance(values, (list, tuple)) or not values:
                raise TushareCalendarError(f"{source} completed-session calendar returned no dates")
            normalized: list[str] = []
            seen: set[str] = set()
            for value in values:
                target_date = self._canonical_screen_date(value)
                if not target_date or target_date in seen or target_date > cutoff or target_date > requested_date:
                    raise TushareCalendarError(f"{source} completed-session calendar returned an invalid date")
                seen.add(target_date)
                normalized.append(target_date)
            if normalized != sorted(normalized, reverse=True):
                raise TushareCalendarError(f"{source} completed-session calendar returned unordered dates")
            if require_exact and len(normalized) < session_count:
                raise TushareCalendarError(
                    f"{source} completed-session calendar returned only {len(normalized)} sessions; {session_count} required",
                    available_dates=normalized,
                    required_sessions=session_count,
                )
            return normalized[:session_count]

        def evidence(dates: list[str], *, source: str, policy: str, fallback_reason: str | None, details: dict | None = None) -> dict:
            result = {
                "calendar_resolved": True,
                "calendar_target_date": dates[0],
                "calendar_dates": dates,
                "calendar_session_count": len(dates),
                "calendar_source": source,
                "calendar_policy": policy,
                "calendar_cutoff_date": cutoff,
                "calendar_fallback_reason": fallback_reason,
                "requested_date": requested_date,
                # Keep the evidence self-describing for durable JSON readers;
                # prefixed keys above remain the provider compatibility shape.
                "target": dates[0],
                "dates": dates,
                "session_count": len(dates),
                "source": source,
                "policy": policy,
                "cutoff": cutoff,
                "fallback_reason": fallback_reason,
            }
            if source == TUSHARE_DAILY_CALENDAR_SOURCE and policy == TUSHARE_DAILY_CALENDAR_RULE:
                result.update({
                    "evidence_version": TUSHARE_DAILY_CALENDAR_EVIDENCE_VERSION,
                    "policy_version": TUSHARE_DAILY_CALENDAR_POLICY_VERSION,
                    "provider_policy_fingerprint": tushare_daily_calendar_policy_fingerprint(session_count),
                })
            if details:
                result.update(details)
            return result

        def normalize_daily_symbol_evidence(value: dict, fallback_reason: str) -> dict:
            if not isinstance(value, dict):
                raise ValueError("Tushare daily calendar evidence is not an object")
            source = str(value.get("source") or "").strip()
            calendar_source = str(value.get("calendar_source") or "").strip()
            policy = str(value.get("policy") or "").strip()
            calendar_policy = str(value.get("calendar_policy") or "").strip()
            if (
                source != TUSHARE_DAILY_CALENDAR_SOURCE
                or calendar_source != TUSHARE_DAILY_CALENDAR_SOURCE
                or policy != TUSHARE_DAILY_CALENDAR_RULE
                or calendar_policy != TUSHARE_DAILY_CALENDAR_RULE
            ):
                raise TushareCalendarError("Tushare daily calendar evidence identity is invalid")
            if int(value.get("evidence_version", -1)) != TUSHARE_DAILY_CALENDAR_EVIDENCE_VERSION:
                raise TushareCalendarError("Tushare daily calendar evidence version is invalid")
            if str(value.get("policy_version") or "") != TUSHARE_DAILY_CALENDAR_POLICY_VERSION:
                raise TushareCalendarError("Tushare daily calendar policy version is invalid")
            if str(value.get("requested_date") or "") != requested_date:
                raise TushareCalendarError("Tushare daily calendar requested date is invalid")
            if str(value.get("provider_policy_fingerprint") or "") != tushare_daily_calendar_policy_fingerprint(session_count):
                raise TushareCalendarError("Tushare daily calendar provider policy fingerprint is invalid")
            if value.get("calendar_resolved") is not True:
                raise TushareCalendarError("Tushare daily calendar evidence is unresolved")
            symbols = value.get("symbols")
            if list(symbols or []) != list(TUSHARE_DAILY_CALENDAR_SYMBOLS):
                raise TushareCalendarError("Tushare daily calendar evidence symbols are invalid")
            if str(value.get("consensus_rule") or "") != TUSHARE_DAILY_CALENDAR_RULE:
                raise TushareCalendarError("Tushare daily calendar consensus rule is invalid")
            if str(value.get("calendar_cutoff_date") or "") != cutoff or str(value.get("cutoff") or "") != cutoff:
                raise TushareCalendarError("Tushare daily calendar cutoff is invalid")
            try:
                if (
                    int(value.get("calendar_session_count")) != session_count
                    or int(value.get("session_count")) != session_count
                    or int(value.get("consensus_count")) != session_count
                ):
                    raise ValueError
            except (TypeError, ValueError, OverflowError) as exc:
                raise TushareCalendarError("Tushare daily calendar session count is invalid") from exc
            dates = normalize_dates(
                value.get("calendar_dates") or value.get("dates"),
                source="Tushare daily-symbol consensus",
                require_exact=True,
            )
            if len(dates) != session_count:
                raise TushareCalendarError("Tushare daily calendar consensus is not exact")
            if list(value.get("calendar_dates") or []) != dates or list(value.get("dates") or []) != dates:
                raise TushareCalendarError("Tushare daily calendar date aliases disagree")
            if str(value.get("calendar_target_date") or "") != dates[0] or str(value.get("target") or "") != dates[0]:
                raise TushareCalendarError("Tushare daily calendar target aliases disagree")
            per_symbol = value.get("per_symbol")
            if not isinstance(per_symbol, list) or len(per_symbol) != len(TUSHARE_DAILY_CALENDAR_SYMBOLS):
                raise TushareCalendarError("Tushare daily calendar per-symbol evidence is incomplete")
            normalized_details = []
            for expected_symbol, detail in zip(TUSHARE_DAILY_CALENDAR_SYMBOLS, per_symbol):
                if not isinstance(detail, dict) or str(detail.get("ts_code") or "").upper() != expected_symbol:
                    raise TushareCalendarError("Tushare daily calendar per-symbol identity is invalid")
                try:
                    row_count = int(detail.get("row_count"))
                    detail_sessions = int(detail.get("session_count"))
                except (TypeError, ValueError, OverflowError) as exc:
                    raise TushareCalendarError("Tushare daily calendar per-symbol counts are invalid") from exc
                digest = str(detail.get("digest") or "").strip().lower()
                detail_dates = normalize_dates(
                    detail.get("dates"),
                    source=f"Tushare daily calendar {expected_symbol}",
                    require_exact=True,
                )
                if (
                    row_count < session_count
                    or detail_sessions != session_count
                    or len(detail_dates) != session_count
                    or not re.fullmatch(r"[0-9a-f]{64}", digest)
                    or digest != tushare_daily_calendar_digest(detail_dates)
                    or detail_dates[0] != dates[0]
                ):
                    raise TushareCalendarError("Tushare daily calendar per-symbol evidence is invalid")
                normalized_details.append({
                    "ts_code": expected_symbol,
                    "row_count": row_count,
                    "session_count": detail_sessions,
                    "dates": detail_dates,
                    "min_date": self._canonical_screen_date(detail.get("min_date") or ""),
                    "max_date": self._canonical_screen_date(detail.get("max_date") or ""),
                    "digest": digest,
                })
                if (
                    not normalized_details[-1]["min_date"]
                    or not normalized_details[-1]["max_date"]
                    or normalized_details[-1]["min_date"] != detail_dates[-1]
                    or normalized_details[-1]["max_date"] != detail_dates[0]
                ):
                    raise TushareCalendarError("Tushare daily calendar per-symbol dates are invalid")
            support_counts: dict[str, int] = {}
            for detail in normalized_details:
                for detail_date in set(detail["dates"]):
                    support_counts[detail_date] = support_counts.get(detail_date, 0) + 1
            derived_dates = normalize_dates(
                sorted((value for value, count in support_counts.items() if count >= 2), reverse=True),
                source="Tushare daily-symbol consensus",
                require_exact=True,
            )
            window_start = self._canonical_screen_date(value.get("window_start") or "")
            window_end = self._canonical_screen_date(value.get("window_end") or "")
            if not window_start or window_end != cutoff or window_start > cutoff:
                raise TushareCalendarError("Tushare daily calendar request window is invalid")
            if any(target_date < window_start for target_date in derived_dates):
                raise TushareCalendarError("Tushare daily calendar consensus is outside its request window")
            if derived_dates != dates:
                raise TushareCalendarError("Tushare daily calendar consensus does not match per-symbol support")
            dates_digest = str(value.get("dates_digest") or "").strip().lower()
            if not re.fullmatch(r"[0-9a-f]{64}", dates_digest) or dates_digest != tushare_daily_calendar_digest(dates):
                raise TushareCalendarError("Tushare daily calendar consensus digest is invalid")
            return evidence(
                dates,
                source=TUSHARE_DAILY_CALENDAR_SOURCE,
                policy=TUSHARE_DAILY_CALENDAR_RULE,
                fallback_reason=fallback_reason,
                details={
                    "symbols": list(TUSHARE_DAILY_CALENDAR_SYMBOLS),
                    "per_symbol": normalized_details,
                    "consensus_rule": TUSHARE_DAILY_CALENDAR_RULE,
                    "consensus_count": len(dates),
                    "window_start": window_start,
                    "window_end": window_end,
                    "dates_digest": dates_digest,
                },
            )

        # A completed daily-symbol consensus is immutable evidence for this
        # exact request window.  Validate it before touching any online
        # calendar endpoint, and return the persisted object unchanged so the
        # bulk provider receives the same evidence that was recorded.
        request_id = f"daily_snapshot:{requested_date}"
        try:
            persisted_request = await self._store_call(self.store.snapshot_request, request_id)
            persisted_json = persisted_request.get("calendar_evidence_json") if isinstance(persisted_request, dict) else None
            if isinstance(persisted_json, dict):
                persisted_value = persisted_json
            elif isinstance(persisted_json, str) and persisted_json.strip():
                persisted_value = json.loads(persisted_json)
            else:
                persisted_value = None
            if isinstance(persisted_value, dict) and self._calendar_evidence_final(persisted_value, requested_date):
                persisted_source = str(persisted_value.get("source") or "").strip()
                if persisted_source == TUSHARE_DAILY_CALENDAR_SOURCE:
                    persisted_target = normalize_daily_symbol_evidence(persisted_value, str(persisted_value.get("fallback_reason") or ""))
                    return persisted_target["calendar_target_date"], persisted_value
                # Reuse only evidence that still covers the current cutoff.
                target = self._canonical_screen_date(persisted_value.get("calendar_target_date") or persisted_value.get("target"))
                dates = [self._canonical_screen_date(value) for value in (persisted_value.get("calendar_dates") or persisted_value.get("dates") or [])]
                if (
                    target
                    and target <= cutoff
                    and target <= requested_date
                    and dates
                    and dates[0] == target
                ):
                    return target, persisted_value
        except asyncio.CancelledError:
            raise
        except (TypeError, ValueError, KeyError, TushareCalendarError, OSError):
            # Corrupt, legacy, or policy-mismatched evidence is not a usable
            # cache hit.  The current online resolution path remains active.
            pass

        if callable(helper):
            args, kwargs = self._calendar_helper_call_form(
                helper,
                requested_date,
                session_count,
            )

            async def resolve():
                dates = await helper(*args, **kwargs)
                normalized = normalize_dates(dates, source="Tushare")
                return normalized[0], evidence(
                    normalized,
                    source="tushare",
                    policy="sse_fallback",
                    fallback_reason=None,
                )

            key = (
                self._calendar_provider_identity(helper),
                "completed_sessions",
                requested_date,
                session_count,
            )
            try:
                return await self._calendar_singleflight(key, resolve)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reason = self._tushare_calendar_fallback_reason(exc)
                if reason is None:
                    self._record_tushare_failure_diagnostics(exc, stage="calendar")
                    raise
                daily_reason = reason
                if callable(daily_symbol_helper):
                    daily_args, daily_kwargs = self._calendar_helper_call_form(
                        daily_symbol_helper,
                        requested_date,
                        session_count,
                    )
                    try:
                        daily_value = await daily_symbol_helper(*daily_args, **daily_kwargs)
                        daily_calendar = normalize_daily_symbol_evidence(daily_value, reason)
                        save_calendar = getattr(self.store, "save_calendar", None)
                        if not callable(save_calendar):
                            raise RuntimeError("calendar evidence storage is unavailable")
                        ttl_seconds = self._int("calendar_ttl_seconds", 86400, 60, 604800)
                        for calendar_date in daily_calendar["calendar_dates"]:
                            await self._store_call(
                                save_calendar,
                                calendar_date,
                                True,
                                TUSHARE_DAILY_CALENDAR_SOURCE,
                                ttl_seconds,
                            )
                        return daily_calendar["calendar_target_date"], daily_calendar
                    except asyncio.CancelledError:
                        raise
                    except Exception as daily_exc:
                        daily_reason = self._tushare_daily_calendar_fallback_reason(daily_exc)
                        if daily_reason is None:
                            self._record_tushare_failure_diagnostics(daily_exc, stage="calendar_daily_symbol")
                            raise
                eastmoney_helper = getattr(self.quotes, "fetch_eastmoney_completed_trade_dates", None)
                if not callable(eastmoney_helper):
                    # Compatibility doubles without the new calendar surface
                    # retain the old caller-level handling of the Tushare error.
                    raise
                em_args, em_kwargs = self._calendar_helper_call_form(
                    eastmoney_helper,
                    requested_date,
                    session_count,
                )
                try:
                    em_dates = await eastmoney_helper(*em_args, **em_kwargs)
                    normalized = normalize_dates(em_dates, source="Eastmoney SSE index", require_exact=True)
                except asyncio.CancelledError:
                    raise
                except Exception as em_exc:
                    # An invalid or unavailable EM calendar cannot authorize
                    # an EM price preview; without verified dates raw loading
                    # must stop at the calendar boundary.
                    self._record_tushare_failure_diagnostics(em_exc, stage="calendar")
                    self._last_screen_diagnostics["fallback_allowed"] = False
                    if isinstance(em_exc, TushareCalendarEndpointError):
                        raise
                    raise ValueError("Eastmoney SSE index calendar evidence is unavailable") from em_exc
                save_calendar = getattr(self.store, "save_calendar", None)
                if not callable(save_calendar):
                    raise RuntimeError("calendar evidence storage is unavailable")
                ttl_seconds = self._int("calendar_ttl_seconds", 86400, 60, 604800)
                for calendar_date in normalized:
                    await self._store_call(
                        save_calendar,
                        calendar_date,
                        True,
                        "eastmoney_sse_index",
                        ttl_seconds,
                    )
                return normalized[0], evidence(
                    normalized,
                    source="eastmoney_sse_index",
                    policy="eastmoney_sse_index",
                    fallback_reason=daily_reason,
                    details={
                        "calendar_preceding_failure": "trade_cal",
                        "calendar_trade_cal_failure_reason": reason,
                        "calendar_daily_symbol_failure_reason": daily_reason if callable(daily_symbol_helper) else None,
                    },
                )

        # Older test/integration doubles may not expose the calendar helper.
        # Weekend arithmetic is an explicit compatibility fallback; a weekday
        # remains the requested target so it cannot silently consume stale data.
        requested = datetime.strptime(requested_date, "%Y-%m-%d").date()
        if requested.weekday() >= 5:
            target = requested - timedelta(days=requested.weekday() - 4)
            return target.isoformat(), {"calendar_resolved": False, "calendar_fallback": "weekend", "calendar_cutoff_date": cutoff}
        return requested_date, {"calendar_resolved": False, "calendar_fallback": "provider_unavailable", "calendar_cutoff_date": cutoff}

    @staticmethod
    def _calendar_evidence_final(evidence: dict, requested_date: str) -> bool:
        cutoff = completed_session_cutoff(requested_date)
        if (evidence.get("calendar_cutoff_date") or evidence.get("cutoff")) != cutoff:
            return False
        target = evidence.get("calendar_target_date") or evidence.get("target")
        # Published price bars prove an open session, not that a later date
        # was closed. Only the exchange calendar can establish that absence.
        return bool(target and (
            target == cutoff
            or (target < cutoff and evidence.get("source") == "tushare"
                and evidence.get("policy") == "sse_fallback")
        ))

    async def _daily_snapshot_tushare(self, trade_date: str) -> tuple[list, bool, str]:
        """Coordinate one durable owner and keep waiters provider-free."""
        canonical_trade_date = self._canonical_screen_date(trade_date)
        today = datetime.now(CHINA_TZ).date().isoformat()
        if not canonical_trade_date or canonical_trade_date > today:
            self._last_screen_diagnostics = {
                "invalid_date": True,
                "future_date": bool(canonical_trade_date and canonical_trade_date > today),
                "fallback_allowed": False,
            }
            raise ValueError("Tushare snapshot requested date is invalid or in the future")
        trade_date = canonical_trade_date
        request_id = f"daily_snapshot:{trade_date}"
        lease_capability = self._snapshot_lease_capability(self.store)
        if not lease_capability.get("supported") and not lease_capability.get("legacy"):
            detail = str(lease_capability.get("reason") or "unsupported")
            raise SnapshotLeaseCapabilityError(f"durable snapshot lease capability is unsafe: {detail}")
        request = await self._store_call(self.store.snapshot_request, request_id) or {}
        gate, retry_at = self._snapshot_request_gate(request)
        if gate == "terminal" and request.get("actual_trade_date") != trade_date:
            try:
                evidence = request.get("calendar_evidence_json") or {}
                if isinstance(evidence, str):
                    evidence = json.loads(evidence)
                final = isinstance(evidence, dict) and self._calendar_evidence_final(evidence, trade_date)
            except (TypeError, ValueError):
                final = False
            reopen = getattr(self.store, "reopen_snapshot_request", None)
            if not final and callable(reopen):
                await self._store_call(reopen, request_id, expected_updated_at=request.get("updated_at"))
                request = await self._store_call(self.store.snapshot_request, request_id) or {}
                gate, retry_at = self._snapshot_request_gate(request)
        if gate != "allow":
            self._daily_retry_after = retry_at or (datetime.now(CHINA_TZ) + timedelta(minutes=5))
            return await self._daily_snapshot_tushare_body(trade_date, allow_network=False)

        if lease_capability.get("legacy"):
            # Keep older storage shims usable; the in-process lock remains the
            # only coordination mechanism on those integrations.
            return await self._daily_snapshot_tushare_body(trade_date)

        owner = f"{PLUGIN_NAME}:{uuid.uuid4().hex}"
        ttl_seconds = self._snapshot_lease_ttl()
        try:
            claim = await self._wait_snapshot_lease(request_id, trade_date, owner, ttl_seconds, lease_capability)
        except asyncio.CancelledError:
            raise
        except SnapshotLeaseError:
            raise
        except Exception as exc:
            raise SnapshotLeaseError("durable snapshot lease claim failed") from exc
        if not self._lease_acquired(claim):
            self._daily_retry_after = retry_at or (datetime.now(CHINA_TZ) + timedelta(minutes=5))
            return await self._daily_snapshot_tushare_body(trade_date, allow_network=False)

        context = {
            "request_id": request_id,
            "owner": str(claim.get("owner") or owner),
            "fence": int(claim.get("fence") or 0),
            "ttl_seconds": ttl_seconds,
            "claim_method": lease_capability["claim"],
            "state_method": lease_capability["state"],
            "renew_method": lease_capability["renew"],
            "release_method": lease_capability["release"],
            "owned_save_method": lease_capability["owned_save"],
            "atomic_method": lease_capability["atomic"],
        }
        lost = asyncio.Event()
        renew_task = asyncio.create_task(self._snapshot_lease_renewer(context, lost))
        self._snapshot_fallback_owner = context["owner"]
        try:
            return await self._daily_snapshot_tushare_body(
                trade_date,
                lease=context,
                lease_guard=lambda: self._snapshot_lease_guard(context, lost),
            )
        except asyncio.CancelledError:
            # Cancellation is deliberately not a classified outcome.  The
            # owner lease is released in ``finally`` while the request remains
            # ``fetching`` for a later bounded takeover.
            raise
        except SnapshotLeaseLostError:
            self._last_screen_diagnostics = {
                **self._raw_diagnostics(self._last_screen_diagnostics),
                "lease_lost": True,
                "fallback_allowed": False,
                "failure_kind": "lease_lost",
            }
            return [], False, trade_date
        finally:
            if renew_task is not None:
                renew_task.cancel()
                try:
                    await renew_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
            try:
                await self._store_call(context["release_method"], request_id, context["owner"], context["fence"])
            except (SnapshotLeaseError, ValueError, TypeError, OSError):
                pass
            if getattr(self, "_snapshot_fallback_owner", None) == context["owner"]:
                self._snapshot_fallback_owner = None

    async def _daily_snapshot_tushare_body(
        self,
        trade_date: str,
        *,
        lease: dict | None = None,
        lease_guard=None,
        allow_network: bool = True,
    ) -> tuple[list, bool, str]:
        """Fetch/publish a raw batch, falling back only to a fresh raw generation."""
        canonical_trade_date = self._canonical_screen_date(trade_date)
        today = datetime.now(CHINA_TZ).date().isoformat()
        if not canonical_trade_date or canonical_trade_date > today:
            self._last_screen_diagnostics = {
                "invalid_date": True,
                "future_date": bool(canonical_trade_date and canonical_trade_date > today),
                "fallback_allowed": False,
            }
            raise ValueError("Tushare snapshot requested date is invalid or in the future")
        trade_date = canonical_trade_date
        request_id = f"daily_snapshot:{trade_date}"
        request = await self._store_call(self.store.snapshot_request, request_id) or {}
        gate, retry_at = self._snapshot_request_gate(request)

        calendar_diagnostics: dict[str, object] = {}
        # Diagnostics belong to this acquisition, not the previous command.
        self._last_screen_diagnostics = self._raw_diagnostics()

        if not allow_network:
            # Waiters and backoff callers are deliberately cache-only.  They
            # must not probe either Tushare or Eastmoney while another owner
            # is still working on this request.
            return await self._cache_only_tushare_snapshot(trade_date)

        async def save_request(**kwargs):
            if lease is not None:
                try:
                    return await self._store_call(
                        lease["owned_save_method"],
                        request_id,
                        trade_date,
                        lease["owner"],
                        lease["fence"],
                        **kwargs,
                    )
                except SnapshotLeaseError:
                    raise
                except Exception as exc:
                    raise SnapshotLeaseError("durable snapshot request persistence failed") from exc
            return await self._store_call(self.store.save_snapshot_request, request_id, trade_date, **kwargs)

        async def save_owned_result(actual_date: str, quotes, **kwargs):
            if lease is None:
                raise SnapshotLeaseCapabilityError("fenced Tushare snapshot persistence requires a lease")
            try:
                return await self._store_call(
                    lease["atomic_method"],
                    request_id,
                    trade_date,
                    lease["owner"],
                    lease["fence"],
                    quotes=quotes,
                    actual_trade_date=actual_date,
                    **kwargs,
                )
            except SnapshotLeaseError:
                raise
            except Exception as exc:
                raise SnapshotLeaseError("durable Tushare snapshot persistence failed") from exc

        async def cache_result() -> tuple[list, bool, str]:
            quotes, actual_date, provenance, state = await self._fresh_raw_snapshot_async(
                trade_date,
                cache_as_of=calendar_diagnostics.get("calendar_target_date") or trade_date,
                max_stale_trading_days=self._raw_stale_days(),
            )
            diagnostics = self._raw_diagnostics(self._last_screen_diagnostics)
            diagnostics.update(calendar_diagnostics)
            diagnostics["cache_state"] = state
            if state == "cache_basis_rejected":
                diagnostics["cache_basis_rejected"] = int(diagnostics.get("cache_basis_rejected", 0)) + 1
            if not quotes or not actual_date:
                diagnostics["history_unavailable"] = True
                self._last_screen_diagnostics = diagnostics
                return [], False, trade_date
            diagnostics.update({
                "raw_cache": True,
                "raw_batch_id": provenance.get("batch_id") or provenance.get("active_batch_id"),
                "raw_generation": provenance.get("generation"),
                "actual_trade_date": actual_date,
                "history_unavailable": False,
            })
            self._raw_screen_provenance = provenance
            self._last_screen_diagnostics = diagnostics
            try:
                retry_at = self._daily_retry_after.isoformat() if self._daily_retry_after else None
                attempts = int(request.get("attempts") or 0)
                if lease is not None:
                    await save_owned_result(
                        actual_date,
                        quotes,
                        source="tushare",
                        quality="cached",
                        complete=False,
                        note="网络失败，仅使用不超过两个交易日的 raw 缓存",
                        keep_days=self._int("daily_cache_keep_days", 180, 7, 730),
                        state="partial",
                        attempts=attempts,
                        last_error=None,
                        next_retry_at=retry_at,
                        terminal=False,
                        failure_kind="network",
                        calendar_evidence=calendar_diagnostics,
                        provenance=provenance,
                        request_state="retry",
                        request_source="tushare",
                        request_quality="cached",
                        request_last_error="network failure; fresh raw cache used",
                        request_next_retry_at=retry_at,
                        request_terminal=False,
                        request_failure_kind="network",
                    )
                else:
                    try:
                        await self._store_call(
                            self.store.save_daily_quotes,
                            trade_date=actual_date,
                            quotes=quotes,
                            keep_days=self._int("daily_cache_keep_days", 180, 7, 730),
                        )
                    except TypeError:
                        await self._store_call(
                            self.store.save_daily_quotes,
                            actual_date,
                            quotes,
                            self._int("daily_cache_keep_days", 180, 7, 730),
                        )
                    await self._store_call(
                        self.store.save_snapshot_meta,
                        actual_date, "tushare", "cached", False, trade_date,
                        "网络失败，仅使用不超过两个交易日的 raw 缓存",
                        attempts=attempts, state="partial", terminal=False,
                    )
                    await save_request(
                        actual_trade_date=actual_date, state="retry",
                        attempts=attempts, source="tushare", quality="cached",
                        last_error="network failure; fresh raw cache used", next_retry_at=retry_at,
                        terminal=False, failure_kind="network", calendar_evidence=calendar_diagnostics,
                        provenance=provenance,
                    )
            except SnapshotLeaseError:
                raise
            except Exception as exc:
                if lease is not None:
                    raise SnapshotLeaseError("durable cached snapshot persistence failed") from exc
            return quotes, False, actual_date

        async def request_attempts(default: int = 0) -> int:
            try:
                current = await self._store_call(self.store.snapshot_request, request_id) or {}
                return max(0, int(current.get("attempts") or request.get("attempts") or default))
            except (AttributeError, TypeError, ValueError, OverflowError):
                try:
                    return max(0, int(request.get("attempts") or default))
                except (TypeError, ValueError, OverflowError):
                    return max(0, int(default))

        async def persist_transient_retry(
            reason: str,
            *,
            stage: str,
            attempts: int | None = None,
            failure_kind: str | None = None,
            last_error: str | None = None,
        ) -> None:
            retry_at = self._daily_retry_after or (datetime.now(CHINA_TZ) + timedelta(minutes=5))
            self._daily_retry_after = retry_at
            current = await self._store_call(self.store.snapshot_request, request_id) or {}
            persisted_failure_kind = str(failure_kind or reason or "network")
            values = {
                "actual_trade_date": self._canonical_screen_date(current.get("actual_trade_date") or ""),
                "state": "retry",
                "attempts": await request_attempts() if attempts is None else max(0, int(attempts)),
                "source": "tushare",
                "quality": "unknown",
                "last_error": last_error or f"tushare {stage} transient failure: {reason}",
                "next_retry_at": retry_at.isoformat(),
                "terminal": False,
                "failure_kind": persisted_failure_kind,
                "calendar_evidence": calendar_diagnostics,
                "provenance": {},
            }
            if lease is not None:
                await save_owned_result(
                    values["actual_trade_date"],
                    [],
                    source="tushare",
                    quality="unknown",
                    complete=False,
                    state="retry",
                    attempts=values["attempts"],
                    last_error=values["last_error"],
                    next_retry_at=values["next_retry_at"],
                    terminal=False,
                    failure_kind=persisted_failure_kind,
                    calendar_evidence=calendar_diagnostics,
                    provenance={},
                    request_state="retry",
                    request_source="tushare",
                    request_quality="unknown",
                    request_last_error=values["last_error"],
                    request_next_retry_at=values["next_retry_at"],
                    request_terminal=False,
                    request_failure_kind=persisted_failure_kind,
                    persist_meta=False,
                )
            else:
                await save_request(**values)

        def bulk_retry_context(reason: str, diagnostics: dict) -> tuple[str, str | None]:
            stage = str(diagnostics.get("failure_stage") or "daily").strip().lower()
            if stage not in {"calendar", "daily", "universe"}:
                stage = "daily"
            provider_api = str(diagnostics.get("provider_api") or "").strip().lower()
            if reason != "rate_limit" or stage != "universe" or provider_api != "stock_basic":
                return stage, None
            pending = diagnostics.get("universe_missing_statuses")
            if not isinstance(pending, (list, tuple, set)):
                pending = ()
            statuses = [status for status in ("L", "D", "P") if status in pending]
            suffix = f"; pending statuses: {','.join(statuses)}" if statuses else ""
            return stage, "tushare stock_basic rate limited during universe evidence" + suffix

        try:
            if lease_guard is not None:
                await lease_guard()
            target_session, calendar_diagnostics = await self._resolve_tushare_completed_session(trade_date)
            if lease_guard is not None:
                await lease_guard()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if isinstance(exc, SnapshotLeaseLostError):
                raise
            reason = self._tushare_exception_fallback_reason(exc)
            if reason is None:
                self._record_tushare_failure_diagnostics(exc, stage="calendar")
                raise
            endpoint_unavailable = isinstance(exc, TushareCalendarEndpointError)
            failure_kind = "calendar_endpoint_unavailable" if endpoint_unavailable else reason
            diagnostics = {
                "network_failed": int(reason in {"network", "timeout"}),
                "history_invalid": 0,
                "cache_basis_rejected": 0,
                "calendar_unavailable": int(reason == "calendar" or endpoint_unavailable),
                "calendar_endpoint_unavailable": int(endpoint_unavailable),
                "calendar_failure_kind": str(getattr(exc, "category", "") or "") if endpoint_unavailable else "",
                "failure_kind": failure_kind,
                "fallback_allowed": False,
                "error": str(exc)[:240],
            }
            self._last_screen_diagnostics = diagnostics
            self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
            cached = await cache_result()
            if cached[0]:
                return cached
            await persist_transient_retry(
                reason,
                stage="calendar",
                failure_kind=failure_kind,
                last_error=(
                    "calendar endpoint unavailable (network) after 2 attempts"
                    if endpoint_unavailable
                    else None
                ),
            )
            # A ``TushareCalendarError`` is a data rejection (invalid, short,
            # or stale calendar), not a transient outage.  The resolver already
            # refused to authorize an Eastmoney calendar fallback for it, so the
            # price preview must stay fail-closed here too.  The bounded retry
            # state above still applies, so the request is never left "fetching".
            if not endpoint_unavailable and reason != "calendar":
                self._mark_tushare_fallback(reason, diagnostics)
            return cached

        # Resolve the completed session before reading the cache.  Normal
        # cache reuse is exact: a Monday target must never short-circuit on
        # Friday merely because the configured stale window allows it.
        fresh_quotes, fresh_actual, fresh_provenance, fresh_state = await self._fresh_raw_snapshot_async(
            trade_date,
            cache_as_of=target_session,
            max_stale_trading_days=0,
        )
        if fresh_quotes and fresh_actual == target_session:
            fresh_quotes, risk_observations = await self._enrich_daily_snapshot_risk_fields(
                fresh_actual, fresh_quotes, collect_evidence=True,
            )
            self._daily_retry_after = (
                None if fresh_actual == trade_date
                else datetime.now(CHINA_TZ) + timedelta(minutes=5)
            )
            diagnostics = self._raw_diagnostics(self._last_screen_diagnostics)
            diagnostics.update(calendar_diagnostics)
            diagnostics.update({
                "requested_date": trade_date,
                "effective_trade_date": target_session,
                "raw_cache": True,
                "raw_batch_id": fresh_provenance.get("batch_id") or fresh_provenance.get("active_batch_id"),
                "raw_generation": fresh_provenance.get("generation"),
                "actual_trade_date": fresh_actual,
                "cache_state": fresh_state,
                "fallback_allowed": False,
            })
            self._raw_screen_provenance = fresh_provenance
            self._last_screen_diagnostics = diagnostics
            # A previous owner can publish the raw generation and then stop
            # before writing the snapshot request/meta pair.  The cache fast
            # path must repair that durable boundary, but only for the active,
            # date-matched generation under final calendar evidence.
            fresh_batch_id = str(
                fresh_provenance.get("active_batch_id") or fresh_provenance.get("batch_id") or ""
            ).strip()
            complete_from_raw = bool(
                fresh_batch_id
                and fresh_actual == target_session
                and self._calendar_evidence_final(calendar_diagnostics, trade_date)
            )
            if complete_from_raw:
                try:
                    attempts = await request_attempts()
                    if lease is not None:
                        await save_owned_result(
                            fresh_actual,
                            fresh_quotes,
                            source="tushare",
                            quality="good",
                            complete=True,
                            keep_days=self._int("daily_cache_keep_days", 180, 7, 730),
                            state="complete",
                            attempts=attempts,
                            next_retry_at=None,
                            terminal=True,
                            failure_kind="",
                            calendar_evidence=calendar_diagnostics,
                            provenance=fresh_provenance,
                            request_state="complete",
                            request_source="tushare",
                            request_quality="good",
                            request_next_retry_at=None,
                            request_terminal=True,
                            request_failure_kind="",
                        )
                    else:
                        await self._store_call(
                            self.store.save_daily_quotes,
                            fresh_actual,
                            fresh_quotes,
                            self._int("daily_cache_keep_days", 180, 7, 730),
                        )
                        await self._store_call(
                            self.store.save_snapshot_meta,
                            fresh_actual,
                            "tushare",
                            "good",
                            True,
                            trade_date,
                            attempts=attempts,
                            state="complete",
                            terminal=True,
                        )
                        await save_request(
                            actual_trade_date=fresh_actual,
                            state="complete",
                            attempts=attempts,
                            source="tushare",
                            quality="good",
                            next_retry_at=None,
                            terminal=True,
                            failure_kind="",
                            calendar_evidence=calendar_diagnostics,
                            provenance=fresh_provenance,
                        )
                except SnapshotLeaseError:
                    raise
                except Exception as exc:
                    if lease is not None:
                        raise SnapshotLeaseError("durable raw snapshot finalization failed") from exc
            recorder = getattr(self.store, "record_daily_risk_observations", None)
            if complete_from_raw and callable(recorder) and risk_observations:
                bundle = {"trade_date": fresh_actual, "batch_id": fresh_batch_id,
                          "observations": [
                              {**item, "trade_date": fresh_actual, "batch_id": fresh_batch_id,
                               "source": "eastmoney:companion"}
                              for item in risk_observations if isinstance(item, dict)
                          ]}
                try:
                    await self._store_call(recorder, bundle)
                except (sqlite3.Error, ValueError, TypeError, RuntimeError):
                    logger.warning("[%s] 收盘风险证据未能持久化，正式筛选仍按未知处理", PLUGIN_NAME)
            if callable(getattr(self.store, "daily_risk_evidence_for_quotes", None)):
                for quote in fresh_quotes:
                    quote.suspended = quote.limit_up = quote.limit_down = quote.st = None
                fresh_quotes = await self._apply_cached_daily_risk_evidence(
                    fresh_actual, fresh_batch_id, fresh_quotes,
                )
            return fresh_quotes, False, fresh_actual

        if gate != "allow":
            self._daily_retry_after = retry_at or (datetime.now(CHINA_TZ) + timedelta(minutes=5))
            return await cache_result()
        async with self._daily_snapshot_lock:
            request = await self._store_call(self.store.snapshot_request, request_id) or {}
            gate, retry_at = self._snapshot_request_gate(request)
            if gate != "allow":
                self._daily_retry_after = retry_at or (datetime.now(CHINA_TZ) + timedelta(minutes=5))
                return await cache_result()
            attempts = int(request.get("attempts") or 0)
            if lease is None:
                attempts += 1
            await save_request(
                state="fetching",
                attempts=attempts,
                source="tushare",
                quality="unknown",
                failure_kind="",
                calendar_evidence=calendar_diagnostics,
                provenance={},
            )
            fetch_bulk = getattr(self.quotes, "fetch_bulk_daily_result", None)
            result = None
            try:
                if not callable(fetch_bulk):
                    raise TushareBulkError("Tushare bulk provider is unavailable")
                if lease_guard is not None:
                    await lease_guard()
                bulk_args = (trade_date,)
                bulk_kwargs = self._compatible_kwargs(
                    fetch_bulk,
                    bulk_args,
                    {
                        "session_count": int(getattr(self, "raw_session_count", 120)),
                        "calendar_evidence": calendar_diagnostics,
                        "lease_guard": lease_guard,
                        "snapshot_lease": lease,
                    },
                )
                result = await fetch_bulk(*bulk_args, **bulk_kwargs)
                if lease_guard is not None:
                    await lease_guard()
            except (TushareCircuitOpen, httpx.HTTPError, asyncio.TimeoutError, OSError) as exc:
                reason = self._tushare_exception_fallback_reason(exc)
                if reason is None:
                    self._record_tushare_failure_diagnostics(exc, stage="daily")
                    raise
                diagnostics = {"network_failed": int(reason in {"network", "timeout"}), "history_invalid": 0, "cache_basis_rejected": 0, "error": str(exc)[:240]}
                self._last_screen_diagnostics = diagnostics
                self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                cached = await cache_result()
                if cached[0]:
                    return cached
                await persist_transient_retry(reason, stage="daily", attempts=attempts)
                self._mark_tushare_fallback(reason, diagnostics)
                return cached
            except (TushareBulkError, ValueError, TypeError, KeyError) as exc:
                if isinstance(exc, SnapshotLeaseLostError):
                    raise
                reason = self._tushare_exception_fallback_reason(exc)
                if reason is None:
                    self._record_tushare_failure_diagnostics(exc, stage="daily")
                    raise
                diagnostics = {
                    "network_failed": 0,
                    "history_invalid": int(reason == "history_invalid"),
                    "cache_basis_rejected": 0,
                    "error": str(exc)[:240],
                }
                self._last_screen_diagnostics = diagnostics
                self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                cached = await cache_result()
                if cached[0]:
                    return cached
                await persist_transient_retry(reason, stage="daily", attempts=attempts)
                self._mark_tushare_fallback(reason, diagnostics)
                return cached

            if lease_guard is not None:
                await lease_guard()
            if not self._bulk_result_is_typed(result):
                self._last_screen_diagnostics = self._raw_diagnostics({
                    "internal_error": 1,
                    "failure_kind": "internal_error",
                    "error": "Tushare bulk provider returned an untyped result",
                })
                return [], False, trade_date
            diagnostics = self._raw_diagnostics(self._bulk_value(result, "diagnostics", {}))
            provider_diagnostics = getattr(self.quotes, "last_diagnostics", None)
            if isinstance(provider_diagnostics, dict):
                for key, value in provider_diagnostics.items():
                    if key not in diagnostics or value not in (None, 0, False, ""):
                        diagnostics[key] = value
            diagnostics.update(calendar_diagnostics)
            if diagnostics.get("coverage_ok") is False and not diagnostics.get("coverage_failed"):
                diagnostics["coverage_failed"] = 1
            quotes = list(self._bulk_value(result, "quotes", []) or [])
            raw_actual_date = self._bulk_value(result, "trade_date", "")
            actual_date = self._canonical_screen_date(raw_actual_date)
            batch_id = self._bulk_value(result, "batch_id")
            complete = bool(self._bulk_value(result, "complete", False)) and bool(batch_id)
            effective_date = self._canonical_screen_date(
                self._bulk_value(
                    result,
                    "effective_trade_date",
                    diagnostics.get("effective_trade_date") or diagnostics.get("acquisition_date"),
                )
            ) or actual_date
            if (raw_actual_date not in (None, "") and not actual_date) or self._diagnostic_flag(diagnostics.get("invalid_date")):
                diagnostics["invalid_date"] = True
                self._last_screen_diagnostics = diagnostics
                return [], False, trade_date
            if self._diagnostic_flag(diagnostics.get("future_date")) or (actual_date and actual_date > trade_date):
                diagnostics["future_date"] = True
                diagnostics["history_invalid"] = int(diagnostics.get("history_invalid", 0) or 0) + 1
                self._last_screen_diagnostics = diagnostics
                return [], False, trade_date
            shadow = self._shadow_only_diagnostics(diagnostics)
            if shadow:
                diagnostics.update({
                    "configured_shadow": True,
                    "shadow_only": True,
                    "publication_mode": "shadow",
                    "data_mode": "configured_shadow",
                    "fallback_allowed": False,
                    "raw_shadow_rejected": True,
                })
                self._last_screen_diagnostics = diagnostics
                self._raw_screen_provenance = {}
                self._daily_retry_after = None
                try:
                    shadow_date = actual_date or effective_date or trade_date
                    shadow_provenance = {"batch_id": batch_id, "actual_trade_date": shadow_date}
                    if lease is not None:
                        await save_owned_result(
                            shadow_date,
                            [],
                            source="tushare",
                            quality="shadow",
                            complete=False,
                            note="raw 批次已校验但按配置保留为 shadow，未切换 active",
                            state="shadow",
                            attempts=attempts,
                            last_error="configured shadow-only; active generation unchanged",
                            next_retry_at=None,
                            terminal=False,
                            failure_kind="shadow",
                            calendar_evidence=calendar_diagnostics,
                            provenance=shadow_provenance,
                        )
                    else:
                        await save_request(
                            actual_trade_date=shadow_date,
                            state="shadow",
                            attempts=attempts,
                            source="tushare",
                            quality="shadow",
                            last_error="configured shadow-only; active generation unchanged",
                            next_retry_at=None,
                            terminal=False,
                            failure_kind="shadow",
                            calendar_evidence=calendar_diagnostics,
                            provenance=shadow_provenance,
                        )
                        await self._store_call(
                            self.store.save_snapshot_meta,
                            shadow_date,
                            "tushare",
                            "shadow",
                            False,
                            trade_date,
                            "raw 批次已校验但按配置保留为 shadow，未切换 active",
                            attempts=attempts,
                            state="shadow",
                            terminal=False,
                        )
                except SnapshotLeaseError:
                    raise
                except Exception as exc:
                    if lease is not None:
                        raise SnapshotLeaseError("durable shadow snapshot persistence failed") from exc
                return [], False, shadow_date
            if not quotes or not actual_date:
                reason = self._tushare_result_fallback_reason(result)
                if reason:
                    self._last_screen_diagnostics = diagnostics
                    self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                    cached = await cache_result()
                    if cached[0]:
                        return cached
                    retry_stage, retry_error = bulk_retry_context(reason, diagnostics)
                    await persist_transient_retry(reason, stage=retry_stage, attempts=attempts, last_error=retry_error)
                    self._mark_tushare_fallback(reason, diagnostics)
                    return cached
                diagnostics["history_unavailable"] = True
                self._last_screen_diagnostics = diagnostics
                return [], False, trade_date
            if not complete:
                reason = self._tushare_result_fallback_reason(result)
                if reason:
                    self._last_screen_diagnostics = diagnostics
                    self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                    cached = await cache_result()
                    if cached[0]:
                        return cached
                    retry_stage, retry_error = bulk_retry_context(reason, diagnostics)
                    await persist_transient_retry(reason, stage=retry_stage, attempts=attempts, last_error=retry_error)
                    self._mark_tushare_fallback(reason, diagnostics)
                    return cached
                diagnostics["history_unavailable"] = True
                self._last_screen_diagnostics = diagnostics
                return [], False, trade_date
            if actual_date != target_session or (effective_date and effective_date != target_session):
                diagnostics.update({
                    "calendar_race": True,
                    "history_unavailable": True,
                    "expected_trade_date": target_session,
                })
                self._last_screen_diagnostics = diagnostics
                self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                cached = await cache_result()
                if cached[0]:
                    return cached
                return [], False, trade_date
            active_lookup = getattr(self.store, "active_raw_batch", None)
            if batch_id and callable(active_lookup):
                try:
                    active = await self._store_call(
                        active_lookup,
                        self._raw_dataset(),
                        as_of=target_session,
                        max_stale_trading_days=0,
                    )
                except TypeError:
                    try:
                        active = await self._store_call(active_lookup, self._raw_dataset(), as_of=target_session)
                    except Exception:
                        active = None
                except Exception:
                    active = None
                active_batch_id = active.get("active_batch_id") if isinstance(active, dict) else None
                if not active or str(active_batch_id or "") != str(batch_id):
                    diagnostics.update({
                        "raw_generation_race": True,
                        "history_unavailable": True,
                    })
                    self._last_screen_diagnostics = diagnostics
                    self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                    cached = await cache_result()
                    if cached[0]:
                        return cached
                    await save_request(
                        state="failed",
                        attempts=attempts,
                        source="tushare",
                        quality="unknown",
                        last_error="raw active generation changed during acquisition",
                        next_retry_at=None,
                        terminal=False,
                        failure_kind="generation_race",
                        calendar_evidence=calendar_diagnostics,
                        provenance={},
                    )
                    return [], False, trade_date
            generation = diagnostics.get("generation") or diagnostics.get("raw_generation")
            dataset_id = diagnostics.get("dataset_id")
            if self._bool("market_comparison_enabled", False):
                diagnostics["market_comparison"] = await self._collect_market_comparison(result, actual_date, batch_id)
            try:
                diagnostics["recommendation_daily_bars_saved"] = await self._persist_recommendation_daily_bars(result, actual_date)
            except (sqlite3.Error, RuntimeError, ValueError, TypeError, KeyError):
                diagnostics["recommendation_daily_bars_saved"] = 0
                diagnostics["recommendation_daily_bars_persist_failed"] = True
            self._raw_screen_provenance = {
                "batch_id": batch_id,
                "dataset_id": dataset_id,
                "generation": generation,
                "source": "tushare",
                "basis": "unadjusted",
                "actual_trade_date": actual_date,
            }
            diagnostics.update({
                "raw_cache": False,
                "raw_batch_id": batch_id,
                "raw_generation": generation,
                "actual_trade_date": actual_date,
                "history_unavailable": False,
            })
            self._last_screen_diagnostics = diagnostics
            try:
                complete_for_request = bool(
                    complete
                    and actual_date
                    and actual_date <= trade_date
                    and actual_date == (effective_date or actual_date)
                    and self._calendar_evidence_final(calendar_diagnostics, trade_date)
                )
                if lease_guard is not None:
                    await lease_guard()
                retry_at = None if complete_for_request else (datetime.now(CHINA_TZ) + timedelta(minutes=5)).isoformat()
                snapshot_state = "complete" if complete_for_request else "partial"
                snapshot_quality = "good" if complete_for_request else "partial"
                failure_kind = "" if complete_for_request else str(diagnostics.get("failure_kind") or "partial")
                if lease is not None:
                    await save_owned_result(
                        actual_date,
                        quotes,
                        source="tushare",
                        quality=snapshot_quality,
                        complete=complete_for_request,
                        note="" if complete_for_request else "raw 批次未能完整发布或使用了较早交易日",
                        keep_days=self._int("daily_cache_keep_days", 180, 7, 730),
                        state=snapshot_state,
                        attempts=attempts,
                        next_retry_at=retry_at,
                        terminal=complete_for_request,
                        failure_kind=failure_kind,
                        calendar_evidence=calendar_diagnostics,
                        provenance=self._raw_screen_provenance,
                        request_state=snapshot_state,
                        request_source="tushare",
                        request_quality="good" if complete else "partial",
                        request_next_retry_at=retry_at,
                        request_terminal=complete_for_request,
                        request_failure_kind=failure_kind,
                    )
                else:
                    await self._store_call(
                        self.store.save_daily_quotes,
                        actual_date,
                        quotes,
                        self._int("daily_cache_keep_days", 180, 7, 730),
                    )
                    await self._store_call(
                        self.store.save_snapshot_meta,
                        actual_date, "tushare", snapshot_quality, complete_for_request,
                        trade_date, "" if complete_for_request else "raw 批次未能完整发布或使用了较早交易日",
                        attempts=attempts, state=snapshot_state, terminal=complete_for_request,
                    )
                    await save_request(
                        actual_trade_date=actual_date,
                        state=snapshot_state, attempts=attempts,
                        source="tushare", quality="good" if complete else "partial",
                        next_retry_at=retry_at,
                        terminal=complete_for_request,
                        failure_kind=failure_kind,
                        calendar_evidence=calendar_diagnostics,
                        provenance=self._raw_screen_provenance,
                    )
            except SnapshotLeaseError:
                raise
            except Exception as exc:
                if lease is not None:
                    raise SnapshotLeaseError("Tushare snapshot persistence failed") from exc
            self._daily_retry_after = None if complete_for_request else datetime.now(CHINA_TZ) + timedelta(minutes=5)
            return quotes, complete_for_request, actual_date

    async def _score_eastmoney_transient(
        self,
        quotes,
        limit: int,
        actual_date: str,
        *,
        requested_date: str = "",
        base_diagnostics: Mapping[str, object] | None = None,
        invocation_id: str = "",
    ) -> ScreenScoreResult:
        """Score one isolated EM preview without touching persistent state."""
        requested = self._canonical_screen_date(requested_date or actual_date)
        actual = self._canonical_screen_date(actual_date)
        if not requested or not actual or actual > requested:
            raise ValueError("Eastmoney fallback dates violate requested/actual bounds")

        # Only the snapshot batch is allowed to contribute to market breadth.
        batch = []
        seen: set[str] = set()
        for quote in list(quotes or []):
            code = normalize_code(getattr(quote, "code", ""))
            try:
                price = float(getattr(quote, "price", 0) or 0)
            except (TypeError, ValueError, OverflowError):
                price = 0.0
            if not re.fullmatch(r"\d{6}", code) or price <= 0 or code in seen:
                continue
            source = str(getattr(quote, "source", "") or "").strip().lower()
            if source not in {"eastmoney", "eastmoney_fallback"}:
                continue
            seen.add(code)
            batch.append(quote)
        minimum_snapshot = self._int("daily_snapshot_min_size", 4000, 1, 10000)
        prior_diagnostics = dict(base_diagnostics or {})
        diagnostics = {
            "fallback_reason": prior_diagnostics.get("fallback_reason", ""),
            "data_mode": "eastmoney_transient",
            "source": "eastmoney_fallback",
            "candidate_source": "eastmoney_fallback",
            "quality": "degraded",
            "screen_quality": "degraded",
            "complete": False,
            "requested_date": requested,
            "actual_trade_date": actual,
            "snapshot_count": len(batch),
            "snapshot_minimum": minimum_snapshot,
            "history_coverage": 0.0,
            "market_breadth_source": "same_eastmoney_snapshot",
            "input": len(batch),
            "diagnostics_invocation_id": str(invocation_id or ""),
            "diagnostics_requested_date": requested,
            "diagnostics_actual_date": actual,
        }
        if len(batch) < minimum_snapshot:
            diagnostics.update({
                "degraded_unavailable": True,
                "degraded_reason": "snapshot_coverage",
                "history_unavailable": True,
            })
            return ScreenScoreResult.build((), diagnostics)

        tradable = [quote for quote in batch if is_tradable(
            quote,
            self._float("price_min", 2, 0.01, 100000),
            self._float("price_max", 80, 0.01, 100000),
        )]
        def amount_value(quote) -> float:
            try:
                amount = float(getattr(quote, "amount", 0) or 0)
                return amount if math.isfinite(amount) else 0.0
            except (TypeError, ValueError, OverflowError):
                return 0.0
        tradable.sort(key=lambda quote: (amount_value(quote), str(quote.code)), reverse=True)
        deep_limit = self._int("deep_screen_limit", 300, 1, 1000)
        enrich_targets = tradable[:deep_limit]
        diagnostics.update({
            "tradable": len(tradable),
            "indicator_targets": len(enrich_targets),
        })
        history_status: dict[str, str] = {}
        if enrich_targets:
            enrich = getattr(self.quotes, "enrich_indicators", None)
            if not callable(enrich):
                diagnostics.update({"degraded_unavailable": True, "degraded_reason": "history_provider_missing", "history_unavailable": True})
                return ScreenScoreResult.build((), diagnostics)
            try:
                result = await enrich(
                    enrich_targets,
                    self._int("max_concurrency", 8, 1, 64),
                    actual,
                )
            except asyncio.CancelledError:
                raise
            except (httpx.HTTPError, asyncio.TimeoutError, OSError, ValueError, TypeError, KeyError) as exc:
                diagnostics.update({
                    "degraded_unavailable": True,
                    "degraded_reason": "history_network",
                    "history_unavailable": True,
                    "history_error": str(exc)[:240],
                })
                return ScreenScoreResult.build((), diagnostics)
            except Exception:
                # A programming/provider integration error must not be turned
                # into a plausible zero-candidate preview.
                raise
            if isinstance(result, dict):
                history_status = {str(key): str(value or "failed") for key, value in result.items()}

        usable: list[Quote] = []
        for quote in enrich_targets:
            status = history_status.get(quote.code, "failed")
            status_key = status.strip().lower()
            last_date = self._canonical_screen_date(getattr(quote, "indicator_last_date", ""))
            try:
                history_days = int(getattr(quote, "history_days", 0) or 0)
                atr = float(getattr(quote, "atr14", 0) or 0)
            except (TypeError, ValueError, OverflowError):
                history_days, atr = 0, 0.0
            basis = str(getattr(quote, "indicator_price_basis", "") or "").strip().lower()
            source = str(getattr(quote, "indicator_source", "") or "").strip().lower()
            if (
                status_key not in {"", "failed"}
                and not any(marker in status_key for marker in ("fail", "invalid", "unavailable"))
                and last_date
                and last_date <= actual
                and basis == "unadjusted"
                and source == "eastmoney"
                and history_days >= 20
                and math.isfinite(atr)
                and atr > 0
            ):
                usable.append(quote)
        coverage = len(usable) / len(enrich_targets) if enrich_targets else 1.0
        coverage_floor = self._float("screen_min_indicator_coverage", 0.8, 0.0, 1.0)
        diagnostics.update({
            "indicator_network": sum(1 for value in history_status.values() if value == "network"),
            "indicator_failed": max(0, len(enrich_targets) - len(usable)),
            "enriched": len(usable),
            "indicator_coverage": round(coverage, 4),
            "history_coverage": round(coverage, 4),
            "screen_min_indicator_coverage": coverage_floor,
        })
        if enrich_targets and coverage < coverage_floor:
            diagnostics.update({
                "degraded_unavailable": True,
                "degraded_reason": "history_coverage",
                "history_unavailable": True,
            })
            return ScreenScoreResult.build((), diagnostics)

        market = assess_market_context(batch)
        diagnostics.update({
            "market_regime": market.regime,
            "market_breadth": round(market.breadth, 4),
            "market_advancing": market.advancing,
            "market_declining": market.declining,
            "market_flat": market.flat,
            "market_sample_size": market.sample_size,
            "market_median_return": market.median_return,
            "qualified": 0,
            "min_score": self._int("min_score", 10, -100, 100),
            "history_unavailable": False,
            "degraded_unavailable": False,
        })
        scored = []
        for quote in usable:
            candidate = score_quote(quote)
            # A transient EM preview is never a validated close plan or an
            # active monitoring candidate, even when its technical score is
            # high enough to be shown.
            candidate.price_plan = None
            candidate.risk_level = "watch_only"
            candidate.risk_flags = ["东方财富临时降级预览", "仅本次预览"]
            candidate.reasons = ["降级数据仅供本次预览"] + list(candidate.reasons)
            quote.source = "eastmoney_fallback"
            scored.append(candidate)
        minimum = diagnostics["min_score"]
        qualified = [item for item in scored if item.base_score >= minimum]
        qualified.sort(key=candidate_rank_key)
        final = qualified[:max(1, min(int(limit), 100))] if qualified else []
        diagnostics.update({
            "qualified": len(qualified),
            "max_score": max((item.score for item in scored), default=0),
            "candidate_count": len(final),
        })
        return ScreenScoreResult.build(final, diagnostics)

    async def _score_quotes_result(
        self,
        quotes,
        limit: int,
        as_of: str = "",
        include_factors: bool = True,
        *,
        context: str = "realtime",
        data_mode: str = "",
        requested_date: str = "",
        base_diagnostics: Mapping[str, object] | None = None,
        invocation_id: str = "",
    ) -> ScreenScoreResult:
        """Run the bounded screen; only an explicit daily_close may validate plans."""
        quotes = [formal_source_policy.mask_quote_negatives(quote) for quote in (quotes or [])]
        if str(data_mode or "").strip().lower() == "eastmoney_transient":
            return await self._score_eastmoney_transient(
                quotes,
                limit,
                as_of,
                requested_date=requested_date,
                base_diagnostics=base_diagnostics,
                invocation_id=invocation_id,
            )
        if include_factors:
            await self._fill_quote_names(quotes, as_of)
        tradable = [quote for quote in quotes if is_screenable(
            quote,
            self._float("price_min", 2, 0.01, 100000),
            self._float("price_max", 80, 0.01, 100000),
        )]
        tradable.sort(key=lambda quote: (float(quote.amount or 0), str(quote.code)), reverse=True)
        deep_limit = self._int("deep_screen_limit", 300, 1, 1000)
        enrich_targets = tradable[:deep_limit]
        before = as_of or datetime.now(CHINA_TZ).date().isoformat()
        indicator_status: dict[str, str] = {}
        persistent: dict[str, list[dict]] = {}
        history_reason = ""
        raw_mode = self._tushare_mode()
        initial_diagnostics = dict(base_diagnostics or {})
        raw_diagnostics = self._raw_diagnostics(initial_diagnostics) if raw_mode else {}
        prior_history_unavailable = bool(initial_diagnostics.get("history_unavailable")) if raw_mode else False
        raw_provenance: dict[str, object] = {}
        if raw_mode:
            # Tushare mode has one historical source: the currently active,
            # point-in-time raw generation.  Work in bounded chunks so a large
            # market snapshot does not create one task or one temporary list
            # per symbol, and never invoke the legacy per-symbol loader.
            persistent, provenance, history_reason = await self._read_fresh_raw_history_async(
                [q.code for q in enrich_targets], before,
            )
            raw_provenance = dict(provenance or {})
            chunk_size = self._raw_chunk()
            for offset in range(0, len(enrich_targets), chunk_size):
                for quote in enrich_targets[offset:offset + chunk_size]:
                    cached_rows = persistent.get(quote.code, [])
                    bars, rejection = self._raw_indicator_rows(cached_rows, expected_code=quote.code)
                    if rejection == "basis":
                        history_reason = "cache_basis_rejected"
                    elif rejection:
                        history_reason = "invalid"
                    if apply_daily_indicators(quote, bars):
                        indicator_status[quote.code] = "raw_batch"
                    else:
                        indicator_status[quote.code] = "history_failed"
        else:
            load_daily_bars = getattr(self.store, "latest_daily_bars", None)
            persistent = (
                await self._store_call(
                    load_daily_bars,
                    [q.code for q in enrich_targets],
                    before_or_equal=before,
                    limit=60,
                )
                if enrich_targets and callable(load_daily_bars) else {}
            )
            if not isinstance(persistent, dict):
                persistent = {}
            network_targets = []
            for quote in enrich_targets:
                # Let the indicator calculator see the complete cached batch so a
                # mixed basis/source set fails closed instead of being reduced to
                # a seemingly valid subset first.
                cached_rows = persistent.get(quote.code, [])
                try:
                    bars = list(cached_rows or [])
                except TypeError:
                    bars = []
                if apply_daily_indicators(quote, bars):
                    indicator_status[quote.code] = "persistent_cache"
                else:
                    network_targets.append(quote)
            if network_targets:
                concurrency = self._int("max_concurrency", 8, 1, 64)
                try:
                    fetched = await self.quotes.enrich_indicators(network_targets, concurrency, as_of)
                except TypeError as exc:
                    # Keep lightweight integrations written against the pre-v0.12
                    # two-argument provider API usable without hiding other errors.
                    try:
                        fetched = await self.quotes.enrich_indicators(network_targets, concurrency)
                    except TypeError:
                        raise exc
                if not isinstance(fetched, dict):
                    fetched = {}
                for quote in network_targets:
                    indicator_status[quote.code] = fetched.get(quote.code, "failed")
                    history_bars = getattr(self.quotes, "history_bars", {})
                    bars = history_bars.get(quote.code, []) if isinstance(history_bars, dict) else []
                    save_daily_bars = getattr(self.store, "save_daily_bars", None)
                    if bars and callable(save_daily_bars):
                        await self._store_call(
                            save_daily_bars,
                            quote.code,
                            bars,
                            "eastmoney_indicator",
                            "unadjusted",
                        )
        indicator_counts = {
            "network": sum(1 for value in indicator_status.values() if value == "network"),
            "memory_cache": sum(1 for value in indicator_status.values() if value == "memory_cache"),
            "persistent_cache": sum(1 for value in indicator_status.values() if value == "persistent_cache"),
            "raw_batch": sum(1 for value in indicator_status.values() if value == "raw_batch"),
            "failed": sum(1 for value in indicator_status.values() if value not in {"network", "memory_cache", "persistent_cache", "raw_batch"}),
        }
        enriched = sum(indicator_counts[key] for key in ("network", "memory_cache", "persistent_cache", "raw_batch"))
        coverage = enriched / len(enrich_targets) if enrich_targets else 0.0
        risk_complete = sum(
            all(getattr(quote, field, None) is not None for field in ("suspended", "limit_up", "limit_down", "st"))
            for quote in quotes
        )
        valid_empty = bool(quotes and risk_complete == len(quotes) and not tradable)
        diagnostics = {
            "input": len(quotes), "tradable": len(tradable), "deep_screen_limit": deep_limit,
            "risk_tuple_complete": risk_complete, "valid_empty": valid_empty,
            "indicator_targets": len(enrich_targets), "indicator_network": indicator_counts["network"],
            "indicator_memory_cache": indicator_counts["memory_cache"], "indicator_persistent_cache": indicator_counts["persistent_cache"],
            "indicator_raw_batch": indicator_counts["raw_batch"], "indicator_failed": indicator_counts["failed"], "enriched": enriched,
            "indicator_coverage": round(coverage, 4),
            "diagnostics_invocation_id": str(invocation_id or ""),
            "diagnostics_requested_date": str(requested_date or as_of or ""),
            "diagnostics_actual_date": str(as_of or ""),
        }
        if raw_mode:
            diagnostics.update({
                "raw_dataset_key": self._raw_dataset(),
                "raw_batch_id": raw_provenance.get("batch_id") or raw_provenance.get("active_batch_id"),
                "raw_generation": raw_provenance.get("generation"),
                "diagnostics_raw_batch_id": raw_provenance.get("batch_id") or raw_provenance.get("active_batch_id"),
                "diagnostics_raw_generation": raw_provenance.get("generation"),
                "network_failed": int(raw_diagnostics.get("network_failed", 0) or 0),
                "history_invalid": int(raw_diagnostics.get("history_invalid", 0) or 0),
                "cache_basis_rejected": int(raw_diagnostics.get("cache_basis_rejected", 0) or 0),
                "history_unavailable": prior_history_unavailable or (bool(enrich_targets) and (not bool(persistent) or not bool(enriched))),
                "history_missing": max(0, len(enrich_targets) - enriched),
            })
            if history_reason == "cache_basis_rejected":
                diagnostics["cache_basis_rejected"] = int(diagnostics.get("cache_basis_rejected", 0)) + 1
            elif history_reason in {"invalid", "source"}:
                diagnostics["history_invalid"] = int(diagnostics.get("history_invalid", 0)) + 1
        # Scores are only produced for deep-screen objects, never for the
        # entire cheap-filter universe.
        scored = []
        for quote in enrich_targets:
            candidate = score_quote(quote)
            if context == "daily_close":
                candidate.price_plan = self._daily_close_plan(quote, as_of)
                review = review_risk(quote, candidate)
                candidate.risk_level = review.verdict
                candidate.risk_flags = review.flags
            scored.append(candidate)
        load_market_quotes = getattr(self.store, "daily_quotes", None)
        full_market = await self._store_call(load_market_quotes, as_of) if as_of and callable(load_market_quotes) else []
        if not isinstance(full_market, list):
            full_market = []
        market_minimum = self._int("market_min_snapshot_size", 4000, 1000, 10000)
        market_quotes = full_market if len(full_market) >= market_minimum else quotes
        market = assess_market_context(market_quotes)
        save_market_context = getattr(self.store, "save_market_context", None)
        if as_of and callable(save_market_context):
            await self._store_call(save_market_context, as_of, {
                "regime": market.regime, "breadth": market.breadth, "advancing": market.advancing,
                "declining": market.declining, "flat": market.flat, "sample_size": market.sample_size,
                "median_return": market.median_return, "total_amount": market.total_amount, "evidence": market.evidence,
            }, "daily_snapshot" if len(full_market) >= market_minimum else "input_subset", "good" if len(full_market) >= market_minimum else "partial")

        factor_limit = self._int("factor_screen_limit", 100, 0, 500)
        factor_targets = sorted(scored, key=lambda item: (item.base_score, item.quote.amount), reverse=True)[:factor_limit]
        factor_codes = [item.quote.code for item in factor_targets]
        factor_url = str(self.config.get("factor_data_url", "")).strip() if include_factors else ""
        factor_source = str(self.config.get("factor_source", "auto")).strip().lower() if include_factors else "disabled"
        factor_mode = str(self.config.get("factor_mode", "report_only")).strip().lower()
        historical_factor_date = bool(as_of and as_of < datetime.now(CHINA_TZ).date().isoformat())
        raw_factors: dict[str, dict] = {}
        factor_name, factor_quality = "", "unknown"
        if factor_url and factor_codes:
            try:
                raw_factors = await self.quotes.fetch_custom_factors(factor_url, factor_codes, as_of)
                factor_name, factor_quality = "custom", "good" if raw_factors else "unknown"
            except Exception:
                logger.warning("[%s] 自定义因子源不可用，继续技术筛选", PLUGIN_NAME)
        if not raw_factors and factor_source == "tushare" and self.quotes.tushare_token and factor_codes:
            try:
                raw_factors = await self.quotes.fetch_tushare_factors(factor_codes, as_of)
                factor_name, factor_quality = "tushare", "partial" if raw_factors else "unknown"
            except Exception:
                logger.warning("[%s] Tushare 因子源不可用，继续技术筛选", PLUGIN_NAME)
        if not raw_factors and not historical_factor_date and factor_source in {"auto", "eastmoney", "custom", "tushare"} and factor_codes:
            try:
                raw_factors = await self.quotes.fetch_eastmoney_factors(factor_codes)
                factor_name, factor_quality = "eastmoney", "partial" if raw_factors else "unknown"
            except Exception:
                logger.warning("[%s] 东方财富因子源不可用，继续技术筛选", PLUGIN_NAME)
        if factor_source == "auto" and self.quotes.tushare_token:
            missing_codes = [code for code in factor_codes if any(
                (raw_factors.get(code) or {}).get(field) is None
                for field in ("roe", "profit_growth", "cash_quality"))]
            if missing_codes:
                try:
                    tushare_rows = await self.quotes.fetch_tushare_factors(missing_codes, as_of)
                    if tushare_rows:
                        for code, incoming in tushare_rows.items():
                            previous = raw_factors.get(code) or {}
                            records = list(previous.get("evidence_records") or []) + list(incoming.get("evidence_records") or [])
                            raw_factors[code] = {**previous, **incoming, "evidence_records": records}
                        factor_name = f"{factor_name}+tushare" if factor_name else "tushare"
                        factor_quality = "partial"
                except Exception:
                    logger.warning("[%s] Tushare 因子补充不可用", PLUGIN_NAME)
        if as_of and callable(getattr(self.store, "factor_snapshots", None)):
            cached_rows = await self._store_call(self.store.factor_snapshots, as_of)
            if not isinstance(cached_rows, dict):
                cached_rows = {}
            cache_hits = 0
            for code in [item.quote.code for item in factor_targets]:
                if code not in raw_factors and code in cached_rows:
                    raw_factors[code] = cached_rows[code]
                    cache_hits += 1
            if cache_hits:
                factor_name = f"{factor_name}+cache" if factor_name else "cache"
                factor_quality = "cached" if factor_name == "cache" else "partial"
        risk_service = getattr(self, "risk_evidence", None)
        if include_factors and risk_service and as_of:
            for code in factor_codes[:self._int("official_evidence_candidate_limit", 20, 1, 100)]:
                evidence = await risk_service.get(code, as_of)
                row = raw_factors.setdefault(code, {})
                row["evidence_records"] = list(row.get("evidence_records") or []) + evidence["records"]
                row["official_evidence_status"] = evidence["status"]
                row["official_evidence_reason"] = evidence["reason"]
                await asyncio.sleep(2)
        # Evidence fetched after the screen's daily decision cutoff must not
        # be backfilled into a historical run. For today's still-running close
        # screen the cutoff is simply the observation time.
        known_at = self._factor_evidence_cutoff(as_of)
        raw_factors = {code: safe_factor_row(row, code, as_of or datetime.now(CHINA_TZ).date().isoformat(), known_at)
                       for code, row in raw_factors.items() if code in factor_codes}
        save_factor_snapshots = getattr(self.store, "save_factor_snapshots", None)
        if as_of and raw_factors and callable(save_factor_snapshots):
            await self._store_call(save_factor_snapshots, as_of, raw_factors, factor_name or factor_source, factor_quality)

        adjustment = market_adjustment(market.regime)
        momentum_values = [item.quote.momentum5 for item in factor_targets if item.quote.momentum5 is not None and math.isfinite(item.quote.momentum5)]
        benchmark_momentum = sum(momentum_values) / len(momentum_values) if momentum_values else None
        industry_rows: dict[str, list] = {}
        for item in factor_targets:
            industry = str((raw_factors.get(item.quote.code) or {}).get("industry") or "").strip()
            if industry and item.quote.momentum5 is not None and math.isfinite(item.quote.momentum5):
                industry_rows.setdefault(industry, []).append(item.quote)

        def tri_flag(value):
            if isinstance(value, bool):
                return value
            text = str(value or "").strip().lower()
            if text in {"1", "true", "yes", "on", "是", "y"}:
                return True
            if text in {"0", "false", "no", "off", "否", "n"}:
                return False
            return None

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
            st_state, audit_state = tri_flag(row.get("st_flag")), tri_flag(row.get("audit_flag"))
            fundamental_fields = {
                "fundamental_score": fundamental,
                "roe": roe,
                "profit_growth": profit_growth,
                "cash_quality": cash_quality,
                "valuation": valuation,
            }
            fundamental_missing = tuple(key for key, value in fundamental_fields.items() if value is None)
            fundamental_coverage = (len(fundamental_fields) - len(fundamental_missing)) / len(fundamental_fields)
            risk_fields = {"st_flag": st_state, "audit_flag": audit_state}
            risk_missing = tuple(key for key, value in risk_fields.items() if value is None)
            risk_coverage = (len(risk_fields) - len(risk_missing)) / len(risk_fields)
            fundamental_risk = st_state is True or audit_state is True
            calculated_fundamental = fundamental_score(roe, profit_growth, cash_quality, valuation, st_state, audit_state)
            if fundamental is None and calculated_fundamental is not None:
                fundamental = calculated_fundamental
            if fundamental is None and roe is not None:
                fundamental = max(-10, min(10, round(roe / 2, 1)))
            if fundamental is None and pe is not None:
                fundamental = max(-3, min(3, round(2 - pe / 20, 1)))
            item.quote.industry_score, item.quote.fundamental_score = industry, fundamental
            item.factor_overlay = FactorOverlay(
                industry_name=industry_name,
                industry_score=industry,
                fundamental_score=fundamental,
                market_regime=market.regime,
                market_adjustment=adjustment,
                source=str(row.get("source") or factor_name),
                as_of=as_of,
                quality=str(row.get("quality") or factor_quality),
                fundamental_coverage=fundamental_coverage,
                fundamental_missing=fundamental_missing,
                risk_factor_coverage=risk_coverage,
                risk_factor_missing=risk_missing,
            )
            if fundamental_risk:
                item.risk_level = "blocked"
                item.risk_flags.append("基本面硬风险")
            elif st_state is None or audit_state is None:
                item.risk_level = "unknown"
                item.risk_flags.append("ST/审计状态未知")
            if factor_mode == "score" and item.risk_level != "blocked":
                extra = max(-20, min(20, item.factor_overlay.adjustment))
                item.composite_score = item.base_score + extra
                item.score = item.composite_score
                item.score_max = 70
                if extra:
                    item.reasons.append(f"因子综合修正{extra:+d}")
        minimum = self._int("min_score", 10, -100, 100)
        scored.sort(key=candidate_rank_key)
        # Unknown quote or fundamental-risk evidence is not a weaker long
        # candidate. It is a signal-blocking state until a validated source
        # establishes the missing fields.
        qualified = [item for item in scored if item.base_score >= minimum and item.risk_level not in {"blocked", "unknown"}]
        fallback_limit = self._int("fallback_limit", 5, 0, 30)
        fallback = []
        if not qualified and fallback_limit and scored:
            fallback = [item for item in scored if item.risk_level not in {"blocked", "unknown"} and item.quote.history_days >= 20][:fallback_limit]
            for item in fallback:
                item.reasons = ["未达到最低分，列入观察候选"] + item.reasons
        coverage_floor = self._float("screen_min_indicator_coverage", 0.8, 0.0, 1.0)
        diagnostics.update({
            "qualified": len(qualified), "fallback": len(fallback), "factor_screen_limit": factor_limit,
            "factor_screen_count": len(factor_targets), "max_score": max((item.score for item in scored), default=0),
            "min_score": minimum, "market_regime": market.regime, "market_breadth": round(market.breadth, 4),
            "market_advancing": market.advancing, "market_declining": market.declining,
            "market_flat": market.flat, "market_sample_size": market.sample_size,
            "market_median_return": market.median_return,
            "factor_source": factor_name or "unknown", "factor_quality": factor_quality,
            "screen_min_indicator_coverage": coverage_floor, "coverage_ok": coverage >= coverage_floor,
            "risk_evidence_coverage": round(diagnostics["risk_tuple_complete"] / len(quotes), 4) if quotes else 0.0,
        })
        final_candidates = (qualified or fallback)[:limit]
        # Industry labels are a current display annotation.  Fetch them only
        # for the final pool, after scoring/filtering, so this cannot alter the
        # score, risk, reasons, or historical factor fields above.
        if include_factors and final_candidates:
            fetch_industries = getattr(self.quotes, "fetch_eastmoney_industries", None)
            if callable(fetch_industries):
                try:
                    current_rows = await fetch_industries([item.quote.code for item in final_candidates])
                except Exception:
                    logger.warning("[%s] 东方财富当前行业源不可用，继续候选结果", PLUGIN_NAME)
                    current_rows = {}
                if isinstance(current_rows, dict):
                    for item in final_candidates:
                        current = current_rows.get(item.quote.code)
                        if isinstance(current, dict):
                            current = current.get("current_industry_name") or current.get("industry") or current.get("f127")
                        current = self._clean_external_text(current, 80)
                        if not current:
                            continue
                        if item.factor_overlay is None:
                            item.factor_overlay = FactorOverlay(current_industry_name=current)
                        else:
                            item.factor_overlay.current_industry_name = current
        diagnostics["candidate_count"] = len(final_candidates)
        return ScreenScoreResult.build(final_candidates, diagnostics)

    @staticmethod
    def _factor_evidence_cutoff(as_of: str) -> str:
        """Return the latest time whose evidence may affect this daily screen."""
        now = datetime.now(timezone.utc)
        canonical = str(as_of or "").strip()
        try:
            target = datetime.strptime(canonical, "%Y-%m-%d").date()
        except ValueError:
            return now.isoformat()
        close_cutoff = datetime.combine(target, time(23, 59, 59), tzinfo=CHINA_TZ).astimezone(timezone.utc)
        return min(now, close_cutoff).isoformat()

    async def _score_quotes(self, *args, **kwargs):
        """Compatibility wrapper for integrations that expect a candidate list."""
        result = await self._score_quotes_result(*args, **kwargs)
        self._last_screen_diagnostics = dict(result.diagnostics)
        self._raw_screen_provenance = {
            "batch_id": result.diagnostics.get("raw_batch_id"),
            "generation": result.diagnostics.get("raw_generation"),
        }
        return list(result.candidates)

    async def _scan_result(
        self,
        codes: list[str],
        limit: int,
        *,
        record: bool = True,
        job_name: str = "manual_screen",
        invocation_id: str = "",
    ) -> ScreenScoreResult:
        """Fetch, score, and retain one run's diagnostics as an explicit result."""
        requested_date = datetime.now(CHINA_TZ).date().isoformat()
        actual_date = requested_date
        quotes = []
        candidates = []
        score_diagnostics: Mapping[str, object] = MappingProxyType({})
        status, quality, error = "completed", "partial", None
        invocation_id = str(invocation_id or uuid.uuid4().hex)
        try:
            if self._tushare_mode():
                quotes, _fetched, fetched_date = await self._daily_snapshot(requested_date)
                base_diagnostics = dict(self._last_screen_diagnostics or {})
                requested_codes = set(parse_codes(codes))
                if requested_codes:
                    quotes = [quote for quote in quotes if quote.code in requested_codes]
                actual_date = self._canonical_screen_date(fetched_date) or requested_date
                if actual_date > requested_date:
                    raise ValueError("snapshot actual date is after requested date")
                if not quotes:
                    base_diagnostics.setdefault("history_unavailable", True)
            else:
                quotes = await self.quotes.fetch_quotes(codes)
                actual_date = requested_date
                base_diagnostics = {}
            score_result = await self._score_quotes_result(
                quotes,
                limit,
                actual_date,
                context="daily_close",
                requested_date=requested_date,
                base_diagnostics=base_diagnostics,
                invocation_id=invocation_id,
            )
            candidates = list(score_result.candidates)
            score_diagnostics = score_result.diagnostics
            floor = self._float("screen_min_indicator_coverage", 0.8, 0.0, 1.0)
            coverage = float(score_diagnostics.get("indicator_coverage", 0.0) or 0.0)
            status = "completed" if coverage >= floor else "degraded"
            quality = "good" if coverage >= floor and quotes else "partial"
        except Exception as exc:
            status, quality, error = "failed", "unknown", str(exc)[:240]
            logger.exception("[%s] 选股行情抓取失败", PLUGIN_NAME)
        if record:
            cached_date = await self._store_call(self.store.latest_daily_trade_date, requested_date)
            if cached_date:
                cached_date = self._canonical_screen_date(cached_date)
                if cached_date and cached_date <= requested_date:
                    actual_date = cached_date
            source = str(getattr(quotes[0], "source", "") if quotes else "") or "universe"
            await self._await_result(self._record_screen(
                requested_date,
                actual_date or requested_date,
                source,
                quotes,
                candidates,
                status=status,
                quality=quality,
                error=error,
                job_name=job_name,
                diagnostics=score_diagnostics,
            ))
        return ScreenScoreResult.build(candidates, score_diagnostics)

    async def _scan(self, codes: list[str], limit: int, *, record: bool = True, job_name: str = "manual_screen"):
        """Compatibility wrapper for callers that expect a candidate list."""
        result = await self._scan_result(codes, limit, record=record, job_name=job_name)
        self._last_screen_diagnostics = dict(result.diagnostics)
        return list(result.candidates)

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

    async def _run_recommendation_ai_shadow_review(self, run_id: str, candidates) -> dict:
        prompt_version = str(self.config.get("llm_shadow_prompt_version", "shadow-risk-v1") or "shadow-risk-v1").strip()
        review_input = self.llm.shadow_review_input(list(candidates or []))
        canonical_input = json.dumps(
            {"prompt_version": prompt_version, **review_input},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        )
        input_sha256 = hashlib.sha256(canonical_input.encode("utf-8")).hexdigest()
        begin = await self._store_call(
            self.store.begin_recommendation_ai_review,
            run_id,
            model=self.llm.model,
            prompt_version=prompt_version,
            input_sha256=input_sha256,
        )
        if not begin.get("claimed"):
            return {"status": str(begin.get("status") or "skipped"), "reason": str(begin.get("reason") or "existing_review")}
        batch_id = str(begin["review_batch_id"])
        if len(begin.get("records") or []) != len(review_input.get("items") or []):
            await self._store_call(
                self.store.finish_recommendation_ai_review,
                batch_id,
                status="failed",
                error="recommendation_candidate_count_mismatch",
            )
            return {"status": "failed", "reason": "recommendation_candidate_count_mismatch"}
        result = await self.llm.review_candidates(
            list(candidates or []),
            max_tokens=self._int("llm_shadow_max_tokens", 8192, 400, 8192),
            prompt_version=prompt_version,
        )
        terminal = str(result.get("status") or "failed")
        if terminal not in {"complete", "unknown", "failed", "skipped"}:
            terminal = "failed"
        await self._store_call(
            self.store.finish_recommendation_ai_review,
            batch_id,
            status=terminal,
            model_returned=str(result.get("model_returned") or ""),
            usage=result.get("usage") if isinstance(result.get("usage"), dict) else {},
            error=str(result.get("error") or ""),
            decisions=result.get("decisions") if isinstance(result.get("decisions"), list) else [],
        )
        return {"status": terminal, "reason": str(result.get("error") or ""), "review_batch_id": batch_id}

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
        try:
            cost_price = float(cost_price) if cost_price is not None else None
        except (TypeError, ValueError, OverflowError):
            cost_price = None
        if (
            cost_price is None
            or quote.suspended is None
            or quote.limit_up is None
            or quote.limit_down is None
            or quote.st is None
            or quote.suspended is True
            or quote.limit_up is True
            or quote.limit_down is True
            or quote.st is True
            or not math.isfinite(cost_price)
            or not math.isfinite(quote.price)
            or quote.price <= 0
        ):
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

    def _fresh_quotes(self, quotes, *, now: datetime | None = None):
        """Accept only timestamps in the bounded source-clock window (Line B)."""
        current = now or datetime.now(CHINA_TZ)
        current = current.astimezone(CHINA_TZ) if current.tzinfo else current.replace(tzinfo=CHINA_TZ)
        max_age = max(30, self._int("quote_interval_seconds", 30, 10, 600) * 2)
        allowed_future_skew = self._int("quote_clock_skew_seconds", 120, 0, 900)
        source_health = getattr(self, "_source_health", None)
        if not isinstance(source_health, dict):
            source_health = self._source_health = {}
        source = source_health.setdefault("sina", {})
        reasons: dict[str, int] = {}
        fresh = []

        def reject(reason: str) -> None:
            reasons[reason] = reasons.get(reason, 0) + 1

        for quote in quotes:
            fetched_at = self._intraday_timestamp(getattr(quote, "fetched_at", None))
            source_time = self._quote_source_timestamp(quote)
            if fetched_at is None or source_time is None:
                reject("missing_quote_timestamp")
                continue
            fetched_age = (current - fetched_at).total_seconds()
            provider_age = (current - source_time).total_seconds()
            if fetched_age < -allowed_future_skew:
                reject("future_fetched_at")
            elif fetched_age > max_age:
                reject("stale_fetched_at")
            elif provider_age < -allowed_future_skew:
                reject("future_provider_ts")
            elif provider_age > max_age:
                reject("stale_provider_ts")
            else:
                fresh.append(quote)
        source["last_rejections"] = reasons
        source["rejected_quotes"] = int(source.get("rejected_quotes", 0) or 0) + sum(reasons.values())
        return fresh

    @staticmethod
    def _quote_freshness_clock(_cycle_started_at: datetime) -> datetime:
        """Sample the local clock after quote fetches, not at cycle entry."""
        return datetime.now(CHINA_TZ)

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
        sina = self._source_health.get("sina", {})
        realtime = self._source_health.get("tushare_rt_k", {})
        return (
            f"行情健康：{state}\n"
            f"最近成功：{display(health['last_success_at'])}；最近错误：{display(health['last_error_at'])}；最近轮询：{display(health['last_cycle_at'])}\n"
            f"轮询 {health['cycles']} 次，成功 {health['successful_cycles']} 次，失败 {health['failed_cycles']} 次，"
            f"连续失败 {health['consecutive_failures']} 次\n"
            f"最近累计接收 {health['accepted_quotes']} 条有效行情，过期/丢弃 {health['stale_quotes']} 条；"
            f"分钟线 {self.minute_bars.symbol_count()} 只股票/{self.minute_bars.bar_count()} 根已完成\n"
            f"新浪行情批次：{sina['successes']}/{sina['batches']} 成功，失败 {sina['failures']} 次；"
            f"最近错误：{display(sina['last_error_at'])}\n"
            f"Tushare rt_k：模式 {realtime.get('mode', 'disabled')}；尝试 {realtime.get('attempts', 0)} 次，"
            f"成功 {realtime.get('successes', 0)} 次，失败 {realtime.get('failures', 0)} 次，跳过 {realtime.get('skipped', 0)} 次；"
            f"状态 {realtime.get('last_status', 'disabled')}；当前来源 {realtime.get('selected_source', 'sina')}\n"
            f"盘中目标：{health.get('selected_targets', 0)}；有效候选 {health.get('candidate_targets', 0)}；"
            f"过期候选 {health.get('expired_candidates', 0)}；配置重点 {health.get('focus_targets', 0)}；"
            f"超限省略 {health.get('dropped_targets', 0)}；累计事件 {health.get('triggered_events', 0)}\n"
            f"全市场环境：{self._market_label(health.get('market_regime', 'unknown'))}；"
            f"质量 {health.get('market_quality', 'unknown')}；样本 {health.get('market_sample_size', 0)}/"
            f"{health.get('market_expected_size', 0)}（覆盖 {float(health.get('market_coverage', 0) or 0):.1%}）；"
            f"原因 {health.get('market_reason', '暂无')}\n"
            f"最近状态：{health.get('last_state', 'unknown')}；invocation：{health.get('last_invocation_id') or '暂无'}"
        )

    def _intraday_status_text(self, origin: str) -> str:
        watch_count = len(self.store.list_watch(origin)) if origin else 0
        summary = self.store.intraday_delivery_summary(origin) if origin else {}
        states = self.store.recent_intraday_states(origin, 5) if origin else []
        reasons = [f"{row.get('code')}:{row.get('signal')}={row.get('last_reason')}" for row in states]
        intraday_checker = getattr(self.store, "is_intraday_enabled", None)
        intraday_enabled = bool(intraday_checker(origin)) if callable(intraday_checker) and origin else True
        active = bool(
            origin
            and self.store.is_subscribed(origin)
            and intraday_enabled
        )
        return (
            f"盯盘状态：{'运行' if active else '暂停'}\n"
            f"当前会话自选：{watch_count}；本轮总目标：{self._intraday_health.get('selected_targets', 0)}；"
            f"有效候选：{self._intraday_health.get('candidate_targets', 0)}；配置重点：{self._intraday_health.get('focus_targets', 0)}\n"
            f"本会话投递：待发{int(summary.get('pending', 0) or 0)}、发送中{int(summary.get('sending', 0) or 0)}、"
            f"已发{int(summary.get('sent', 0) or 0)}、失败{int(summary.get('failed', 0) or 0)}、"
            f"结果不明{int(summary.get('unknown_delivery', 0) or 0)}、取消{int(summary.get('cancelled', 0) or 0)}\n"
            f"最近判定：{'；'.join(reasons) if reasons else '暂无'}\n"
            + self._health_text()
        )

    async def _calendar_open(self, trade_date: str) -> bool | None:
        """Return True/False only for verified answers; None means unknown."""
        cached = await self._store_call(self.store.calendar_lookup, trade_date)
        if cached.get("freshness") == "fresh":
            state = str(cached.get("status") or "unknown")
            if state == "open":
                return True
            if state == "closed":
                return False
            # A fresh unknown is a deliberate negative cache result.  Do not
            # turn a short TTL into a request on every loop iteration.
            return None
        helper = getattr(self.quotes, "fetch_trade_calendar", None)
        provider_identity = self._calendar_provider_identity(helper if callable(helper) else self.quotes)

        async def resolve_online():
            online = None
            if callable(helper):
                try:
                    online = await helper(trade_date)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    online = None
            if online is not None:
                await self._store_call(
                    self.store.save_calendar,
                    trade_date,
                    online,
                    "tushare",
                    self._int("calendar_ttl_seconds", 86400, 60, 604800),
                )
                return online
            latest_helper = getattr(self.quotes, "fetch_eastmoney_latest_trade_date", None)
            latest = None
            if callable(latest_helper):
                try:
                    latest = await latest_helper()
                except asyncio.CancelledError:
                    raise
                except Exception:
                    latest = None
            if latest:
                # The index's last daily bar proves that this date was open
                # only when it exactly matches the requested date. A prior
                # bar cannot prove that today was closed.
                if latest == trade_date:
                    await self._store_call(self.store.save_calendar, trade_date, True, "eastmoney_index", self._int("calendar_ttl_seconds", 86400, 60, 604800))
                    return True
                await self._store_call(self.store.save_calendar, trade_date, None, "eastmoney_index", self._int("calendar_unknown_ttl_seconds", 900, 60, 86400))
                return None
            await self._store_call(self.store.save_calendar, trade_date, None, "unknown", self._int("calendar_unknown_ttl_seconds", 900, 60, 86400))
            # A weekday is not proof of an open A-share session. Do not start
            # a background scan or intraday listener when the calendar is
            # unknown.
            return None

        return await self._calendar_singleflight((provider_identity, "calendar_open", trade_date), resolve_online)

    @staticmethod
    def _snapshot_request_gate(request: dict, *, now: datetime | None = None) -> tuple[str, datetime | None]:
        """Return allow/retry/terminal without mutating persisted request state."""
        if not request:
            return "allow", None
        terminal_value = request.get("terminal")
        terminal = terminal_value is True or str(terminal_value or "").lower() in {"1", "true", "yes", "on"}
        if terminal or str(request.get("state") or "").lower() in {"complete", "terminal"}:
            return "terminal", None
        raw = str(request.get("next_retry_at") or "").strip()
        if not raw:
            return "allow", None
        try:
            retry_at = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=timezone.utc)
            retry_at = retry_at.astimezone(timezone.utc)
        except (TypeError, ValueError, OverflowError):
            # A corrupt retry marker must not be bypassed by a fresh request.
            return "retry", None
        current = now or datetime.now(timezone.utc)
        if current.tzinfo is None:
            current = current.replace(tzinfo=timezone.utc)
        current = current.astimezone(timezone.utc)
        return ("retry", retry_at) if current < retry_at else ("allow", retry_at)

    def _snapshot_lease_ttl(self) -> float:
        return self._float("tushare_snapshot_lease_ttl_seconds", 1800, 5, 86400)

    def _snapshot_lease_wait(self) -> tuple[float, float]:
        wait = self._float("tushare_snapshot_lease_wait_seconds", 30, 0, 300)
        poll = self._float("tushare_snapshot_lease_poll_seconds", 0.25, 0.01, 10)
        return wait, poll

    @staticmethod
    def _lease_acquired(value) -> bool:
        if not isinstance(value, dict) or not bool(value.get("acquired")) or not str(value.get("owner") or "").strip():
            return False
        try:
            return int(value.get("fence") or 0) > 0
        except (TypeError, ValueError, OverflowError):
            return False

    async def _wait_snapshot_lease(self, request_id: str, requested_date: str, owner: str, ttl_seconds: float, capability: dict | None = None):
        """Claim a durable snapshot lease or wait without touching providers."""
        capability = capability or self._snapshot_lease_capability(self.store)
        if not capability.get("supported"):
            if capability.get("legacy"):
                raise SnapshotLeaseCapabilityError("durable snapshot lease is unavailable")
            raise SnapshotLeaseCapabilityError(
                f"durable snapshot lease capability is unsafe: {capability.get('reason', 'unsupported')}"
            )
        claim_method = capability["claim"]
        state_method = capability["state"]
        claim = await self._store_call(
            claim_method,
            request_id,
            requested_date,
            owner,
            ttl_seconds=ttl_seconds,
        )
        if not isinstance(claim, dict):
            raise SnapshotLeaseCapabilityError("snapshot lease claim returned an invalid result")
        if self._lease_acquired(claim):
            return claim
        if str(claim.get("reason") or "").lower() == "terminal":
            return claim
        wait_seconds, poll_seconds = self._snapshot_lease_wait()
        deadline = asyncio.get_running_loop().time() + wait_seconds
        state = claim
        def terminal(value: dict | None) -> bool:
            if not isinstance(value, dict):
                return False
            return bool(value.get("terminal")) or str(value.get("state") or "").strip().lower() in {"complete", "terminal"}

        while bool(state.get("lease_active")) and not terminal(state) and asyncio.get_running_loop().time() < deadline:
            remaining = max(0.0, deadline - asyncio.get_running_loop().time())
            await asyncio.sleep(min(poll_seconds, remaining))
            state = await self._store_call(state_method, request_id)
            if not state:
                break
            if not isinstance(state, dict):
                raise SnapshotLeaseCapabilityError("snapshot lease state returned an invalid result")
            if not bool(state.get("lease_active")) or terminal(state):
                break
        if state and bool(state.get("lease_active")):
            if not terminal(state):
                state = dict(state)
                state["reason"] = "wait_timeout"
                return state
        if state and terminal(state):
            state = dict(state)
            state["reason"] = "terminal"
            return state
        return await self._store_call(
            claim_method,
            request_id,
            requested_date,
            owner,
            ttl_seconds=ttl_seconds,
        )

    async def _snapshot_lease_renewer(self, context: dict, lost: asyncio.Event) -> None:
        interval = max(1.0, min(float(context["ttl_seconds"]) / 3.0, 60.0))
        while True:
            await asyncio.sleep(interval)
            try:
                renewed = await self._store_call(
                    context["renew_method"],
                    context["request_id"],
                    context["owner"],
                    context["fence"],
                    ttl_seconds=context["ttl_seconds"],
                )
            except asyncio.CancelledError:
                raise
            except Exception:
                renewed = None
            if renewed is None or renewed is False:
                lost.set()
                return

    async def _snapshot_lease_guard(self, context: dict, lost: asyncio.Event) -> None:
        if lost.is_set():
            raise SnapshotLeaseLostError("snapshot lease was lost")
        try:
            renewed = await self._store_call(
                context["renew_method"],
                context["request_id"],
                context["owner"],
                context["fence"],
                ttl_seconds=context["ttl_seconds"],
            )
        except Exception as exc:
            lost.set()
            raise SnapshotLeaseLostError("snapshot lease renewal failed") from exc
        if renewed is None or renewed is False:
            lost.set()
            raise SnapshotLeaseLostError("snapshot lease was lost")

    async def _cache_only_tushare_snapshot(self, trade_date: str) -> tuple[list, bool, str]:
        """Read the configured raw cache without consulting any provider."""
        request = await self._store_call(self.store.snapshot_request, f"daily_snapshot:{trade_date}") or {}
        evidence = request.get("calendar_evidence_json") or {}
        if isinstance(evidence, str):
            try:
                evidence = json.loads(evidence)
            except (TypeError, ValueError):
                evidence = {}
        cache_as_of = trade_date
        diagnostics = self._raw_diagnostics()
        if isinstance(evidence, dict):
            target = self._canonical_screen_date(evidence.get("calendar_target_date") or evidence.get("target"))
            try:
                reusable = target and self._calendar_evidence_final(evidence, trade_date)
            except (TypeError, ValueError):
                reusable = False
            if reusable:
                cache_as_of = target
                diagnostics.update(evidence)
                diagnostics.update({"calendar_resolved": True, "calendar_target_date": target})
        failure_kind = str(request.get("failure_kind") or "")
        if failure_kind and not diagnostics.get("calendar_resolved"):
            diagnostics["failure_kind"] = failure_kind
            if failure_kind == "calendar_endpoint_unavailable":
                diagnostics["calendar_endpoint_unavailable"] = True
        quotes, actual_date, provenance, state = await self._fresh_raw_snapshot_async(
            trade_date,
            cache_as_of=cache_as_of,
            max_stale_trading_days=self._raw_stale_days(),
        )
        diagnostics["cache_state"] = state
        if state == "cache_basis_rejected":
            diagnostics["cache_basis_rejected"] = int(diagnostics.get("cache_basis_rejected", 0)) + 1
        if not quotes or not actual_date:
            diagnostics["history_unavailable"] = True
            diagnostics["fallback_allowed"] = False
            self._last_screen_diagnostics = diagnostics
            return [], False, trade_date
        diagnostics.update({
            "raw_cache": True,
            "raw_batch_id": provenance.get("batch_id") or provenance.get("active_batch_id"),
            "raw_generation": provenance.get("generation"),
            "actual_trade_date": actual_date,
            "fallback_allowed": False,
            "history_unavailable": False,
        })
        if actual_date == cache_as_of:
            self._daily_retry_after = None
        self._raw_screen_provenance = provenance
        self._last_screen_diagnostics = diagnostics
        quotes = await self._apply_cached_daily_risk_evidence(
            actual_date,
            str(provenance.get("batch_id") or provenance.get("active_batch_id") or ""),
            quotes,
        )
        return quotes, False, actual_date

    async def _daily_snapshot(self, trade_date: str) -> tuple[list, bool, str]:
        """Return a snapshot, whether it was fetched now, and its actual trade date."""
        canonical_trade_date = self._canonical_screen_date(trade_date)
        today = datetime.now(CHINA_TZ).date().isoformat()
        if not canonical_trade_date or canonical_trade_date > today:
            self._last_screen_diagnostics = {
                "invalid_date": True,
                "future_date": bool(canonical_trade_date and canonical_trade_date > today),
                "fallback_allowed": False,
            }
            raise ValueError("snapshot requested date is invalid or in the future")
        trade_date = canonical_trade_date
        if self._tushare_mode():
            return await self._daily_snapshot_tushare(trade_date)
        request_id = f"daily_snapshot:{trade_date}"
        request = await self._store_call(self.store.snapshot_request, request_id) or {}
        gate, retry_at = self._snapshot_request_gate(request)
        lookup_date = self._daily_date_alias.get(trade_date, trade_date)
        cached = await self._store_call(self.store.daily_quotes, lookup_date)
        cached_meta = await self._store_call(self.store.snapshot_meta, lookup_date)
        if gate != "allow":
            self._daily_retry_after = retry_at or (datetime.now(CHINA_TZ) + timedelta(minutes=5))
            if cached:
                return cached, False, lookup_date
            actual_date = await self._store_call(self.store.latest_daily_trade_date, trade_date)
            if actual_date:
                fallback = await self._store_call(self.store.daily_quotes, actual_date)
                if fallback:
                    return fallback, False, actual_date
            return [], False, trade_date
        if cached and lookup_date != trade_date:
            return cached, False, lookup_date
        if cached and cached_meta and bool(cached_meta.get("complete")):
            cached = await self._enrich_daily_snapshot_risk_fields(lookup_date, cached, persist=True)
            return cached, False, lookup_date
        async with self._daily_snapshot_lock:
            # Re-check after waiting so the background loop and manual command do not fetch twice.
            lookup_date = self._daily_date_alias.get(trade_date, trade_date)
            cached = await self._store_call(self.store.daily_quotes, lookup_date)
            cached_meta = await self._store_call(self.store.snapshot_meta, lookup_date)
            if cached and lookup_date != trade_date:
                self._daily_retry_after = None
                return cached, False, lookup_date
            if cached and cached_meta and bool(cached_meta.get("complete")):
                self._daily_retry_after = None
                cached = await self._enrich_daily_snapshot_risk_fields(lookup_date, cached, persist=True)
                return cached, False, lookup_date
            request = await self._store_call(self.store.snapshot_request, request_id) or {}
            gate, retry_at = self._snapshot_request_gate(request)
            if gate != "allow":
                self._daily_retry_after = retry_at or (datetime.now(CHINA_TZ) + timedelta(minutes=5))
                if cached:
                    return cached, False, lookup_date
                actual_date = await self._store_call(self.store.latest_daily_trade_date, trade_date)
                if actual_date:
                    fallback = await self._store_call(self.store.daily_quotes, actual_date)
                    if fallback:
                        return fallback, False, actual_date
                return [], False, trade_date
            attempts = int(request.get("attempts") or 0) + 1
            await self._store_call(
                self.store.save_snapshot_request,
                request_id,
                trade_date,
                state="fetching",
                attempts=attempts,
                source=str(request.get("source") or ""),
                quality=str(request.get("quality") or "unknown"),
            )
            try:
                result = await self.quotes.fetch_market_snapshot_result(
                    str(self.config.get("daily_market_url", "")), trade_date
                )
                minimum = self._int("daily_snapshot_min_size", 4000, 1000, 10000)
                valid_codes = {quote.code for quote in result.quotes if str(getattr(quote, "code", "")).isdigit() and len(str(quote.code)) == 6 and float(getattr(quote, "price", 0) or 0) > 0}
                if result.quotes and len(valid_codes) < minimum:
                    result.quality = "partial"
                await self._store_call(
                    self.store.update_provider_health,
                    result.source or "unknown",
                    bool(result.quotes),
                    result.quality,
                )
            except Exception:
                await self._store_call(self.store.update_provider_health, "unknown", False, "unknown", "fetch failed")
                result = None
            if result is None or not result.quotes:
                retry_at = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                await self._store_call(
                    self.store.save_snapshot_request,
                    request_id, trade_date, state="failed", attempts=attempts,
                    source=(result.source if result else "unknown"),
                    quality=(result.quality if result else "unknown"),
                    last_error="snapshot source returned no usable quotes",
                    next_retry_at=retry_at.isoformat(), terminal=False,
                )
                await self._store_call(
                    self.store.save_snapshot_meta,
                    trade_date, result.source if result else "unknown", result.quality if result else "unknown",
                    False, trade_date, "快照为空，保留旧行情并等待重试", attempts=attempts,
                    last_error="snapshot source returned no usable quotes", next_retry_at=retry_at.isoformat(),
                    terminal=False, state="failed",
                )
                # A transient source failure should not discard a usable prior snapshot.
                actual_date = await self._store_call(self.store.latest_daily_trade_date, trade_date)
                if actual_date:
                    fallback = await self._store_call(self.store.daily_quotes, actual_date)
                    if fallback:
                        self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                        await self._store_call(self.store.save_snapshot_request, request_id, trade_date, actual_trade_date=actual_date, state="retry", attempts=attempts, source="cache", quality="cached", next_retry_at=self._daily_retry_after.isoformat(), terminal=False)
                        logger.warning("[%s] 当日快照不可用，使用最近缓存交易日：%s", PLUGIN_NAME, actual_date)
                        return fallback, False, actual_date
                if result is None:
                    raise RuntimeError("daily snapshot source unavailable")
                return [], True, trade_date
            actual_date = result.trade_date
            if not actual_date:
                cached_date = await self._store_call(self.store.latest_daily_trade_date, trade_date)
                if cached_date:
                    self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                    await self._store_call(self.store.save_snapshot_request, request_id, trade_date, actual_trade_date=cached_date, state="retry", attempts=attempts, source="cache", quality="cached", next_retry_at=self._daily_retry_after.isoformat(), terminal=False)
                    return await self._store_call(self.store.daily_quotes, cached_date), False, cached_date
                if result.source == "eastmoney":
                    try:
                        actual_date = await self.quotes.fetch_eastmoney_latest_trade_date()
                    except Exception:
                        actual_date = None
                    if not actual_date:
                        self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                        await self._store_call(self.store.save_snapshot_request, request_id, trade_date, state="unknown", attempts=attempts, source=result.source, quality="unknown", last_error="actual trade date not verified", next_retry_at=self._daily_retry_after.isoformat(), terminal=False)
                        logger.warning("[%s] 东方财富快照无法验证交易日，拒绝缓存", PLUGIN_NAME)
                        return [], True, trade_date
                    if actual_date != trade_date:
                        self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                        await self._store_call(self.store.save_snapshot_request, request_id, trade_date, state="unknown", attempts=attempts, source=result.source, quality="unknown", last_error="latest index bar is not the requested date", next_retry_at=self._daily_retry_after.isoformat(), terminal=False)
                        logger.warning("[%s] 东方财富最近日线为 %s，不足以证明请求日 %s，拒绝缓存", PLUGIN_NAME, actual_date, trade_date)
                        return [], True, trade_date
                    result.quality = "degraded"
                    logger.warning("[%s] 东方财富快照交易日由指数日线验证为 %s，标记 degraded", PLUGIN_NAME, actual_date)
                else:
                    self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                    await self._store_call(self.store.save_snapshot_request, request_id, trade_date, state="unknown", attempts=attempts, source=result.source, quality="unknown", last_error="actual trade date not returned", next_retry_at=self._daily_retry_after.isoformat(), terminal=False)
                    return [], True, trade_date
            prior_meta = await self._store_call(self.store.snapshot_meta, actual_date) or {}
            if str(prior_meta.get("quality") or "").lower() == "good" and result.quality != "good":
                saved = len(await self._store_call(self.store.daily_quotes, actual_date))
            else:
                saved = await self._store_call(
                    self.store.save_daily_quotes,
                    actual_date, result.quotes, self._int("daily_cache_keep_days", 180, 7, 730)
                )
            complete = actual_date == trade_date and result.quality == "good" and saved >= self._int("daily_snapshot_min_size", 4000, 1000, 10000)
            await self._store_call(
                self.store.save_snapshot_meta,
                actual_date, result.source, result.quality, complete, trade_date,
                "" if complete else "交易日回退或来源未确认，不能视为当日完整收盘快照",
                attempts=attempts, state="complete" if complete else "partial",
                next_retry_at=None if complete else (datetime.now(CHINA_TZ) + timedelta(minutes=5)).isoformat(),
                terminal=complete,
            )
            await self._store_call(
                self.store.save_snapshot_request,
                request_id, trade_date, actual_trade_date=actual_date,
                state="complete" if complete else "partial", attempts=attempts,
                source=result.source, quality=result.quality,
                next_retry_at=None if complete else (datetime.now(CHINA_TZ) + timedelta(minutes=5)).isoformat(),
                terminal=complete,
            )
            logger.info("[%s] 全市场日快照已缓存：%s 只", PLUGIN_NAME, saved)
            if complete:
                self._daily_date_alias[trade_date] = actual_date
                self._daily_retry_after = None
                fetched = True
            else:
                self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
                fetched = False
            return await self._store_call(self.store.daily_quotes, actual_date), fetched, actual_date

    async def _daily_candidates_result(self, limit: int, *, invocation_id: str = "") -> ScreenScoreResult:
        if self._tushare_mode():
            trade_date = datetime.now(CHINA_TZ).date().isoformat()
            try:
                cached, _, actual_date = await self._daily_snapshot(trade_date)
                base_diagnostics = dict(self._last_screen_diagnostics or {})
            except Exception:
                logger.exception("[%s] Tushare raw 快照失败", PLUGIN_NAME)
                cached, actual_date = [], trade_date
            if cached:
                if invocation_id.startswith("automatic_close:") and actual_date == trade_date:
                    batch_id = str(base_diagnostics.get("raw_batch_id") or "")
                    cached = await self._collect_automatic_close_risk_evidence(actual_date, batch_id, cached)
                return await self._score_quotes_result(
                    cached,
                    limit,
                    actual_date or trade_date,
                    context="daily_close",
                    requested_date=trade_date,
                    base_diagnostics=base_diagnostics,
                    invocation_id=invocation_id,
                )
            base_diagnostics = dict(locals().get("base_diagnostics", {}))
            base_diagnostics.setdefault("history_unavailable", True)
            return ScreenScoreResult.build((), base_diagnostics)
        if self._bool("daily_cache_enabled", True):
            trade_date = datetime.now(CHINA_TZ).date().isoformat()
            try:
                cached, _, actual_date = await self._daily_snapshot(trade_date)
                base_diagnostics = dict(self._last_screen_diagnostics or {})
            except Exception:
                logger.exception("[%s] 全市场日快照失败，退回股票池扫描", PLUGIN_NAME)
                cached = []
                actual_date = trade_date
            if cached:
                return await self._score_quotes_result(
                    cached,
                    limit,
                    actual_date or trade_date,
                    context="daily_close",
                    requested_date=trade_date,
                    base_diagnostics=base_diagnostics,
                    invocation_id=invocation_id,
                )
        return await self._scan_result(
            self._universe(),
            limit,
            record=False,
            job_name="daily_screen",
            invocation_id=invocation_id,
        )

    async def _daily_candidates(self, limit: int):
        result = await self._daily_candidates_result(limit)
        self._last_screen_diagnostics = dict(result.diagnostics)
        return list(result.candidates)

    async def _eastmoney_transient_preview(self, requested_date: str, limit: int):
        """Build an EM-only candidate preview after a classified Tushare miss."""
        canonical_requested = self._canonical_screen_date(requested_date)
        today = datetime.now(CHINA_TZ).date().isoformat()
        if not canonical_requested or canonical_requested > today:
            raise ValueError("Eastmoney fallback requested date is invalid or in the future")
        requested_date = canonical_requested
        base_diagnostics = dict(self._last_screen_diagnostics or {})
        fallback_owner = str(base_diagnostics.get("fallback_owner") or "").strip()
        authorized_owner = str(getattr(self, "_authorized_fallback_owner", None) or "").strip()
        invocation_id = authorized_owner or fallback_owner or uuid.uuid4().hex

        def unavailable(reason: str, *, actual_date: str = "", **extra):
            diagnostics = {
                **base_diagnostics,
                "data_mode": "eastmoney_transient",
                "source": "eastmoney_fallback",
                "quality": "degraded",
                "complete": False,
                "requested_date": requested_date,
                "actual_trade_date": actual_date,
                "diagnostics_invocation_id": invocation_id,
                "diagnostics_requested_date": requested_date,
                "diagnostics_actual_date": actual_date,
                "degraded_unavailable": True,
                "degraded_reason": reason,
                **extra,
            }
            self._last_screen_diagnostics = dict(diagnostics)
            return [], None, MappingProxyType(diagnostics)

        if fallback_owner:
            # New Tushare failures carry a one-shot owner token.  A different
            # Main instance, or a replay after consumption, cannot start EM.
            if not authorized_owner or fallback_owner != authorized_owner:
                return unavailable("fallback_owner_lost", fallback_allowed=False)
            self._authorized_fallback_owner = None
        fetch = getattr(self.quotes, "fetch_eastmoney_fallback_result", None)
        if not callable(fetch):
            fetch = getattr(self.quotes, "fetch_eastmoney_snapshot_result", None)
        if not callable(fetch):
            return unavailable("snapshot_provider_missing")
        try:
            result = await fetch(requested_date)
        except asyncio.CancelledError:
            raise
        except (httpx.HTTPError, asyncio.TimeoutError, OSError, ValueError, TypeError, KeyError) as exc:
            return unavailable("snapshot_network", error=str(exc)[:240])
        except Exception:
            raise
        result_diagnostics = self._bulk_value(result, "diagnostics", {})
        result_diagnostics = result_diagnostics if isinstance(result_diagnostics, dict) else {}
        result_source = str(self._bulk_value(result, "source", "") or "").strip().lower()
        actual = self._canonical_screen_date(
            self._bulk_value(result, "trade_date", self._bulk_value(result, "actual_date", ""))
        )
        if not actual or actual > requested_date or self._diagnostic_flag(result_diagnostics.get("future_date")):
            return unavailable(
                "actual_date_unverified" if actual is None else "future_date",
                actual_date=actual or "",
                **result_diagnostics,
            )
        quotes = list(self._bulk_value(result, "quotes", []) or [])
        # The provider contract is EM-only.  A mixed or unknown source is an
        # integrity failure for this preview, not an invitation to guess.
        sources = {str(getattr(quote, "source", "") or "").strip().lower() for quote in quotes}
        sources.discard("")
        if result_source and result_source not in {"eastmoney", "eastmoney_fallback"}:
            sources.add(result_source)
        if sources and not sources.issubset({"eastmoney", "eastmoney_fallback"}):
            return unavailable("mixed_source", actual_date=actual)
        score_result = await self._score_quotes_result(
            quotes,
            limit,
            actual,
            include_factors=False,
            context="daily_close",
            data_mode="eastmoney_transient",
            requested_date=requested_date,
            base_diagnostics={**base_diagnostics, **result_diagnostics},
            invocation_id=invocation_id,
        )
        diagnostics = dict(score_result.diagnostics)
        diagnostics.update({
            "requested_date": requested_date,
            "actual_trade_date": actual,
            "source": "eastmoney_fallback",
            "quality": "degraded",
            "complete": False,
            "provider_date_verified": bool(result_diagnostics.get("date_verified", True)),
            "fallback_provenance": {
                "mode": "transient",
                "source": "eastmoney",
                "requested_date": requested_date,
                "actual_trade_date": actual,
                "date_verified": bool(result_diagnostics.get("date_verified", True)),
            },
        })
        return list(score_result.candidates), actual, MappingProxyType(diagnostics)

    def _configured_shadow_report(self, requested_date: str, actual_date: str | None) -> str:
        actual_label = self._canonical_screen_date(actual_date) or "未确认"
        return (
            "全市场选股｜Tushare shadow-only\n"
            f"raw 批次已完成校验，但按配置未切换 active｜请求日期：{requested_date}｜批次日期：{actual_label}\n"
            "当前未使用 shadow 批次进行筛选，也未调用东方财富。请将 tushare_raw_publish_enabled 设为 true 后重试。"
        )

    def _eastmoney_fallback_report(
        self,
        requested_date: str,
        actual_date: str | None,
        quotes,
        candidates,
        diagnostics: Mapping[str, object] | None = None,
    ) -> str:
        """Render a transient preview without consulting persisted reports."""
        diagnostics = dict(diagnostics if diagnostics is not None else self._last_screen_diagnostics)
        actual_label = actual_date or "未验证"
        lines = [
            "全市场选股｜东方财富临时降级预览",
            f"已回退东方财富｜请求日期：{requested_date}｜实际交易日：{actual_label}",
            f"行情：东方财富 · 降级｜同批快照 {int(diagnostics.get('snapshot_count', len(quotes or [])) or 0)} 只",
        ]
        if diagnostics.get("market_regime"):
            lines.append(
                f"市场：{self._market_label(diagnostics.get('market_regime'))}｜上涨占比 {float(diagnostics.get('market_breadth', 0) or 0):.1%}"
            )
        if diagnostics.get("degraded_unavailable"):
            reason = {
                "snapshot_coverage": "全市场快照数量不足",
                "history_coverage": "历史日线覆盖不足",
                "history_provider_missing": "历史日线接口不可用",
                "snapshot_provider_missing": "东方财富快照接口不可用",
                "snapshot_network": "东方财富快照请求失败",
                "actual_date_unverified": "实际交易日未能验证",
                "future_date": "实际交易日校验失败",
                "mixed_source": "快照来源不一致",
            }.get(str(diagnostics.get("degraded_reason") or ""), "降级数据未达到完整评分条件")
            lines.append(f"降级数据不可用：{reason}，本次不报告候选数量结论。")
        elif candidates:
            lines.append(f"候选：{len(candidates)} 只（仅供本次预览，均需人工复核）")
            for index, item in enumerate(candidates[: self._int("report_candidate_limit", 10, 1, 30)], 1):
                if index > 1:
                    lines.append("")
                lines.extend(format_compact_candidate(item, index).splitlines())
        else:
            lines.append("完整评分完成：0 候选；结论仅覆盖本批已验证的东方财富数据。")
        lines.append("仅本次预览：不写入候选池、不更新收盘计划、不用于回放；可再次执行 /全市场选股 重试。")
        return "\n".join(lines)

    async def _record_screen(
        self,
        requested_date: str,
        actual_date: str,
        source: str,
        quotes,
        candidates,
        status: str = "completed",
        quality: str = "good",
        error: str | None = None,
        *,
        job_name: str = "daily_screen",
        diagnostics: Mapping[str, object] | None = None,
        publication_key: str = "",
        publication_payload: str = "",
        publication_invocation_id: str = "",
        publication_origins=None,
    ) -> str:
        requested_value = self._canonical_screen_date(requested_date)
        actual_value = self._canonical_screen_date(actual_date) if actual_date else None
        today_value = datetime.now(CHINA_TZ).date().isoformat()
        if not requested_value or not actual_value:
            raise ValueError("screen run dates must be canonical")
        if requested_value > today_value or actual_value > today_value or actual_value > requested_value:
            raise ValueError("screen run dates violate requested/actual bounds")
        requested_date, actual_date = requested_value, actual_value
        run_id = uuid.uuid4().hex
        now = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
        diagnostics = dict(diagnostics if diagnostics is not None else self._last_screen_diagnostics)
        try:
            coverage = max(0.0, min(1.0, float(diagnostics.get("indicator_coverage", 0.0) or 0.0)))
        except (TypeError, ValueError):
            coverage = 0.0
        report_key = f"{job_name}:{actual_date or requested_date}"
        options = {
            "diagnostics": diagnostics,
            "coverage": coverage,
            "deep_screen_count": int(diagnostics.get("indicator_targets", 0) or 0),
            "factor_screen_count": int(diagnostics.get("factor_screen_count", 0) or 0),
            "report_key": report_key,
            "report_version": 0,
            "valid_until": self._candidate_valid_until(actual_date or requested_date),
            "coverage_floor": self._float("screen_min_indicator_coverage", 0.8, 0.0, 1.0),
        }
        if publication_key:
            options.update({
                "publication_key": publication_key,
                "publication_payload": publication_payload,
                "publication_invocation_id": publication_invocation_id,
                "publication_origins": list(publication_origins or ()),
            })
        result = await self._store_call(
            self.store.save_screen_bundle_atomic,
            (run_id, job_name, requested_date, actual_date, source, now, now, len(quotes), len(candidates), status, quality, error),
            candidates,
            **options,
        )
        self._last_screen_report_claimed = bool(result.get("report_claimed"))
        shadow_default = self._bool("llm_annotation_enabled", False)
        if (
            self._last_screen_report_claimed
            and status == "completed"
            and candidates
            and job_name in {"daily_screen", "automatic_close"}
            and self._bool("llm_shadow_enabled", shadow_default)
        ):
            try:
                shadow = await self._run_recommendation_ai_shadow_review(str(result.get("run_id") or run_id), candidates)
                logger.info(
                    "[%s] AI影子评审完成：run=%s status=%s reason=%s",
                    PLUGIN_NAME,
                    str(result.get("run_id") or run_id),
                    shadow.get("status"),
                    shadow.get("reason") or "",
                )
            except (sqlite3.Error, httpx.HTTPError, ValueError, TypeError, KeyError, RuntimeError):
                logger.exception("[%s] AI影子评审失败，不影响规则候选", PLUGIN_NAME)
        # Close screening is an appropriate deterministic checkpoint for
        # mature recommendations. This is a local DB-only pass: commands only
        # read its durable output and no future/provider data is requested.
        evaluator = getattr(self.store, "evaluate_recommendation_outcomes", None)
        if callable(evaluator):
            try:
                await self._store_call(evaluator)
            except (sqlite3.Error, ValueError, TypeError, OverflowError):
                logger.warning("[%s] recommendation outcome checkpoint deferred", PLUGIN_NAME)
        return str(result.get("run_id") or run_id) if self._last_screen_report_claimed else None

    @staticmethod
    def _daily_signal_scope(_actual_date: str) -> str:
        """Keep scheduled daily signal cooldown stable across screen runs."""
        return "daily-scheduler"

    def _automatic_close_time(self) -> time:
        raw = str(self.config.get("daily_scan_time", "15:10") or "15:10").strip()
        try:
            hour, minute = (int(value) for value in raw.split(":", 1))
            configured = time(hour, minute)
        except (TypeError, ValueError, OverflowError):
            configured = time(15, 10)
        return configured

    def _automatic_close_phase(self, now: datetime) -> str:
        current = now.astimezone(CHINA_TZ)
        if current.weekday() >= 5:
            return "weekend"
        if current.time() < self._automatic_close_time():
            return "before_close"
        if self._daily_retry_after is not None and current < self._daily_retry_after:
            return "backoff"
        return "calendar_check"

    def _daily_acceptance_time(self) -> time:
        raw = str(self.config.get("daily_acceptance_time", "15:40") or "15:40").strip()
        try:
            hour, minute = (int(value) for value in raw.split(":", 1))
            configured = time(hour, minute)
        except (TypeError, ValueError, OverflowError):
            configured = time(15, 40)
        return max(configured, self._automatic_close_time())

    @staticmethod
    def _daily_acceptance_message(result: Mapping[str, object]) -> str:
        recovered = str(result.get("event_kind") or "") == "recovery"
        lines = [
            "每日链路恢复" if recovered else "每日链路主动告警",
            f"交易日：{result.get('trade_date') or 'unknown'}",
            f"状态：{result.get('status') or 'unknown'}",
        ]
        findings = result.get("findings") if isinstance(result.get("findings"), list) else []
        if recovered:
            lines.append("结果：此前异常已消失，本轮验收通过。")
        else:
            for item in findings[:10]:
                if not isinstance(item, Mapping):
                    continue
                count = int(item.get("count") or 0)
                suffix = f"（{count}项）" if count else ""
                lines.append(f"- {item.get('message') or item.get('code') or 'unknown'}{suffix}")
        lines.extend([
            "处理边界：异常来自本地持久化证据；缺失或不可证明保持 unknown，不会补造结果。",
            "仅研究/模拟盘，不自动下单。",
        ])
        return "\n".join(lines)

    async def _dispatch_daily_acceptance_alert(self, alert: Mapping[str, object]) -> str:
        alert_id = str(alert.get("alert_id") or "")
        origin = str(alert.get("origin") or "")
        if not alert_id or not origin:
            return "invalid"
        if not await self._store_call(self.store.is_subscribed, origin) or not self._push_allowed(origin):
            await self._store_call(self.store.cancel_daily_acceptance_alert, alert_id, "destination is no longer subscribed or allowed")
            return "cancelled"
        claim = await self._store_call(
            self.store.claim_daily_acceptance_alert,
            alert_id,
            self._daily_acceptance_owner,
            ttl_seconds=self._float("daily_acceptance_alert_lease_seconds", 120, 5, 3600),
        )
        if not claim.get("acquired"):
            return str(claim.get("reason") or claim.get("state") or "skipped")
        outcome = await self._send_claimed_delivery_once("daily_acceptance", alert_id, claim)
        fence = int(claim.get("fence") or claim.get("lease_fence") or 0)
        if outcome.state == "unknown_delivery":
            await self._store_call(
                self.store.mark_daily_acceptance_alert_unknown,
                alert_id,
                self._daily_acceptance_owner,
                fence,
                error=outcome.error,
            )
            logger.warning("[%s] 每日验收告警投递结果不明，禁止自动重发：alert=%s origin=%s", PLUGIN_NAME, alert_id, origin)
            return "unknown_delivery"
        sent = outcome.state == "sent"
        try:
            finished = await self._store_call(
                self.store.finish_daily_acceptance_alert,
                alert_id,
                self._daily_acceptance_owner,
                fence,
                sent=sent,
                error="" if sent else outcome.error,
                retry_after_seconds=self._float("daily_acceptance_alert_retry_seconds", 60, 1, 3600),
                max_attempts=self._int("daily_acceptance_alert_max_attempts", 5, 1, 20),
                retry_window_seconds=self._int("daily_acceptance_alert_retry_window_seconds", 3600, 60, 86400),
            )
        except Exception:
            if sent:
                logger.exception("[%s] 每日验收告警已发送但 ACK 持久化不明，停止自动重发：alert=%s", PLUGIN_NAME, alert_id)
                return "unknown_delivery"
            raise
        return str(finished.get("state") or ("sent" if sent else "failed"))

    async def _recover_daily_acceptance_alerts(self) -> dict[str, int]:
        loader = getattr(self.store, "recoverable_daily_acceptance_alerts", None)
        summary_loader = getattr(self.store, "daily_acceptance_alert_summary", None)
        if not callable(loader) or not callable(summary_loader):
            return {}
        alerts = await self._store_call(
            loader,
            limit=100,
            max_attempts=self._int("daily_acceptance_alert_max_attempts", 5, 1, 20),
            retry_window_seconds=self._int("daily_acceptance_alert_retry_window_seconds", 3600, 60, 86400),
        )
        for alert in alerts:
            try:
                await self._dispatch_daily_acceptance_alert(alert)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[%s] 每日验收告警恢复失败：alert=%s", PLUGIN_NAME, alert.get("alert_id", ""))
        summary = await self._store_call(summary_loader)
        unknown_count = int(summary.get("unknown_delivery", 0) or 0)
        if unknown_count and unknown_count != int(getattr(self, "_last_daily_acceptance_unknown_count", 0) or 0):
            logger.warning("[%s] 每日验收告警存在 %s 条 unknown_delivery，需人工核对", PLUGIN_NAME, unknown_count)
        self._last_daily_acceptance_unknown_count = unknown_count
        return summary

    async def _daily_acceptance_tick(self, now: datetime | None = None) -> dict[str, object]:
        if not self._bool("daily_acceptance_enabled", True):
            return {"state": "disabled"}
        current = (now or datetime.now(CHINA_TZ)).astimezone(CHINA_TZ)
        if current.weekday() >= 5:
            return {"state": "weekend"}
        if current.time() < self._daily_acceptance_time():
            return {"state": "before_acceptance"}
        trade_date = current.date().isoformat()
        calendar = await self._calendar_open(trade_date)
        if calendar is False:
            return {"state": "calendar_closed"}
        evaluator = getattr(self.store, "evaluate_daily_acceptance", None)
        if not callable(evaluator):
            return {"state": "unsupported"}
        destinations = (
            [origin for origin in await self._store_call(self.store.subscriptions) if self._push_allowed(origin)]
            if self._bool("daily_acceptance_alert_enabled", True) else []
        )
        result = await self._store_call(
            evaluator,
            trade_date,
            now=current,
            calendar_state=calendar,
            ai_expected=self._bool("llm_shadow_enabled", self._bool("llm_annotation_enabled", False)),
            ai_pending_stale_minutes=self._int("daily_acceptance_ai_pending_minutes", 30, 1, 1440),
            outcome_overdue_days=self._int("daily_acceptance_outcome_overdue_days", 2, 0, 30),
            alert_origins=destinations,
            alert_message=self._daily_acceptance_message,
        )
        await self._recover_daily_acceptance_alerts()
        return {"state": str(result.get("status") or "unknown"), **dict(result)}

    @staticmethod
    def _automatic_publication_key(actual_date: str) -> str:
        return f"automatic_close:{actual_date}"

    def _automatic_job_limits(self) -> tuple[int, int, int]:
        return (
            self._int("automatic_close_max_attempts", 6, 1, 48),
            self._int("automatic_close_retry_window_seconds", 14400, 300, 86400),
            self._int("automatic_close_retry_seconds", 300, 30, 3600),
        )

    async def _finish_automatic_close_job(self, job_key: str, status: str, error: str | None = None) -> dict:
        max_attempts, retry_window, retry_seconds = self._automatic_job_limits()
        finisher = getattr(self.store, "finish_automatic_close_job", None)
        if callable(finisher):
            result = await self._store_call(
                finisher,
                job_key,
                status=status,
                error=error,
                retry_after_seconds=retry_seconds,
                max_attempts=max_attempts,
                retry_window_seconds=retry_window,
            )
            return dict(result or {})
        await self._store_call(self.store.finish_job, job_key, status, error)
        return {"job_key": job_key, "status": status}

    async def _send_claimed_delivery_once(self, kind: str, key: str, claim: Mapping[str, object]) -> AutomaticDeliverySendResult:
        owners = {
            "automatic_close": getattr(self, "_automatic_delivery_owner", ""),
            "intraday": getattr(self, "_intraday_delivery_owner", ""),
            "daily_acceptance": getattr(self, "_daily_acceptance_owner", ""),
        }
        owner = owners.get(kind)
        if not owner:
            return AutomaticDeliverySendResult("failed", "delivery owner unavailable before send")
        try:
            recorded = await self._store_call(
                self.store.record_delivery_capabilities, kind, key, owner,
                int(claim.get("fence") or claim.get("lease_fence") or 0),
                delivery_capabilities(getattr(self, "context", None)),
            )
            if not recorded:
                return AutomaticDeliverySendResult("failed", "capability evidence lease lost before send")
        except Exception as exc:
            return AutomaticDeliverySendResult("failed", f"capability evidence unavailable before send: {type(exc).__name__}")
        return await self._send_automatic_delivery_once(str(claim.get("origin") or ""), str(claim.get("payload") or ""))

    async def _send_automatic_delivery_once(self, origin: str, text: str) -> AutomaticDeliverySendResult:
        """Send each chunk once; any exception after invocation is ambiguous."""
        sender = getattr(getattr(self, "context", None), "send_message", None)
        if not callable(sender):
            return AutomaticDeliverySendResult("failed", "send_message unavailable before send")
        try:
            chunks = self._message_chunks(text)
        except Exception as exc:
            return AutomaticDeliverySendResult("failed", f"payload preparation failed: {type(exc).__name__}: {exc}")
        accepted = 0
        for index, chunk in enumerate(chunks, start=1):
            try:
                result = await sender(origin, MessageChain([Plain(chunk)]))
                if result is False:
                    return AutomaticDeliverySendResult(
                        "unknown_delivery", "sender returned False; delivery not confirmed",
                        attempted_chunks=index, accepted_chunks=accepted,
                    )
            except Exception as exc:
                return AutomaticDeliverySendResult(
                    "unknown_delivery",
                    f"send outcome unknown at chunk {index}/{len(chunks)} after {accepted} accepted: {type(exc).__name__}: {exc}",
                    attempted_chunks=index,
                    accepted_chunks=accepted,
                )
            accepted += 1
        return AutomaticDeliverySendResult("sent", attempted_chunks=len(chunks), accepted_chunks=accepted)

    async def _dispatch_automatic_delivery(self, delivery: Mapping[str, object]) -> str:
        delivery_id = str(delivery.get("delivery_id") or "")
        origin = str(delivery.get("origin") or "")
        if not delivery_id or not origin:
            return "invalid"
        if not await self._store_call(self.store.is_subscribed, origin) or not self._push_allowed(origin):
            await self._store_call(
                self.store.cancel_automatic_close_delivery,
                delivery_id,
                "destination is no longer subscribed or allowed",
            )
            logger.info("[%s] 自动收盘推送已取消：delivery=%s origin=%s", PLUGIN_NAME, delivery_id, origin)
            return "cancelled"
        claim = await self._store_call(
            self.store.claim_automatic_close_delivery,
            delivery_id,
            self._automatic_delivery_owner,
            ttl_seconds=self._float("automatic_delivery_lease_seconds", 120, 5, 3600),
        )
        if not claim.get("acquired"):
            return str(claim.get("reason") or claim.get("state") or "skipped")
        outcome = await self._send_claimed_delivery_once("automatic_close", delivery_id, claim)
        if outcome.state == "unknown_delivery":
            marker = getattr(self.store, "mark_automatic_close_delivery_unknown", None)
            if callable(marker):
                try:
                    await self._store_call(
                        marker,
                        delivery_id,
                        self._automatic_delivery_owner,
                        int(claim.get("fence") or claim.get("lease_fence") or 0),
                        error=outcome.error,
                    )
                except Exception:
                    logger.exception(
                        "[%s] 自动收盘推送结果不明且状态持久化失败，将由 lease 过期恢复：delivery=%s origin=%s",
                        PLUGIN_NAME,
                        delivery_id,
                        origin,
                    )
            logger.warning(
                "[%s] 自动收盘推送结果不明，禁止自动重发：delivery=%s origin=%s chunks=%s accepted=%s error=%s",
                PLUGIN_NAME,
                delivery_id,
                origin,
                outcome.attempted_chunks,
                outcome.accepted_chunks,
                outcome.error,
            )
            return "unknown_delivery"
        sent = outcome.state == "sent"
        try:
            finished = await self._store_call(
                self.store.finish_automatic_close_delivery,
                delivery_id,
                self._automatic_delivery_owner,
                int(claim.get("fence") or claim.get("lease_fence") or 0),
                sent=sent,
                error="" if sent else outcome.error,
                retry_after_seconds=self._float("automatic_delivery_retry_seconds", 60, 1, 3600),
                max_attempts=self._int("automatic_delivery_max_attempts", 5, 1, 20),
                retry_window_seconds=self._int("automatic_delivery_retry_window_seconds", 3600, 60, 86400),
            )
        except Exception:
            if sent:
                logger.exception(
                    "[%s] 自动收盘推送已发送但 ACK 持久化不明，保守停止自动重发：delivery=%s origin=%s",
                    PLUGIN_NAME,
                    delivery_id,
                    origin,
                )
                return "unknown_delivery"
            raise
        logger.info(
            "[%s] 自动收盘推送%s：delivery=%s publication=%s run=%s origin=%s",
            PLUGIN_NAME,
            "完成" if sent else "失败待重试",
            delivery_id,
            claim.get("publication_key", ""),
            claim.get("run_id", ""),
            origin,
        )
        return str(finished.get("state") or ("sent" if sent else "failed"))

    async def _recover_automatic_deliveries(self) -> dict[str, int]:
        preparer = getattr(self.store, "prepare_all_automatic_close_deliveries", None)
        loader = getattr(self.store, "recoverable_automatic_close_deliveries", None)
        summary_loader = getattr(self.store, "automatic_close_delivery_summary", None)
        if not callable(preparer) or not callable(loader) or not callable(summary_loader):
            return {}
        await self._store_call(preparer, limit=20)
        deliveries = await self._store_call(
            loader,
            limit=100,
            max_attempts=self._int("automatic_delivery_max_attempts", 5, 1, 20),
            retry_window_seconds=self._int("automatic_delivery_retry_window_seconds", 3600, 60, 86400),
        )
        for delivery in deliveries:
            try:
                await self._dispatch_automatic_delivery(delivery)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "[%s] 自动收盘推送恢复失败：delivery=%s",
                    PLUGIN_NAME,
                    delivery.get("delivery_id", ""),
                )
        summary = await self._store_call(summary_loader)
        unknown_count = int(summary.get("unknown_delivery", 0) or 0)
        if unknown_count and unknown_count != int(getattr(self, "_last_automatic_unknown_count", 0) or 0):
            logger.warning(
                "[%s] 存在 %s 条自动收盘推送结果不明，需人工核对；不会自动重发",
                PLUGIN_NAME,
                unknown_count,
            )
        self._last_automatic_unknown_count = unknown_count
        return summary

    @staticmethod
    def _intraday_delivery_time(value: object, *, naive_utc: bool = False) -> datetime | None:
        try:
            parsed = datetime.fromisoformat(str(value or "").replace("Z", "+00:00"))
        except (TypeError, ValueError, OverflowError):
            return None
        if parsed.tzinfo is None:
            if not naive_utc:
                return None
            parsed = parsed.replace(tzinfo=timezone.utc)
        return parsed.astimezone(CHINA_TZ)

    async def _validate_intraday_delivery_before_send(self, delivery: Mapping[str, object], now: datetime | None = None) -> str | None:
        """Return a conservative cancellation reason, or ``None`` to send.

        A persisted opportunity is not proof that its quote and candidate plan
        still exist.  Risk invalidations retain their warning text but have a
        separately bounded lifetime; neither kind is ever refreshed from stale
        outbox data.
        """
        current = (now or datetime.now(CHINA_TZ)).astimezone(CHINA_TZ)
        risk_event = bool(delivery.get("risk_event")) or self._intraday_is_risk_signal(str(delivery.get("signal") or ""))
        # Our durable SQLite timestamps are legacy UTC-naive values; external
        # quote evidence is not granted that compatibility exception.
        created = self._intraday_delivery_time(delivery.get("created_at"), naive_utc=True)
        if created is None:
            return "delivery creation time is unverified"
        age = (current - created).total_seconds()
        if age < 0:
            return "delivery creation time is in the future"
        if str(delivery.get("signal") or "") == "radar_observation":
            quote_at = self._intraday_delivery_time(delivery.get("quote_fetched_at"))
            maximum = max(30, self._int("quote_interval_seconds", 30, 10, 600) * 2)
            if quote_at is None or not 0 <= (current - quote_at).total_seconds() <= maximum or age > maximum:
                return "radar observation quote expired"
            pools = await self._current_research_pools(current.date().isoformat())
            if not pools or pools.get("run_id") != delivery.get("run_id"):
                return "research radar batch replaced or expired"
            member = await self._store_call(self.store.current_research_radar,
                                            str(delivery.get("code") or ""), str(delivery.get("run_id") or ""))
            return None if member else "stock is no longer in the research radar"
        if risk_event:
            maximum = self._int("intraday_risk_delivery_max_age_seconds", 900, 30, 86400)
            return None if age <= maximum else "risk invalidation expired before delivery"

        # Opportunities are bound to a verified whole-market cross-section at
        # publication and checked again immediately before transport.  Older
        # rows without this binding are cancelled rather than sent under an
        # environment whose current validity cannot be established.
        snapshot_at = self._intraday_delivery_time(delivery.get("market_snapshot_at"))
        regime = str(delivery.get("market_regime") or "").strip().lower()
        if snapshot_at is None or regime not in {"strong", "neutral", "weak"}:
            return "opportunity market environment is unverified"
        market_loader = getattr(self.store, "intraday_market_regime_state", None)
        current_market = await self._store_call(market_loader) if callable(market_loader) else None
        if not self._intraday_market_state_is_fresh(current_market, current):
            return "opportunity market environment is stale or unavailable"
        if str(current_market.get("regime") or "") != regime:
            return "opportunity market regime changed before delivery"

        fetched = self._intraday_delivery_time(delivery.get("quote_fetched_at"))
        quote_max_age = max(30, self._int("quote_interval_seconds", 30, 10, 600) * 2)
        if fetched is None:
            return "opportunity quote timestamp is unverified"
        quote_age = (current - fetched).total_seconds()
        if quote_age < 0 or quote_age > quote_max_age:
            return "opportunity quote is stale before delivery"

        valid_until = str(delivery.get("candidate_valid_until") or "").strip()
        run_id = str(delivery.get("run_id") or "").strip()
        if not run_id:
            return None
        candidate_loader = getattr(self.store, "current_intraday_candidate", None)
        if not callable(candidate_loader):
            return "active candidate plan cannot be verified"
        candidate = await self._store_call(candidate_loader, str(delivery.get("code") or ""))
        if not isinstance(candidate, Mapping):
            return "candidate plan is no longer active"
        if str(candidate.get("run_id") or "") != run_id:
            return "candidate run was replaced before delivery"
        if self._intraday_plan_version(candidate) != str(delivery.get("plan_version") or ""):
            return "candidate plan version was replaced before delivery"
        if valid_until and valid_until != str(candidate.get("valid_until") or ""):
            return "candidate expiry was replaced before delivery"
        valid = await self._candidate_is_valid_async(
            str(candidate.get("actual_trade_date") or ""), current.date().isoformat(), str(candidate.get("valid_until") or ""),
        )
        return None if valid else "candidate plan expired or calendar is unverified"

    async def _process_research_radar(self, origin: str, quote: Quote, target: Mapping[str, object],
                                      *, now: datetime, invocation_id: str) -> tuple[int, str]:
        item = target["research"]
        threshold = self._float("research_radar_threshold_pct", 2.0, 0.5, 20.0)
        qualifies = radar_crossed(quote, float(item["close"]), threshold_pct=threshold, now=now)
        qualifies = qualifies and float(quote.amount or 0) >= self._float("intraday_min_amount", 5_000_000, 0, 1_000_000_000_000)
        source_time = self._quote_source_timestamp(quote)
        if source_time is None:
            return 0, "radar:no_source_timestamp"
        payload = (
            f"名单外观察警戒｜{quote.code} {quote.name}\n"
            f"冻结收盘价 {float(item['close']):.2f}，当前价 {float(quote.price):.2f}，警戒线 +{threshold:.1f}%\n"
            f"数据日期 {target.get('research_trade_date', '')}；行情时间 {source_time.astimezone(CHINA_TZ).isoformat()}\n"
            f"停牌/涨跌停及公告状态未逐股核证，风险未知；只提示观察，不是买入建议或交易信号。"
        )
        decision = await self._store_call(
            self.store.observe_and_enqueue_intraday_event, origin, quote.code, "radar_observation",
            str(target["plan_version"]), qualifies=qualifies, rearm_ready=False, required=2,
            max_gap_seconds=self._int("intraday_confirmation_max_gap_seconds", 90, 10, 900),
            cooldown_seconds=86400, reason="radar_threshold" if qualifies else "below_or_unverified",
            name=quote.name, run_id=str(target["research_run_id"]), invocation_id=invocation_id,
            payload=payload, quote_fetched_at=source_time.astimezone(timezone.utc).isoformat(),
            risk_event=False, now=now,
        )
        if not decision.get("triggered"):
            return 0, "radar:" + str(decision.get("reason") or "not_triggered")
        outcome = await self._dispatch_intraday_delivery(decision.get("outbox") or {})
        return 1, "radar:" + outcome

    async def _dispatch_intraday_delivery(self, delivery: Mapping[str, object]) -> str:
        event_key = str(delivery.get("event_key") or "")
        origin = str(delivery.get("origin") or "")
        if not event_key or not origin:
            return "invalid"
        subscribed = await self._store_call(self.store.is_subscribed, origin)
        intraday_checker = getattr(self.store, "is_intraday_enabled", None)
        intraday_enabled = await self._store_call(intraday_checker, origin) if callable(intraday_checker) else True
        if not subscribed or not intraday_enabled or not self._push_allowed(origin):
            await self._store_call(self.store.cancel_intraday_delivery, event_key, "destination is no longer enabled for intraday delivery")
            return "cancelled"
        claim = await self._store_call(
            self.store.claim_intraday_delivery,
            event_key,
            self._intraday_delivery_owner,
            ttl_seconds=self._float("intraday_delivery_lease_seconds", 120, 5, 3600),
        )
        if not claim.get("acquired"):
            return str(claim.get("reason") or claim.get("state") or "skipped")
        fence = int(claim.get("fence") or claim.get("lease_fence") or 0)
        blocked = await self._validate_intraday_delivery_before_send(claim)
        if blocked:
            await self._store_call(
                self.store.cancel_intraday_delivery,
                event_key,
                blocked,
                owner=self._intraday_delivery_owner,
                fence=fence,
            )
            logger.info("[%s] 盘中提醒发送前取消：event=%s reason=%s", PLUGIN_NAME, event_key, blocked)
            return "cancelled"
        outcome = await self._send_claimed_delivery_once("intraday", event_key, claim)
        if outcome.state == "unknown_delivery":
            try:
                await self._store_call(
                    self.store.mark_intraday_delivery_unknown,
                    event_key,
                    self._intraday_delivery_owner,
                    fence,
                    error=outcome.error,
                )
            except Exception:
                logger.exception("[%s] 盘中提醒结果不明且状态持久化失败：event=%s", PLUGIN_NAME, event_key)
            logger.warning(
                "[%s] 盘中提醒结果不明，禁止自动重发：event=%s code=%s signal=%s chunks=%s accepted=%s",
                PLUGIN_NAME, event_key, claim.get("code", ""), claim.get("signal", ""),
                outcome.attempted_chunks, outcome.accepted_chunks,
            )
            return "unknown_delivery"
        sent = outcome.state == "sent"
        try:
            finished = await self._store_call(
                self.store.finish_intraday_delivery,
                event_key,
                self._intraday_delivery_owner,
                fence,
                sent=sent,
                error="" if sent else outcome.error,
                retry_after_seconds=self._float("intraday_delivery_retry_seconds", 60, 1, 3600),
                max_attempts=self._int("intraday_delivery_max_attempts", 5, 1, 20),
                retry_window_seconds=self._int("intraday_delivery_retry_window_seconds", 3600, 60, 86400),
            )
        except Exception:
            if sent:
                try:
                    await self._store_call(
                        self.store.mark_intraday_delivery_unknown,
                        event_key,
                        self._intraday_delivery_owner,
                        fence,
                        error="message accepted but acknowledgement persistence failed",
                    )
                except Exception:
                    logger.exception("[%s] 盘中提醒 ACK 与 unknown 标记均未能持久化：event=%s", PLUGIN_NAME, event_key)
                return "unknown_delivery"
            raise
        return str(finished.get("state") or ("sent" if sent else "failed"))

    async def _recover_intraday_deliveries(self) -> dict[str, int]:
        loader = getattr(self.store, "recoverable_intraday_deliveries", None)
        summary_loader = getattr(self.store, "intraday_delivery_summary", None)
        if not callable(loader) or not callable(summary_loader):
            return {}
        deliveries = await self._store_call(
            loader,
            limit=self._int("intraday_delivery_recovery_limit", 100, 1, 500),
            max_attempts=self._int("intraday_delivery_max_attempts", 5, 1, 20),
            retry_window_seconds=self._int("intraday_delivery_retry_window_seconds", 3600, 60, 86400),
        )
        for delivery in deliveries:
            try:
                await self._dispatch_intraday_delivery(delivery)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[%s] 盘中提醒恢复失败：event=%s", PLUGIN_NAME, delivery.get("event_key", ""))
        summary = await self._store_call(summary_loader)
        unknown_count = int(summary.get("unknown_delivery", 0) or 0)
        if unknown_count and unknown_count != self._last_intraday_unknown_count:
            logger.warning("[%s] 存在 %s 条盘中提醒结果不明，需人工核对且不会自动重发", PLUGIN_NAME, unknown_count)
        self._last_intraday_unknown_count = unknown_count
        return summary

    @staticmethod
    def _automatic_report_failures(diagnostics: Mapping[str, object], snapshot: Mapping[str, object]) -> list[str]:
        failures: list[str] = []
        if not bool(snapshot.get("complete")) or str(snapshot.get("quality") or "").lower() != "good":
            failures.append("snapshot_incomplete")
        if not bool(diagnostics.get("market_stats_confirmed")):
            failures.append("market_stats_unconfirmed")
        try:
            coverage = float(diagnostics.get("indicator_coverage", 0.0) or 0.0)
            floor = float(diagnostics.get("screen_min_indicator_coverage", 0.8) or 0.8)
            count = int(diagnostics.get("input", 0) or 0)
            risk_missing = False
            if count and "risk_tuple_complete" in diagnostics:
                known = int(diagnostics["risk_tuple_complete"] or 0)
                risk_missing = known < 0 or known > count or known / count < floor
            if risk_missing:
                failures.append("risk_evidence_missing")
            valid_empty = (
                count > 0 and int(diagnostics.get("risk_tuple_complete", -1)) == count
                and int(diagnostics.get("tradable", -1)) == 0
                and int(diagnostics.get("indicator_targets", -1)) == 0
                and int(diagnostics.get("candidate_count", -1)) == 0
                and diagnostics.get("valid_empty") is True
            )
            if (not math.isfinite(coverage) or coverage < floor) and not risk_missing and not valid_empty:
                failures.append("indicator_coverage")
        except (TypeError, ValueError, OverflowError):
            failures.append("indicator_coverage")
        return failures

    async def _recommendation_outcome_checkpoint_tick(self, now: datetime | None = None) -> dict:
        current = (now or datetime.now(CHINA_TZ)).astimezone(CHINA_TZ)
        if current < getattr(self, "_outcome_retry_after", current):
            return {"state": "retry_later"}
        due = getattr(self.store, "recommendation_checkpoint_due", None)
        evaluator = getattr(self.store, "evaluate_recommendation_outcomes", None)
        if not callable(due) or not callable(evaluator):
            return {"state": "unavailable"}
        try:
            trade_date = await self._store_call(due, current.date().isoformat())
            if not trade_date or (trade_date == current.date().isoformat() and current.time() < time(15, 0)):
                return {"state": "not_due"}
            result = await self._store_call(evaluator, as_of=trade_date)
            return {"state": "evaluated", "trade_date": trade_date, "result": result}
        except (sqlite3.Error, ValueError, TypeError, OverflowError):
            self._outcome_retry_after = current + timedelta(minutes=5)
            logger.warning("[%s] recommendation outcome checkpoint deferred", PLUGIN_NAME)
            return {"state": "deferred"}

    @staticmethod
    def _snapshot_calendar_evidence(value: object) -> dict[str, object]:
        if isinstance(value, Mapping):
            return dict(value)
        if isinstance(value, str):
            try:
                parsed = json.loads(value)
            except (TypeError, ValueError):
                return {}
            return dict(parsed) if isinstance(parsed, dict) else {}
        return {}

    async def _reconcile_automatic_close_snapshot(
        self,
        requested_date: str,
        diagnostics: Mapping[str, object] | None = None,
    ) -> tuple[list, dict[str, object]] | None:
        """Finalize one exact active raw generation without another provider call."""
        try:
            if not self._tushare_mode():
                return None
        except (AttributeError, TypeError, ValueError):
            return None

        request_id = f"daily_snapshot:{requested_date}"
        request = await self._store_call(self.store.snapshot_request, request_id) or {}
        diagnostic_values = dict(diagnostics or {})
        evidence_candidates = (
            self._snapshot_calendar_evidence(request.get("calendar_evidence_json")),
            self._snapshot_calendar_evidence(diagnostic_values.get("calendar_evidence")),
            diagnostic_values,
        )
        calendar_evidence = next(
            (
                evidence
                for evidence in evidence_candidates
                if evidence and self._calendar_evidence_final(evidence, requested_date)
            ),
            {},
        )
        if not calendar_evidence:
            calendar_lookup = getattr(self.store, "calendar_lookup", None)
            cached_calendar = (
                await self._store_call(calendar_lookup, requested_date)
                if callable(calendar_lookup)
                else {}
            )
            calendar_source = str(cached_calendar.get("source") or "").strip().lower()
            if (
                cached_calendar.get("freshness") == "fresh"
                and cached_calendar.get("status") == "open"
                and calendar_source in {"tushare", "eastmoney_index"}
                and completed_session_cutoff(requested_date) == requested_date
            ):
                calendar_evidence = {
                    "calendar_resolved": True,
                    "calendar_cutoff_date": requested_date,
                    "calendar_target_date": requested_date,
                    "source": calendar_source,
                    "policy": "verified_open_day_cache",
                    "calendar_fetched_at": cached_calendar.get("fetched_at"),
                }
        if not calendar_evidence:
            return None

        exact_quotes = await self._store_call(self.store.daily_quotes, requested_date)
        exact_meta = await self._store_call(self.store.snapshot_meta, requested_date) or {}
        if exact_quotes and bool(exact_meta.get("complete")) and str(exact_meta.get("quality") or "").lower() == "good":
            return exact_quotes, {
                **diagnostic_values,
                **calendar_evidence,
                "automatic_close_reconciled": True,
                "automatic_close_reconciliation_source": "daily_snapshot",
                "actual_trade_date": requested_date,
            }

        capability = self._snapshot_lease_capability(self.store)
        if not capability.get("supported"):
            return None
        owner = f"{PLUGIN_NAME}:automatic-reconcile:{uuid.uuid4().hex}"
        ttl_seconds = self._snapshot_lease_ttl()
        claim = await self._store_call(
            capability["claim"],
            request_id,
            requested_date,
            owner,
            ttl_seconds=ttl_seconds,
        )
        if not self._lease_acquired(claim):
            return None
        lease_owner = str(claim.get("owner") or owner)
        fence = int(claim.get("fence") or 0)
        try:
            quotes, actual_date, provenance, state = await self._fresh_raw_snapshot_async(
                requested_date,
                cache_as_of=requested_date,
                max_stale_trading_days=0,
            )
            batch_id = str(provenance.get("active_batch_id") or provenance.get("batch_id") or "").strip()
            source = str(provenance.get("source") or "").strip().lower()
            basis = str(provenance.get("basis") or provenance.get("dataset_basis") or "").strip().lower()
            if (
                state != "ok"
                or not quotes
                or actual_date != requested_date
                or not batch_id
                or source != "tushare"
                or basis != "unadjusted"
            ):
                return None
            attempts = max(int(claim.get("attempts") or 0), int(request.get("attempts") or 0))
            finalized = await self._store_call(
                capability["atomic"],
                request_id,
                requested_date,
                lease_owner,
                fence,
                quotes=quotes,
                actual_trade_date=requested_date,
                source="tushare",
                quality="good",
                complete=True,
                keep_days=self._int("daily_cache_keep_days", 180, 7, 730),
                state="complete",
                attempts=attempts,
                next_retry_at=None,
                terminal=True,
                failure_kind="",
                calendar_evidence=calendar_evidence,
                provenance=provenance,
                request_state="complete",
                request_source="tushare",
                request_quality="good",
                request_next_retry_at=None,
                request_terminal=True,
                request_failure_kind="",
            )
            if not isinstance(finalized, dict) or not bool(finalized.get("finalized")):
                return None
            self._daily_retry_after = None
            return quotes, {
                **diagnostic_values,
                **calendar_evidence,
                "raw_cache": True,
                "raw_batch_id": batch_id,
                "raw_generation": provenance.get("generation"),
                "actual_trade_date": requested_date,
                "history_unavailable": False,
                "automatic_close_reconciled": True,
                "automatic_close_reconciliation_source": "active_raw_generation",
            }
        except (SnapshotLeaseError, RuntimeError, ValueError, TypeError, KeyError, sqlite3.Error):
            return None
        finally:
            try:
                await self._store_call(capability["release"], request_id, lease_owner, fence)
            except (SnapshotLeaseError, ValueError, TypeError, OSError, sqlite3.Error):
                pass

    async def _run_automatic_close_job(self, requested_date: str, job_key: str) -> dict[str, object]:
        publication_key = self._automatic_publication_key(requested_date)
        existing = await self._store_call(self.store.automatic_close_publication, publication_key)
        if existing:
            await self._store_call(self.store.prepare_automatic_close_deliveries, publication_key)
            await self._recover_automatic_deliveries()
            await self._finish_automatic_close_job(job_key, "completed", None)
            self.last_daily_scan = requested_date
            return {"state": "recovered", "publication_key": publication_key, "run_id": existing.get("run_id")}

        score_result = await self._daily_candidates_result(
            self._int("candidate_limit", 30, 1, 100),
            invocation_id=job_key,
        )
        async def record_gate(phase: str, diagnostics: Mapping[str, object] | None = None) -> None:
            recorder = getattr(self.store, "record_screen_gate_diagnostics", None)
            if callable(recorder):
                try:
                    await self._store_call(recorder, job_key, requested_date, phase, dict(diagnostics or {}))
                except (sqlite3.Error, TypeError, ValueError, RuntimeError):
                    logger.warning("[%s] 自动收盘门控诊断持久化失败：phase=%s", PLUGIN_NAME, phase)
        candidates = list(score_result.candidates)
        actual_date = await self._store_call(self.store.latest_daily_trade_date, requested_date) or requested_date
        cached_for_record = await self._store_call(self.store.daily_quotes, actual_date) if actual_date else []
        snapshot = await self._await_result(self._snapshot_context(requested_date, actual_date, cached_for_record))
        if actual_date != requested_date or not snapshot["complete"]:
            reconciled = await self._reconcile_automatic_close_snapshot(requested_date, score_result.diagnostics)
            if reconciled is not None:
                cached_for_record, reconciliation_diagnostics = reconciled
                actual_date = requested_date
                score_result = await self._score_quotes_result(
                    cached_for_record,
                    self._int("candidate_limit", 30, 1, 100),
                    actual_date,
                    include_factors=False,
                    context="daily_close",
                    requested_date=requested_date,
                    base_diagnostics=reconciliation_diagnostics,
                    invocation_id=job_key,
                )
                candidates = list(score_result.candidates)
                snapshot = await self._await_result(
                    self._snapshot_context(requested_date, actual_date, cached_for_record)
                )
        if actual_date != requested_date or not snapshot["complete"]:
            await record_gate("waiting_snapshot", score_result.diagnostics)
            self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
            await self._finish_automatic_close_job(job_key, "failed", "当日完整收盘数据尚未就绪，等待重试")
            logger.info(
                "[%s] 自动收盘等待当日数据：invocation=%s requested=%s actual=%s complete=%s",
                PLUGIN_NAME,
                job_key,
                requested_date,
                actual_date,
                bool(snapshot.get("complete")),
            )
            return {"state": "waiting_snapshot", "requested_date": requested_date, "actual_date": actual_date}

        # Research-only freeze is independent of the formal recommendation gate.
        try:
            await self._freeze_research_pools(requested_date)
        except Exception:
            logger.exception("[%s] 自动收盘研究双池冻结失败", PLUGIN_NAME)

        report_diagnostics = await self._report_diagnostics_for_send(
            score_result.diagnostics,
            requested_date=requested_date,
            actual_date=actual_date,
            quote_count=len(cached_for_record),
        )
        failures = self._automatic_report_failures(report_diagnostics, snapshot)
        if failures:
            await record_gate("fail_closed:" + ",".join(failures), report_diagnostics)
            self._daily_retry_after = datetime.now(CHINA_TZ) + timedelta(minutes=5)
            reason = "自动收盘报告校验失败：" + ",".join(failures)
            await self._finish_automatic_close_job(job_key, "failed", reason)
            logger.warning(
                "[%s] 自动收盘 fail-closed：invocation=%s requested=%s actual=%s reasons=%s",
                PLUGIN_NAME,
                job_key,
                requested_date,
                actual_date,
                ",".join(failures),
            )
            return {"state": "fail_closed", "reasons": failures}

        report_text = "\n".join(self._market_report_lines(
            requested_date,
            actual_date,
            cached_for_record,
            candidates,
            snapshot,
            report_diagnostics,
        ))
        destinations = [
            origin
            for origin in await self._store_call(self.store.subscriptions)
            if self._push_allowed(origin)
        ]
        run_id = await self._await_result(self._record_screen(
            requested_date,
            actual_date,
            snapshot["source"],
            cached_for_record,
            candidates,
            status="completed",
            quality=snapshot["quality"],
            job_name="automatic_close",
            diagnostics=report_diagnostics,
            publication_key=publication_key,
            publication_payload=report_text,
            publication_invocation_id=job_key,
            publication_origins=destinations,
        ))
        if not run_id:
            await record_gate("report_gate", report_diagnostics)
            await self._finish_automatic_close_job(job_key, "failed", "报告版本门控未通过，未切换候选池")
            return {"state": "report_gate"}
        publication = await self._store_call(self.store.automatic_close_publication, publication_key)
        if not publication:
            raise RuntimeError("automatic close publication was not persisted")
        await record_gate("completed", report_diagnostics)
        await self._store_call(self.store.prepare_automatic_close_deliveries, publication_key)
        summary = await self._recover_automatic_deliveries()
        await self._finish_automatic_close_job(job_key, "completed", None)
        self.last_daily_scan = requested_date
        self._daily_retry_after = None
        logger.info(
            "[%s] 自动收盘完成：invocation=%s publication=%s run=%s requested=%s actual=%s destinations=%s sent=%s unknown=%s",
            PLUGIN_NAME,
            job_key,
            publication_key,
            publication.get("run_id", run_id),
            requested_date,
            actual_date,
            len(destinations),
            summary.get("sent", 0),
            summary.get("unknown_delivery", 0),
        )
        return {"state": "completed", "publication_key": publication_key, "run_id": publication.get("run_id", run_id), "summary": summary}

    async def _automatic_close_tick(self, now: datetime | None = None) -> dict[str, object]:
        current = (now or datetime.now(CHINA_TZ)).astimezone(CHINA_TZ)
        snapshot_terminalizer = getattr(self.store, "terminalize_prior_snapshot_requests", None)
        if callable(snapshot_terminalizer):
            stale_snapshots = await self._store_call(
                snapshot_terminalizer,
                current.date().isoformat(),
                reason="unfinished snapshot request crossed the trading-date boundary",
                limit=20,
                now=current,
            )
            if stale_snapshots:
                logger.warning(
                    "[%s] 历史快照请求已终止，避免跨日继续抓取：current=%s requests=%s",
                    PLUGIN_NAME,
                    current.date().isoformat(),
                    ",".join(str(item.get("request_id") or "") for item in stale_snapshots),
                )
        terminalizer = getattr(self.store, "terminalize_prior_automatic_close_jobs", None)
        if callable(terminalizer):
            missed = await self._store_call(
                terminalizer,
                current.date().isoformat(),
                reason="unfinished automatic close job crossed the trading-date boundary",
                limit=20,
                now=current,
            )
            if missed:
                logger.warning(
                    "[%s] 自动收盘历史任务已终止为 missed，避免跨日误发：current=%s jobs=%s",
                    PLUGIN_NAME,
                    current.date().isoformat(),
                    ",".join(str(item.get("job_key") or "") for item in missed),
                )
        phase = self._automatic_close_phase(current)
        if phase != "calendar_check":
            return {"state": phase}
        requested_date = current.date().isoformat()
        calendar = await self._calendar_open(requested_date)
        if calendar is not True:
            return {"state": "calendar_closed" if calendar is False else "calendar_unknown"}
        legacy_job = await self._store_call(self.store.job_run, f"daily_screen:{requested_date}")
        if legacy_job and str(legacy_job.get("status") or "").lower() == "completed":
            logger.warning(
                "[%s] 检测到升级前同日自动收盘任务已完成，保守跳过新调度以避免重复推送：requested=%s",
                PLUGIN_NAME,
                requested_date,
            )
            self.last_daily_scan = requested_date
            return {"state": "legacy_completed", "job_key": str(legacy_job.get("job_key") or "")}
        job_key = f"automatic_close:{requested_date}"
        claimer = getattr(self.store, "claim_automatic_close_job", None)
        if callable(claimer):
            max_attempts, retry_window, _retry_seconds = self._automatic_job_limits()
            claim = await self._store_call(
                claimer,
                job_key,
                requested_date,
                lease_seconds=900,
                max_attempts=max_attempts,
                retry_window_seconds=retry_window,
                now=current,
            )
            if not claim.get("acquired"):
                return {"state": str(claim.get("reason") or "duplicate"), "job_key": job_key}
        elif not await self._store_call(self.store.begin_job, job_key, "automatic_close", requested_date, 900):
            return {"state": "duplicate", "job_key": job_key}
        try:
            return await self._run_automatic_close_job(requested_date, job_key)
        except asyncio.CancelledError:
            raise
        except Exception:
            recorder = getattr(self.store, "record_screen_gate_diagnostics", None)
            if callable(recorder):
                try:
                    await self._store_call(recorder, job_key, requested_date, "exception", {})
                except Exception:
                    logger.warning("[%s] 自动收盘异常诊断无法持久化", PLUGIN_NAME)
            await self._finish_automatic_close_job(job_key, "failed", "自动收盘扫描异常")
            logger.exception("[%s] 自动收盘扫描失败：invocation=%s", PLUGIN_NAME, job_key)
            return {"state": "failed", "job_key": job_key}

    async def _daily_loop(self):
        while True:
            try:
                await self._recover_automatic_deliveries()
                await self._automatic_close_tick()
                await self._refresh_research_qualifications()
                await self._recommendation_outcome_checkpoint_tick()
                await self._recover_daily_acceptance_alerts()
                await self._daily_acceptance_tick()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[%s] 自动收盘调度循环异常", PLUGIN_NAME)
            await asyncio.sleep(20)

    async def _refresh_research_qualifications(self) -> None:
        """Ingest late sidecar facts as new versions, never rewrite a freeze."""
        current = datetime.now(timezone.utc)
        last = getattr(self, "_last_paper_qualification_check_at", None)
        if isinstance(last, datetime) and (current - last).total_seconds() < 300:
            return
        self._last_paper_qualification_check_at = current
        loader = getattr(self.store, "latest_research_pools", None)
        recorder = getattr(self.store, "record_daily_risk_observations", None)
        qualifier = getattr(self.store, "qualify_research_pick", None)
        if not all(callable(item) for item in (loader, recorder, qualifier)):
            return
        pools = await self._store_call(loader)
        if not pools or not pools.get("batch_id") or not pools.get("trade_date"):
            return
        trade_date = str(pools["trade_date"])
        batch_id = str(pools["batch_id"])
        if trade_date > current.astimezone(CHINA_TZ).date().isoformat():
            return
        from . import risk_qualification as risk
        path = self.store.path.parent / "research_risk_evidence" / (trade_date + ".json")
        try:
            bundle = await self._store_call(risk.read_research_sidecar, path,
                                            trade_date=trade_date, batch_id=batch_id)
            await self._store_call(recorder, bundle)
        except (OSError, sqlite3.Error, ValueError, TypeError, KeyError):
            pass
        for pick in pools.get("picks", {}).get("primary", []):
            try:
                await self._store_call(qualifier, str(pools["run_id"]), str(pick["code"]), current)
            except (sqlite3.Error, ValueError, TypeError, KeyError):
                logger.warning("[%s] 研究资格版本待核：code=%s", PLUGIN_NAME, pick.get("code"))

    def _intraday_artifact_market_extra(self, state: Mapping[str, object] | None) -> dict[str, object]:
        """Publish compact live-market evidence across the Web read boundary."""
        values = state if isinstance(state, Mapping) else {}
        regime = str(values.get("regime") or "unknown").strip().lower()
        quality = str(values.get("quality") or "unknown").strip().lower()
        reason = str(values.get("reason") or "live_market_evidence_unavailable").strip()
        source = str(values.get("source") or "").strip()
        confirmed = regime in {"strong", "neutral", "weak", "risk_off"} and quality == "good" and reason == "confirmed"
        return {"market": {
            "regime": regime if regime in {"strong", "neutral", "weak", "risk_off"} else "unknown",
            "pending_regime": str(values.get("pending_regime") or "unknown").strip().lower(),
            "quality": quality if quality else "unknown",
            "reason": reason or "live_market_evidence_unavailable",
            "source": source,
            "source_timestamp": str(values.get("source_timestamp") or ""),
            "updated_at": str(values.get("updated_at") or ""),
            "sample_size": int(values.get("sample_size") or 0),
            "expected_size": int(values.get("expected_size") or 0),
            "coverage": float(values.get("coverage") or 0),
            "opportunity_allowed": bool(confirmed and regime != "risk_off"),
        }, "valid_for_seconds": max(30, self._int("quote_interval_seconds", 30, 5, 600) * 2)}

    async def _intraday_cycle(self, now: datetime | None = None) -> dict[str, object]:
        current = (now or datetime.now(CHINA_TZ)).astimezone(CHINA_TZ)
        today = current.date().isoformat()
        health = self._intraday_health
        health["last_cycle_at"] = current.isoformat()
        if not in_trading_session(current):
            await asyncio.to_thread(publish_intraday_artifact, getattr(self, "intraday_artifact_path", Path("intraday_quotes.json")),
                                    target_codes=(), quotes=(), collected_at=current, published_at=current,
                                    source="sina", status="unknown", reason="outside_session")
            health["last_state"] = "outside_session"
            health["last_nontrigger_reasons"] = {"outside_session": 1}
            return {"state": "outside_session", "date": today}
        calendar = await self._calendar_open(today)
        if calendar is not True:
            state = "calendar_closed" if calendar is False else "calendar_unknown"
            await asyncio.to_thread(publish_intraday_artifact, getattr(self, "intraday_artifact_path", Path("intraday_quotes.json")),
                                    target_codes=(), quotes=(), collected_at=current, published_at=current,
                                    source="sina", status="unknown", reason=state)
            health["last_state"] = state
            health["last_nontrigger_reasons"] = {state: 1}
            return {"state": state, "date": today}
        if self._intraday_date != today:
            self.minute_bars.reset()
            self._intraday_date = today
            self._minute_restore_pending = True
            await self._store_call(self.store.cleanup_minute_bars, self._int("minute_bar_keep_days", 7, 1, 60))

        watch = await self._store_call(self.store.all_watch)
        watch_details = await self._store_call(self.store.all_watch_details)
        subscription_loader = getattr(self.store, "intraday_subscriptions", None)
        if not callable(subscription_loader):
            subscription_loader = self.store.subscriptions
        subscriptions = set(await self._store_call(subscription_loader))
        if not subscriptions:
            await asyncio.to_thread(publish_intraday_artifact, getattr(self, "intraday_artifact_path", Path("intraday_quotes.json")),
                                    target_codes=(), quotes=(), collected_at=current, published_at=current,
                                    source="sina", status="unknown", reason="no_subscriptions")
            health["last_state"] = "no_subscriptions"
            health["last_nontrigger_reasons"] = {"no_subscriptions": 1}
            return {"state": "no_subscriptions", "date": today}
        stored_candidates: dict[str, dict] = {}
        expired_candidate_rows: dict[str, dict] = {}
        if self._bool("auto_watch_candidates", True):
            candidate_loader = getattr(self.store, "latest_screen_candidates_for_intraday", None)
            if not callable(candidate_loader):
                candidate_loader = self.store.latest_screen_candidates
            rows = await self._store_call(candidate_loader, self._int("candidate_limit", 30, 1, 100))
            for item in rows:
                code = str(item.get("code") or "")
                if not code:
                    continue
                if await self._candidate_is_valid_async(
                    str(item.get("actual_trade_date") or ""), today, str(item.get("valid_until") or "")
                ):
                    stored_candidates[code] = item
                else:
                    expired_candidate_rows[code] = item
        focus_codes = self._configured_intraday_focus_codes()
        research_loader = getattr(self.store, "latest_research_pools", None)
        research_pools = await self._current_research_pools(today) if callable(research_loader) else {}
        targets = self._build_intraday_targets(watch, watch_details, subscriptions, stored_candidates, focus_codes, research_pools)
        union = list(dict.fromkeys(code for rows in targets.values() for code in rows))
        total_targets = len(union)
        limit = self._int("intraday_focus_limit", 200, 10, 500)
        union = union[:limit]
        allowed_codes = set(union)
        targets = {origin: {code: value for code, value in rows.items() if code in allowed_codes} for origin, rows in targets.items()}
        health["selected_targets"] = len(union)
        health["candidate_targets"] = len(stored_candidates)
        health["expired_candidates"] = len(expired_candidate_rows)
        health["focus_targets"] = len(focus_codes)
        health["dropped_targets"] = max(0, total_targets - len(union))
        if union and self._minute_restore_pending and self._bool("minute_enabled", True):
            restored = await self._store_call(
                self.store.restore_minute_bars, today, union,
                limit=self._int("minute_bar_history", 120, 10, 2000),
            )
            self.minute_bars.restore(restored, today)
            self._minute_restore_pending = False

        invocation_id = f"intraday:{today}:{uuid.uuid4().hex}"
        health["cycles"] += 1
        health["last_invocation_id"] = invocation_id
        market_context = await self._refresh_intraday_market_regime_state(current)
        market_extra = self._intraday_artifact_market_extra(market_context)
        live_regime = str(market_context.get("regime") or "unknown")
        live_reason = str(market_context.get("reason") or "live_market_evidence_unavailable")
        health["market_regime"] = live_regime
        health["market_quality"] = str(market_context.get("quality") or "unknown")
        health["market_reason"] = live_reason
        health["market_sample_size"] = int(market_context.get("sample_size") or 0)
        health["market_expected_size"] = int(market_context.get("expected_size") or 0)
        health["market_coverage"] = float(market_context.get("coverage") or 0)
        invalidation_events = 0
        invalidation_reasons: list[str] = []
        for origin in sorted(subscriptions):
            if live_regime == "unknown":
                count, detail = await self._emit_intraday_invalidation(
                    origin,
                    {
                        "code": "__market__", "name": "市场环境", "plan_version": "live-market-v1",
                        "provenance": {"live_market"}, "valid_until": "",
                    },
                    signal="market_regime_invalidated",
                    reason=live_reason,
                    invocation_id=invocation_id,
                    now=current,
                )
                invalidation_events += count
                invalidation_reasons.append(detail)
            for code, stored in expired_candidate_rows.items():
                expired_target = {
                    "code": code,
                    "name": str(stored.get("name") or code),
                    "stored": stored,
                    "provenance": {"close_candidate"},
                    "plan_version": self._intraday_plan_version(stored),
                    "run_id": str(stored.get("run_id") or ""),
                    "valid_until": str(stored.get("valid_until") or ""),
                }
                count, detail = await self._emit_intraday_invalidation(
                    origin,
                    expired_target,
                    signal="plan_expired",
                    reason="candidate_plan_expired_or_calendar_unverified",
                    invocation_id=invocation_id,
                    now=current,
                )
                invalidation_events += count
                invalidation_reasons.append(detail)
        await self._recover_intraday_deliveries()
        if not union:
            await asyncio.to_thread(publish_intraday_artifact, getattr(self, "intraday_artifact_path", Path("intraday_quotes.json")),
                                    target_codes=(), quotes=(), collected_at=current, published_at=current,
                                    source="sina", status="unknown", reason="no_targets", extra=market_extra)
            health["triggered_events"] += invalidation_events
            health["last_state"] = "no_targets"
            health["last_nontrigger_reasons"] = {"no_targets": 1, **{item: 1 for item in invalidation_reasons[-8:]}}
            return {"state": "no_targets", "date": today, "invocation_id": invocation_id, "triggered": invalidation_events}
        if live_regime == "unknown":
            health["failed_cycles"] += 1
            health["consecutive_failures"] += 1
            health["last_error_at"] = current.isoformat()
            health["last_state"] = "failed_closed"
            health["triggered_events"] += invalidation_events
            health["last_nontrigger_reasons"] = {
                live_reason: 1,
                **{item: 1 for item in invalidation_reasons[-8:]},
            }
            await asyncio.to_thread(
                publish_intraday_artifact,
                getattr(self, "intraday_artifact_path", Path("intraday_quotes.json")),
                target_codes=union,
                quotes=(),
                collected_at=current,
                published_at=current,
                source="sina",
                status="unknown",
                reason=live_reason,
                extra=market_extra,
            )
            logger.warning("[%s] 盘中提醒本轮 fail-closed：invocation=%s reason=%s", PLUGIN_NAME, invocation_id, live_reason)
            return {"state": "failed_closed", "invocation_id": invocation_id, "reason": live_reason, "triggered": invalidation_events}
        raw_quotes: list[Quote] = []
        cycle_failed = False
        for start in range(0, len(union), 100):
            source = self._source_health["sina"]
            source["batches"] += 1
            try:
                fetch_target = getattr(self.quotes, "fetch_target_quotes", None)
                fetch_target = fetch_target if callable(fetch_target) else self.quotes.fetch_quotes
                raw_quotes.extend(await fetch_target(union[start:start + 100]))
                source["successes"] += 1
                source["last_success_at"] = current.isoformat()
                diagnostics = getattr(self.quotes, "last_realtime_backup_diagnostics", None)
                if isinstance(diagnostics, dict):
                    realtime = self._source_health.setdefault("tushare_rt_k", {})
                    for field in ("mode", "attempts", "successes", "failures", "skipped", "last_status", "last_error", "last_attempt_at", "selected_source", "quote_count", "primary_quote_count", "overlapping_codes", "price_mismatches"):
                        if field in diagnostics:
                            realtime[field] = diagnostics[field]
            except Exception:
                cycle_failed = True
                source["failures"] += 1
                source["last_error_at"] = current.isoformat()
                logger.exception("[%s] 盘中行情分批抓取失败：批次 %s invocation=%s", PLUGIN_NAME, start // 100 + 1, invocation_id)
        quote_clock = self._quote_freshness_clock(current)
        quote_clock = quote_clock.astimezone(CHINA_TZ) if quote_clock.tzinfo else quote_clock.replace(tzinfo=CHINA_TZ)
        collected_times = [getattr(item, "fetched_at", None) for item in raw_quotes if getattr(item, "fetched_at", None)]
        collected_at = max(collected_times) if collected_times else quote_clock
        artifact_reason = "partial_quote_batches" if cycle_failed else ""
        artifact_status = "partial" if cycle_failed else "available"
        if not raw_quotes and not cycle_failed:
            artifact_status, artifact_reason = "unknown", "quote_missing"
        quote_sources = {str(getattr(item, "source", "") or "") for item in raw_quotes}
        artifact_source = quote_sources.pop() if len(quote_sources) == 1 else "mixed" if quote_sources else "sina"
        try:
            await asyncio.to_thread(
                publish_intraday_artifact,
                getattr(self, "intraday_artifact_path", Path("intraday_quotes.json")),
                target_codes=union,
                quotes=raw_quotes,
                collected_at=collected_at,
                published_at=quote_clock,
                source=artifact_source,
                status=artifact_status,
                reason=artifact_reason,
                extra=market_extra,
            )
        except Exception:
            logger.exception("[%s] 盘中 target artifact 发布失败", PLUGIN_NAME)
            artifact_status, artifact_reason = "unknown", "artifact_publish_failed"
        # Freshness must be evaluated after the awaited quote fetches, not at
        # the cycle's pre-fetch clock.
        quotes = self._fresh_quotes(raw_quotes, now=quote_clock)
        health["stale_quotes"] += max(0, len(raw_quotes) - len(quotes))
        health["accepted_quotes"] += len(quotes)
        if cycle_failed or not quotes:
            health["failed_cycles"] += 1
            health["consecutive_failures"] += 1
            health["last_error_at"] = current.isoformat()
            reason = "partial_quote_batches" if cycle_failed else "no_fresh_quotes"
            await self._invalidate_intraday_target_batch(targets, reason, now=current)
            for origin in sorted(subscriptions):
                count, detail = await self._emit_intraday_invalidation(
                    origin,
                    {
                        "code": "__market_data__", "name": "盘中行情", "plan_version": "market-data-v1",
                        "provenance": {"market_data"}, "valid_until": "",
                    },
                    signal="data_invalidated",
                    reason=reason,
                    invocation_id=invocation_id,
                    now=current,
                )
                invalidation_events += count
                invalidation_reasons.append(detail)
            health["last_state"] = "failed_closed"
            health["triggered_events"] += invalidation_events
            health["last_nontrigger_reasons"] = {
                reason: 1,
                **{item: 1 for item in invalidation_reasons[-8:]},
            }
            logger.warning("[%s] 盘中提醒本轮 fail-closed：invocation=%s reason=%s", PLUGIN_NAME, invocation_id, reason)
            return {"state": "failed_closed", "invocation_id": invocation_id, "reason": reason, "triggered": invalidation_events}

        minute_signals: dict[str, str] = {}
        completed_bars = []
        completed_bars_by_code = {}
        if self._bool("minute_enabled", True):
            for quote in quotes:
                completed = self.minute_bars.update(quote)
                if completed is not None:
                    completed_bars.append(completed)
                    completed_bars_by_code[completed.code] = completed
                    health["completed_bars"] += 1
                    signal = self._minute_signal_text(quote, completed)
                    if signal:
                        minute_signals[quote.code] = signal
            if completed_bars:
                await self._store_call(
                    self.store.save_minute_bars, completed_bars,
                    self._int("minute_bar_keep_days", 7, 1, 60), "sina",
                )

        raw_quotes_by_code = {quote.code: quote for quote in raw_quotes}
        quotes_by_code = {quote.code: quote for quote in quotes}
        by_code = {quote.code: score_quote(quote) for quote in quotes}
        health["successful_cycles"] += 1
        health["last_success_at"] = current.isoformat()
        health["consecutive_failures"] = 0
        if self._bool("llm_annotation_enabled", False) and by_code and not self._annotation_task:
            interval = self._int("llm_annotation_interval_seconds", 180, 30, 3600)
            if not self._last_annotation_at or (current - self._last_annotation_at).total_seconds() >= interval:
                batch_limit = self._int("llm_annotation_limit", 10, 5, 20)
                batches = []
                for rows in targets.values():
                    top = [by_code[code] for code in rows if code in by_code]
                    top.sort(key=lambda item: (item.score, item.quote.amount), reverse=True)
                    if top:
                        batches.append(top[:batch_limit])
                self._last_annotation_at = current
                if batches:
                    self._annotation_task = asyncio.create_task(self._annotate_batches(batches))

        triggered = invalidation_events
        reason_counts: dict[str, int] = {detail: 1 for detail in invalidation_reasons[-12:]}
        threshold = self._int("intraday_failure_threshold", 0, 0, 100)
        suppressed = threshold > 0 and health["consecutive_failures"] >= threshold
        for origin, rows in targets.items():
            for code, target in rows.items():
                quote = quotes_by_code.get(code)
                if quote is None:
                    raw_quote = raw_quotes_by_code.get(code)
                    reason = "stale_quote" if raw_quote is not None else "quote_missing"
                    await self._invalidate_intraday_target_batch({origin: {code: target}}, reason, now=current)
                    count, detail = await self._emit_intraday_invalidation(
                        origin,
                        target,
                        signal="data_invalidated",
                        reason=reason,
                        invocation_id=invocation_id,
                        now=current,
                        quote=raw_quote,
                    )
                    triggered += count
                    reason_counts[detail] = reason_counts.get(detail, 0) + 1
                    continue
                if suppressed:
                    await self._invalidate_intraday_target_batch({origin: {code: target}}, "provider_health_suppressed", now=current)
                    count, detail = await self._emit_intraday_invalidation(
                        origin,
                        target,
                        signal="data_invalidated",
                        reason="provider_health_suppressed",
                        invocation_id=invocation_id,
                        now=current,
                        quote=quote,
                    )
                    triggered += count
                    reason_counts[detail] = reason_counts.get(detail, 0) + 1
                    continue
                if target.get("research") and not target.get("stored"):
                    if target.get("research_pool") == "radar":
                        event_count, detail = await self._process_research_radar(
                            origin, quote, target, now=current, invocation_id=invocation_id,
                        )
                        triggered += event_count
                        reason_counts[detail] = reason_counts.get(detail, 0) + 1
                    elif target.get("research_pool") == "primary":
                        qualification_loader = getattr(self.store, "latest_paper_qualification", None)
                        paper_observer = getattr(self.store, "observe_paper_completed_bar", None)
                        completed = completed_bars_by_code.get(code)
                        source_time = self._quote_source_timestamp(quote)
                        if (callable(qualification_loader) and callable(paper_observer)
                                and completed is not None and source_time is not None):
                            record_id = f"{target['research_run_id']}:{code}"
                            qualification = await self._store_call(qualification_loader, record_id)
                            if qualification and qualification.get("state") == "eligible":
                                # Quote aggregation has not passed the minute
                                # volume/amount provenance gate. It cannot be
                                # labeled validated by this runtime path.
                                outcome = await self._store_call(
                                    paper_observer, record_id, str(qualification["version"]),
                                    bar_start=completed.start, bar_close=completed.close,
                                    quote_at=source_time, quote_price=quote.price,
                                    suspended=quote.suspended, limit_up=quote.limit_up,
                                    limit_down=quote.limit_down, st=getattr(quote, "st", None),
                                    source_quality="aggregated_quotes_unverified", now=current,
                                )
                                detail = "paper:" + str(outcome.get("reason") or outcome.get("fill_status") or "unknown")
                                reason_counts[detail] = reason_counts.get(detail, 0) + 1
                    continue
                event_count, reasons = await self._process_intraday_events(
                    origin, quote, target, minute_signal=minute_signals.get(code, ""),
                    completed_bar_close=getattr(completed_bars_by_code.get(code), "close", None),
                    completed_bar_start=getattr(completed_bars_by_code.get(code), "start", None),
                    invocation_id=invocation_id, now=current, live_regime=live_regime,
                    market_context=market_context,
                )
                triggered += event_count
                for reason in reasons:
                    reason_counts[reason] = reason_counts.get(reason, 0) + 1
        health["triggered_events"] += triggered
        health["last_state"] = "completed"
        health["last_nontrigger_reasons"] = dict(sorted(reason_counts.items(), key=lambda item: (-item[1], item[0]))[:12])
        logger.info(
            "[%s] 盘中监听完成：invocation=%s targets=%s quotes=%s triggered=%s reasons=%s",
            PLUGIN_NAME, invocation_id, len(union), len(quotes), triggered,
            json.dumps(health["last_nontrigger_reasons"], ensure_ascii=False, separators=(",", ":")),
        )
        return {"state": "completed", "invocation_id": invocation_id, "targets": len(union), "quotes": len(quotes), "triggered": triggered}

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
                await self._intraday_cycle()
            except asyncio.CancelledError:
                raise
            except Exception:
                health = self._intraday_health
                health["failed_cycles"] += 1
                health["consecutive_failures"] += 1
                health["last_error_at"] = datetime.now(CHINA_TZ).isoformat()
                logger.exception("[%s] 盘中监听失败", PLUGIN_NAME)
            await asyncio.sleep(self._int("quote_interval_seconds", 30, 5, 600))

    async def _intraday_market_loop(self):
        """Refresh whole-market regime independently of the 5s target feed."""
        while True:
            try:
                now = datetime.now(CHINA_TZ)
                if in_trading_session(now) and await self._calendar_open(now.date().isoformat()) is True:
                    await self._refresh_intraday_market_regime_state(now)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("[%s] 全市场盘中环境刷新失败", PLUGIN_NAME)
            await asyncio.sleep(self._int("intraday_market_refresh_seconds", 120, 30, 900))

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
            if not self._last_screen_report_claimed:
                yield event.plain_result("本次结果未通过报告版本门控，未更新候选池。")
                return
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
            "/观察选股：冻结当日技术观察和名单外警戒双池\n"
            "/观察池、/警戒池：查看已冻结的研究名单\n"
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
        """Start a full-market sync in the background and push the report when ready."""
        origin = self._origin(event)
        invocation_id = uuid.uuid4().hex
        try:
            limit = max(1, min(int(count or self._int("candidate_limit", 30, 1, 100)), 100))
        except (TypeError, ValueError):
            limit = max(1, min(self._int("candidate_limit", 30, 1, 100), 100))
        asyncio.create_task(self._run_market_sync(origin, invocation_id, limit))
        yield event.plain_result(
            "已开始全市场同步（后台执行）。\n"
            "· 若本地已有当日缓存，稍后会直接把结果推送给你；\n"
            "· 若需要回填 120 日日线，约需 20~30 分钟，完成后会自动推送结果。\n"
            "期间可正常使用其他指令。"
        )

    async def _run_market_sync(self, origin: str, invocation_id: str, limit: int):
        """Run one full-market sync and push the report to the requesting origin."""
        response = "全市场同步失败，请检查行情接口和插件配置。\n可再次执行 /全市场选股 重试。"
        branch = "exception"
        run_diagnostics: Mapping[str, object] = MappingProxyType({})
        trade_date = datetime.now(CHINA_TZ).date().isoformat()
        try:
            snapshot_diagnostics: dict[str, object] = {}
            if self._tushare_mode():
                quotes, fetched, actual_date = await self._daily_snapshot(trade_date)
                snapshot_diagnostics = dict(self._last_screen_diagnostics or {})
                if self._shadow_only_diagnostics(snapshot_diagnostics):
                    response = self._configured_shadow_report(trade_date, actual_date)
                    branch = "configured_shadow"
                elif await self._preview_fallback_needed(quotes, actual_date, snapshot_diagnostics):
                    candidates, fallback_date, fallback_diagnostics = await self._eastmoney_transient_preview(trade_date, limit)
                    run_diagnostics = fallback_diagnostics
                    response = self._eastmoney_fallback_report(
                        trade_date,
                        fallback_date,
                        [],
                        candidates,
                        fallback_diagnostics,
                    )
                    branch = "eastmoney_preview"
                else:
                    source = ("已同步交易日 " if fetched else "已使用交易日 ") + actual_date + " 全市场数据"
            elif self._bool("daily_cache_enabled", True):
                quotes, fetched, actual_date = await self._daily_snapshot(trade_date)
                source = ("已同步交易日 " if fetched else "已使用交易日 ") + actual_date + " 全市场数据"
            else:
                result = await self.quotes.fetch_market_snapshot_result(
                    str(self.config.get("daily_market_url", "")), trade_date
                )
                actual_date = result.trade_date
                if not actual_date:
                    response = "行情源没有返回真实交易日，已拒绝把数据标记为今天；请使用 Tushare 或先同步可验证的缓存。\n可再次执行 /全市场选股 重试。"
                    branch = "missing_actual_date"
                else:
                    quotes = result.quotes
                    self.store.save_snapshot_meta(actual_date, result.source, result.quality, actual_date == trade_date and result.quality == "good", trade_date, "未启用本地日快照缓存")
                    source = "已抓取交易日 " + actual_date + " 全市场数据（未启用缓存）"
            if branch == "exception" and "quotes" in locals():
                if not quotes:
                    if not snapshot_diagnostics.get("calendar_resolved") and (
                        snapshot_diagnostics.get("calendar_endpoint_unavailable")
                        or snapshot_diagnostics.get("failure_kind") == "calendar_endpoint_unavailable"
                    ):
                        response = (
                        f"交易日历证据不可用（请求日期：{trade_date}）。\n"
                        "本次尚未开始 Tushare daily 行情抓取，请稍后重试；不能据此判断 Tushare 尚未发布该日期数据。"
                        )
                    else:
                        response = (
                        f"未找到可用的全市场日行情（请求日期：{trade_date}）。\n"
                        "可能是 Tushare 尚未发布该日期数据，或行情接口暂时不可用。"
                        )
                    branch = "empty_snapshot"
                else:
                    score_result = await self._score_quotes_result(
                        quotes,
                        limit,
                        actual_date,
                        context="daily_close",
                        requested_date=trade_date,
                        base_diagnostics=snapshot_diagnostics,
                        invocation_id=invocation_id,
                    )
                    candidates = list(score_result.candidates)
                    snapshot = await self._await_result(self._snapshot_context(trade_date, actual_date, quotes))
                    run_diagnostics = await self._report_diagnostics_for_send(
                        score_result.diagnostics,
                        requested_date=trade_date,
                        actual_date=actual_date,
                        quote_count=len(quotes),
                    )
                    run_id = await self._await_result(self._record_screen(
                        trade_date,
                        actual_date,
                        snapshot["source"],
                        quotes,
                        candidates,
                        status="completed" if snapshot["complete"] else "degraded",
                        quality=snapshot["quality"],
                        diagnostics=run_diagnostics,
                    ))
                    if not run_id:
                        response = "本次结果未通过报告版本门控，未更新候选池。\n可再次执行 /全市场选股 重试。"
                        branch = "report_gate"
                    else:
                        lines = self._market_report_lines(
                            trade_date,
                            actual_date,
                            quotes,
                            candidates,
                            snapshot,
                            run_diagnostics,
                        )
                        response = "\n".join(lines)
                        branch = "report"
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("[%s] 手动全市场同步失败", PLUGIN_NAME)
            branch = "exception"
        diagnostics = dict(run_diagnostics or snapshot_diagnostics or {})
        diagnostics.update({
            "market_sync_invocation_id": invocation_id,
            "market_sync_branch": branch,
            "market_sync_origin": str(origin or ""),
            "market_sync_yield_count": 1,
        })
        self._last_screen_diagnostics = diagnostics
        logger.info(
            "[%s] market_sync invocation=%s branch=%s failure_kind=%s cache_state=%s calendar_target=%s origin=%s",
            PLUGIN_NAME, invocation_id, branch, diagnostics.get("failure_kind", ""),
            diagnostics.get("cache_state", ""), diagnostics.get("calendar_target_date", ""),
            origin or "<empty>",
        )
        await self._reply_origin(origin, response)

    async def _reply_origin(self, origin: str, text: str) -> None:
        """Send a report to one origin without a whitelist gate (explicit command)."""
        if not str(origin or "").strip():
            return
        try:
            for chunk in self._message_chunks(text):
                try:
                    await self.context.send_message(origin, MessageChain([Plain(chunk)]))
                except TypeError:
                    await self.context.send_message(origin, chunk)
        except Exception:
            logger.exception("[%s] 回推全市场结果失败：%s", PLUGIN_NAME, origin or "<empty>")

    @staticmethod
    def _stored_factor_payload(row: dict) -> dict:
        raw = row.get("factor_payload") if isinstance(row, dict) else None
        if isinstance(raw, dict):
            return raw
        if not isinstance(raw, str):
            return {}
        try:
            payload = json.loads(raw)
        except (TypeError, ValueError, json.JSONDecodeError):
            return {}
        return payload if isinstance(payload, dict) else {}

    @classmethod
    def _stored_candidate_industry(cls, row: dict) -> str:
        payload = cls._stored_factor_payload(row)
        current = cls._clean_external_text(payload.get("current_industry_name"), 80)
        if current:
            return current
        legacy = cls._clean_external_text(payload.get("industry_name"), 80)
        return legacy or "行业未记录"

    @classmethod
    def _stored_candidate_group(cls, row: dict) -> str:
        payload = cls._stored_factor_payload(row)
        current = cls._clean_external_text(payload.get("current_industry_name"), 80)
        if current:
            return current
        legacy = cls._clean_external_text(payload.get("industry_name"), 80)
        return f"候选快照行业：{legacy}" if legacy else "行业未记录"

    @staticmethod
    def _stored_trade_date(row: dict) -> str:
        value = str(row.get("actual_trade_date") or "").strip() if isinstance(row, dict) else ""
        return value if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value) else ""

    def _candidate_pool_chunks(self, rows: list[dict]) -> list[str]:
        """Build bounded paragraphs while keeping each candidate's three lines intact."""
        rows = [row for row in (rows or []) if isinstance(row, dict)]
        if not rows:
            return []

        dates = list(dict.fromkeys(self._stored_trade_date(row) for row in rows if self._stored_trade_date(row)))
        quote_by_date: dict[str, dict[str, object]] = {}
        for trade_date in dates:
            try:
                quotes = self.store.daily_quotes(trade_date)
            except Exception:
                quotes = []
            values: dict[str, object] = {}
            for quote in quotes or []:
                if isinstance(quote, dict):
                    raw_code = quote.get("code", "")
                    raw_pct = quote.get("pct_change")
                else:
                    raw_code = getattr(quote, "code", "")
                    raw_pct = getattr(quote, "pct_change", None)
                code = normalize_code(raw_code)
                if re.fullmatch(r"\d{6}", code) and code not in values:
                    values[code] = raw_pct
            quote_by_date[trade_date] = values

        grouped: dict[str, list[tuple[int, dict, float]]] = {}
        for position, row in enumerate(rows):
            try:
                score = float(row.get("score"))
                score = score if math.isfinite(score) else float("-inf")
            except (TypeError, ValueError, OverflowError):
                score = float("-inf")
            grouped.setdefault(self._stored_candidate_group(row), []).append((position, row, score))
        ordered_groups = sorted(
            grouped.items(),
            key=lambda item: (-max(entry[2] for entry in item[1]), min(entry[0] for entry in item[1])),
        )

        numbered: list[tuple[str, str]] = []
        number = 0
        for industry, entries in ordered_groups:
            entries.sort(key=lambda entry: (-entry[2], entry[0]))
            for _position, row, _score in entries:
                number += 1
                trade_date = self._stored_trade_date(row)
                pct_change = quote_by_date.get(trade_date, {}).get(normalize_code(row.get("code", "")))
                numbered.append((industry, format_stored_compact_candidate(row, number, pct_change)))

        limit = self._int("push_max_chars", 3500, 500, 12000)
        header = [
            "最近候选池（仅研究）",
            "分类说明：东方财富当前行业分类，仅展示，非历史因子；旧记录按候选快照行业标注",
            f"数据日期：{self._stored_trade_date(rows[0]) or '未知'}；来源：{self._clean_external_text(rows[0].get('source'), 40) or '未知'}",
        ]
        chunks: list[str] = []
        current = list(header)
        current_group = ""
        has_candidate = False

        def flush() -> None:
            nonlocal current, current_group, has_candidate
            if current:
                chunks.append("\n".join(current))
            current = []
            current_group = ""
            has_candidate = False

        for industry, text in numbered:
            block_lines = text.splitlines()
            heading_needed = current_group != industry
            prefix: list[str] = []
            if has_candidate:
                prefix.append("")
            if heading_needed:
                prefix.append(f"板块：{industry}")
            proposed = current + prefix + block_lines
            if current and len("\n".join(proposed)) > limit:
                flush()
                current = [f"板块：{industry}"]
                current_group = industry
                has_candidate = False
                proposed = current + block_lines
            current = proposed
            current_group = industry
            has_candidate = True
        flush()
        return chunks

    @filter.command("候选池", alias={"候选详情"})
    async def candidate_pool(self, event: AstrMessageEvent, count: int = 0):
        rows = self.store.latest_screen_candidates(max(1, min(int(count or self._int("candidate_limit", 30, 1, 100)), 100)))
        if not rows:
            yield event.plain_result("暂无已保存候选池，请先执行 /全市场选股。")
            return
        for chunk in self._candidate_pool_chunks(rows):
            yield event.plain_result(chunk)

    async def _freeze_research_pools(self, as_of: str) -> dict:
        existing = await self._store_call(self.store.latest_research_pools)
        active = await self._store_call(self.store.active_raw_batch, as_of=as_of, max_stale_trading_days=1)
        if (not active or not active.get("fresh") or str(active.get("quality") or "") != "good"
                or str(active.get("actual_trade_date") or "") != as_of):
            return {}
        batch_id = str(active.get("batch_id") or active.get("active_batch_id") or "")
        if existing and existing.get("batch_id") == batch_id:
            return existing
        snapshot = await self._store_call(self.store.research_input, as_of=as_of, deep_limit=300)
        if not snapshot or snapshot["batch_id"] != batch_id:
            return {}
        primary, radar, diagnostics = select_pools(snapshot["day_rows"], snapshot["histories"])
        diagnostics["selection_policy"] = "technical-research-v1"
        return await self._store_call(self.store.save_research_pools, snapshot, primary, radar, diagnostics)

    async def _current_research_pools(self, today: str) -> dict:
        pools = await self._store_call(self.store.latest_research_pools)
        if not pools or str(pools.get("trade_date") or "") >= today:
            return {}
        active = await self._store_call(self.store.active_raw_batch, as_of=today, max_stale_trading_days=1)
        if not active or not active.get("fresh") or str(active.get("batch_id") or "") != pools.get("batch_id"):
            return {}
        return pools

    async def _research_evidence(self, pools: dict) -> dict:
        if not pools or not getattr(self.store, "path", None):
            return {}
        path = self.store.path.parent / "research_risk_evidence" / (str(pools["trade_date"]) + ".json")
        return await self._store_call(load_evidence, path, pools)

    @staticmethod
    def _research_pool_text(pools: Mapping[str, object], pool: str, evidence: dict | None = None) -> str:
        rows = pools.get("picks", {}).get(pool, [])
        title = "次日重点观察" if pool == "primary" else "名单外警戒观察"
        lines = [f"{title}｜{pools['trade_date']} 收盘后生成",
                 f"原始批次：{pools['batch_id']}；冻结：{pools['frozen_at']} UTC；来源：{pools['source']} 未复权",
                 "技术排序仅供研究；停牌/涨跌停/公告未逐股核验时为未知，不代表可买。"]
        if pool == "radar":
            lines.append("盘中仅监听下列有限名单；相对冻结收盘价上涨达到警戒幅度才提示观察，不是全市场扫描。")
        for row in rows:
            lines.append(f"{row['rank']}. {row['code']} {row['name']} 评分{row['score']} 收盘{row['close']:.2f} 风险{row['risk_level']}")
            label = evidence_label(evidence or {}, row["code"], pools["frozen_at"])
            if label:
                lines.append("  " + label)
        if not rows:
            lines.append("本批次没有达到基本历史/流动性要求的研究对象。")
        return "\n".join(lines)

    @filter.command("观察选股")
    async def research_screen(self, event: AstrMessageEvent):
        now = datetime.now(CHINA_TZ)
        if now.time() < time(15, 0):
            yield event.plain_result("收盘前不生成新研究名单；可用 /观察池 查看上一批次。")
            return
        try:
            pools = await self._freeze_research_pools(now.date().isoformat())
        except Exception:
            logger.exception("[%s] 研究双池冻结失败", PLUGIN_NAME)
            yield event.plain_result("研究双池冻结失败，旧名单未更改；请检查原始批次与插件日志。")
            return
        if not pools:
            yield event.plain_result("暂无完整且已发布的近一交易日原始日线；未生成或替换研究名单。")
            return
        evidence = await self._research_evidence(pools)
        yield event.plain_result(self._research_pool_text(pools, "primary", evidence))
        yield event.plain_result(self._research_pool_text(pools, "radar", evidence))

    @filter.command("观察池")
    async def research_primary(self, event: AstrMessageEvent):
        pools = await self._store_call(self.store.latest_research_pools)
        evidence = await self._research_evidence(pools)
        yield event.plain_result(self._research_pool_text(pools, "primary", evidence) if pools else "暂无已冻结的研究观察名单。")

    @filter.command("警戒池")
    async def research_radar(self, event: AstrMessageEvent):
        pools = await self._store_call(self.store.latest_research_pools)
        evidence = await self._research_evidence(pools)
        yield event.plain_result(self._research_pool_text(pools, "radar", evidence) if pools else "暂无已冻结的名单外警戒池。")

    @filter.command("研究状态", alias={"数据质量"})
    async def research_status(self, event: AstrMessageEvent):
        runs = self.store.recent_screen_runs(5)
        if not runs:
            pools = await self._store_call(self.store.latest_research_pools)
            research = (f"研究双池：{pools['trade_date']} 已冻结，重点{len(pools['picks']['primary'])}只，"
                        f"警戒{len(pools['picks']['radar'])}只；正式候选仍为空。") if pools else "研究双池：尚未冻结。"
            yield event.plain_result("研究状态：尚未运行正式收盘扫描。\n" + research + "\n盘中监听只输出白名单会话，且不会自动下单。")
            return
        latest = runs[0]
        health_rows = self.store.provider_health_rows()
        factor_meta = self.store.factor_snapshot_meta(str(latest.get("actual_trade_date") or "")) or {}
        market_meta = self.store.market_context_meta(str(latest.get("actual_trade_date") or "")) or {}
        snapshot_meta = self.store.snapshot_meta(str(latest.get("actual_trade_date") or "")) or {}
        delivery_summary = self.store.automatic_close_delivery_summary() if callable(getattr(self.store, "automatic_close_delivery_summary", None)) else {}
        acceptance = self.store.latest_daily_acceptance() if callable(getattr(self.store, "latest_daily_acceptance", None)) else None
        acceptance_alerts = self.store.daily_acceptance_alert_summary() if callable(getattr(self.store, "daily_acceptance_alert_summary", None)) else {}
        delivery_text = (
            f"自动收盘推送：已发送{int(delivery_summary.get('sent', 0) or 0)}，"
            f"待处理{int(delivery_summary.get('pending', 0) or 0) + int(delivery_summary.get('failed', 0) or 0)}，"
            f"结果不明{int(delivery_summary.get('unknown_delivery', 0) or 0)}"
            if delivery_summary else "自动收盘推送：当前存储不支持持久化状态"
        )
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
            f"{delivery_text}\n"
            f"每日验收：{(acceptance or {}).get('status', '尚无记录')}，日期{(acceptance or {}).get('trade_date', '未知')}，"
            f"告警待处理{int(acceptance_alerts.get('pending', 0) or 0) + int(acceptance_alerts.get('failed', 0) or 0)}，"
            f"结果不明{int(acceptance_alerts.get('unknown_delivery', 0) or 0)}\n"
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
        try:
            as_of_date = datetime.strptime(as_of, "%Y-%m-%d").date()
            if as_of_date.isoformat() != as_of:
                raise ValueError("non-canonical as_of")
        except (TypeError, ValueError, OverflowError):
            yield event.plain_result("验证暂不可用：候选基准日无效。")
            return
        evaluated = 0
        details = []
        returns = []
        raw_mode = self._tushare_mode()
        load_base_quotes = getattr(self.store, "daily_quotes", None)
        base = load_base_quotes(as_of) if not raw_mode and callable(load_base_quotes) else []
        if not isinstance(base, list):
            base = []
        evaluation_bars: dict[str, list[dict]] = {}
        evaluation_available = False
        evaluation_dataset_id = None
        evaluation_batch_id = None
        evaluation_generation = None
        if raw_mode:
            raw_codes = [str(row.get("code") or "").strip() for row in rows if str(row.get("code") or "").strip()]
            fetch_evaluation = getattr(self.quotes, "fetch_evaluation_daily_result", None)
            if not callable(fetch_evaluation):
                fetch_evaluation = getattr(self.quotes, "fetch_evaluation_daily", None)
            if callable(fetch_evaluation) and raw_codes:
                try:
                    try:
                        evaluation = await fetch_evaluation(as_of, horizon, codes=raw_codes)
                    except TypeError:
                        evaluation = await fetch_evaluation(as_of, horizon)
                    evaluation_diagnostics = self._raw_diagnostics(self._bulk_value(evaluation, "diagnostics", {}))
                    evaluation_bars = self._bulk_value(evaluation, "bars", {}) or {}
                    evaluation_dataset_id = evaluation_diagnostics.get("dataset_id") or self._bulk_value(evaluation, "dataset_id", None)
                    evaluation_batch_id = evaluation_diagnostics.get("batch_id") or self._bulk_value(evaluation, "batch_id", None)
                    evaluation_generation = evaluation_diagnostics.get("generation") or self._bulk_value(evaluation, "generation", None)
                    evaluation_available = bool(self._bulk_value(evaluation, "complete", False)) and isinstance(evaluation_bars, dict)
                    if str(evaluation_diagnostics.get("evaluation", "")).lower() not in {"true", "1"}:
                        evaluation_available = False
                    if not str(evaluation_diagnostics.get("dataset_key") or "").strip().lower().endswith("_evaluation"):
                        evaluation_available = False
                    try:
                        evaluation_available = evaluation_available and bool(str(evaluation_dataset_id or "").strip()) and bool(str(evaluation_batch_id or "").strip()) and int(evaluation_generation) > 0
                    except (TypeError, ValueError, OverflowError):
                        evaluation_available = False
                    if not evaluation_available:
                        evaluation_bars = {}
                except (TushareCircuitOpen, TushareBulkError, httpx.HTTPError, asyncio.TimeoutError, OSError, ValueError, TypeError, KeyError):
                    evaluation_available = False
                    evaluation_bars = {}
        for row in rows:
            plan = self._price_plan_from_payload(str(row.get("price_plan") or ""))
            if not price_plan_is_validated(plan):
                # Legacy or adjusted plans are intentionally not replayable.
                continue
            provenance = plan.provenance if isinstance(plan.provenance, dict) else {}
            if str(provenance.get("actual_date") or "") != as_of:
                continue
            code = str(row.get("code") or "").strip()
            if not code:
                continue
            if raw_mode:
                # Replay the exact generation that produced the plan.  The
                # active generation may have advanced since the original
                # screen, and legacy daily_bars are not a valid substitute.
                raw_batch_id = provenance.get("batch_id") or provenance.get("raw_batch_id")
                load_raw_batch = getattr(self.store, "raw_batch_bars", None)
                if not raw_batch_id or not callable(load_raw_batch):
                    continue
                try:
                    base_rows = load_raw_batch(raw_batch_id, [code], before_or_equal=as_of)
                    base_candidates, _ = self._quotes_from_raw_bars(base_rows, as_of)
                    base_quote = next((item for item in base_candidates if item.code == code and item.indicator_last_date == as_of), None)
                    # Future bars come from a separate evaluation generation;
                    # the screening batch is immutable at the candidate date.
                    raw_future = evaluation_bars.get(code, []) if evaluation_available else []
                except (RuntimeError, ValueError, TypeError, KeyError):
                    continue
            else:
                base_quote = next((q for q in base if getattr(q, "code", "") == code), None)
                # This is deliberately evaluation-only. It never changes a prior screen.
                if base_quote:
                    evaluation_end = (as_of_date + timedelta(days=horizon * 3 + 7)).isoformat()
                    try:
                        await self.quotes.enrich_indicators([base_quote], 1, evaluation_end)
                    except TypeError as exc:
                        try:
                            await self.quotes.enrich_indicators([base_quote], 1)
                        except TypeError:
                            raise exc
                    history_bars = getattr(self.quotes, "history_bars", {})
                    bars = self._unadjusted_bars(history_bars.get(base_quote.code, []) if isinstance(history_bars, dict) else [])
                    save_daily_bars = getattr(self.store, "save_daily_bars", None)
                    if bars and callable(save_daily_bars):
                        save_daily_bars(base_quote.code, bars, "eastmoney_evaluation", "unadjusted")
                load_future_bars = getattr(self.store, "daily_bars", None)
                raw_future = load_future_bars(code, after=as_of) if callable(load_future_bars) else []
            future = []
            future_rows = raw_future
            if raw_mode:
                future_rows, _ = self._raw_indicator_rows(raw_future, expected_code=code)
            for item in self._unadjusted_bars(future_rows):
                row_date = self._canonical_screen_date(item.get("trade_date")) if isinstance(item, dict) else None
                if not row_date or row_date <= as_of or row_date > datetime.now(CHINA_TZ).date().isoformat():
                    continue
                try:
                    values = {key: float(item[key]) for key in ("open", "high", "low", "close")}
                    if not all(math.isfinite(value) and value > 0 for value in values.values()):
                        continue
                    if values["high"] < values["low"] or values["high"] < max(values["open"], values["close"]) or values["low"] > min(values["open"], values["close"]):
                        continue
                except (KeyError, TypeError, ValueError, OverflowError):
                    continue
                future.append({**item, **values})
                if len(future) >= horizon:
                    break
            if not base_quote or len(future) < horizon:
                continue
            last = future[-1]
            try:
                base_price = float(plan.reference_price)
            except (TypeError, ValueError, OverflowError):
                base_price = 0.0
            if not math.isfinite(base_price) or base_price <= 0:
                base_price = 0.0
            if not base_price:
                continue
            ret = (last["close"] - base_price) / base_price * 100
            highs = [(item["high"] - base_price) / base_price * 100 for item in future]
            lows = [(item["low"] - base_price) / base_price * 100 for item in future]
            first_touch = None
            for item in future:
                touches = []
                if plan.invalidation and item["low"] <= plan.invalidation:
                    touches.append("失效位")
                if plan.sell_low and item["high"] >= plan.sell_low:
                    touches.append("参考卖出区")
                if plan.confirmation and item["high"] >= plan.confirmation:
                    touches.append("建议买入价")
                if touches:
                    first_touch = "、".join(touches) if len(touches) == 1 else "同日多价位触达，日线无法判断先后（" + "、".join(touches) + "）"
                    break
            save_evaluation = getattr(self.store, "save_result_evaluation", None)
            if callable(save_evaluation):
                values = (f"{run_id}:{code}:{horizon}", run_id, code, as_of, horizon, "complete", last["close"], ret, max(highs), min(lows), first_touch, True, "unadjusted", True)
                try:
                    save_evaluation(*values, evaluation_dataset_id=evaluation_dataset_id, evaluation_batch_id=evaluation_batch_id, evaluation_generation=evaluation_generation)
                except TypeError:
                    # Keep lightweight pre-v0.13 store adapters usable while
                    # the built-in store records the full evaluation lineage.
                    save_evaluation(*values)
            evaluated += 1
            returns.append(ret)
            details.append(f"{row.get('name') or code}（{code}）：{ret:+.2f}%｜最大浮盈{max(highs):+.2f}%｜最大回撤{min(lows):+.2f}%｜关键位{first_touch or '未触达'}")
        if not details:
            if raw_mode and not evaluation_available:
                yield event.plain_result(f"验证暂不可用：未取得独立、已校验的未来 raw K 线，不能从筛选批次推断后续 {horizon} 个交易日。")
            else:
                yield event.plain_result(f"验证暂不可用：候选后续 K 线不足 {horizon} 个交易日。")
            return
        avg = sum(returns) / len(returns)
        yield event.plain_result(f"回放验证（仅研究）：基准日 {as_of}，周期 {horizon} 日，完成 {evaluated} 条，平均收益{avg:+.2f}%\n" + "\n".join(details[:20]))

    @filter.command("结果")
    async def results(self, event: AstrMessageEvent, count: int = 20):
        rows = [
            row for row in self.store.evaluations(limit=max(1, min(int(count or 20), 100)))
            if str(row.get("price_basis") or "unknown").strip().lower() == "unadjusted" and bool(row.get("plan_validated"))
        ]
        if not rows:
            yield event.plain_result("暂无已保存的回放结果，请先执行 /验证 [天数]。")
            return
        lines = ["已保存回放结果（仅研究）"]
        for row in rows:
            ret = row.get("return_pct")
            text = f"{row['code']}｜基准{row['as_of']}｜{row['horizon']}日收益{float(ret):+.2f}%" if ret is not None else f"{row['code']}｜基准{row['as_of']}｜结果未完成"
            lines.append(text + f"｜关键位{row.get('first_touch') or '未触达'}")
        yield event.plain_result("\n".join(lines))

    @staticmethod
    def _recommendation_percent(value, *, signed=False):
        try:
            number = float(value)
            if math.isfinite(number):
                return f"{number:+.2f}%" if signed else f"{number:.1%}"
        except (TypeError, ValueError, OverflowError):
            pass
        return "数据不足"

    @filter.command("推荐复盘")
    async def recommendation_review(self, event: AstrMessageEvent, count: int = 20):
        """Read the durable, point-in-time review only; never fetch or trade."""
        reader = getattr(self.store, "recommendation_reviews", None)
        if not callable(reader):
            yield event.plain_result("推荐复盘暂不可用：当前存储尚未完成升级。")
            return
        rows = await self._store_call(reader, self._origin(event), max(1, min(int(count or 20), 100)))
        if not rows:
            yield event.plain_result("暂无推荐复盘记录；收盘候选会在完成验证后建立可审计快照。")
            return
        lines = ["推荐复盘（仅研究，T+5 交易日口径）"]
        for row in rows:
            status = str(row.get("status") or "pending")
            name = str(row.get("name") or row.get("code") or "")
            if status == "complete":
                lines.append(f"{name}（{row['code']}）｜{row['recommended_date']}｜收益{self._recommendation_percent(row.get('return_pct'), signed=True)}｜区间高{self._recommendation_percent(row.get('max_gain_pct'), signed=True)}｜回撤{self._recommendation_percent(row.get('max_drawdown_pct'), signed=True)}｜建议买入价{row.get('confirmation_order')}｜目标{row.get('target_order')}｜失效{row.get('invalidation_order')}")
            elif status == "unknown_order":
                lines.append(f"{name}（{row['code']}）｜{row['recommended_date']}｜顺序未知：日线同日触及，不能证明建议买入价/目标与失效先后。")
            else:
                lines.append(f"{name}（{row['code']}）｜{row['recommended_date']}｜{status}：{row.get('reason') or '观察期或数据尚不足'}")
        yield event.plain_result("\n".join(lines))

    @filter.command("策略表现")
    async def strategy_performance(self, event: AstrMessageEvent, horizon: int = 5):
        reader = getattr(self.store, "recommendation_performance", None)
        if not callable(reader):
            yield event.plain_result("策略表现暂不可用：当前存储尚未完成升级。")
            return
        try:
            horizon = int(horizon)
        except (TypeError, ValueError, OverflowError):
            horizon = 0
        if horizon not in {1, 3, 5, 10}:
            yield event.plain_result("策略表现仅支持 1、3、5、10 个交易日口径。")
            return
        rows = await self._store_call(reader, horizon, self._origin(event))
        if not rows:
            yield event.plain_result("策略表现：暂无样本；不会用未成熟观察期伪造命中率。")
            return
        lines = [f"价位计划表现（T+{horizon}；收益/MFE/收盘序列回撤按价格完备样本，路径命中/失效按顺序可证样本）"]
        for row in rows[:20]:
            mature = int(row.get("mature_count") or 0)
            if not mature:
                lines.append(f"价位计划配置{str(row.get('strategy_version') or 'unknown')[:26]}｜市场{row.get('market_regime') or 'unknown'}｜样本{row.get('sample_count') or 0}，成熟0：数据不足。")
                continue
            distribution = row.get("return_distribution") or {}
            percent = self._recommendation_percent
            lines.append(f"价位计划配置{str(row.get('strategy_version') or 'unknown')[:26]}｜市场{row.get('market_regime') or 'unknown'}｜样本{row.get('sample_count') or 0}/成熟{mature}/价格可评{row.get('price_evaluable_count') or 0}/顺序可证{row.get('order_evaluable_count') or 0}｜待成熟{row.get('pending_count') or 0}｜未知{row.get('unknown_count') or 0}｜顺序未知{row.get('unknown_order_count') or 0}｜正收益{percent(row.get('positive_return_rate'))}｜目标触及{percent(row.get('target_hit_rate'))}｜失效{row.get('invalidation_count') or 0}/{row.get('invalidation_eligible_count') or 0}({percent(row.get('invalidation_rate'))})｜中位收益{percent(row.get('median_return_pct'), signed=True)}｜中位MFE{percent(row.get('median_max_gain_pct'), signed=True)}｜范围{percent(distribution.get('min'), signed=True)}~{percent(distribution.get('max'), signed=True)}｜收盘序列最大回撤{percent(row.get('max_drawdown_pct'), signed=True)}")
        yield event.plain_result("\n".join(lines))

    @filter.command("自选")
    async def watch(self, event: AstrMessageEvent, action: str = "", code: str = "", cost: str = ""):
        origin, action = self._origin(event), str(action or "").strip().lower()
        raw_code = str(code or "").strip()
        raw_cost = str(cost or "").strip()
        # Some adapters pass the command tail as the first argument.  Split
        # only the optional numeric suffix and keep Chinese names intact.
        if not raw_cost and raw_code:
            parts = raw_code.replace("，", " ").replace(",", " ").split()
            if len(parts) > 1 and self._parse_cost(parts[-1]) is not None:
                raw_code, raw_cost = " ".join(parts[:-1]), parts[-1]
        cost_price = self._parse_cost(raw_cost) if raw_cost else None
        if action in {"添加", "add"}:
            if raw_cost and cost_price is None:
                yield event.plain_result("用法：/自选 添加 600000 [成本价]")
                return
            resolved = self._resolve_stock_query(raw_code)
            if resolved.get("status") == "ambiguous":
                yield event.plain_result(self._ambiguous_stock_text(resolved))
                return
            if resolved.get("status") != "ok" or not resolved.get("code"):
                yield event.plain_result("未找到股票，请使用六位代码或已同步的股票名称。")
                return
            code_value = str(resolved["code"])
            stock_name = str(resolved.get("name") or "").strip() or None
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
        elif action in {"删除", "移除", "del", "remove"}:
            resolved = self._resolve_stock_query(raw_code)
            if resolved.get("status") == "ambiguous":
                yield event.plain_result(self._ambiguous_stock_text(resolved))
                return
            if resolved.get("status") != "ok" or not resolved.get("code"):
                yield event.plain_result("未找到股票，请使用六位代码或已同步的股票名称。")
                return
            existing = {item_code: item_name for item_code, item_name, _ in self.store.list_watch_details_with_names(origin)}
            code_value = str(resolved["code"])
            label = f"{existing.get(code_value) or code_value}（{code_value}）" if existing.get(code_value) else code_value
            yield event.plain_result(("已移除：" if self.store.remove_watch(origin, code_value) else "自选中没有：") + label)
        else:
            current = self.store.list_watch_details_with_names(origin)
            values = [f"{name or code}（{code}）" + (f"(成本{cost:.2f})" if cost else "") for code, name, cost in current]
            yield event.plain_result("自选股：" + ("、".join(values) if values else "暂无。用 /自选 添加 600000 [成本价]"))

    @filter.command("盯盘状态")
    async def intraday_status(self, event: AstrMessageEvent):
        origin = self._origin(event)
        if not origin:
            yield event.plain_result("无法读取当前会话标识，不能查询盯盘状态。")
            return
        yield event.plain_result(self._intraday_status_text(origin))

    @filter.command("暂停盯盘")
    async def pause_intraday(self, event: AstrMessageEvent):
        origin = self._origin(event)
        if not origin:
            yield event.plain_result("无法读取当前会话标识，不能暂停盯盘。")
            return
        checker = getattr(self.store, "is_intraday_enabled", None)
        setter = getattr(self.store, "set_intraday_enabled", None)
        enabled = bool(checker(origin)) if callable(checker) else self.store.is_subscribed(origin)
        active = self.store.is_subscribed(origin) and enabled
        if callable(setter):
            setter(origin, False)
        else:
            self.store.set_subscription(origin, False)
        yield event.plain_result("已暂停当前会话盯盘。" if active else "当前会话盯盘已经暂停。")

    @filter.command("恢复盯盘")
    async def resume_intraday(self, event: AstrMessageEvent):
        origin = self._origin(event)
        if not origin:
            yield event.plain_result("无法读取当前会话标识，不能恢复盯盘。")
            return
        if not self._push_allowed(origin):
            yield event.plain_result("当前会话不在推送白名单，不能恢复盯盘。")
            return
        if not self.store.is_subscribed(origin):
            yield event.plain_result("当前会话尚未开启提醒，请先使用 /监听 开启。")
            return
        checker = getattr(self.store, "is_intraday_enabled", None)
        setter = getattr(self.store, "set_intraday_enabled", None)
        active = bool(checker(origin)) if callable(checker) else self.store.is_subscribed(origin)
        if callable(setter):
            setter(origin, True)
        else:
            self.store.set_subscription(origin, True)
        yield event.plain_result("当前会话盯盘已经运行。" if active else "已恢复当前会话盯盘。")

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
        raw_query = str(code or "").strip()
        codes = parse_codes(raw_query)
        if not codes and raw_query:
            resolved = self._resolve_stock_query(raw_query)
            if resolved.get("status") == "ambiguous":
                yield event.plain_result(self._ambiguous_stock_text(resolved))
                return
            if resolved.get("status") == "ok" and resolved.get("code"):
                codes = [str(resolved["code"])]
            else:
                yield event.plain_result("未找到股票，请使用六位代码或已同步的股票名称。")
                return
        if not codes:
            yield event.plain_result("用法：/行情 600000")
            return
        try:
            quotes = await self.quotes.fetch_quotes(codes[:10])
            if self._tushare_mode():
                as_of = datetime.now(CHINA_TZ).date().isoformat()
                bars, provenance, _history_state = await self._read_fresh_raw_history_async([quote.code for quote in quotes], as_of)
                self._raw_screen_provenance = provenance
                for quote in quotes:
                    rows, _rejection = self._raw_indicator_rows(bars.get(quote.code, []), expected_code=quote.code)
                    apply_daily_indicators(quote, rows)
            else:
                concurrency = self._int("max_concurrency", 5, 1, 20)
                try:
                    await self.quotes.enrich_indicators(quotes, concurrency)
                except TypeError as exc:
                    try:
                        await self.quotes.enrich_indicators(quotes, concurrency, "")
                    except TypeError:
                        raise exc
            load_candidates = getattr(self.store, "latest_screen_candidates", None)
            rows = load_candidates(self._int("candidate_limit", 30, 1, 100)) if callable(load_candidates) else []
            if not isinstance(rows, list):
                rows = []
            stored = {str(row.get("code")): row for row in rows if isinstance(row, dict) and row.get("code")}
            candidates = []
            for item in quotes:
                candidate = score_quote(item)
                row = stored.get(item.code)
                if row:
                    plan = self._price_plan_from_payload(str(row.get("price_plan") or ""))
                    candidate.price_plan = plan if price_plan_is_validated(plan) else None
                    candidate.risk_level = str(row.get("risk_level") or candidate.risk_level)
                    try:
                        flags = json.loads(row.get("risk_flags") or "[]")
                        candidate.risk_flags = flags if isinstance(flags, list) else []
                    except (TypeError, ValueError, json.JSONDecodeError):
                        candidate.risk_flags = []
                else:
                    candidate.price_plan = None
                candidates.append(candidate)
            yield event.plain_result("\n".join(format_candidate(item) for item in candidates) or "没有拿到行情，可能是接口限流。")
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
