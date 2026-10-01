"""Read-only projections over existing plugin tables; no StockStore lifecycle."""
from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
import math
from pathlib import Path
import re
import sqlite3
import statistics
import time
from data_evidence import resolve as resolve_evidence, stamp as evidence_stamp


ROOT = Path(__file__).resolve().parents[1]
CHINA = timezone(timedelta(hours=8))
TABLES = {
    "web_demo_metadata", "screen_runs", "screen_candidates", "active_candidate_runs",
    "daily_quotes", "daily_bars", "market_contexts", "daily_snapshot_meta", "batches", "datasets", "active_generations",
    "provider_health", "provider_api_state", "job_runs", "intraday_event_outbox",
    "intraday_signal_states", "automatic_close_deliveries", "recommendation_records",
    "recommendation_outcomes", "recommendation_ai_review_batches", "recommendation_ai_reviews",
    "daily_acceptance_runs", "daily_acceptance_events", "daily_acceptance_alerts",
    "trading_calendar", "corporate_action_factors",
    "risk_events", "market_comparison_observations",
    "data_evidence_records", "factor_snapshots", "evidence_fetch_cache",
}
PUBLIC_SETTINGS = {
    "min_score", "price_min", "price_max", "deep_screen_limit", "factor_screen_limit",
    "screen_min_indicator_coverage", "intraday_confirmation_periods",
    "intraday_cooldown_seconds", "intraday_min_amount", "market_comparison_enabled",
    "market_comparison_benchmark", "paper_trading_only", "price_plan_close_tolerance_pct",
    "official_evidence_enabled", "official_evidence_candidate_limit", "official_evidence_cache_seconds",
}


def obj(value):
    try:
        result = json.loads(value) if isinstance(value, str) else value
        return result if isinstance(result, dict) else {}
    except (ValueError, TypeError):
        return {}


def arr(value):
    try:
        result = json.loads(value) if isinstance(value, str) else value
        return result if isinstance(result, list) else []
    except (ValueError, TypeError):
        return []


def stable_data_revision(snapshot_revision, artifact_revision):
    """One stable revision for client polling; component revisions stay separate."""
    payload = json.dumps({"snapshot": snapshot_revision or None, "artifact": artifact_revision or None}, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def stable_artifact_state_revision(revision, status, reason, market):
    """Expose a client revision when artifact validity changes without new rows."""
    payload = json.dumps({"artifact": revision or None, "status": status or "unknown",
                          "reason": reason or "", "market": market or {}},
                         sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]


def number(value):
    if isinstance(value, bool):
        return None
    try:
        value = float(value)
        return value if math.isfinite(value) else None
    except (TypeError, ValueError, OverflowError):
        return None


def instant(value):
    try:
        value = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
    except (TypeError, ValueError):
        return None


def error_category(value):
    text = str(value or "").lower()
    for token, label in (("permission", "permission_denied"), ("rate", "rate_limited"),
                         ("timeout", "timeout"), ("locked", "database_busy"), ("calendar", "calendar_unknown")):
        if token in text:
            return label
    return "error_recorded" if text else None


def safe_text(value, limit=120):
    return re.sub(r"[\x00-\x1f\x7f]", "", str(value or ""))[:limit]


class Unavailable(RuntimeError):
    pass


class Snapshot:
    def __init__(self, db, origin, now):
        self.db, self.origin, self.now = db, origin, now
        self.notices = set()
        self.tables = {r[0] for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        self._columns = {}
        self.demo = False
        if "web_demo_metadata" in self.tables:
            row = db.execute("SELECT value FROM web_demo_metadata WHERE key='kind'").fetchone()
            self.demo = bool(row and row[0] == "synthetic_demo")

    def snapshot_metadata(self):
        if "web_snapshot_metadata" not in self.tables:
            self.notices.add("web_snapshot_metadata:unavailable")
            return {"status": "unavailable", "reason": "snapshot_metadata_missing",
                    "captured_at": None, "revision": None, "refresh_interval_seconds": None,
                    "poll_interval_seconds": 5}
        try:
            values = dict(self.db.execute("SELECT key,value FROM web_snapshot_metadata"))
        except sqlite3.Error:
            self.notices.add("web_snapshot_metadata:unavailable")
            return {"status": "unavailable", "reason": "snapshot_metadata_unreadable",
                    "captured_at": None, "revision": None, "refresh_interval_seconds": None,
                    "poll_interval_seconds": 5}
        captured = instant(values.get("captured_at"))
        age = (self.now - captured).total_seconds() if captured else None
        fingerprint = values.get("source_fingerprint")
        try:
            parsed_fingerprint = json.loads(fingerprint) if isinstance(fingerprint, str) else fingerprint
            canonical_fingerprint = json.dumps(parsed_fingerprint, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            revision = hashlib.sha256(canonical_fingerprint.encode("utf-8")).hexdigest()[:16] if fingerprint else None
        except (TypeError, ValueError, json.JSONDecodeError):
            revision = None
        incomplete = captured is None or not fingerprint or revision is None
        if incomplete:
            self.notices.add("web_snapshot_metadata:partial")
        return {"captured_at": captured.isoformat() if captured else None,
                "age_seconds": max(0, int(age)) if age is not None else None,
                "status": "partial" if incomplete else "unknown" if age is None or age < 0 else "stale" if age > 7200 else "recent",
                "method": values.get("method"), "integrity": values.get("integrity"),
                "refresh_interval_seconds": 3600, "revision": revision,
                "poll_interval_seconds": 5}

    def columns(self, table):
        if table not in TABLES:
            raise ValueError("table not allowed")
        if table not in self.tables:
            self.notices.add(table + ":unavailable")
            return set()
        if table not in self._columns:
            self._columns[table] = {r[1] for r in self.db.execute(f"PRAGMA table_info({table})")}
        return self._columns[table]

    def rows(self, table, *, required=(), where="", params=(), order="", limit=500):
        columns = self.columns(table)
        if not columns or not set(required).issubset(columns):
            self.notices.add(table + ":schema_unavailable")
            return []
        sql = f"SELECT * FROM {table}" + (" WHERE " + where if where else "") + (" ORDER BY " + order if order else "")
        return [dict(r) for r in self.db.execute(sql + " LIMIT ?", (*params, min(max(int(limit), 1), 10000)))]

    def scoped(self, table, **kwargs):
        return self.rows(table, required=("origin", *kwargs.pop("required", ())),
                         where="origin=?", params=(self.origin,), **kwargs) if self.origin else []

    def recommendations(self, code=None):
        where, params = ("(visibility='public' OR origin=?)", [self.origin]) if self.origin else ("visibility='public'", [])
        if code:
            where += " AND code=?"
            params.append(code)
        return self.rows("recommendation_records", required=("visibility", "origin", "recommended_date", "code"),
                         where=where, params=params, order="recommended_date DESC,code", limit=1000)

    def ai_review_maps(self, run_ids):
        run_ids = [str(value) for value in dict.fromkeys(run_ids or []) if str(value)]
        if not run_ids:
            return {}, {}
        batches = self.rows(
            "recommendation_ai_review_batches",
            required=("review_batch_id", "run_id", "status", "model_requested", "prompt_version", "requested_at"),
            where="run_id IN (" + ",".join("?" for _ in run_ids) + ")",
            params=run_ids,
            order="requested_at DESC",
            limit=2000,
        )
        latest = {}
        for batch in batches:
            latest.setdefault(str(batch["run_id"]), batch)
        batch_ids = [str(batch["review_batch_id"]) for batch in latest.values()]
        if not batch_ids:
            return latest, {}
        reviews = self.rows(
            "recommendation_ai_reviews",
            required=("review_batch_id", "recommendation_id", "code", "decision"),
            where="review_batch_id IN (" + ",".join("?" for _ in batch_ids) + ")",
            params=batch_ids,
            limit=10000,
        )
        return latest, {(str(row["review_batch_id"]), str(row["recommendation_id"])): row for row in reviews}

    def candidates(self):
        refs = self.rows("active_candidate_runs", required=("scope", "run_id"),
                         where="scope IN (?,?)", params=("global", self.origin or "global"), limit=10)
        runs = self.rows("screen_runs", required=("run_id", "actual_trade_date"),
                         order="actual_trade_date DESC,finished_at DESC", limit=100)
        run_map = {r["run_id"]: r for r in runs}
        allowed = [r["run_id"] for r in refs]
        if not allowed:
            self.notices.add("candidate_visibility_or_active_run:unavailable")
            return []
        raw = self.rows("screen_candidates", required=("run_id", "code", "price_plan"),
                        where="run_id IN (" + ",".join("?" for _ in allowed) + ")",
                        params=allowed, order="score DESC,code", limit=500)
        recommendations = self.rows(
            "recommendation_records",
            required=("recommendation_id", "run_id", "code"),
            where="run_id IN (" + ",".join("?" for _ in allowed) + ")",
            params=allowed,
            limit=1000,
        )
        recommendation_map = {(str(row["run_id"]), str(row["code"])): row for row in recommendations}
        ai_batches, ai_reviews = self.ai_review_maps(allowed)
        result, seen = [], set()
        for row in raw:
            if row["code"] in seen:
                continue
            seen.add(row["code"])
            plan, factors = obj(row.get("price_plan")), obj(row.get("factor_payload"))
            run = run_map.get(row["run_id"], {})
            diagnostics = obj(run.get("diagnostics"))
            provenance = obj(plan.get("provenance"))
            total, adjustment = number(row.get("score")), number(factors.get("market_adjustment"))
            components = [number(factors.get(k)) for k in ("industry_score", "fundamental_score", "market_adjustment")]
            base = total if row.get("score_max") == 50 else (
                total - max(-20, min(20, int(round(sum(components[:2]))) + components[2]))
                if total is not None and all(v is not None for v in components) else None)
            coverage_value = number(run.get("coverage"))
            recommendation = recommendation_map.get((str(row["run_id"]), str(row["code"])), {})
            ai_batch = ai_batches.get(str(row["run_id"]), {})
            ai_review = ai_reviews.get((str(ai_batch.get("review_batch_id") or ""), str(recommendation.get("recommendation_id") or "")), {})
            result.append({
                "rank": len(result) + 1, "code": safe_text(row["code"]), "name": safe_text(row.get("name")),
                "score": total, "score_max": number(row.get("score_max")), "technical_score": base,
                "industry_score": components[0], "fundamental_score": components[1], "market_adjustment": adjustment,
                "industry": safe_text(factors.get("industry_name") or factors.get("current_industry_name")) or "unavailable",
                "risk_level": safe_text(row.get("risk_level")) or "unknown",
                "date": run.get("actual_trade_date"), "run_id": row["run_id"],
                "plan_version": row["run_id"] + ":" + hashlib.sha256(str(row.get("price_plan") or "").encode()).hexdigest()[:16],
                "plan_validated": plan.get("validated") is True and provenance.get("basis") == "unadjusted",
                "attention_low": number(plan.get("attention_low")), "attention_high": number(plan.get("attention_high")),
                "target_low": number(plan.get("sell_low")), "target_high": number(plan.get("sell_high")),
                "invalidation": number(plan.get("invalidation")), "confirmation": number(plan.get("confirmation")),
                "reference": number(plan.get("reference_price")), "atr": number(plan.get("atr")),
                "confidence": "plan_only / low" if plan.get("validated") is True else "unknown",
                "coverage": coverage_value, "fundamental_coverage": number(factors.get("fundamental_coverage")),
                "coverage_status": "unknown" if coverage_value is None else "available" if coverage_value >= 1 else "partial",
                "missing_reason": safe_text(diagnostics.get("degraded_reason") or diagnostics.get("reason")) or None,
                "source_timestamp": safe_text(diagnostics.get("source_timestamp") or diagnostics.get("quote_timestamp_max")) or None,
                "ai_review": {
                    "status": safe_text(ai_batch.get("status")) or "unavailable",
                    "decision": safe_text(ai_review.get("decision")) or "unknown",
                    "score_adjustment": number(ai_review.get("score_adjustment")),
                    "reason": safe_text(ai_review.get("reason"), 400) or None,
                    "risk_tags": [safe_text(value, 80) for value in arr(ai_review.get("risk_tags_json"))[:8]],
                    "model": safe_text(ai_batch.get("model_returned") or ai_batch.get("model_requested")) or "unknown",
                    "prompt_version": safe_text(ai_batch.get("prompt_version")) or "unknown",
                },
            })
        return result

    def signals(self, code=None):
        events = self.scoped("intraday_event_outbox", required=("code", "created_at"), order="created_at DESC", limit=200)
        result = []
        for event in events:
            if code and event["code"] != code:
                continue
            original = self.rows("screen_candidates", required=("run_id", "code", "price_plan"),
                                 where="run_id=? AND code=?", params=(event.get("run_id"), event["code"]), limit=1)
            plan = {}
            if original:
                encoded = str(original[0].get("price_plan") or "")
                version = str(event.get("run_id")) + ":" + hashlib.sha256(encoded.encode()).hexdigest()[:16]
                candidate_plan = obj(encoded)
                if (version == event.get("plan_version") and candidate_plan.get("validated") is True
                        and obj(candidate_plan.get("provenance")).get("basis") == "unadjusted"):
                    plan = candidate_plan
            quote = self.rows("daily_quotes", required=("code", "trade_date"),
                              where="code=? AND trade_date<=?",
                              params=(event["code"], self.now.astimezone(CHINA).date().isoformat()),
                              order="trade_date DESC", limit=1)
            quote = quote[0] if quote else {}
            fetched = quote.get("provider_ts") or quote.get("fetched_at")
            stamp, received = instant(fetched), instant(quote.get("fetched_at"))
            future = any(s is not None and s > self.now for s in (stamp, received))
            price = None if future else number(quote.get("price"))
            if price is not None and price <= 0:
                price = None
            low, high = number(plan.get("attention_low")), number(plan.get("attention_high"))
            distance = None
            if price is not None and price > 0 and low and high and low <= high:
                distance = ((price / low - 1) if price < low else (price / high - 1) if price > high else 0) * 100
            freshness = "unknown" if stamp is None or future else "fresh" if 0 <= (self.now - stamp).total_seconds() <= 180 else "stale"
            result.append({
                "code": safe_text(event["code"]), "name": safe_text(event.get("name")), "signal": safe_text(event.get("signal")),
                "state": safe_text(event.get("state")) or "unknown_delivery", "created_at": event.get("created_at"),
                "sent_at": event.get("sent_at"), "plan_version": safe_text(event.get("plan_version"), 180),
                "quote_at": event.get("quote_fetched_at") or None, "market_at": event.get("market_snapshot_at") or None,
                "last_stored_price": price, "last_stored_quote_at": fetched, "freshness": freshness,
                "price_evidence": self.price_evidence(quote, event["code"], "daily_quotes"),
                "distance_pct": distance, "attention_low": low, "attention_high": high,
                "invalidation": number(plan.get("invalidation")), "risk_event": bool(event.get("risk_event")),
                "error": error_category(event.get("last_error")),
            })
        return result

    @staticmethod
    def price_evidence(row, code, table):
        collected = instant(row.get("fetched_at"))
        return {"code": code, "business_date": row.get("trade_date"), "announcement_date": None,
                "collected_at": collected.isoformat() if collected else None,
                "source": safe_text(row.get("source")) or "unknown",
                "evidence": f"sqlite:{table}:{code}:{row.get('trade_date') or 'unknown'}",
                "quality": "stored_observation" if collected and row.get("source") else "unknown"}

    def overview(self):
        snapshots = self.rows("daily_snapshot_meta", required=("trade_date",), order="trade_date DESC", limit=1)
        runs = self.rows("screen_runs", required=("actual_trade_date",), order="actual_trade_date DESC,finished_at DESC", limit=8)
        contexts = self.rows("market_contexts", required=("as_of", "payload"), order="as_of DESC", limit=1)
        market = obj(contexts[0]["payload"]) if contexts else {}
        latest = runs[0] if runs else {}
        snapshot = snapshots[0] if snapshots else {}
        candidates = self.candidates()
        acceptance_rows = self.rows("daily_acceptance_runs", required=("trade_date", "status", "checked_at"), order="trade_date DESC,checked_at DESC", limit=1)
        acceptance = acceptance_rows[0] if acceptance_rows else {}
        acceptance_findings = arr(acceptance.get("findings_json")) if acceptance else []
        return {
            "data_date": snapshot.get("trade_date") or latest.get("actual_trade_date"),
            "requested_date": snapshot.get("requested_date"), "quality": snapshot.get("quality", "unknown"),
            "complete": snapshot.get("complete") == 1, "coverage": number(latest.get("coverage")),
            "market": {key: market.get(key) for key in ("regime", "breadth", "advancing", "declining", "flat", "median_return", "total_amount")},
            "market_date": contexts[0]["as_of"] if contexts else None,
            "candidate_count": len(candidates), "candidates": candidates[:5],
            "acceptance": {"trade_date": acceptance.get("trade_date"), "checked_at": acceptance.get("checked_at"),
                           "status": acceptance.get("status", "unknown"), "summary": acceptance.get("summary") or "尚无每日验收记录",
                           "findings": acceptance_findings},
            "signal_counts": dict(Counter(e["state"] for e in self.signals())),
            "batches": [{"date": r.get("actual_trade_date"), "generation": number(r.get("generation")),
                         "state": r.get("status", "unknown"), "rows": number(r.get("row_count"))}
                        for r in self.rows("batches", required=("actual_trade_date", "created_at"),
                                           order="actual_trade_date DESC,created_at DESC", limit=5)],
            "runs": [{"run_id": r.get("run_id"), "job": r.get("job_name"), "date": r.get("actual_trade_date"),
                      "status": r.get("status", "unknown"), "quality": r.get("quality", "unknown"),
                      "count": r.get("quote_count"), "coverage": number(r.get("coverage")),
                      "error": error_category(r.get("error"))} for r in runs],
        }

    def bars(self, code):
        rows = self.rows("daily_bars", required=("code", "trade_date", "open", "high", "low", "close"),
                         where="code=? AND trade_date<=?", params=(code, self.now.astimezone(CHINA).date().isoformat()),
                         order="trade_date DESC", limit=120)
        result = []
        for row in reversed(rows):
            values = [number(row[k]) for k in ("open", "high", "low", "close")]
            if (any(v is None or v <= 0 for v in values) or values[1] < max(values[0], values[3])
                    or values[2] > min(values[0], values[3]) or row.get("price_basis") != "unadjusted"):
                self.notices.add("daily_bars:unusable_rows_excluded")
                continue
            result.append({"date": row["trade_date"], "open": values[0], "high": values[1], "low": values[2], "close": values[3],
                           "volume": number(row.get("volume")), "source": row.get("source") or "unknown",
                           "data_evidence": self.price_evidence(row, code, "daily_bars")})
        return result

    def attach_latest_closes(self, items):
        """Read-only display fields: latest two valid unadjusted daily closes per candidate."""
        today = self.now.astimezone(CHINA).date().isoformat()
        for item in items:
            item.update({"close": None, "close_date": None, "prev_close": None, "pct_change": None})
            rows = self.rows("daily_bars", required=("code", "trade_date", "open", "high", "low", "close"),
                             where="code=? AND trade_date<=?", params=(item["code"], today),
                             order="trade_date DESC", limit=6)
            valid = []
            for row in rows:
                values = [number(row[k]) for k in ("open", "high", "low", "close")]
                if (any(v is None or v <= 0 for v in values) or values[1] < max(values[0], values[3])
                        or values[2] > min(values[0], values[3]) or row.get("price_basis") != "unadjusted"):
                    continue
                valid.append((row["trade_date"], values[3]))
                if len(valid) == 2:
                    break
            if valid:
                item["close_date"], item["close"] = valid[0]
            if len(valid) == 2 and valid[1][1]:
                item["prev_close"] = valid[1][1]
                item["pct_change"] = round((valid[0][1] / valid[1][1] - 1) * 100, 4)
        return items

    def stock(self, code):
        if not re.fullmatch(r"(?:\d{6}|DEMO\d{2})", code):
            raise Unavailable("invalid_stock_code")
        candidate = next((r for r in self.candidates() if r["code"] == code), None)
        bars = self.bars(code)
        recommendations = self.recommendations(code)
        if not candidate and not bars and not recommendations:
            raise Unavailable("stock_unavailable")
        closes = [r["close"] for r in bars]
        indicators = {"MA" + str(n): statistics.fmean(closes[-n:]) if len(closes) >= n else None for n in (5, 10, 20)}
        cutoff = (candidate or {}).get("date") or (bars[-1]["date"] if bars else self.now.astimezone(CHINA).date().isoformat())
        evidence = self.stock_evidence(code, cutoff)
        bar_collected = [instant(row.get("data_evidence", {}).get("collected_at")) for row in bars]
        source_timestamp = max((value for value in bar_collected if value), default=None)
        if bars:
            data_status, missing_reason = ("available", None) if len(bars) >= 20 else ("pending", "insufficient_history")
        else:
            data_status, missing_reason = "unknown", "daily_bars_not_collected"
        return {
            "code": code, "name": (candidate or {}).get("name") or (recommendations[0].get("name") if recommendations else code),
            "candidate": candidate, "bars": bars, "indicators": indicators,
            "last_close": closes[-1] if closes else None, "bar_date": bars[-1]["date"] if bars else None,
            "signals": self.signals(code),
            "recommendations": [{"date": r.get("recommended_date"), "plan_version": r.get("plan_version"),
                                 "plan_status": r.get("plan_status", "unknown"),
                                 "comparability": r.get("comparability_status", "unknown")} for r in recommendations[:30]],
            "announcements": evidence["announcements"], "data_evidence": evidence,
            "technical_history": {"bars": len(bars), "status": "available" if len(bars) >= 20 else "insufficient"},
            "data_quality": {"status": data_status, "missing_reason": missing_reason,
                             "source_timestamp": source_timestamp.isoformat() if source_timestamp else None},
        }

    def stock_evidence(self, code, cutoff):
        records, documents = [], []
        for row in self.rows("data_evidence_records",
                             required=("code", "business_date", "collected_at", "kind", "payload"),
                             where="code=? AND business_date<=?", params=(code, cutoff),
                             order="collected_at DESC", limit=200):
            payload = obj(row["payload"])
            collected = evidence_stamp(payload.get("collected_at"))
            announced = payload.get("announcement_date")
            if not collected or collected > self.now or not announced or announced > cutoff:
                continue
            if row["kind"] == "assertion":
                if payload.get("business_date") == cutoff:
                    records.append(payload)
            elif row["kind"] == "announcement" and payload.get("code") == code and payload.get("quality") == "readable":
                documents.append({key: safe_text(payload.get(key), 500 if key == "quote" else 160)
                                  for key in ("title", "source", "business_date", "announcement_date", "collected_at", "quote")})
        factors = self.rows("factor_snapshots", required=("code", "as_of", "payload", "source"),
                            where="code=? AND as_of=?", params=(code, cutoff), limit=20)
        for factor in factors:
            nested = obj(factor["payload"]).get("evidence_records", [])
            if isinstance(nested, list):
                records.extend(nested)
        # Deduplicate identical persisted observations, not contradictory ones.
        records = list({json.dumps(r, sort_keys=True): r for r in records if isinstance(r, dict)}.values())
        financial = {kind: resolve_evidence(records, code, cutoff, self.now.isoformat(), kind,
                                            exact_date=kind in ("pe", "pb"))
                     for kind in ("roe", "profit_growth", "cash_quality", "pe", "pb")}
        risks = {kind: resolve_evidence(records, code, cutoff, self.now.isoformat(), kind)
                 for kind in ("st_flag", "audit_flag", "suspended", "delisting_risk")}
        return {
            "as_of": cutoff,
            "financial": financial, "risk": risks,
            "financial_known": sum(v["quality"] == "verified" for v in financial.values()),
            "financial_total": len(financial),
            "risk_known": sum(v["quality"] == "verified" for v in risks.values()),
            "risk_total": len(risks),
            "sources": [{"source": safe_text(r.get("source")), "business_date": r.get("business_date"),
                         "announcement_date": r.get("announcement_date"), "collected_at": r.get("collected_at"),
                         "quality": safe_text(r.get("quality"))} for r in records[:30]],
            "announcements": {"status": "available" if documents else "unavailable", "items": documents},
        }

    def performance(self, horizon):
        if horizon not in (1, 3, 5, 10):
            raise Unavailable("unsupported_horizon")
        records = self.recommendations()
        ai_batches, ai_reviews = self.ai_review_maps([record.get("run_id") for record in records])
        outcomes = self.rows("recommendation_outcomes", required=("recommendation_id", "horizon"),
                             where="horizon=?", params=(horizon,), limit=2000)
        saved = {r["recommendation_id"]: r for r in outcomes}
        calendar = {r["trade_date"]: r for r in self.rows("trading_calendar", required=("trade_date", "status", "is_open"), limit=10000)}
        evaluations = []
        for record in records:
            old = saved.get(record["recommendation_id"], {})
            ai_batch = ai_batches.get(str(record.get("run_id") or ""), {})
            ai_review = ai_reviews.get((str(ai_batch.get("review_batch_id") or ""), str(record["recommendation_id"])), {})
            row = {"code": record["code"], "name": safe_text(record.get("name")), "date": record["recommended_date"],
                   "strategy": safe_text(record.get("strategy_version")) or "unknown",
                   "stored_status": old.get("status") or "pending", "status": "pending",
                   "maturity": "pending", "return_pct": None, "max_gain_pct": None, "drawdown_pct": None,
                   "target_hit": None, "invalidation_hit": None, "reason": "window_or_calendar_incomplete",
                   "ai_status": safe_text(ai_batch.get("status")) or "unavailable",
                   "ai_decision": safe_text(ai_review.get("decision")) or "unknown",
                   "ai_adjustment": number(ai_review.get("score_adjustment")),
                   "ai_reason": safe_text(ai_review.get("reason"), 400) or None,
                   "ai_model": safe_text(ai_batch.get("model_returned") or ai_batch.get("model_requested")) or "unknown"}
            try:
                cursor = date.fromisoformat(record["recommended_date"]) + timedelta(days=1)
            except (TypeError, ValueError):
                row.update(status="unknown", reason="recommendation_date_invalid")
                evaluations.append(row)
                continue
            days, calendar_unknown = [], False
            while cursor <= self.now.astimezone(CHINA).date() and len(days) < horizon:
                day = calendar.get(cursor.isoformat(), {})
                if not day.get("source") or day.get("status") not in ("open", "closed") or bool(day.get("is_open")) != (day["status"] == "open"):
                    calendar_unknown = True
                    break
                if day["is_open"]:
                    if cursor == self.now.astimezone(CHINA).date() and self.now.astimezone(CHINA).hour < 15:
                        break
                    days.append(cursor.isoformat())
                cursor += timedelta(days=1)
            if len(days) < horizon:
                if calendar_unknown:
                    row.update(status="unknown", maturity="unknown", reason="calendar_window_unverified")
                evaluations.append(row)
                continue
            row["maturity"] = "mature"
            base = number(record.get("confirmation_price") if record.get("confirmation_price") is not None else record.get("candidate_price"))
            factor = number(record.get("corporate_action_factor"))
            raw = self.rows("daily_bars", required=("code", "trade_date", "corporate_action_factor", "corporate_action_evidence"),
                            where="code=? AND trade_date IN (" + ",".join("?" for _ in days) + ")",
                            params=(record["code"], *days), order="trade_date", limit=10)
            snapshots = self.rows("daily_snapshot_meta", required=("trade_date", "complete"),
                                  where="trade_date IN (" + ",".join("?" for _ in days) + ")", params=days, limit=10)
            usable = (record.get("plan_status") == "validated" and record.get("comparability_status") == "comparable"
                      and record.get("price_basis") == "unadjusted" and base is not None and base > 0
                      and factor is not None and factor > 0 and len(raw) == horizon
                      and len(snapshots) == horizon and all(r["complete"] == 1 for r in snapshots))
            if not self.demo:
                factor_days = [record["recommended_date"], *days]
                factors = self.rows("corporate_action_factors",
                                    required=("code", "trade_date", "adj_factor", "source", "evidence"),
                                    where="code=? AND trade_date IN (" + ",".join("?" for _ in factor_days) + ")",
                                    params=(record["code"], *factor_days), limit=11)
                code = str(record["code"])
                exchange = "SH" if code.startswith("6") else "SZ" if code.startswith(("0", "3")) else "BJ" if code.startswith(("4", "8", "92")) else None
                expected = lambda day: f"tushare:adj_factor:{day}:{code}.{exchange}"
                usable = bool(usable and exchange and len(factors) == len(factor_days)
                              and all(f.get("source") == "tushare_adj_factor"
                                      and f.get("evidence") == expected(f["trade_date"])
                                      and number(f.get("adj_factor")) is not None
                                      and math.isclose(float(f["adj_factor"]), factor, rel_tol=1e-10)
                                      for f in factors)
                              and all(b.get("corporate_action_evidence") == expected(b["trade_date"]) for b in raw))
            for bar in raw:
                nums = [number(bar.get(k)) for k in ("open", "high", "low", "close", "volume", "amount")]
                f = number(bar.get("corporate_action_factor"))
                usable = bool(usable and all(v is not None and v > 0 for v in nums)
                              and nums[1] >= max(nums[0], nums[3]) and nums[2] <= min(nums[0], nums[3])
                              and f is not None and math.isclose(f, factor, rel_tol=1e-10)
                              and bar.get("price_basis") == "unadjusted" and bar.get("corporate_action_evidence"))
            if not usable:
                row.update(status="unknown", reason="price_calendar_or_comparability_missing")
                evaluations.append(row)
                continue
            closes = [float(r["close"]) for r in raw]
            peak, drawdown = base, 0.0
            for close in closes:
                peak = max(peak, close)
                drawdown = min(drawdown, (close / peak - 1) * 100)
            target, invalidation = number(record.get("target_low")), number(record.get("invalidation_price"))
            t = next((i for i, r in enumerate(raw) if target and r["high"] >= target), None)
            inv = next((i for i, r in enumerate(raw) if invalidation and r["low"] <= invalidation), None)
            unknown_order = old.get("status") == "unknown_order" or (t is not None and t == inv)
            row.update(status="unknown_order" if unknown_order else "complete", reason="daily_order_unprovable" if unknown_order else "",
                       return_pct=(closes[-1] / base - 1) * 100,
                       max_gain_pct=max((r["high"] / base - 1) * 100 for r in raw), drawdown_pct=drawdown)
            if not unknown_order:
                row["target_hit"] = t is not None if target else None
                row["invalidation_hit"] = inv is not None if invalidation else None
            evaluations.append(row)
        complete = [r for r in evaluations if r["return_pct"] is not None]
        paths = [r for r in evaluations if r["status"] == "complete"]
        targets = [r["target_hit"] for r in paths if r["target_hit"] is not None]
        invalidations = [r["invalidation_hit"] for r in paths if r["invalidation_hit"] is not None]
        returns = [r["return_pct"] for r in complete]
        def ai_group(decision):
            rows = evaluations if decision == "all" else [row for row in evaluations if row["ai_decision"] == decision]
            values = [row["return_pct"] for row in rows if row["return_pct"] is not None]
            return {
                "sample_count": len(rows),
                "price_evaluable_count": len(values),
                "positive_return_rate": sum(value > 0 for value in values) / len(values) if values else None,
                "median_return_pct": statistics.median(values) if values else None,
                "mean_return_pct": statistics.fmean(values) if values else None,
            }
        return {
            "horizon": horizon, "sample_count": len(evaluations), "mature_count": sum(r["maturity"] == "mature" for r in evaluations),
            "price_evaluable_count": len(complete), "order_evaluable_count": len(paths),
            "status_counts": dict(Counter(r["status"] for r in evaluations)),
            "median_return_pct": statistics.median(returns) if returns else None,
            "positive_return_rate": sum(v > 0 for v in returns) / len(returns) if returns else None,
            "target_hit_rate": sum(targets) / len(targets) if targets else None,
            "target_denominator": len(targets), "invalidation_rate": sum(invalidations) / len(invalidations) if invalidations else None,
            "invalidation_denominator": len(invalidations),
            "max_drawdown_pct": min((r["drawdown_pct"] for r in complete), default=None),
            "benchmark": {"status": "unavailable", "reason": "no_verified_same_window_benchmark", "return_pct": None},
            "ai_groups": {key: ai_group(key) for key in ("all", "keep", "watch", "veto")},
            "ai_batch_status_counts": dict(Counter(str(batch.get("status") or "unknown") for batch in ai_batches.values())),
            "records": evaluations, "basis": "gross_unadjusted_close_to_close",
        }

    def health(self):
        providers = self.rows("provider_api_state", required=("api_name",), limit=30)
        if not providers:
            providers = self.rows("provider_health", required=("provider",), limit=30)
        batches = self.rows("batches", required=("actual_trade_date",), order="actual_trade_date DESC,created_at DESC", limit=10)
        jobs = self.rows("job_runs", required=("job_key",), order="started_at DESC", limit=15)
        failures = self.rows("risk_events", required=("code",), order="event_at DESC", limit=20)
        outbox = self.signals()
        deliveries = self.scoped("automatic_close_deliveries", order="created_at DESC", limit=100)
        acceptance = self.rows("daily_acceptance_runs", required=("trade_date", "status", "checked_at"), order="trade_date DESC,checked_at DESC", limit=10)
        acceptance_alerts = self.rows("daily_acceptance_alerts", required=("state",), order="created_at DESC", limit=100)
        return {
            "database": "readable", "integrity": "not_checked", "tables": len(self.tables),
            "providers": [{"name": r.get("api_name") or r.get("provider"), "success_at": r.get("last_success_at"),
                           "error_at": r.get("last_error_at"), "quality": r.get("last_quality") or "unknown",
                           "error": error_category(r.get("last_error")), "blocked_until": r.get("blocked_until")} for r in providers],
            "batches": [{"id": r.get("batch_id"), "date": r.get("actual_trade_date"), "generation": r.get("generation"),
                         "state": r.get("status", "unknown"), "rows": r.get("row_count"), "basis": r.get("basis", "unknown"),
                         "published_at": r.get("published_at")} for r in batches],
            "jobs": [{"key": r.get("job_key"), "name": r.get("job_name"), "date": r.get("trade_date"),
                      "state": r.get("status", "unknown"), "error": error_category(r.get("error"))} for r in jobs],
            "failures": [{"code": r.get("code"), "state": r.get("state"), "risk": r.get("risk_level"), "at": r.get("event_at")} for r in failures],
            "outbox": dict(Counter(r["state"] for r in outbox)),
            "automatic_outbox": dict(Counter(r.get("state", "unknown_delivery") for r in deliveries)),
            "daily_acceptance": [{"date": r.get("trade_date"), "checked_at": r.get("checked_at"),
                                  "status": r.get("status", "unknown"), "summary": r.get("summary") or ""} for r in acceptance],
            "daily_acceptance_outbox": dict(Counter(r.get("state", "unknown_delivery") for r in acceptance_alerts)),
            "evidence": self.source_summary(),
        }

    def source_summary(self):
        sources = []
        for table, date_field, time_field in (("daily_bars", "trade_date", "fetched_at"),
                                            ("factor_snapshots", "as_of", "fetched_at"),
                                            ("data_evidence_records", "business_date", "collected_at")):
            if table not in self.tables:
                sources.append({"dataset": table, "status": "not_collected", "count": None})
                continue
            columns = self.columns(table)
            if not {"source", date_field, time_field}.issubset(columns):
                sources.append({"dataset": table, "status": "unavailable", "count": None})
                continue
            rows = self.db.execute(f"SELECT source,COUNT(*) AS n,MAX({date_field}) AS date,MAX({time_field}) AS collected FROM {table} GROUP BY source LIMIT 30")
            rows = list(rows)
            if not rows:
                sources.append({"dataset": table, "status": "not_collected", "count": 0})
            for row in rows:
                sources.append({"dataset": table, "source": safe_text(row["source"]), "count": row["n"],
                                "date": row["date"], "collected_at": row["collected"], "status": "stored_not_live"})
        batch_columns = self.columns("batches")
        generation_columns = self.columns("active_generations")
        dataset_columns = self.columns("datasets")
        required_batch = {"batch_id", "dataset_id", "actual_trade_date", "status", "quality", "source", "row_count", "published_at"}
        if required_batch.issubset(batch_columns) and {"dataset_id", "active_batch_id", "generation"}.issubset(generation_columns) and {"dataset_id", "dataset_key"}.issubset(dataset_columns):
            rows = self.db.execute(
                "SELECT d.dataset_key,b.source,b.actual_trade_date,b.quality,b.row_count,b.published_at,a.generation "
                "FROM active_generations a JOIN batches b ON b.batch_id=a.active_batch_id "
                "JOIN datasets d ON d.dataset_id=a.dataset_id "
                "WHERE b.status='published' ORDER BY d.dataset_key LIMIT 30"
            )
            for row in rows:
                sources.append({"dataset": "raw_active_generation", "dataset_key": row["dataset_key"],
                                "source": safe_text(row["source"]), "count": row["row_count"],
                                "date": row["actual_trade_date"], "collected_at": row["published_at"],
                                "generation": row["generation"], "quality": row["quality"],
                                "status": "active_published_snapshot"})
        elif batch_columns or generation_columns or dataset_columns:
            sources.append({"dataset": "raw_active_generation", "status": "unavailable", "count": None})
        else:
            sources.append({"dataset": "raw_active_generation", "status": "not_collected", "count": None})
        snapshot_columns = self.columns("daily_snapshot_meta")
        required_snapshot = {"trade_date", "requested_date", "source", "quality", "complete", "fetched_at"}
        if required_snapshot.issubset(snapshot_columns):
            row = self.db.execute(
                "SELECT trade_date,requested_date,source,quality,complete,fetched_at "
                "FROM daily_snapshot_meta ORDER BY trade_date DESC,fetched_at DESC LIMIT 1"
            ).fetchone()
            if row:
                sources.append({"dataset": "daily_snapshot_meta", "source": safe_text(row["source"]),
                                "count": None, "date": row["trade_date"], "requested_date": row["requested_date"],
                                "collected_at": row["fetched_at"], "quality": row["quality"],
                                "complete": row["complete"] == 1,
                                "status": "complete_stored_snapshot" if row["complete"] == 1 else "incomplete_snapshot"})
            else:
                sources.append({"dataset": "daily_snapshot_meta", "status": "not_collected", "count": 0})
        elif snapshot_columns:
            sources.append({"dataset": "daily_snapshot_meta", "status": "unavailable", "count": None})
        else:
            sources.append({"dataset": "daily_snapshot_meta", "status": "not_collected", "count": None})
        return sources

class Dashboard:
    def __init__(self, database: Path, *, origin="", settings=None, artifact_path=None, now=None):
        self.database = Path(database).absolute()
        self.origin, self.settings = origin, settings
        self.artifact_configured = artifact_path is not None
        self.artifact_path = Path(artifact_path).absolute() if artifact_path else self.database.with_name("intraday_quotes.json")
        self.clock = now or (lambda: datetime.now(timezone.utc))

    @contextmanager
    def snapshot(self):
        if not self.database.is_file():
            raise Unavailable("database_missing")
        db = sqlite3.connect(self.database.resolve().as_uri() + "?mode=ro", uri=True, timeout=0.3)
        db.row_factory = sqlite3.Row
        deadline = time.monotonic() + 3
        try:
            db.execute("PRAGMA query_only=ON")
            db.execute("BEGIN")
            db.set_progress_handler(lambda: int(time.monotonic() > deadline), 5000)
            yield Snapshot(db, self.origin, self.clock())
        finally:
            db.rollback()
            db.close()

    def public_settings(self):
        schema = obj((ROOT / "_conf_schema.json").read_text(encoding="utf-8"))
        supplied = {}
        if self.settings is not None:
            supplied = obj(Path(self.settings).read_text(encoding="utf-8")).get("values", {})
            if not isinstance(supplied, dict):
                supplied = {}
        result = []
        for key in sorted(PUBLIC_SETTINGS):
            spec = schema.get(key, {})
            value = supplied.get(key)
            kind = spec.get("type")
            valid = ((kind == "bool" and isinstance(value, bool))
                     or (kind in ("int", "float") and not isinstance(value, bool) and number(value) is not None)
                     or (key == "market_comparison_benchmark" and isinstance(value, str) and re.fullmatch(r"\d{6}\.(SH|SZ|CSI)", value)))
            result.append({"key": key, "label": spec.get("description", key), "default": spec.get("default"),
                           "effective": value if valid else None, "source": "explicit_public_snapshot" if valid else "effective_unknown"})
        return {"items": result, "read_only": True, "sensitive_fields": "not_exposed"}

    def intraday(self):
        try:
            artifact = json.loads(self.artifact_path.read_text(encoding="utf-8"))
            if not isinstance(artifact, dict) or artifact.get("schema_version") != 1:
                raise ValueError("artifact_schema_invalid")
        except FileNotFoundError:
            artifact = {"schema_version": 1, "status": "unknown", "reason": "artifact_missing", "quotes": []}
        except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError):
            artifact = {"schema_version": 1, "status": "unknown", "reason": "artifact_parse_failed", "quotes": []}
        now = self.clock()
        published = instant(artifact.get("published_at"))
        age = (now - published).total_seconds() if published else None
        status, reason = str(artifact.get("status") or "unknown"), str(artifact.get("reason") or "")
        valid_for = number(artifact.get("valid_for_seconds"))
        valid_for_seconds = max(30, min(int(valid_for), 900)) if valid_for is not None else 30
        if published is None:
            status, reason = "unknown", reason or "published_at_missing"
        elif age < 0:
            status, reason = "unknown", "published_at_future"
        elif age > valid_for_seconds:
            status, reason = "unknown", "artifact_expired"
        rows, clean, times = artifact.get("quotes"), [], []
        if not isinstance(rows, list):
            rows = []
            status, reason = "unknown", "artifact_quotes_invalid"
        for row in rows:
            if not isinstance(row, dict):
                status, reason = "unknown", reason or "quote_parse_failed"
                continue
            provider, collected = instant(row.get("provider_ts")), instant(row.get("collected_at"))
            if provider is None or collected is None or provider > now or collected > now:
                status, reason = "unknown", reason or "quote_timestamp_invalid"
                continue
            times.append(provider)
            clean.append({key: row.get(key) for key in ("code", "name", "price", "pct_change", "amount", "volume", "provider_ts", "collected_at", "source")})
        if times and (max(times) - min(times)).total_seconds() > 90:
            status, reason = "unknown", "mixed_provider_timestamps"
        market = self._artifact_live_market(artifact, now, status, reason)
        effective_revision = stable_artifact_state_revision(artifact.get("revision"), status, reason, market)
        return {"artifact_path": self.artifact_path.name, "artifact_path_configured": self.artifact_configured, "revision": artifact.get("revision"),
                "source": artifact.get("source") or "unknown", "status": status, "reason": reason,
                "target_count": int(artifact.get("target_count") or 0), "returned_count": int(artifact.get("returned_count") or len(clean)),
                "missing_count": int(artifact.get("missing_count") or 0), "provider_ts": artifact.get("provider_ts") or [],
                "collected_at": artifact.get("collected_at"), "published_at": artifact.get("published_at"),
                "age_seconds": max(0, int(age)) if age is not None and age >= 0 else None, "valid_for_seconds": valid_for_seconds,
                "market": market, "effective_revision": effective_revision, "quotes": clean}

    @staticmethod
    def _unknown_live_market(reason, *, pending=False):
        return {"status": "pending" if pending else "unknown", "regime": "unknown", "quality": "unknown",
                "reason": str(reason or "live_market_evidence_unavailable"), "source": "",
                "source_timestamp": None, "updated_at": None, "sample_size": 0, "expected_size": 0,
                "coverage": None, "opportunity_allowed": False}

    def _artifact_live_market(self, artifact, now, artifact_status, artifact_reason):
        if artifact_reason in {"artifact_missing", "artifact_parse_failed", "published_at_missing", "published_at_future", "artifact_expired"}:
            return self._unknown_live_market(artifact_reason or "artifact_unavailable")
        values = artifact.get("market") if isinstance(artifact, dict) else None
        if not isinstance(values, dict):
            return self._unknown_live_market("live_market_missing")
        regime = str(values.get("regime") or "unknown").strip().lower()
        pending_regime = str(values.get("pending_regime") or "unknown").strip().lower()
        quality = str(values.get("quality") or "unknown").strip().lower()
        reason = str(values.get("reason") or "live_market_evidence_unavailable").strip()
        source = str(values.get("source") or "").strip()
        source_timestamp, updated_at = instant(values.get("source_timestamp")), instant(values.get("updated_at"))
        if regime == "unknown":
            return self._unknown_live_market(reason, pending=pending_regime in {"strong", "neutral", "weak", "risk_off"})
        if regime not in {"strong", "neutral", "weak", "risk_off"} or quality != "good" or reason != "confirmed" or source != "sina":
            return self._unknown_live_market(reason or "live_market_unconfirmed")
        if source_timestamp is None or updated_at is None:
            return self._unknown_live_market("live_market_timestamp_missing")
        local_now = now.astimezone(CHINA)
        if source_timestamp > now or updated_at > now:
            return self._unknown_live_market("live_market_timestamp_future")
        if source_timestamp.astimezone(CHINA).date() != local_now.date() or updated_at.astimezone(CHINA).date() != local_now.date():
            return self._unknown_live_market("live_market_not_today")
        if (now - source_timestamp).total_seconds() > 150 or (now - updated_at).total_seconds() > 150:
            return self._unknown_live_market("live_market_expired")
        sample, expected, coverage = number(values.get("sample_size")), number(values.get("expected_size")), number(values.get("coverage"))
        if sample is None or expected is None or coverage is None or sample < 1 or expected < 1 or sample > expected or not 0 <= coverage <= 1:
            return self._unknown_live_market("live_market_coverage_invalid")
        return {"status": "available", "regime": regime, "quality": quality, "reason": reason, "source": source,
                "source_timestamp": source_timestamp.isoformat(), "updated_at": updated_at.isoformat(),
                "sample_size": int(sample), "expected_size": int(expected), "coverage": coverage,
                "opportunity_allowed": regime != "risk_off"}

    def query(self, route, params=None):
        params = params or {}
        if route == "intraday":
            try:
                data = self.intraday()
                return {"meta": {"status": "partial" if data.get("status") != "available" else "available", "read_only": True,
                                 "dataset_kind": "intraday_artifact", "database": self.database.name,
                                 "artifact_path": self.artifact_path.name, "at": self.clock().isoformat(),
                                 "sources": [{"dataset": "intraday_artifact", "status": data.get("status"), "count": data.get("returned_count")}], "notices": []}, "data": data}
            except Exception:
                return {"meta": {"status": "unavailable", "reason": "artifact_unavailable", "read_only": True,
                                 "dataset_kind": "intraday_artifact", "at": self.clock().isoformat()}, "data": None}
        try:
            with self.snapshot() as snapshot:
                artifact = self.intraday()
                if artifact.get("status") != "available":
                    snapshot.notices.add("intraday_artifact:" + safe_text(artifact.get("reason") or artifact.get("status")))
                if route == "overview":
                    data = snapshot.overview()
                    data["live_market"] = artifact.get("market")
                elif route == "revision":
                    metadata = snapshot.snapshot_metadata()
                    snapshot_revision = metadata.get("revision")
                    artifact_revision = artifact.get("revision")
                    data = {"data_revision": stable_data_revision(snapshot_revision, artifact.get("effective_revision")),
                            "snapshot_revision": snapshot_revision, "artifact_revision": artifact_revision,
                            "captured_at": metadata.get("captured_at"),
                            "status": metadata.get("status", "unknown"),
                            "refresh_interval_seconds": metadata.get("refresh_interval_seconds"),
                            "poll_interval_seconds": metadata.get("poll_interval_seconds", 5)}
                elif route == "signals":
                    data = {"items": snapshot.signals(), "origin_configured": bool(self.origin)}
                elif route == "candidates":
                    data = {"items": snapshot.attach_latest_closes(snapshot.candidates())}
                elif route.startswith("stocks/"):
                    data = snapshot.stock(route.split("/", 1)[1])
                elif route == "performance":
                    data = snapshot.performance(int(params.get("horizon", "5")))
                elif route == "health":
                    data = snapshot.health()
                elif route == "settings":
                    data = self.public_settings()
                elif route == "intraday":
                    data = self.intraday()
                else:
                    raise Unavailable("route_not_found")
                if route == "revision":
                    return {"meta": {"status": "partial" if snapshot.notices else "available", "read_only": True,
                                     "dataset_kind": "synthetic_demo" if snapshot.demo else "local_database",
                                     "database": self.database.name, "at": self.clock().isoformat(),
                                     "data_revision": data.get("data_revision"),
                                     "artifact_revision": data.get("artifact_revision"),
                                     "snapshot_revision": data.get("snapshot_revision"),
                                     "snapshot": snapshot.snapshot_metadata(), "sources": [], "notices": sorted(snapshot.notices)},
                            "data": data}
                sources = snapshot.source_summary()
                snapshot_metadata = snapshot.snapshot_metadata()
                artifact_revision = artifact.get("revision")
                data_revision = stable_data_revision(snapshot_metadata.get("revision"), artifact.get("effective_revision"))
                return {"meta": {"status": "partial" if snapshot.notices else "available", "read_only": True,
                                 "dataset_kind": "synthetic_demo" if snapshot.demo else "local_database",
                                 "database": self.database.name, "at": self.clock().isoformat(),
                                 "data_revision": data_revision, "artifact_revision": artifact_revision,
                                 "snapshot_revision": snapshot_metadata.get("revision"),
                                 "snapshot": snapshot_metadata,
                                 "sources": sources,
                                 "notices": sorted(snapshot.notices)}, "data": data}
        except (sqlite3.Error, OSError, ValueError, TypeError, KeyError, AttributeError, OverflowError, Unavailable) as exc:
            reason = str(exc) if isinstance(exc, Unavailable) else "database_busy_or_schema_unavailable" if isinstance(exc, sqlite3.Error) else "input_or_source_unavailable"
            return {"meta": {"status": "unavailable", "reason": reason, "read_only": True,
                             "dataset_kind": "unknown", "at": self.clock().isoformat()}, "data": None}
