"""Point-in-time assertions and opt-in, backend-only official risk evidence."""
from __future__ import annotations

import asyncio
from datetime import date, datetime, timedelta, timezone
import hashlib
from html.parser import HTMLParser
import json
import math
from pathlib import Path
import re
import sqlite3
from urllib.parse import urlsplit
import uuid


CHINA = timezone(timedelta(hours=8))
RISK_KINDS = ("st_flag", "audit_flag", "suspended", "delisting_risk")
OFFICIAL_HOSTS = ("cninfo.com.cn", "sse.com.cn", "szse.cn", "bse.cn")


def day(value):
    text = str(value or "")
    if re.fullmatch(r"\d{8}", text):
        text = f"{text[:4]}-{text[4:6]}-{text[6:]}"
    try:
        return date.fromisoformat(text).isoformat()
    except ValueError:
        return None


def stamp(value):
    try:
        result = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        return result.astimezone(timezone.utc) if result.tzinfo else None
    except ValueError:
        return None


def factor_capture(record, code, trade_date, cutoff=None):
    """Validate a locally observed factor; its trade date is not publication time."""
    if not isinstance(record, dict):
        return None
    code = str(code or "")
    business = day(trade_date)
    observed = stamp(record.get("observed_at"))
    digest = str(record.get("response_sha256") or "").lower()
    suffix = "SH" if code.startswith("6") else "SZ" if code.startswith(("0", "3")) else "BJ"
    expected = f"tushare:adj_factor:{business}:{code}.{suffix}"
    try:
        factor = float(record.get("adj_factor") if record.get("adj_factor") is not None else record.get("factor"))
    except (TypeError, ValueError, OverflowError):
        return None
    if (not re.fullmatch(r"\d{6}", code) or not business or not observed
            or record.get("conflicted") not in (None, 0, False)
            or record.get("source") != "tushare_adj_factor"
            or record.get("evidence") != expected
            or not re.fullmatch(r"[0-9a-f]{64}", digest)
            or not math.isfinite(factor) or factor <= 0
            or (cutoff is not None and (not stamp(cutoff) or observed > stamp(cutoff)))):
        return None
    return {"factor": factor, "observed_at": observed.isoformat(),
            "response_sha256": digest, "evidence": expected, "source": "tushare_adj_factor"}


def envelope(code, business_date, kind, value, *, source, evidence,
             announcement_date=None, collected_at=None, quality="verified"):
    return {"code": code, "business_date": day(business_date), "kind": kind,
            "value": value, "announcement_date": day(announcement_date),
            "collected_at": collected_at or datetime.now(timezone.utc).isoformat(),
            "source": source, "evidence": evidence, "quality": quality}


def validate(record, code, as_of, known_at, *, exact_date=True):
    """Validation never manufactures a value or backdates collection."""
    if not isinstance(record, dict) or not re.fullmatch(r"\d{6}", str(code)):
        return False
    business, cutoff = day(record.get("business_date")), day(as_of)
    collected, known = stamp(record.get("collected_at")), stamp(known_at)
    announced = day(record.get("announcement_date"))
    value = record.get("value")
    try:
        valid_value = isinstance(value, bool) if record.get("kind") in RISK_KINDS else (
            not isinstance(value, bool) and isinstance(value, (float, int)) and math.isfinite(value))
    except OverflowError:
        valid_value = False
    return bool(
        record.get("code") == code and business and cutoff and known and collected
        and business <= cutoff and (not exact_date or business == cutoff)
        and (record.get("announcement_date") is None or announced)
        and cutoff <= known.astimezone(CHINA).date().isoformat()
        and (not announced or announced <= cutoff)
        and (record.get("kind") not in ("roe", "profit_growth", "cash_quality") or (announced and business <= announced))
        and (record.get("kind") not in RISK_KINDS or announced)
        and collected <= known
        and (not announced or collected.astimezone(CHINA).date().isoformat() >= announced)
        and record.get("quality") == "verified" and valid_value
        and str(record.get("source") or "").strip()
        and str(record.get("evidence") or "").strip()
        and str(record.get("source")).lower() not in ("unknown", "unavailable", "none")
        and record.get("kind")
    )


def resolve(records, code, as_of, known_at, kind, *, exact_date=True):
    candidates = [r for r in records if isinstance(r, dict) and r.get("kind") == kind]
    if not candidates:
        return {"value": None, "quality": "unknown", "reason": "evidence_missing"}
    if any(not validate(r, code, as_of, known_at, exact_date=exact_date) for r in candidates):
        return {"value": None, "quality": "unknown", "reason": "evidence_invalid"}
    values = {(type(r["value"]).__name__, str(r["value"])) for r in candidates}
    if len(values) != 1:
        return {"value": None, "quality": "unknown", "reason": "source_conflict"}
    return {"value": candidates[0]["value"], "quality": "verified", "reason": ""}


def safe_factor_row(row, code, as_of, known_at):
    """Revalidate numerical/risk evidence before scoring or reusing a cache."""
    if not isinstance(row, dict):
        return {}
    if (row.get("code") is not None and row["code"] != code) or (
            row.get("as_of") is not None and day(row["as_of"]) != day(as_of)):
        return {"evidence_status": {"row": "code_or_date_mismatch"}}
    result = dict(row)
    records = row.get("evidence_records")
    records = records if isinstance(records, list) else []
    reasons = {}
    for kind in ("pe", "pb", "roe", "profit_growth", "cash_quality", *RISK_KINDS):
        outcome = resolve(records, code, as_of, known_at, kind,
                          exact_date=kind not in ("roe", "profit_growth", "cash_quality"))
        result[kind] = outcome["value"]
        if outcome["reason"]:
            reasons[kind] = outcome["reason"]
    # An upstream aggregate score cannot bypass missing component evidence.
    for kind in ("fundamental_score", "industry_score"):
        result[kind] = resolve(records, code, as_of, known_at, kind)["value"]
    result["evidence_status"] = reasons
    result["evidence_records"] = records
    return result


class TextBody(HTMLParser):
    def __init__(self):
        super().__init__()
        self.parts, self.hidden = [], 0

    def handle_starttag(self, tag, attrs):
        if tag in ("script", "style"):
            self.hidden += 1

    def handle_endtag(self, tag):
        if tag in ("script", "style"):
            self.hidden = max(0, self.hidden - 1)

    def handle_data(self, data):
        if not self.hidden:
            self.parts.append(data)


class EvidenceUnavailable(RuntimeError):
    pass


class OfficialEvidenceAdapter:
    """Search JSON contract or explicit document descriptors; no browser engine."""
    def __init__(self, http, *, search_url="", documents=(), trusted_hosts=OFFICIAL_HOSTS,
                 min_interval=1.0):
        self.http, self.search_url = http, str(search_url)
        self.documents = tuple(documents)
        self.hosts = tuple(str(host).lower().strip(".") for host in trusted_hosts)
        self.interval = max(0.1, float(min_interval))
        self._last_request = 0.0
        self._lock = asyncio.Lock()

    def allowed(self, url):
        try:
            parsed = urlsplit(str(url))
            return (parsed.scheme == "https" and parsed.port in (None, 443)
                    and not parsed.username and not parsed.password
                    and not re.search(r"(?:^|&)(?:token|api_key|key|secret|password)=", parsed.query, re.I)
                    and any(parsed.hostname == host or (parsed.hostname or "").endswith("." + host)
                            for host in self.hosts if host and "." in host))
        except ValueError:
            return False

    async def fetch(self, url, params=None):
        if not self.allowed(url):
            raise EvidenceUnavailable("untrusted_source")
        async with self._lock:
            loop = asyncio.get_running_loop()
            await asyncio.sleep(max(0, self.interval - (loop.time() - self._last_request)))
            self._last_request = loop.time()
            async with self.http.slot() as client:
                async with client.stream("GET", url, params=params, timeout=10,
                                         follow_redirects=False) as response:
                    if response.status_code in (401, 403):
                        raise EvidenceUnavailable("permission_denied")
                    if response.status_code != 200:
                        raise EvidenceUnavailable("fetch_failed")
                    content = bytearray()
                    async for chunk in response.aiter_bytes():
                        content.extend(chunk)
                        if len(content) > 1_000_000:
                            raise EvidenceUnavailable("body_too_large")
                    content_type = response.headers.get("content-type", "").lower()
                    if not any(t in content_type for t in ("text/", "application/json")):
                        raise EvidenceUnavailable("body_unreadable")
                    text = content.decode("utf-8", errors="strict")
                    if any(term in text.lower() for term in ("captcha", "验证码", "访问验证", "人机验证")):
                        raise EvidenceUnavailable("captcha_or_access_block")
                    return text

    async def collect(self, code, as_of):
        descriptors = [d for d in self.documents if isinstance(d, dict) and d.get("code") == code]
        if self.search_url:
            body = json.loads(await self.fetch(self.search_url, {"code": code, "as_of": as_of,
                                                               "kinds": ",".join(RISK_KINDS)}))
            if not isinstance(body, dict) or body.get("status") != "complete" or not isinstance(body.get("items"), list):
                raise EvidenceUnavailable("search_incomplete")
            descriptors += body["items"]
        if not descriptors:
            return {"records": [], "documents": [], "status": "unknown", "reason": "search_no_evidence"}
        if len(descriptors) > 3:
            raise EvidenceUnavailable("document_limit_requires_narrower_search")
        records, documents = [], []
        for item in descriptors:
            if not isinstance(item, dict) or item.get("code") != code:
                raise EvidenceUnavailable("document_code_mismatch")
            url = item.get("url")
            text = await self.fetch(url)
            parser = TextBody()
            parser.feed(text)
            visible = " ".join(parser.parts)
            quote = str(item.get("quote") or "").strip()
            business, announced = day(item.get("business_date")), day(item.get("announcement_date"))
            if not business or not announced or not (announced <= as_of and business == as_of):
                raise EvidenceUnavailable("document_date_unverified")
            dates = (business, business.replace("-", ""), date.fromisoformat(business).strftime("%Y年%m月%d日"))
            publication_dates = (announced, announced.replace("-", ""), date.fromisoformat(announced).strftime("%Y年%m月%d日"))
            publication = r"(?:公告日期|披露日期|发布日期)\s*[:：]?\s*(?:" + "|".join(re.escape(d) for d in publication_dates) + ")"
            if (not re.search(rf"(?<!\d){re.escape(code)}(?!\d)", visible)
                    or not any(d in visible for d in dates) or not re.search(publication, visible)
                    or len(quote) < 8 or quote not in visible):
                raise EvidenceUnavailable("body_binding_unverified")
            collected = datetime.now(timezone.utc).isoformat()
            documents.append({"code": code, "business_date": business, "announcement_date": announced,
                              "collected_at": collected, "source": urlsplit(url).hostname,
                              "evidence": url, "title": str(item.get("title") or "")[:160],
                              "quote": quote[:500], "body_sha256": hashlib.sha256(text.encode()).hexdigest(),
                              "quality": "readable"})
            # Only explicit implemented actions/opinions are machine-readable.
            # Applications, intentions and keyword mentions cannot prove safety.
            if (re.search(r"申请|拟|可能|未|不|非", quote)
                    or not re.search(rf"(?<!\d){re.escape(code)}(?!\d)", quote)
                    or not any(d in quote for d in dates)):
                continue
            kind, value = item.get("kind"), item.get("value")
            patterns = {
                ("st_flag", True): r"实施(?:退市风险警示|其他风险警示)",
                ("st_flag", False): r"撤销(?:退市风险警示及其他风险警示|其他风险警示|退市风险警示)",
                ("audit_flag", True): r"出具.{0,12}(?:无法表示意见|否定意见|(?<!无)保留意见)",
                ("audit_flag", False): r"出具.{0,12}标准无保留意见",
                ("suspended", True): r"起停牌",
                ("suspended", False): r"起复牌",
                ("delisting_risk", True): r"终止上市|实施退市风险警示",
            }
            pattern = patterns.get((kind, value)) if isinstance(value, bool) else None
            if kind == "st_flag" and value is False and (
                    "恢复正常交易" not in quote or "实施其他风险警示" in quote):
                pattern = None
            if pattern and re.search(pattern, quote):
                record = envelope(code, business, kind, value, source=urlsplit(url).hostname,
                                  evidence=url, announcement_date=announced, collected_at=collected)
                # An announcement can precede its effective date.
                record["effective_announcement"] = True
                records.append(record)
        return {"records": records, "documents": documents, "status": "complete", "reason": ""}


class EvidenceStore:
    """Owns only additive evidence tables; fencing covers cache and evidence."""
    def __init__(self, path):
        self.path = Path(path)
        with sqlite3.connect(self.path) as db:
            db.executescript("""
                CREATE TABLE IF NOT EXISTS data_evidence_records(
                    evidence_id TEXT PRIMARY KEY, code TEXT NOT NULL, business_date TEXT NOT NULL,
                    announcement_date TEXT, collected_at TEXT NOT NULL, source TEXT NOT NULL,
                    evidence TEXT NOT NULL, quality TEXT NOT NULL, kind TEXT NOT NULL,
                    payload TEXT NOT NULL);
                CREATE INDEX IF NOT EXISTS idx_data_evidence_code_date
                    ON data_evidence_records(code,business_date,collected_at);
                CREATE TABLE IF NOT EXISTS evidence_fetch_cache(
                    cache_key TEXT PRIMARY KEY, payload TEXT NOT NULL, expires_at REAL NOT NULL);
                CREATE TABLE IF NOT EXISTS evidence_fetch_lease(
                    name TEXT PRIMARY KEY, owner TEXT NOT NULL, expires_at REAL NOT NULL,
                    next_allowed_at REAL NOT NULL);
            """)

    def cached(self, key, now):
        with sqlite3.connect(self.path, timeout=0.3) as db:
            row = db.execute("SELECT payload FROM evidence_fetch_cache WHERE cache_key=? AND expires_at>?", (key, now)).fetchone()
        return json.loads(row[0]) if row else None

    def claim(self, owner, now):
        with sqlite3.connect(self.path, timeout=0.3) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT expires_at,next_allowed_at FROM evidence_fetch_lease WHERE name='official-risk'").fetchone()
            if row and max(row) > now:
                return False
            db.execute("INSERT OR REPLACE INTO evidence_fetch_lease VALUES('official-risk',?,?,?)", (owner, now + 90, now))
            return True

    def finish(self, key, owner, result, now, *, ttl=3600, interval=2):
        with sqlite3.connect(self.path, timeout=0.3) as db:
            db.execute("BEGIN IMMEDIATE")
            row = db.execute("SELECT owner,expires_at FROM evidence_fetch_lease WHERE name='official-risk'").fetchone()
            if not row or row[0] != owner or row[1] <= now:
                return False
            payload = json.dumps(result, ensure_ascii=False, allow_nan=False, sort_keys=True)
            db.execute("INSERT OR REPLACE INTO evidence_fetch_cache VALUES(?,?,?)", (key, payload, now + ttl))
            db.execute("DELETE FROM evidence_fetch_cache WHERE expires_at<=?", (now,))
            db.execute("DELETE FROM evidence_fetch_cache WHERE cache_key IN (SELECT cache_key FROM evidence_fetch_cache ORDER BY expires_at DESC LIMIT -1 OFFSET 2000)")
            for kind, records in (("assertion", result.get("records", [])), ("announcement", result.get("documents", []))):
                for record in records:
                    encoded = json.dumps(record, ensure_ascii=False, allow_nan=False, sort_keys=True)
                    db.execute("INSERT OR IGNORE INTO data_evidence_records VALUES(?,?,?,?,?,?,?,?,?,?)",
                               (hashlib.sha256(encoded.encode()).hexdigest(), record["code"], record["business_date"],
                                record.get("announcement_date"), record["collected_at"], record["source"],
                                record["evidence"], record["quality"], kind, encoded))
            db.execute("UPDATE evidence_fetch_lease SET owner='',expires_at=0,next_allowed_at=? WHERE name='official-risk'", (now + interval,))
            return True


class RiskEvidenceService:
    def __init__(self, store, adapter, *, cache_seconds=3600, clock=None):
        self.store, self.adapter = store, adapter
        self.ttl = max(60, min(int(cache_seconds), 86400))
        self.clock = clock or (lambda: datetime.now(timezone.utc).timestamp())
        configuration = {"search": adapter.search_url, "hosts": adapter.hosts, "documents": adapter.documents}
        self.namespace = hashlib.sha256(json.dumps(configuration, sort_keys=True).encode()).hexdigest()

    async def get(self, code, as_of):
        unknown = lambda reason: {"records": [], "documents": [], "status": "unknown", "reason": reason}
        if not re.fullmatch(r"\d{6}", str(code)) or day(as_of) != as_of:
            return unknown("invalid_request")
        key = hashlib.sha256(f"{self.namespace}:{code}:{as_of}".encode()).hexdigest()
        try:
            cached = await asyncio.to_thread(self.store.cached, key, self.clock())
            if cached is not None:
                if (not isinstance(cached, dict) or not isinstance(cached.get("records"), list)
                        or not isinstance(cached.get("documents"), list)
                        or cached.get("status") not in ("complete", "unknown")):
                    return unknown("cache_invalid")
                return cached
            owner = uuid.uuid4().hex
            if not await asyncio.to_thread(self.store.claim, owner, self.clock()):
                return unknown("writer_busy_or_rate_limited")
            try:
                result = await asyncio.wait_for(self.adapter.collect(code, as_of), timeout=45)
            except EvidenceUnavailable as exc:
                result = unknown(str(exc))
            except Exception:
                result = unknown("search_or_fetch_failed")
            ttl = self.ttl if result.get("records") else min(self.ttl, 300)
            saved = await asyncio.to_thread(self.store.finish, key, owner, result, self.clock(), ttl=ttl)
            return result if saved else unknown("lease_lost")
        except (sqlite3.Error, ValueError, TypeError):
            return unknown("evidence_store_unavailable")
