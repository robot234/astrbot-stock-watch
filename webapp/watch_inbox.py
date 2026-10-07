"""Queue "add to watchlist" requests for the plugin.

The dashboard stays read-only over its database snapshot: it only writes small request files into the
plugin's inbox directory and reads back the plugin's results file.  It never learns the chat session id.
"""
from __future__ import annotations

from collections import deque
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import re
import secrets
import threading
import time

CODE = re.compile(r"\d{6}")
REQUEST_ID = re.compile(r"[0-9a-f]{32}")
REASON = re.compile(r"[a-z_]{1,40}")
RESULTS_NAME = "web_watch_results.json"
RESULTS_MAX_BYTES = 262144
MAX_PENDING, PER_MINUTE, PER_DAY = 30, 10, 200
FRESH_SECONDS = 180
RESULT_STATUSES = {"added", "exists", "limit_reached", "invalid_request", "scope_unresolved", "disabled", "error"}
SCOPE_STATUSES = {"configured", "whitelisted_with_watchlist", "only_whitelisted", "only_watchlist_scope",
                  "ambiguous", "no_scope"}


def _instant(value):
    try:
        parsed = datetime.fromisoformat(str(value))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else None


def _text(value, limit):
    return re.sub(r"[\x00-\x1f\x7f]", "", str(value if value is not None else "")).strip()[:limit] or None


class WatchInbox:
    def __init__(self, inbox, clock=time.time):
        self.inbox = Path(inbox)
        self.results_path = self.inbox.with_name(RESULTS_NAME)
        self.clock = clock
        self.lock = threading.Lock()
        self.recent = deque()

    def pending(self):
        try:
            return [p.name for p in self.inbox.iterdir() if p.suffix == ".json" and not p.name.startswith(".")]
        except OSError:
            return None

    def submit(self, code, known):
        code = str(code if code is not None else "")
        if not CODE.fullmatch(code):
            return 400, {"status": "rejected", "reason": "invalid_code"}
        if not known(code):
            return 404, {"status": "rejected", "reason": "unknown_code"}
        if not self.inbox.is_dir():
            return 503, {"status": "rejected", "reason": "inbox_missing"}
        with self.lock:
            now = self.clock()
            while self.recent and now - self.recent[0] > 86400:
                self.recent.popleft()
            if sum(1 for t in self.recent if now - t <= 60) >= PER_MINUTE or len(self.recent) >= PER_DAY:
                return 429, {"status": "rejected", "reason": "rate_limited"}
            pending = self.pending()
            if pending is None:
                return 503, {"status": "rejected", "reason": "inbox_unreadable"}
            if len(pending) >= MAX_PENDING:
                return 503, {"status": "rejected", "reason": "inbox_full"}
            request_id = secrets.token_hex(16)
            name = f"{int(now * 1000):013d}-{request_id}.json"
            body = json.dumps({"v": 1, "action": "add", "code": code, "request_id": request_id,
                               "requested_at": datetime.fromtimestamp(now, timezone.utc).isoformat(timespec="seconds")})
            temporary = self.inbox / f".{name}.tmp"
            try:
                with temporary.open("x", encoding="utf-8") as stream:
                    stream.write(body)
                os.replace(temporary, self.inbox / name)
            except OSError:
                try:
                    temporary.unlink()
                except OSError:
                    pass
                return 503, {"status": "rejected", "reason": "inbox_unwritable"}
            self.recent.append(now)
        return 202, {"status": "queued", "request_id": request_id, "code": code}

    def plugin_results(self):
        try:
            if self.results_path.stat().st_size > RESULTS_MAX_BYTES:
                return None, "results_too_large"
            payload = json.loads(self.results_path.read_text(encoding="utf-8"))
        except FileNotFoundError:
            return None, "results_missing"
        except (OSError, ValueError):
            return None, "results_unreadable"
        return (payload, None) if isinstance(payload, dict) else (None, "results_unreadable")

    def status(self, request_id=None):
        """What the page needs: is the plugin applying requests, where they go (masked), codes already there."""
        payload, problem = self.plugin_results()
        now = datetime.fromtimestamp(self.clock(), timezone.utc)
        out = {"configured": True, "inbox": "ready" if self.inbox.is_dir() else "missing", "plugin": None,
               "scope": None, "codes": [], "request": None}
        if payload is not None:
            written = _instant(payload.get("written_at"))
            age = (now - written).total_seconds() if written else None
            scope = payload.get("scope") if isinstance(payload.get("scope"), dict) else {}
            status = scope.get("status") if scope.get("status") in SCOPE_STATUSES else "unknown"
            out["plugin"] = {"written_at": written.isoformat() if written else None,
                             "age_seconds": round(age) if age is not None else None,
                             "fresh": bool(age is not None and -60 <= age <= FRESH_SECONDS),
                             "enabled": payload.get("enabled") is True,
                             "inbox": "ready" if payload.get("inbox") == "ready" else "missing",
                             "version": _text(payload.get("plugin_version"), 20)}
            out["scope"] = {"status": status, "label": _text(scope.get("label"), 60),
                            "count": scope.get("count") if isinstance(scope.get("count"), int) else None,
                            "limit": scope.get("limit") if isinstance(scope.get("limit"), int) else None}
            codes = payload.get("codes") if isinstance(payload.get("codes"), list) else []
            out["codes"] = sorted({c for c in codes if isinstance(c, str) and CODE.fullmatch(c)})[:1000]
        else:
            out["problem"] = problem
        if request_id is not None:
            out["request"] = self.request_state(str(request_id), payload)
        return out

    def request_state(self, request_id, payload):
        if not REQUEST_ID.fullmatch(request_id):
            return {"state": "invalid_id"}
        results = payload.get("results") if payload and isinstance(payload.get("results"), list) else []
        match = next((r for r in reversed(results) if isinstance(r, dict) and r.get("request_id") == request_id), None)
        if match is not None:
            status = match.get("status") if match.get("status") in RESULT_STATUSES else "error"
            reason = match.get("reason") if isinstance(match.get("reason"), str) and REASON.fullmatch(match["reason"]) else None
            code = match.get("code") if isinstance(match.get("code"), str) and CODE.fullmatch(match["code"]) else None
            processed = _instant(match.get("processed_at"))
            return {"state": "done", "status": status, "reason": reason, "code": code, "name": _text(match.get("name"), 40),
                    "processed_at": processed.isoformat() if processed else None}
        pending = self.pending() or []
        return {"state": "pending" if any(name.endswith(f"-{request_id}.json") for name in pending) else "unknown"}
