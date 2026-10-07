"""Watchlist additions queued by the dashboard.

The dashboard is read-only over a database snapshot, so it only drops small request files into an inbox
directory beside the intraday artifact.  The plugin applies them to one chat-session scope and publishes
a results file; session ids never leave the plugin, the dashboard only sees a masked label.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

INBOX_DIRNAME = "web_watch_inbox"
RESULTS_NAME = "web_watch_results.json"
REQUEST_MAX_BYTES = 2048
MAX_FILES_PER_PASS = 20
RESULTS_KEEP = 50
REQUEST_ID = re.compile(r"[0-9a-f]{32}")
CODE = re.compile(r"\d{6}")
MESSAGE_TYPES = {"FriendMessage": "私聊", "GroupMessage": "群聊"}


def parse_request(raw: bytes) -> dict:
    """Return {request_id, code} or {error, request_id?}; nothing else from the file is trusted."""
    if len(raw) > REQUEST_MAX_BYTES:
        return {"error": "too_large"}
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        return {"error": "not_json"}
    if not isinstance(payload, dict) or payload.get("v") != 1 or payload.get("action") != "add":
        return {"error": "unsupported"}
    request_id, code = str(payload.get("request_id") or ""), str(payload.get("code") or "")
    if not REQUEST_ID.fullmatch(request_id):
        return {"error": "invalid_request_id"}
    if not CODE.fullmatch(code):
        return {"error": "invalid_code", "request_id": request_id}
    return {"request_id": request_id, "code": code}


def resolve_scope(configured, whitelist, scopes) -> tuple[str | None, str]:
    """Pick the scope that receives web additions; more than one plausible scope is reported, never guessed."""
    configured = str(configured or "").strip()
    if configured:
        return configured, "configured"
    explicit = sorted({str(s).strip() for s in whitelist if str(s).strip() and str(s).strip() != "*"})
    used = sorted({str(s) for s in scopes if str(s)})
    both = [s for s in explicit if s in used]
    for candidates, how in ((both, "whitelisted_with_watchlist"), (explicit, "only_whitelisted"),
                            (used, "only_watchlist_scope")):
        if len(candidates) == 1:
            return candidates[0], how
        if len(candidates) > 1:
            return None, "ambiguous"
    return None, "no_scope"


def scope_label(scope) -> str:
    """Platform, chat kind and the last four characters of the session id; enough to recognise, not to address."""
    parts = str(scope or "").split(":", 2)
    if len(parts) != 3 or not parts[2]:
        return "已配置的会话"
    platform = re.sub(r"[^\w.-]", "", parts[0])[:24] or "平台未知"
    tail = re.sub(r"\W", "", parts[2])[-4:]
    return f"{platform} · {MESSAGE_TYPES.get(parts[1], '其它会话')} · 尾号 {tail or '未知'}"


def pending_requests(inbox: Path, limit: int = MAX_FILES_PER_PASS) -> list[Path]:
    """Oldest first by file name (millisecond prefix); dot files are writes still in progress."""
    try:
        files = [p for p in Path(inbox).iterdir() if p.suffix == ".json" and not p.name.startswith(".") and p.is_file()]
    except OSError:
        return []
    return sorted(files, key=lambda p: p.name)[:limit]


def process_inbox(inbox: Path, apply, limit: int = MAX_FILES_PER_PASS) -> list[dict]:
    """Apply queued requests oldest first and remove each file once handled, so it is applied at most once."""
    results = []
    for path in pending_requests(inbox, limit):
        try:
            raw = path.read_bytes() if path.stat().st_size <= REQUEST_MAX_BYTES else b"\0" * (REQUEST_MAX_BYTES + 1)
        except OSError:
            continue
        request = parse_request(raw)
        if "error" in request:
            outcome = {"request_id": request.get("request_id"), "code": None, "status": "invalid_request",
                       "reason": request["error"]}
        else:
            try:
                applied = apply(request["code"])
            except Exception:
                applied = {"status": "error", "reason": "apply_failed"}
            outcome = {"request_id": request["request_id"], "code": request["code"], **applied}
        try:
            path.unlink()
        except FileNotFoundError:
            pass
        except OSError:
            outcome["reason"] = "request_file_not_removed"
        results.append(outcome)
    return results


def merge_results(previous, new, keep: int = RESULTS_KEEP) -> list[dict]:
    kept = [r for r in previous if isinstance(r, dict)] if isinstance(previous, list) else []
    return (kept + list(new))[-keep:]


def payload(*, enabled: bool, scope_status: str, label: str | None, codes, limit: int, results, written_at: str,
            version: str, inbox_ready: bool) -> dict:
    codes = sorted({str(c) for c in codes if CODE.fullmatch(str(c))})
    return {"schema": 1, "written_at": written_at, "plugin_version": version, "enabled": bool(enabled),
            "inbox": "ready" if inbox_ready else "missing",
            "scope": {"status": scope_status, "label": label, "count": len(codes), "limit": int(limit)},
            "codes": codes, "results": list(results)}
