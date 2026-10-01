from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Iterable, Mapping


def _iso(value):
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc).isoformat()
    return str(value or "")


def _quote_row(quote, collected_at: datetime) -> dict:
    provider_ts = getattr(quote, "provider_ts", None)
    return {
        "code": str(getattr(quote, "code", "") or ""),
        "name": str(getattr(quote, "name", "") or ""),
        "price": getattr(quote, "price", None),
        "pct_change": getattr(quote, "pct_change", None),
        "amount": getattr(quote, "amount", None),
        "volume": getattr(quote, "volume", None),
        "provider_ts": _iso(provider_ts),
        "collected_at": _iso(collected_at),
        "source": str(getattr(quote, "source", "") or ""),
    }


def _semantic_payload(payload: Mapping[str, object]) -> dict:
    """Return the revision input without publication-only clock noise."""
    value = dict(payload)
    value.pop("published_at", None)
    value.pop("collected_at", None)
    rows = value.get("quotes")
    if isinstance(rows, list):
        value["quotes"] = [
            {key: item for key, item in row.items() if key != "collected_at"}
            for row in rows
            if isinstance(row, dict)
        ]
    return value


def publish(path: str | os.PathLike, *, target_codes: Iterable[str], quotes: Iterable[object],
            collected_at: datetime, published_at: datetime | None = None,
            source: str = "sina", status: str = "available", reason: str = "",
            extra: Mapping[str, object] | None = None) -> dict:
    """Publish one small target-quote snapshot with an atomic replace."""
    target = tuple(dict.fromkeys(str(code).strip() for code in target_codes if str(code).strip()))
    rows = [_quote_row(quote, collected_at) for quote in quotes]
    returned = {row["code"] for row in rows if row["code"]}
    missing = [code for code in target if code not in returned]
    now = published_at or datetime.now(timezone.utc)
    payload = {
        "schema_version": 1,
        "source": str(source or "unknown"),
        "target_count": len(target),
        "returned_count": len(returned),
        "missing_count": len(missing),
        "missing_codes": missing,
        "status": str(status or "unknown"),
        "reason": str(reason or ""),
        "provider_ts": sorted(row["provider_ts"] for row in rows if row["provider_ts"]),
        "collected_at": _iso(collected_at),
        "published_at": _iso(now),
        "quotes": rows,
    }
    if extra:
        payload.update({str(key): value for key, value in extra.items() if str(key) not in payload})
    canonical = json.dumps(_semantic_payload(payload), ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    payload["revision"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=f".{destination.name}.", suffix=".tmp", dir=str(destination.parent))
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as handle:
            json.dump(payload, handle, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        # The artifact is the deliberately public, read-only boundary for the
        # separate unprivileged Web process.  mkstemp defaults to 0600.
        os.chmod(temporary, 0o644)
        os.replace(temporary, destination)
    finally:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
    return payload


def read(path: str | os.PathLike) -> dict:
    """Read and minimally validate an artifact; never raises for bad input."""
    try:
        raw = json.loads(Path(path).read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or raw.get("schema_version") != 1:
            raise ValueError("artifact_schema_invalid")
        quotes = raw.get("quotes")
        if not isinstance(quotes, list):
            raise ValueError("artifact_quotes_invalid")
        return raw
    except FileNotFoundError:
        return {"schema_version": 1, "status": "unknown", "reason": "artifact_missing", "quotes": []}
    except (OSError, UnicodeError, ValueError, TypeError, json.JSONDecodeError):
        return {"schema_version": 1, "status": "unknown", "reason": "artifact_parse_failed", "quotes": []}
