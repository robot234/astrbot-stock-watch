"""Bounded read-only companion evidence for immutable research pools."""

from __future__ import annotations

import json
import math
from datetime import datetime, timezone
from pathlib import Path


def load_evidence(path: Path, pools: dict) -> dict:
    """Reject mismatched or malformed sidecars; never change frozen picks."""
    try:
        if path.stat().st_size > 65536:
            return {}
        bundle = json.loads(path.read_text(encoding="utf-8"))
        if (not isinstance(bundle, dict) or bundle.get("version") != 1
                or bundle.get("trade_date") != pools.get("trade_date")
                or bundle.get("batch_id") != pools.get("batch_id")
                or bundle.get("sources") != {"trading": "baostock:daily:unadjusted", "limit": "akshare:dated-pools"}):
            return {}
        captured = datetime.fromisoformat(bundle["captured_at"])
        if captured.tzinfo is None or captured > datetime.now(timezone.utc):
            return {}
        rows = bundle["rows"]
        if not isinstance(rows, dict) or len(rows) > 100:
            return {}
    except (OSError, ValueError, TypeError, KeyError):
        return {}
    frozen = {item["code"]: item for pool in ("primary", "radar")
              for item in pools.get("picks", {}).get(pool, [])}
    result = {}
    for code, row in rows.items():
        if code not in frozen or not isinstance(row, dict):
            continue
        try:
            close = float(row["close"])
            if not math.isfinite(close) or abs(close - float(frozen[code]["close"])) > 0.005:
                continue
        except (ValueError, TypeError, KeyError, OverflowError):
            continue
        fields = ("suspended", "st", "limit_up", "limit_down")
        if any(not any(row.get(field) is value for value in (True, False, None)) for field in fields):
            continue
        result[code] = {field: row.get(field) for field in fields}
    return {"captured_at": captured, "rows": result}


def evidence_label(evidence: dict, code: str, frozen_at: str) -> str:
    row = evidence.get("rows", {}).get(code)
    if not row:
        return ""
    captured = evidence["captured_at"]
    try:
        frozen = datetime.fromisoformat(frozen_at)
        # Legacy freeze timestamps are naive UTC.
        frozen = frozen.replace(tzinfo=timezone.utc) if frozen.tzinfo is None else frozen
        timing = "事后核验" if captured > frozen else "冻结前采集"
    except (ValueError, TypeError):
        timing = "采集时点待核"
    def flag(value, positive, negative):
        return positive if value is True else negative if value is False else "待核"
    return (f"[{timing} {captured.isoformat()} BaoStock/AKShare] "
            f"停牌{flag(row['suspended'], '是', '否')} "
            f"ST{flag(row['st'], '是', '否')} "
            f"涨停{flag(row['limit_up'], '命中', '待核')} "
            f"跌停{flag(row['limit_down'], '命中', '待核')}")
