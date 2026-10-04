"""Offline research capture using process environment or an existing gateway."""
from __future__ import annotations

import argparse
import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import sys
from urllib.parse import urlsplit


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent))
from astrbot_stock_watch.daily_risk_sources import DailyRiskSourceCollector, collect_companion_snapshots
from astrbot_stock_watch.providers import HttpRuntime, TushareRequestGateway


async def capture(sessions: dict, *, request_budget: int = 150,
                  companion_symbols: int = 0, max_seconds: float = 1800) -> dict:
    """No dotenv, plugin config, credential-file access, storage or publication."""
    instant = datetime.now(timezone.utc).isoformat()
    credential = os.environ.get("TUSHARE_TOKEN", "")
    url = os.environ.get("TUSHARE_URL", "") or "https://api.tushare.pro"
    output = {"started_at": instant, "observed_at": instant, "licensed": False, "credential_files_read": False,
              "destination_host": urlsplit(url).hostname,
              "tushare": {"status": "not_executed", "requests": 0,
                          "reason": "TUSHARE_TOKEN_missing_from_process_environment"},
              "companion": {"status": "not_requested"},
              "production_mutation": False, "formal_calls": 0}
    http = HttpRuntime(timeout=10, max_concurrency=1)
    try:
        if credential:
            gateway = TushareRequestGateway(
                credential, url, http_runtime=http, storage=None,
                retry_attempts=1, enforce_rate_limits=True)
            output["tushare"] = await DailyRiskSourceCollector(
                gateway, request_budget=request_budget, max_seconds=max_seconds).collect(sessions)
            output["tushare"]["status"] = (
                "captured_unlicensed" if all(table["complete"] for table in output["tushare"]["tables"].values())
                else "incomplete")
        if companion_symbols:
            day = max(sessions)
            session = sessions[day]
            output["companion"] = await collect_companion_snapshots(
                http, session["rows"], trade_date=day, batch_id=session["batch_id"],
                max_symbols=companion_symbols, max_seconds=max_seconds)
            output["companion"]["status"] = "captured_unlicensed"
    finally:
        await http.close()
    output["observed_at"] = datetime.now(timezone.utc).isoformat()
    return output


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--trade-date")
    parser.add_argument("--request-budget", type=int, default=150)
    parser.add_argument("--companion-symbols", type=int, default=0)
    parser.add_argument("--max-seconds", type=float, default=1800)
    args = parser.parse_args()
    if args.output.exists():
        parser.error("output already exists; first capture must not be overwritten")
    raw = args.input.read_bytes()
    data = json.loads(raw)
    sessions = data.get("sessions")
    if not isinstance(sessions, dict) or not sessions:
        parser.error("input must contain dated raw sessions")
    if args.trade_date:
        if args.trade_date not in sessions:
            parser.error("trade date absent from raw sessions")
        sessions = {args.trade_date: sessions[args.trade_date]}
    result = asyncio.run(capture(
        sessions, request_budget=args.request_budget,
        companion_symbols=args.companion_symbols, max_seconds=args.max_seconds))
    result["input_hash"] = hashlib.sha256(raw).hexdigest()
    result["collector_hash"] = hashlib.sha256((ROOT / "daily_risk_sources.py").read_bytes()).hexdigest()
    result["runner_hash"] = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    with args.output.open("x", encoding="utf-8", newline="\n") as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
        output.write("\n")
    print(json.dumps({"tushare_status": result["tushare"]["status"],
                      "companion_status": result["companion"]["status"], "licensed": False}))
    return 4 if result["tushare"]["status"] == "not_executed" else 5


if __name__ == "__main__":
    raise SystemExit(main())
