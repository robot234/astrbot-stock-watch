"""Bounded, isolated BaoStock research capture, never a formal activation."""
from __future__ import annotations

import argparse
import contextlib
from datetime import datetime, timezone
import hashlib
import io
import importlib.util
import json
from pathlib import Path
import socket
import subprocess
import sys


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT.parent))
from astrbot_stock_watch.baostock_daily_risk import BaoStockRiskCollector


def _load_guard(guard_dir: Path):
    spec = importlib.util.spec_from_file_location("capture_baostock_guard", guard_dir / "baostock_guard.py")
    guard_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(guard_module)
    return guard_module


def capture(sessions: dict, *, max_symbols: int, request_budget: int, max_seconds: float,
            guard_dir: Path | None = None) -> dict:
    denied = {"licensed": False, "requests": 0, "tushare_requests": 0,
              "credential_files_read": False, "formal_calls": 0, "production_mutation": False}
    if guard_dir is None:
        return {**denied, "status": "shared_budget_guard_required"}
    if (ROOT / ".local_records/baostock_budget_owner.json").exists():
        return {**denied, "status": "external_budget_owner_requires_owner_host_execution"}
    guard_dir = guard_dir.resolve()
    guard_module = _load_guard(guard_dir)
    import baostock as bs

    started = datetime.now(timezone.utc).isoformat()
    previous_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(10)
    try:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            with guard_module.BaoStockGuard(purpose="stock-watch-baostock-first-v2",
                    max_calls=max(1, min(request_budget, 12001)) + 2,
                    rules_path=guard_dir / "baostock_access_rules.json",
                    ledger_path=guard_dir / "baostock_request_ledger.json",
                    lock_path=guard_dir / "baostock_guard.lock") as guard:
                if guard.remaining() < max(1, min(request_budget, 12001)) + 2:
                    return {**denied, "status": "shared_daily_budget_insufficient"}
                ip = guard_module.detect_ipv4()
                if not guard_module.release_status([ip])["ok"]:
                    return {**denied, "status": "official_blacklist_check_unverified"}
                login = guard.login(bs)
                if login is None or login.error_code != "0":
                    return {**denied, "status": "login_failed", "guard": guard.snapshot()}
                try:
                    result = BaoStockRiskCollector(bs, max_symbols=max_symbols,
                        request_budget=request_budget, max_seconds=max_seconds).collect(sessions)
                    result.update(started_at=started, status="captured_unlicensed", production_mutation=False)
                finally:
                    guard.logout(bs)
                result["guard"] = guard.snapshot()
                return result
    finally:
        socket.setdefaulttimeout(previous_timeout)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--max-symbols", type=int, default=3)
    parser.add_argument("--request-budget", type=int, default=10)
    parser.add_argument("--max-seconds", type=float, default=90)
    parser.add_argument("--codes", default="")
    parser.add_argument("--guard-dir", type=Path)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    protocol_raw = args.protocol.read_bytes()
    protocol = json.loads(protocol_raw)
    if protocol.get("source") != "derived:baostock-first" or protocol.get("scenario_version") != "2026-10-v2":
        parser.error("BaoStock v2 preregistered protocol required")
    input_raw = args.input.read_bytes()
    sessions = json.loads(input_raw)["sessions"]
    if sorted(sessions) != sorted(protocol["dates"]):
        parser.error("sessions must match frozen dates")
    if args.codes:
        codes = args.codes.split(",")
        available = {row["code"] for session in sessions.values() for row in session["rows"]}
        if len(set(codes)) != len(codes) or any(code not in available for code in codes):
            parser.error("requested symbols must exist in frozen universe")
        sessions = {day: {**session, "rows": [row for row in session["rows"] if row["code"] in codes]}
                    for day, session in sessions.items()}
    if args.worker:
        try:
            result = capture(sessions, max_symbols=args.max_symbols, request_budget=args.request_budget,
                             max_seconds=max(1, min(args.max_seconds, 1800)), guard_dir=args.guard_dir)
        except Exception as exc:
            result = {"status": "capture_failed", "error_type": type(exc).__name__, "licensed": False}
        print(json.dumps(result, ensure_ascii=False, allow_nan=False))
        return 0
    if args.output is None or args.output.exists():
        parser.error("new exclusive output path required")
    command = [sys.executable, "-B", "-X", "utf8", str(Path(__file__).resolve()), "--worker",
               "--input", str(args.input), "--protocol", str(args.protocol),
               "--max-symbols", str(args.max_symbols), "--request-budget", str(args.request_budget),
               "--max-seconds", str(args.max_seconds)]
    if args.codes:
        command.extend(["--codes", args.codes])
    if args.guard_dir:
        command.extend(["--guard-dir", str(args.guard_dir)])
    try:
        completed = subprocess.run(command, capture_output=True, text=True, encoding="utf-8",
                                   timeout=max(1, min(args.max_seconds, 1800)) + 45)
        if completed.returncode:
            result = {"status": "worker_failed", "exit_code": completed.returncode, "licensed": False}
        else:
            result = json.loads(completed.stdout)
    except subprocess.TimeoutExpired:
        result = {"status": "worker_timeout", "licensed": False, "local_worker_stopped": True}
    result["hashes"] = {"input": hashlib.sha256(input_raw).hexdigest(),
                        "protocol": hashlib.sha256(protocol_raw).hexdigest(),
                        "collector": hashlib.sha256((ROOT / "baostock_daily_risk.py").read_bytes()).hexdigest(),
                        "runner": hashlib.sha256(Path(__file__).read_bytes()).hexdigest()}
    with args.output.open("x", encoding="utf-8", newline="\n") as output:
        json.dump(result, output, ensure_ascii=False, indent=2)
        output.write("\n")
    print(json.dumps({"status": result["status"], "requests": result.get("requests"),
                      "tushare_requests": result.get("tushare_requests", 0),
                      "licensed": False, "errors": len(result.get("errors", []))}))
    return 5


if __name__ == "__main__":
    raise SystemExit(main())
