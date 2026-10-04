"""Capture bounded retrospective BaoStock/AKShare evidence for a frozen pool.

Run in the Pi's isolated data-probe venv. The plugin reads the resulting
sidecar; this script never updates SQLite or the frozen recommendation.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import math
import os
import re
import sqlite3
import socket
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo


def _guard_session(max_calls: int, target_date: str = "", on_first_send=None):
    local = Path(__file__).resolve().parents[1] / "shared_baostock.py"
    helper = local if local.exists() else Path("/home/pi/apps/stock-fund-fetch-20260929/shared_baostock.py")
    spec = importlib.util.spec_from_file_location("legacy_shared_baostock", helper)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.owner_session(purpose="stock-watch-research-risk", max_calls=max_calls,
                                target_date=target_date or None, work_seconds=600,
                                on_first_send=on_first_send)


def _read_pool(db_path: Path, container: str):
    if container:
        query = (
            "import json,sqlite3,sys;"
            "db=sqlite3.connect('file:'+sys.argv[1]+'?mode=ro',uri=True);"
            "db.execute('PRAGMA query_only=ON');"
            "r=db.execute('SELECT run_id,trade_date,batch_id FROM research_pool_runs ORDER BY trade_date DESC,frozen_at DESC LIMIT 1').fetchone();"
            "p=db.execute('SELECT code,close FROM research_pool_picks WHERE run_id=? ORDER BY pool,rank',(r[0],)).fetchall() if r else [];"
            "print(json.dumps({'run':r,'picks':p}))"
        )
        output = subprocess.run(["docker", "exec", container, "python", "-c", query, str(db_path)],
                                capture_output=True, text=True, timeout=30, check=True)
        data = json.loads(output.stdout)
        return tuple(data["run"]) if data["run"] else (), [tuple(row) for row in data["picks"]]
    with sqlite3.connect(f"file:{db_path}?mode=ro", uri=True) as db:
        db.execute("PRAGMA query_only=ON")
        run = db.execute("SELECT run_id,trade_date,batch_id FROM research_pool_runs "
                         "ORDER BY trade_date DESC,frozen_at DESC LIMIT 1").fetchone()
        picks = db.execute("SELECT code,close FROM research_pool_picks WHERE run_id=? ORDER BY pool,rank", (run[0],)).fetchall() if run else []
        return tuple(run) if run else (), picks


def capture(db_path: Path, out_dir: Path, *, container: str = "", expected_date: str = "",
            on_first_send=None, before_request=None) -> dict:
    import akshare as ak
    import baostock as bs

    run, picks = _read_pool(db_path, container)
    if not run:
        raise RuntimeError("no frozen research pool")
    run_id, trade_date, batch_id = run
    if expected_date and trade_date != expected_date:
        return {"status": "waiting_for_today_freeze", "latest_trade_date": trade_date}
    if not 1 <= len(picks) <= 100 or len({code for code, _ in picks}) != len(picks):
        raise RuntimeError("invalid frozen pool size")
    if any(not re.fullmatch(r"[0-9]{6}", code) for code, _ in picks):
        raise RuntimeError("invalid frozen stock code")
    destination = out_dir / f"{trade_date}.json"
    if destination.exists():
        raise RuntimeError("evidence already captured; do not overwrite first observation")

    rows = {}
    failures = []
    source_captured_at = {}
    previous_timeout = socket.getdefaulttimeout()
    socket.setdefaulttimeout(15)
    try:
        with _guard_session(2 * len(picks) + 2, expected_date, on_first_send) as guard:
            if before_request is not None:
                before_request()
            login = guard.login(bs)
            if login is None or login.error_code != "0":
                raise RuntimeError("BaoStock login failed")
            try:
                for code, frozen_close in picks:
                    symbol = ("sh." if code.startswith("6") else "sz.") + code
                    try:
                        result = bs.query_history_k_data_plus(
                            symbol, "date,code,close,tradestatus,isST",
                            start_date=trade_date, end_date=trade_date, frequency="d", adjustflag="3")
                        found = []
                        if result.error_code == "0":
                            while result.next():
                                found.append(dict(zip(result.fields, result.get_row_data())))
                        if (result.error_code != "0" or len(found) != 1 or found[0]["date"] != trade_date
                                or found[0]["code"] != symbol
                                or abs(float(found[0]["close"]) - float(frozen_close)) > 0.005):
                            failures.append(code)
                            break
                        row = found[0]
                        if row["tradestatus"] not in ("0", "1") or row["isST"] not in ("0", "1"):
                            failures.append(code)
                            break
                        rows[code] = {
                            "close": float(frozen_close),
                            "suspended": {"0": True, "1": False}.get(row["tradestatus"]),
                            "st": {"0": False, "1": True}.get(row["isST"]),
                            "limit_up": None, "limit_down": None,
                        }
                    except (KeyError, ValueError, TypeError, OverflowError):
                        failures.append(code)
                        break
            finally:
                guard.logout(bs)
    finally:
        socket.setdefaulttimeout(previous_timeout)
    if failures:
        raise RuntimeError(f"BaoStock missing or inconsistent rows: {len(failures)}")
    source_captured_at["trading"] = datetime.now(timezone.utc).isoformat()
    pool_digests = {}
    for field, fetch in (("limit_up", ak.stock_zt_pool_em),
                         ("limit_down", ak.stock_zt_pool_dtgc_em)):
        try:
            frame = fetch(date=trade_date.replace("-", ""))
            values = [str(value) for value in frame["代码"].tolist()]
            if any(not re.fullmatch(r"[0-9]{6}", value) for value in values) or len(set(values)) != len(values):
                raise ValueError("invalid or duplicate limit pool code")
            codes = set(values)
            prices = frame["最新价"].tolist()
            if len(prices) != len(values):
                raise ValueError("limit pool price count mismatch")
            for code, price in zip(values, prices):
                if code in rows:
                    number = float(price)
                    if not math.isfinite(number) or abs(number - rows[code]["close"]) > 0.005:
                        raise ValueError("limit pool price conflicts with frozen close")
            pool_digests[field] = hashlib.sha256(
                frame.to_json(orient="records", force_ascii=False).encode("utf-8")
            ).hexdigest()
            for code, row in rows.items():
                if code in codes:
                    row[field] = True
            source_captured_at[field] = datetime.now(timezone.utc).isoformat()
        except Exception as exc:
            raise RuntimeError(f"AKShare {field} pool unavailable") from exc

    if any(row["suspended"] and (row["limit_up"] or row["limit_down"])
           or row["limit_up"] and row["limit_down"] for row in rows.values()):
        raise RuntimeError("provider conflict: suspended/limit status")

    bundle = {"version": 1, "trade_date": trade_date, "batch_id": batch_id,
              "captured_at": datetime.now(timezone.utc).isoformat(),
              "sources": {"trading": "baostock:daily:unadjusted", "limit": "akshare:dated-pools"},
              "source_captured_at": source_captured_at,
              "pool_digests": pool_digests, "failed_codes": failures, "rows": rows}
    out_dir.mkdir(parents=True, exist_ok=True)
    data = json.dumps(bundle, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")
    fd, name = tempfile.mkstemp(prefix=".research-risk-", dir=out_dir)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(name, 0o644)
        if destination.exists():
            raise RuntimeError("concurrent evidence capture")
        os.replace(name, destination)
    finally:
        if os.path.exists(name):
            os.unlink(name)
    return {"trade_date": trade_date, "batch_id": batch_id, "captured_at": bundle["captured_at"],
            "matched": len(rows), "failed": len(failures), "up_members": sum(r["limit_up"] is True for r in rows.values()),
            "down_members": sum(r["limit_down"] is True for r in rows.values())}


def _container_file_exists(container: str, path: str) -> bool:
    check = subprocess.run(
        ["docker", "exec", container, "python", "-c",
         "import pathlib,sys; print(int(pathlib.Path(sys.argv[1]).exists()))", path],
        capture_output=True, text=True, check=True, timeout=30)
    return check.stdout.strip() == "1"


def _published_batch(container: str, path: str) -> str | None:
    check = subprocess.run(
        ["docker", "exec", container, "python", "-c",
         "import json,pathlib,sys; p=pathlib.Path(sys.argv[1]);"
         "print(json.loads(p.read_text())['batch_id'] if p.exists() else '')", path],
        capture_output=True, text=True, check=True, timeout=30)
    return check.stdout.strip() or None


def scheduled_capture(db_path: Path, out_dir: Path, container: str, publish_dir: str) -> dict:
    today = datetime.now(ZoneInfo("Asia/Shanghai")).date().isoformat()
    run, _ = _read_pool(db_path, container)
    if not run or run[1] != today:
        return {"status": "waiting_for_today_freeze", "latest_trade_date": run[1] if run else None}
    target = publish_dir.rstrip("/") + "/" + today + ".json"
    existing_batch = _published_batch(container, target)
    if existing_batch:
        if existing_batch != run[2]:
            raise RuntimeError("published evidence belongs to a different batch")
        return {"status": "already_published", "trade_date": today}
    staged = out_dir / (today + ".json")
    if not staged.exists():
        attempt = out_dir / (".attempt-" + today + "-" + hashlib.sha256(str(run[2]).encode()).hexdigest()[:16] + ".json")
        if attempt.exists():
            return {"status": "partial_no_automatic_retry", "trade_date": today, "batch_id": run[2]}
        out_dir.mkdir(parents=True, exist_ok=True)
        def check_attempt():
            if attempt.exists():
                raise RuntimeError("batch already sent BaoStock messages")

        def record_first_send():
            with attempt.open("x", encoding="utf-8") as stream:
                json.dump({"status": "first_message_sent", "trade_date": today, "batch_id": run[2],
                           "observed_at": datetime.now(timezone.utc).isoformat()}, stream)
                stream.flush()
                os.fsync(stream.fileno())

        capture(db_path, out_dir, container=container, expected_date=today,
                on_first_send=record_first_send, before_request=check_attempt)
    bundle = json.loads(staged.read_text(encoding="utf-8"))
    if (bundle.get("trade_date") != today or bundle.get("batch_id") != run[2]
            or bundle.get("failed_codes") or not bundle.get("source_captured_at")
            or len(bundle.get("rows", {})) == 0):
        raise RuntimeError("staged evidence does not match current freeze")
    current, _ = _read_pool(db_path, container)
    if current != run:
        raise RuntimeError("frozen pool changed during capture")
    next_path = target + ".next"
    if _container_file_exists(container, next_path):
        raise RuntimeError("stale publication staging path")
    subprocess.run(["docker", "cp", str(staged), container + ":" + next_path],
                   check=True, timeout=30)
    publish = (
        "import json,os,pathlib,sys;"
        "src,dst=map(pathlib.Path,sys.argv[1:3]);"
        "j=json.loads(src.read_text());"
        "assert j['trade_date']==sys.argv[3] and j['batch_id']==sys.argv[4];"
        "assert not dst.exists();"
        "os.link(src,dst);src.unlink();print('published')"
    )
    subprocess.run(["docker", "exec", container, "python", "-c", publish,
                    next_path, target, today, run[2]], check=True, timeout=30)
    return {"status": "published", "trade_date": today, "matched": len(bundle["rows"])}


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--container", default="")
    parser.add_argument("--publish-dir", default="")
    args = parser.parse_args()
    if args.publish_dir:
        if not args.container:
            parser.error("--publish-dir requires --container")
        result = scheduled_capture(args.db, args.out_dir, args.container, args.publish_dir)
    else:
        result = capture(args.db, args.out_dir, container=args.container)
    print(json.dumps(result, ensure_ascii=False))
