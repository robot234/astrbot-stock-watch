"""Build and run an offline 120-session historical re-selection fixture.

The fixture joins the accepted local 64-session archive with one explicitly
identified, read-only Pi batch.  Pi is contacted only through SSH stdout; the
remote SQLite connection is read-only and no token/configuration is involved.
"""

from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import sqlite3
import subprocess
import sys
import tempfile
from typing import Any


ARCHIVE_SHA256 = "6663f860ae7af6a124315a62a90ded65e122babf40f8507d8a64e9ac65236b45"
ARCHIVE_SUMMARY_NAME = "stockwatch-tushare-aggregate-64days-20260927.json"
PINNED_BATCH_ID = "batch-b50052632b3b4000977ece1ec5070c37"
REMOTE_DB = "/AstrBot/data/plugin_data/astrbot_stock_watch/stock_watch.sqlite3"
REMOTE_CORE = "/AstrBot/data/plugins/astrbot_stock_watch/core.py"
CONTAINER = "astrbot"
EXPECTED_CORE_SHA256 = "2a3bb0ac04e3d267649b9bcce8d3e3663aff09ece52ea41ca2f859db3847259f"
SOURCE = "tushare"
BASIS = "unadjusted"
DATASET_KEY = "tushare_daily"
AS_OF = "2026-06-26"
THROUGH = "2026-09-24"
REMOTE_TIMEOUT = 300


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def stable_digest(value: Any) -> str:
    payload = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def raw_payload(row: dict) -> dict:
    return {
        "trade_date": str(row.get("trade_date") or ""),
        "code": str(row.get("code") or ""),
        "ts_code": str(row.get("ts_code") or ""),
        "name": str(row.get("name") or ""),
        "open": float(row["open"]),
        "high": float(row["high"]),
        "low": float(row["low"]),
        "close": float(row["close"]),
        "pre_close": float(row["pre_close"]),
        "pct_change": float(row["pct_change"]),
        "volume": float(row["volume"]),
        "amount": float(row["amount"]),
        "source": str(row.get("source") or ""),
        "basis": str(row.get("basis") or "").strip().lower(),
    }


def raw_partition_digest(rows: list[dict]) -> str:
    ordered = [raw_payload(row) for row in sorted(rows, key=lambda item: (str(item.get("code") or ""), str(item.get("ts_code") or "")))]
    return stable_digest(ordered)


def resolve_path(value: str | None, env_name: str, label: str) -> Path:
    resolved = str(value or os.environ.get(env_name) or "").strip()
    if not resolved:
        raise ValueError(f"{label} path is required via argument or {env_name}")
    return Path(resolved)


def validate_ssh_target(value: str | None) -> str:
    target = str(value or os.environ.get("STOCKWATCH_SSH_TARGET") or "").strip()
    if not target:
        raise ValueError("SSH target is required via --ssh-target or STOCKWATCH_SSH_TARGET")
    unsafe = set(";|&$`<>()[]{}\\\"'")
    if target.startswith("-") or any(character.isspace() or ord(character) < 32 or character in unsafe for character in target):
        raise ValueError("SSH target contains unsafe argument characters")
    if target.count("@") != 1:
        raise ValueError("SSH target must have exactly one user@host separator")
    user, host = target.split("@", 1)
    if not user or not host:
        raise ValueError("SSH target must have non-empty user and host")
    return target


def build_ssh_command(ssh_target: str, *remote_args: str) -> list[str]:
    target = validate_ssh_target(ssh_target)
    return [
        "ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=8", target,
        *remote_args,
    ]


def load_archive(archive_path: Path, summary_path: Path) -> tuple[dict[str, list[dict]], dict]:
    if not archive_path.is_file():
        raise ValueError(f"archive absent: {archive_path}")
    actual_sha256 = sha256_file(archive_path)
    if actual_sha256 != ARCHIVE_SHA256:
        raise ValueError(f"archive sha256 mismatch: {actual_sha256}")
    if not summary_path.is_file():
        raise ValueError(f"archive summary absent: {summary_path}")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    expected = {item["date"]: item for item in summary.get("days", [])}
    if len(expected) != 64:
        raise ValueError(f"archive summary has {len(expected)} days, expected 64")
    rows: dict[str, list[dict]] = {day: [] for day in expected}
    with gzip.open(archive_path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if not line.strip():
                continue
            row = json.loads(line)
            day = str(row.get("trade_date") or "")
            if day not in rows:
                raise ValueError(f"archive has unexpected date: {day}")
            if row.get("source") != SOURCE or row.get("basis") != BASIS:
                raise ValueError(f"archive provenance mismatch: {day}")
            rows[day].append(row)
    for day, values in rows.items():
        expected_day = expected[day]
        if len(values) != expected_day["row_count"]:
            raise ValueError(f"archive row count mismatch: {day}")
        if stable_digest(values) != expected_day["row_digest"]:
            raise ValueError(f"archive row digest mismatch: {day}")
        if len({row["code"] for row in values}) != len(values):
            raise ValueError(f"archive duplicate code: {day}")
    return rows, {
        "path": str(archive_path),
        "sha256": actual_sha256,
        "bytes": archive_path.stat().st_size,
        "day_count": len(rows),
        "row_count": sum(len(values) for values in rows.values()),
        "first_day": min(rows),
        "last_day": max(rows),
    }


REMOTE_EXPORT = r'''import json, sqlite3, sys

DB = __DB__
BATCH_ID = __BATCH_ID__
SOURCE = "tushare"
BASIS = "unadjusted"
uri = "file:" + DB + "?mode=ro"

try:
    with sqlite3.connect(uri, uri=True) as db:
        db.row_factory = sqlite3.Row
        batch = db.execute(
            "SELECT b.batch_id,b.status,b.quality,b.source,b.basis,b.published_at,"
            "b.expected_days,b.row_count,b.start_date,b.end_date,b.actual_trade_date,"
            "b.requested_date,b.page_size,b.manifest_hash,d.dataset_key "
            "FROM batches b JOIN datasets d ON d.dataset_id=b.dataset_id WHERE b.batch_id=?",
            (BATCH_ID,),
        ).fetchone()
        if not batch:
            raise RuntimeError("batch_missing")
        days = db.execute(
            "SELECT bd.trade_date,bd.partition_id,bd.row_count,dp.row_count AS actual_count,"
            "dp.validation_status,dp.source,dp.basis,dp.content_hash,dp.market_counts_json "
            "FROM batch_days bd JOIN day_partitions dp ON dp.partition_id=bd.partition_id "
            "WHERE bd.batch_id=? ORDER BY bd.trade_date",
            (BATCH_ID,),
        ).fetchall()
        meta = {
            "batch": dict(batch),
            "days": [dict(day) for day in days],
            "read_only": True,
            "source": SOURCE,
            "basis": BASIS,
        }
        print("META " + json.dumps(meta, ensure_ascii=False, sort_keys=True, separators=(",", ":")))
        rows = db.execute(
            "SELECT pb.trade_date,pb.code,pb.ts_code,pb.name,pb.open,pb.high,pb.low,pb.close,"
            "pb.pre_close,pb.pct_change,pb.volume,pb.amount,pb.source,pb.basis "
            "FROM batch_days bd JOIN partition_bars pb ON pb.partition_id=bd.partition_id "
            "WHERE bd.batch_id=? ORDER BY pb.trade_date,pb.code,pb.ts_code",
            (BATCH_ID,),
        )
        count = 0
        for row in rows:
            item = dict(row)
            print("ROW " + json.dumps(item, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False))
            count += 1
        print("END " + json.dumps({"row_count": count, "day_count": len(days)}, separators=(",", ":")))
except Exception as exc:
    sys.stderr.write("remote_export_failed:" + str(exc).replace("\\n", " ")[:180] + "\\n")
    raise SystemExit(2)
'''


def run_ssh_python(source: str, timeout: int, ssh_target: str) -> tuple[bytes, bytes]:
    command = build_ssh_command(ssh_target, "docker", "exec", "-i", CONTAINER, "python3", "-")
    process = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        stdout, stderr = process.communicate(source.encode("utf-8"), timeout=timeout)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        raise RuntimeError("remote export timeout")
    if process.returncode != 0:
        detail = stderr.decode("utf-8", errors="replace").splitlines()
        raise RuntimeError(detail[0][:180] if detail else f"remote export returncode {process.returncode}")
    return stdout, stderr


def fetch_remote_batch(output_path: Path, ssh_target: str) -> tuple[dict[str, list[dict]], dict]:
    source = REMOTE_EXPORT.replace("__DB__", repr(REMOTE_DB)).replace("__BATCH_ID__", repr(PINNED_BATCH_ID))
    stdout, _stderr = run_ssh_python(source, REMOTE_TIMEOUT, ssh_target)
    meta = None
    end = None
    rows: dict[str, list[dict]] = {}
    with output_path.open("wb") as raw_file:
        for line in stdout.splitlines():
            if line.startswith(b"META "):
                meta = json.loads(line[5:])
            elif line.startswith(b"ROW "):
                row = json.loads(line[4:])
                day = str(row.get("trade_date") or "")
                if row.get("source") != SOURCE or row.get("basis") != BASIS:
                    raise ValueError(f"pinned provenance mismatch: {day}")
                rows.setdefault(day, []).append(row)
                raw_file.write((json.dumps(row, ensure_ascii=False, sort_keys=True, separators=(",", ":")) + "\n").encode("utf-8"))
            elif line.startswith(b"END "):
                end = json.loads(line[4:])
    if not meta or not end:
        raise ValueError("remote export markers missing")
    batch = meta["batch"]
    days_meta = meta["days"]
    expected_days = {item["trade_date"]: item for item in days_meta}
    required = {
        "batch_id": PINNED_BATCH_ID, "status": "published", "quality": "good",
        "source": SOURCE, "basis": BASIS, "dataset_key": DATASET_KEY,
        "start_date": "2026-04-03", "end_date": "2026-09-24", "expected_days": 120,
    }
    for key, value in required.items():
        if batch.get(key) != value:
            raise ValueError(f"pinned batch metadata mismatch: {key}")
    if len(expected_days) != 120 or end.get("day_count") != 120:
        raise ValueError("pinned batch day count mismatch")
    for day, values in rows.items():
        if day not in expected_days:
            raise ValueError(f"pinned unexpected date: {day}")
        partition = expected_days[day]
        if len(values) != partition["row_count"] or partition["row_count"] != partition["actual_count"]:
            raise ValueError(f"pinned row count mismatch: {day}")
        if partition["validation_status"] != "validated" or partition["source"] != SOURCE or partition["basis"] != BASIS:
            raise ValueError(f"pinned partition provenance mismatch: {day}")
        if len({row["code"] for row in values}) != len(values):
            raise ValueError(f"pinned duplicate code: {day}")
        if raw_partition_digest(values) != partition["content_hash"]:
            raise ValueError(f"pinned content hash mismatch: {day}")
    if set(rows) != set(expected_days) or sum(map(len, rows.values())) != batch["row_count"]:
        raise ValueError("pinned exported rows incomplete")
    return rows, {
        "batch_id": PINNED_BATCH_ID,
        "status": batch["status"],
        "quality": batch["quality"],
        "source": batch["source"],
        "basis": batch["basis"],
        "dataset_key": batch["dataset_key"],
        "published_at": batch["published_at"],
        "manifest_hash": batch["manifest_hash"],
        "day_count": len(rows),
        "row_count": sum(len(values) for values in rows.values()),
        "first_day": min(rows),
        "last_day": max(rows),
        "day_counts": {day: len(values) for day, values in sorted(rows.items())},
    }


def fetch_remote_core(output_path: Path, ssh_target: str) -> dict:
    command = build_ssh_command(ssh_target, "docker", "exec", CONTAINER, "cat", REMOTE_CORE)
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        stdout, stderr = process.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        process.kill()
        process.communicate()
        raise RuntimeError("remote core fetch timeout")
    if process.returncode != 0:
        detail = stderr.decode("utf-8", errors="replace").splitlines()
        raise RuntimeError(detail[0][:180] if detail else "remote core fetch failed")
    output_path.write_bytes(stdout)
    actual = sha256_file(output_path)
    if actual != EXPECTED_CORE_SHA256:
        raise ValueError(f"installed core sha256 mismatch: {actual}")
    return {"path": str(output_path), "bytes": len(stdout), "sha256": actual, "remote_path": REMOTE_CORE}


def create_synthetic_db(path: Path, archive_rows: dict[str, list[dict]], pinned_rows: dict[str, list[dict]], pinned_meta: dict) -> dict:
    all_rows = dict(archive_rows)
    overlap = set(all_rows) & set(pinned_rows)
    if overlap:
        raise ValueError(f"archive/pinned date overlap: {sorted(overlap)[:3]}")
    all_rows.update(pinned_rows)
    dates = sorted(all_rows)
    if dates[0] != "2025-12-24" or dates[-1] != "2026-09-24" or len(dates) != 184:
        raise ValueError("combined date window is not the expected 184 sessions")
    prior = sum(day <= AS_OF for day in dates)
    if prior != 120:
        raise ValueError(f"combined history before {AS_OF} has {prior} sessions")
    published_at = pinned_meta["published_at"]
    connection = sqlite3.connect(path)
    try:
        connection.executescript("""
            CREATE TABLE datasets(dataset_id TEXT PRIMARY KEY,dataset_key TEXT);
            CREATE TABLE batches(batch_id TEXT PRIMARY KEY,dataset_id TEXT,status TEXT,quality TEXT,source TEXT,basis TEXT,published_at TEXT);
            CREATE TABLE day_partitions(partition_id TEXT PRIMARY KEY,row_count INTEGER,validation_status TEXT,source TEXT,basis TEXT);
            CREATE TABLE batch_days(batch_id TEXT,trade_date TEXT,partition_id TEXT,row_count INTEGER);
            CREATE TABLE partition_bars(partition_id TEXT,trade_date TEXT,code TEXT,name TEXT,open REAL,high REAL,low REAL,close REAL,pre_close REAL,pct_change REAL,volume REAL,amount REAL,source TEXT,basis TEXT);
        """)
        connection.execute("INSERT INTO datasets VALUES(?,?)", ("synthetic-dataset-20260927", DATASET_KEY))
        connection.execute("INSERT INTO batches VALUES(?,?,?,?,?,?,?)", ("synthetic-archive-plus-pinned-20260927", "synthetic-dataset-20260927", "published", "good", SOURCE, BASIS, published_at))
        for index, day in enumerate(dates):
            values = all_rows[day]
            partition = f"synthetic-p{index:03d}"
            connection.execute("INSERT INTO day_partitions VALUES(?,?,?,?,?)", (partition, len(values), "validated", SOURCE, BASIS))
            connection.execute("INSERT INTO batch_days VALUES(?,?,?,?)", ("synthetic-archive-plus-pinned-20260927", day, partition, len(values)))
            connection.executemany(
                "INSERT INTO partition_bars VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                [(partition, row["trade_date"], row["code"], row.get("name", ""), row["open"], row["high"], row["low"], row["close"], row["pre_close"], row["pct_change"], row["volume"], row["amount"], SOURCE, BASIS) for row in values],
            )
        connection.commit()
    finally:
        connection.close()
    return {"path": str(path), "bytes": path.stat().st_size, "sha256": sha256_file(path), "date_count": len(dates), "row_count": sum(len(values) for values in all_rows.values()), "history_dates_at_start": prior}


def load_core(path: Path):
    spec = importlib.util.spec_from_file_location("stock_watch_installed_core_20260927", path)
    if not spec or not spec.loader:
        raise ValueError("installed core.py could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def load_harness(path: Path):
    spec = importlib.util.spec_from_file_location("historical_reselection_harness_20260927", path)
    if not spec or not spec.loader:
        raise ValueError("historical_reselection.py could not be loaded")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def execute(args) -> dict:
    ssh_target = validate_ssh_target(args.ssh_target)
    archive_path = resolve_path(args.archive, "STOCKWATCH_ARCHIVE_PATH", "--archive")
    summary_value = args.archive_summary or os.environ.get("STOCKWATCH_ARCHIVE_SUMMARY")
    summary_path = Path(summary_value) if summary_value else archive_path.parent / ARCHIVE_SUMMARY_NAME
    archive_rows, archive_meta = load_archive(archive_path, summary_path)
    run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    temp_dir = Path(tempfile.mkdtemp(prefix=f"stockwatch-historical-reselection-{run_id}-"))
    pinned_path = temp_dir / "pinned-batch-bars.ndjson"
    core_path = temp_dir / "core.py"
    synthetic_db = temp_dir / "synthetic-batch.sqlite3"
    report_path = temp_dir / "historical-reselection.json"
    pinned_rows, pinned_meta = fetch_remote_batch(pinned_path, ssh_target)
    core_meta = fetch_remote_core(core_path, ssh_target)
    db_meta = create_synthetic_db(synthetic_db, archive_rows, pinned_rows, pinned_meta)
    harness = load_harness(args.harness)
    core = load_core(core_path)
    with harness.connect_readonly(synthetic_db) as db:
        result = harness.run(db, core, batch_id="synthetic-archive-plus-pinned-20260927", as_of=AS_OF, through=THROUGH, horizon=args.horizon, limit=args.limit, deep_limit=args.deep_limit, min_market_rows=args.min_market_rows)
    result["core_sha256"] = core_meta["sha256"]
    result["archive"] = archive_meta
    result["pinned_batch"] = pinned_meta
    result["pinned_export"] = {"path": str(pinned_path), "bytes": pinned_path.stat().st_size, "sha256": sha256_file(pinned_path)}
    result["synthetic_db"] = db_meta
    result["provenance"] = {
        "mode": "offline_combined_archive_and_readonly_pinned_batch",
        "pi_mutated": False,
        "production_db_mutated": False,
        "production_config_mutated": False,
        "token_transferred": False,
        "selection_is_point_in_time_proven": False,
        "historical_universe_complete": False,
        "ssh_target_configured": True,
    }
    result["artifacts"] = {"directory": str(temp_dir), "report": str(report_path), "synthetic_db": str(synthetic_db), "pinned_export": str(pinned_path), "installed_core": str(core_path)}
    report_path.write_text(json.dumps(result, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n", encoding="utf-8")
    return result


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ssh-target", default=None, help="SSH user@host; or STOCKWATCH_SSH_TARGET")
    parser.add_argument("--archive", default=None, help="accepted gzip archive; or STOCKWATCH_ARCHIVE_PATH")
    parser.add_argument("--archive-summary", default=None, help="64-day summary JSON; defaults beside --archive")
    parser.add_argument("--harness", type=Path, default=Path(__file__).with_name("historical_reselection.py"))
    parser.add_argument("--horizon", type=int, default=5)
    parser.add_argument("--limit", type=int, default=10)
    parser.add_argument("--deep-limit", type=int, default=300)
    parser.add_argument("--min-market-rows", type=int, default=4000)
    args = parser.parse_args(argv)
    try:
        result = execute(args)
        print(json.dumps({
            "status": "complete_verified",
            "summary": result["summary"],
            "history_dates_at_start": result["history_dates_at_start"],
            "archive": result["archive"],
            "pinned_batch": {key: value for key, value in result["pinned_batch"].items() if key != "day_counts"},
            "core": result["core_sha256"],
            "artifacts": result["artifacts"],
            "synthetic_db": result["synthetic_db"],
        }, ensure_ascii=False, sort_keys=True))
        return 0
    except (OSError, ValueError, sqlite3.Error, subprocess.SubprocessError) as exc:
        print(f"historical archive re-selection not executed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
