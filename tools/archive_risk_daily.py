"""Frozen-window research collectors; never license risk or modify AstrBot data."""
from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from datetime import date, datetime, time as day_time, timedelta
import importlib.util
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import sys
import time
import uuid

from archive_eastmoney_close import ArchiveStop, CHINA, append_json, digest, utc_now, validate_frozen


SUPPORTED = ("000", "001", "002", "003", "600", "601", "603", "605", "300", "301")


def read_cursor(cursor, check, maximum=20000):
    rows = []
    if cursor.error_code != "0":
        raise ArchiveStop("sdk_query_failed")
    while cursor.next():
        check()
        values = cursor.get_row_data()
        if len(values) != len(cursor.fields) or len(rows) >= maximum:
            raise ArchiveStop("invalid_or_oversize_sdk_table")
        rows.append(dict(zip(cursor.fields, values)))
    if cursor.error_code != "0":
        raise ArchiveStop("sdk_pagination_failed")
    return rows


def verify_calendar(rows, start, target):
    expected = {(start + timedelta(days=offset)).isoformat()
                for offset in range((target - start).days + 1)}
    if (len(rows) != len(expected) or {row.get("calendar_date") for row in rows} != expected
            or any(row.get("is_trading_day") not in ("0", "1") for row in rows)
            or not any(row.get("calendar_date") == target.isoformat() and row.get("is_trading_day") == "1"
                       for row in rows)):
        raise ArchiveStop("incomplete_or_nontrading_calendar")


def verify_daily(rows, symbol, target):
    if (len(rows) != 1 or rows[0].get("date") != target or rows[0].get("code") != symbol):
        raise ArchiveStop("empty_stale_or_mismatched_daily")


def listed_supported_codes(basic, target):
    codes = []
    seen = set()
    for row in basic:
        symbol = row.get("code")
        if not isinstance(symbol, str) or not re.fullmatch(r"(?:sh|sz|bj)\.[0-9]{6}", symbol) or symbol in seen:
            raise ArchiveStop("duplicate_or_invalid_basic_symbol")
        seen.add(symbol)
        code = symbol[3:]
        if not code.startswith(SUPPORTED):
            continue
        if row.get("type") not in ("1", "2", "3") or row.get("status") not in ("0", "1"):
            raise ArchiveStop("unsupported_basic_metadata")
        if row["type"] != "1" or row["status"] != "1":
            continue
        if symbol[:2] != ("sh" if code.startswith("6") else "sz"):
            raise ArchiveStop("unsupported_basic_metadata")
        try:
            ipo = date.fromisoformat(row["ipoDate"])
        except (KeyError, TypeError, ValueError):
            raise ArchiveStop("listed_basic_ipo_unverified") from None
        if row.get("outDate") != "":
            raise ArchiveStop("listed_basic_delisting_conflict")
        if ipo <= date.fromisoformat(target):
            codes.append(code)
    if not codes:
        raise ArchiveStop("empty_supported_universe")
    return sorted(codes)


def validate_amendment(raw, approval, protocol_hash, now):
    if raw is None:
        return None
    amendment = json.loads(raw)
    if (approval.get("collection_amendment_sha256") != digest(raw)
            or amendment.get("base_protocol_sha256") != protocol_hash
            or amendment.get("version") != "formal-risk-collection-amendment/2026-10-04-v1"
            or amendment.get("status") != "user_authorized"
            or amendment.get("daily_universe_source") != "baostock:stock_basic:listed-supported"):
        raise ArchiveStop("collection_amendment_not_approved")
    try:
        confirmed = datetime.fromisoformat(amendment["confirmed_at"])
        if confirmed.tzinfo is None or confirmed > now:
            raise ValueError("invalid_approval_time")
    except (KeyError, TypeError, ValueError):
        raise ArchiveStop("invalid_amendment_time") from None
    return amendment


def suspension_candidate(row, target, *, semantics_confirmed=False):
    result = {"code": row.get("代码"), "suspended": None, "reason": "interval_or_scope_unverified"}
    code = result["code"]
    if not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code) or not code.startswith(SUPPORTED):
        return {**result, "reason": "outside_supported_scope"}
    scope = str(row.get("停牌期限", ""))
    if any(marker in scope for marker in ("盘中", "临时", "小时", "分钟")):
        return {**result, "reason": "intraday_or_temporary"}
    try:
        def parse(value):
            parsed = datetime.fromisoformat(str(value))
            return parsed.replace(tzinfo=CHINA) if parsed.tzinfo is None else parsed.astimezone(CHINA)
        opening = datetime.combine(date.fromisoformat(target), day_time(9, 30), CHINA)
        closing = opening.replace(hour=15, minute=0)
        start = parse(row["停牌时间"])
        if start > opening:
            return {**result, "reason": "future_start"}
        for field in ("预计复牌时间", "实际复牌时间"):
            if row.get(field) and parse(row[field]).date() <= opening.date():
                return {**result, "reason": "resumed_by_target_day"}
        end_text = row.get("停牌截止时间")
        end = parse(end_text) if end_text else None
        if end is not None and end.date() < opening.date():
            return {**result, "reason": "past_end"}
        if not semantics_confirmed:
            return result
        if end is not None and len(str(end_text)) == 10:
            end = end.replace(hour=15)
        if end is not None and end < closing:
            return {**result, "reason": "interval_not_full_day"}
        if scope not in ("全天", "连续停牌") or (end is None and scope != "连续停牌"):
            return result
        return {**result, "suspended": True, "reason": "confirmed_full_day_interval"}
    except (KeyError, ValueError, TypeError):
        return result


@contextmanager
def shared_session(**kwargs):
    helper = Path(__file__).with_name("shared_baostock.py")
    if not helper.exists():
        helper = Path("/home/pi/apps/stock-fund-fetch-20260929/shared_baostock.py")
    spec = importlib.util.spec_from_file_location("daily_shared_baostock", helper)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    with module.owner_session(**kwargs) as guard:
        yield guard


class DailyArchive:
    def __init__(self, root, protocol, kind, *, now=utc_now, monotonic=time.monotonic,
                 sleep=time.sleep, session=shared_session):
        self.root, self.protocol, self.kind = Path(root), protocol, kind
        self.now, self.monotonic, self.sleep, self.session = now, monotonic, sleep, session
        key = "baostock_daily_collection" if kind == "baostock" else "suspension_positive_collection"
        self.settings = protocol[key]
        self.interrupted = False
        self.sequence = 0
        self.guard = None

    def check(self):
        if self.interrupted or (self.root / "STOP").exists():
            raise ArchiveStop("operator_stop")
        if self.now().astimezone(CHINA).date().isoformat() != self.target:
            raise ArchiveStop("session_date_changed")
        if self.monotonic() >= self.deadline:
            raise ArchiveStop("runtime_budget_exhausted")
        if shutil.disk_usage(self.root).free < self.protocol["collection"]["minimum_free_bytes"]:
            raise ArchiveStop("free_disk_below_minimum")

    def table(self, name, rows, parameters, started):
        self.sequence += 1
        raw = json.dumps(rows, ensure_ascii=False, allow_nan=False, separators=(",", ":")).encode()
        filename = f"{self.sequence:05d}-{name}.table.json"
        with (self.run / filename).open("xb") as stream:
            stream.write(raw)
        append_json(self.run / "index.jsonl", {"kind": name, "parameters": parameters,
                    "request_started_at": started, "received_at": self.now().isoformat(),
                    "table_file": filename, "serialized_sdk_table_sha256": digest(raw),
                    "rows": len(rows), "representation": "sdk_table_not_original_wire_bytes"})

    def universe(self):
        deadline = self.monotonic() + self.settings["universe_wait_seconds"]
        while True:
            self.check()
            paths = list((self.root / "archives" / self.target).glob("run-*/universe.json"))
            if len(paths) > 1:
                raise ArchiveStop("ambiguous_same_day_universe")
            if paths:
                path = paths[0]
                if digest((path.parent / "protocol.json").read_bytes()) != self.protocol_hash:
                    raise ArchiveStop("universe_protocol_mismatch")
                rows = json.loads(path.read_bytes())
                codes = []
                for row in rows:
                    if (not isinstance(row, list) or len(row) != 2 or not isinstance(row[0], str)
                            or not re.fullmatch(r"[0-9]{6}", row[0]) or type(row[1]) is not int
                            or row[1] != (1 if row[0].startswith("6") else 0)):
                        raise ArchiveStop("invalid_universe_security")
                    codes.append(row[0])
                if not codes or len(codes) != len(set(codes)):
                    raise ArchiveStop("empty_or_duplicate_universe")
                append_json(self.run / "index.jsonl", {"kind": "universe", "file": str(path),
                            "raw_sha256": digest(path.read_bytes()), "received_at": self.now().isoformat()})
                return [code for code in codes if code.startswith(SUPPORTED)]
            if self.monotonic() >= deadline:
                raise ArchiveStop("complete_today_universe_unavailable")
            self.sleep(min(5, deadline - self.monotonic()))

    def baostock(self, client):
        independent_collection = self.amendment is not None
        codes = None if independent_collection else self.universe()
        if not independent_collection and not codes:
            raise ArchiveStop("empty_supported_universe")
        if codes is not None:
            self.manifest.update(supported_count=len(codes), completed_codes=[], missing_codes=codes.copy())
        with self.session(purpose="stock-watch-baostock-risk-archive",
                          max_calls=self.settings["run_message_cap"],
                          wait_seconds=self.settings["lock_wait_seconds"], target_date=self.target,
                          latest_start_local=self.settings["latest_start_local"],
                          stop_file=self.root / "STOP", work_seconds=self.settings["maximum_runtime_seconds"],
                          before_send_check=self.check) as guard:
            self.guard = guard
            self.deadline = self.monotonic() + self.settings["maximum_runtime_seconds"]
            self.manifest["shared_calls_before"] = guard.snapshot()["calls_today"]
            if self.now().astimezone(CHINA).strftime("%H:%M:%S") > self.settings["latest_start_local"]:
                raise ArchiveStop("latest_start_passed_while_waiting")
            self.check()
            login = guard.login(client)
            if login is None or login.error_code != "0":
                raise ArchiveStop("sdk_login_failed")
            try:
                started = self.now().isoformat()
                basic = read_cursor(client.query_stock_basic(), self.check)
                self.table("basic", basic, {}, started)
                if independent_collection:
                    codes = listed_supported_codes(basic, self.target)
                    self.manifest.update(supported_count=len(codes), completed_codes=[], missing_codes=codes.copy(),
                                         universe_source=self.amendment["daily_universe_source"],
                                         independent_universe_acceptance=False)
                    self.table("collection-universe", [{"code": code} for code in codes],
                               {"source": "same_day_full_basic_before_daily_prices"}, started)
                matches = {}
                for row in basic:
                    symbol = row.get("code")
                    if symbol in matches:
                        raise ArchiveStop("duplicate_basic_symbol")
                    matches[symbol] = row
                if not basic or any(("sh." if code.startswith("6") else "sz.") + code not in matches for code in codes):
                    raise ArchiveStop("incomplete_supported_basic")
                target = date.fromisoformat(self.target)
                start = target - timedelta(days=45)
                parameters = {"start_date": start.isoformat(), "end_date": self.target}
                started = self.now().isoformat()
                calendar = read_cursor(client.query_trade_dates(**parameters), self.check)
                self.table("calendar", calendar, parameters, started)
                verify_calendar(calendar, start, target)
                for code in codes:
                    self.check()
                    symbol = ("sh." if code.startswith("6") else "sz.") + code
                    parameters = {"code": symbol, "fields": self.settings["daily"]["fields"],
                                  "start_date": self.target, "end_date": self.target,
                                  "frequency": "d", "adjustflag": "3"}
                    started = self.now().isoformat()
                    rows = read_cursor(client.query_history_k_data_plus(**parameters), self.check, maximum=2)
                    self.table(code, rows, parameters, started)
                    verify_daily(rows, symbol, self.target)
                    self.manifest["completed_codes"].append(code)
                    self.manifest["missing_codes"].remove(code)
            finally:
                guard.logout(client)
            self.check()

    def suspensions(self, client, requests_module):
        original = requests_module.get
        pages = []
        def fetch(url, **kwargs):
            self.check()
            if url != "https://datacenter-web.eastmoney.com/api/data/v1/get":
                raise ArchiveStop("unexpected_suspension_endpoint")
            if len(pages) >= self.settings["maximum_http_pages"]:
                raise ArchiveStop("http_page_budget_exhausted")
            kwargs["timeout"] = min(15, max(0.1, self.deadline - self.monotonic()))
            started = self.now().isoformat()
            response = original(url, **kwargs)
            raw = response.content
            filename = f"{len(pages)+1:05d}.raw"
            with (self.run / filename).open("xb") as stream:
                stream.write(raw)
            page = {"parameters": dict(kwargs.get("params", {})), "request_started_at": started,
                    "received_at": self.now().isoformat(), "raw_file": filename,
                    "raw_sha256": digest(raw), "http_status": response.status_code}
            append_json(self.run / "index.jsonl", page)
            payload = response.json()
            pages.append(payload)
            response.raise_for_status()
            if len(raw) > self.protocol["collection"]["maximum_response_bytes"]:
                raise ArchiveStop("response_too_large")
            result = payload.get("result")
            if (not payload.get("success") or not isinstance(result, dict)
                    or type(result.get("pages")) is not int or result["pages"] < 1
                    or result["pages"] > self.settings["maximum_http_pages"] - 1
                    or not isinstance(result.get("data"), list)):
                raise ArchiveStop("invalid_suspension_page")
            return response
        requests_module.get = fetch
        try:
            started = self.now().isoformat()
            frame = client.stock_tfp_em(date=self.target.replace("-", ""))
            rows = json.loads(frame.to_json(orient="records", force_ascii=False, date_format="iso"))
            self.table("suspensions", rows, {"date": self.target}, started)
            self.manifest.update(sdk_version=client.__version__, http_pages=len(pages), table_rows=len(rows))
            if (not pages or len(pages) != pages[0]["result"]["pages"] + 1
                    or any(page["result"]["pages"] != pages[0]["result"]["pages"] for page in pages)):
                raise ArchiveStop("incomplete_suspension_pages")
            counts = Counter(row.get("代码") for row in rows)
            candidates = [suspension_candidate(row, self.target) for row in rows]
            for candidate in candidates:
                if counts[candidate["code"]] > 1:
                    candidate.update(suspended=None, reason="duplicate_or_conflicting_record")
            self.table("candidates", candidates, {"semantics_confirmed": False}, started)
            self.manifest["independent_positives"] = sum(row["suspended"] is True for row in candidates)
            self.manifest["interval_semantics"] = "not_yet_accepted"
            self.check()
        finally:
            requests_module.get = original

    def collect(self, raw, approval, client=None, requests_module=None, amendment_raw=None):
        self.target = validate_frozen(self.protocol, raw, approval, self.now())
        self.protocol_hash = digest(raw)
        self.amendment = validate_amendment(amendment_raw, approval, self.protocol_hash, self.now())
        clock = self.now().astimezone(CHINA).strftime("%H:%M:%S")
        latest = self.settings.get("latest_start_local", self.protocol["collection"]["latest_start_local"])
        if not self.settings["start_at_local"] <= clock <= latest:
            raise ArchiveStop("outside_collector_start_window")
        if (self.root / "STOP").exists():
            raise ArchiveStop("operator_stop")
        self.root.mkdir(parents=True, exist_ok=True)
        lock = self.root / (self.kind + ".lock")
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            os.write(descriptor, str(os.getpid()).encode())
            day_root = self.root / self.kind / self.target
            day_root.mkdir(parents=True, exist_ok=False)
            self.run = day_root / ("run-" + uuid.uuid4().hex)
            self.run.mkdir()
            (self.run / "protocol.json").write_bytes(raw)
            if amendment_raw is not None:
                (self.run / "collection-amendment.json").write_bytes(amendment_raw)
            self.log = self.root / "logs" / (self.kind + "-" + self.target + ".jsonl")
            self.log.parent.mkdir(exist_ok=True)
            self.manifest = {"kind": self.kind, "trade_date": self.target, "status": "partial",
                             "licensed": False, "protocol_sha256": self.protocol_hash,
                             "started_at": self.now().isoformat()}
            if amendment_raw is not None:
                self.manifest["collection_amendment_sha256"] = digest(amendment_raw)
            self.deadline = self.monotonic() + self.settings["maximum_runtime_seconds"]
            append_json(self.log, {"event": "started", **self.manifest})
            previous_timeout = socket.getdefaulttimeout()
            socket.setdefaulttimeout(15)
            try:
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    if self.kind == "baostock":
                        self.baostock(client)
                    else:
                        self.suspensions(client, requests_module)
                self.manifest["status"] = "complete_capture_not_acceptance"
            except Exception as exc:
                self.manifest.update(stop_reason=str(exc) if isinstance(exc, ArchiveStop)
                                     or type(exc).__name__ == "GuardStop" else type(exc).__name__)
            finally:
                socket.setdefaulttimeout(previous_timeout)
                if self.guard is not None:
                    self.manifest["shared_guard"] = self.guard.snapshot()
                self.manifest["finished_at"] = self.now().isoformat()
                with (self.run / "manifest.json").open("x", encoding="utf-8") as stream:
                    json.dump(self.manifest, stream, ensure_ascii=False, indent=2)
                append_json(self.log, {"event": "finished", **self.manifest})
            return self.manifest
        finally:
            os.close(descriptor)
            lock.unlink()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=("baostock", "suspensions"), required=True)
    for name in ("root", "protocol", "approval"):
        parser.add_argument("--" + name, type=Path, required=True)
    parser.add_argument("--amendment", type=Path)
    args = parser.parse_args()
    try:
        raw = args.protocol.read_bytes()
        collector = DailyArchive(args.root, json.loads(raw), args.kind)
        def stop(signum, frame):
            collector.interrupted = True
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        if args.kind == "baostock":
            import baostock as client
            requests_module = None
        else:
            import akshare as client
            import requests as requests_module
        result = collector.collect(raw, json.loads(args.approval.read_bytes()), client, requests_module,
                                   args.amendment.read_bytes() if args.amendment else None)
        print(json.dumps(result, ensure_ascii=False))
        return 0 if result["status"] == "complete_capture_not_acceptance" else 5
    except Exception as exc:
        print(json.dumps({"status": "not_started", "reason": str(exc) if isinstance(exc, ArchiveStop)
                          else type(exc).__name__}))
        return 5


if __name__ == "__main__":
    sys.exit(main())
