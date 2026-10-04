"""Append-only research archive; refuses drafts and never enables a service."""
from __future__ import annotations

import argparse
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import time
from urllib.parse import urlencode
from urllib.error import HTTPError
from urllib.request import Request, urlopen
import uuid


CHINA = timezone(timedelta(hours=8))
UNIVERSE_URL = "https://push2.eastmoney.com/api/qt/clist/get"
SNAPSHOT_URL = "https://push2.eastmoney.com/api/qt/stock/get"
UNIVERSE_FILTER = "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23,m:0+t:81"
FIELDS = "f43,f47,f48,f51,f52,f57,f58,f59,f60,f86"
UNIVERSE_FIELDS = "f2,f5,f6,f12,f13,f14,f18,f51,f52,f124"


class ArchiveStop(RuntimeError):
    pass


def utc_now():
    return datetime.now(timezone.utc)


def digest(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def append_json(path: Path, item: dict):
    with path.open("a", encoding="utf-8", newline="\n") as stream:
        stream.write(json.dumps(item, ensure_ascii=False, allow_nan=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


def validate_frozen(protocol: dict, protocol_raw: bytes, approval: dict, now: datetime) -> str:
    local = now.astimezone(CHINA)
    if (protocol.get("protocol_version") != "formal-risk-forward/2026-10-04-v4"
            or protocol.get("status") != "frozen_user_confirmed"
            or approval.get("approved") is not True
            or approval.get("protocol_sha256") != digest(protocol_raw)):
        raise ArchiveStop("protocol_not_approved_and_frozen")
    if json.loads(protocol_raw) != protocol:
        raise ArchiveStop("protocol_object_does_not_match_approved_bytes")
    try:
        confirmed = datetime.fromisoformat(approval["confirmed_at"])
        if confirmed.tzinfo is None or confirmed > now:
            raise ValueError("invalid_approval_time")
    except (KeyError, ValueError, TypeError):
        raise ArchiveStop("invalid_approval_time") from None
    trade_date = local.date().isoformat()
    if trade_date < protocol["start_date"] or trade_date not in protocol["dates"]:
        raise ArchiveStop("outside_declared_trading_window")
    return trade_date


def validate_gate(protocol: dict, protocol_raw: bytes, approval: dict, now: datetime) -> str:
    trade_date = validate_frozen(protocol, protocol_raw, approval, now)
    clock = now.astimezone(CHINA).strftime("%H:%M:%S")
    settings = protocol["collection"]
    if not settings["start_at_local"] <= clock <= settings["latest_start_local"]:
        raise ArchiveStop("outside_after_close_start_window")
    return trade_date


def limit_reference(payload: dict, code: str, trade_date: str, received_at: str) -> dict:
    result = {"limit_up": None, "limit_down": None, "st": None, "suspended": None,
              "valid": False, "price_valid": False}
    try:
        raw = payload["data"]
        received = datetime.fromisoformat(received_at)
        stamp = datetime.fromtimestamp(raw["f86"], timezone.utc)
        if (type(payload["rc"]) is not int or payload["rc"] != 0
                or raw["f57"] != code or type(raw["f59"]) is not int or raw["f59"] != 2
                or type(raw["f86"]) is not int or received.tzinfo is None
                or stamp > received or stamp.astimezone(CHINA).date().isoformat() != trade_date
                or received.astimezone(CHINA).date().isoformat() != trade_date
                or stamp.astimezone(CHINA).hour < 15):
            return result
        prices = {}
        for field in ("f43", "f60"):
            if isinstance(raw[field], bool):
                return result
            value = Decimal(str(raw[field])) / 100
            if not value.is_finite() or value <= 0 or value != value.quantize(Decimal("0.01")):
                return result
            prices[field] = value
        result.update(price_valid=True, source_timestamp=stamp.isoformat(),
                      close=str(prices["f43"]), pre_close=str(prices["f60"]))
        name = raw.get("f58")
        if isinstance(name, str) and name.strip() and name.strip() not in ("-", "--") and not name.isdigit():
            result["security_name"] = name
            if "ST" in name.upper():
                result["st"] = True
            elif "退" not in name:
                result["st"] = False
        try:
            if isinstance(raw.get("f47"), bool) or isinstance(raw.get("f48"), bool):
                raise ValueError("invalid_turnover")
            volume = Decimal(str(raw["f47"]))
            amount = Decimal(str(raw["f48"]))
            if volume.is_finite() and amount.is_finite() and volume > 0 and amount > 0:
                result["suspended"] = False
                result.update(volume=str(volume), amount=str(amount))
        except (KeyError, TypeError, ValueError, InvalidOperation):
            pass
        for field in ("f51", "f52"):
            if isinstance(raw[field], bool):
                return result
            value = Decimal(str(raw[field])) / 100
            if not value.is_finite() or value <= 0 or value != value.quantize(Decimal("0.01")):
                return result
            prices[field] = value
        if not prices["f52"] <= prices["f43"] <= prices["f51"] or not prices["f52"] < prices["f51"]:
            return result
        result.update(valid=True, limit_up=prices["f43"] == prices["f51"],
                      limit_down=prices["f43"] == prices["f52"], source_timestamp=stamp.isoformat(),
                      close=str(prices["f43"]), pre_close=str(prices["f60"]),
                      upper=str(prices["f51"]), lower=str(prices["f52"]))
    except (KeyError, ValueError, TypeError, InvalidOperation, OverflowError, OSError):
        pass
    return result


def download(url: str, timeout: float, maximum_bytes: int) -> bytes:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0", "Referer": "https://quote.eastmoney.com/"})
    try:
        response = urlopen(request, timeout=timeout)
    except HTTPError as exc:
        response = exc
    with response:
        raw = response.read(maximum_bytes + 1)
    return raw


class Collector:
    def __init__(self, root: Path, protocol: dict, *, fetch=download, now=utc_now,
                 monotonic=time.monotonic, sleep=time.sleep):
        self.root = root
        self.protocol = protocol
        self.settings = protocol["collection"]
        self.fetch = fetch
        self.now = now
        self.monotonic = monotonic
        self.sleep = sleep
        self.interrupted = False
        self.requests = 0
        self.failures = 0
        self.valid = 0
        self.snapshots = 0

    def check_stop(self, *, next_request=True):
        if self.interrupted or (self.root / "STOP").exists():
            raise ArchiveStop("operator_stop")
        if self.monotonic() >= self.deadline:
            raise ArchiveStop("runtime_budget_exhausted")
        if self.now().astimezone(CHINA).date().isoformat() != self.trade_date:
            raise ArchiveStop("session_date_changed")
        if next_request and self.requests >= self.settings["maximum_requests"]:
            raise ArchiveStop("request_budget_exhausted")
        if shutil.disk_usage(self.root).free < self.settings["minimum_free_bytes"]:
            raise ArchiveStop("free_disk_below_minimum")

    def request(self, endpoint: str, params: dict, kind: str, identity: str) -> dict | None:
        self.check_stop()
        self.requests += 1
        record = {"kind": kind, "identity": identity, "request_number": self.requests,
                  "request_started_at": self.now().isoformat(), "parameters": params}
        payload = None
        try:
            raw = self.fetch(endpoint + "?" + urlencode(params),
                             min(self.settings["request_timeout_seconds"],
                                 max(0.1, self.deadline - self.monotonic())),
                             self.settings["maximum_response_bytes"])
            record["received_at"] = self.now().isoformat()
            filename = f"{self.requests:05d}-{kind}-{identity}.raw"
            with (self.run / filename).open("xb") as stream:
                stream.write(raw)
            record.update(raw_file=filename, raw_sha256=digest(raw), raw_bytes=len(raw))
            if len(raw) > self.settings["maximum_response_bytes"]:
                raise ValueError("response_too_large")
            candidate = json.loads(raw)
            if not isinstance(candidate, dict) or type(candidate.get("rc")) is not int or candidate["rc"] != 0:
                raise ValueError("invalid_response_envelope")
            if not isinstance(candidate.get("data"), dict):
                raise ValueError("missing_response_data")
            payload = candidate
        except (OSError, ValueError, TypeError) as exc:
            record.update(error_type=type(exc).__name__, failed_at=self.now().isoformat())
        if payload is None:
            self.failures += 1
            record["status"] = "request_failed"
        else:
            self.failures = 0
            record["status"] = "received"
            if kind == "stock":
                reference = limit_reference(payload, identity, self.trade_date, record["received_at"])
                record["reference_candidate"] = reference
                self.valid += reference["valid"]
                self.snapshots += 1
        append_json(self.run / "index.jsonl", record)
        append_json(self.log, {key: value for key, value in record.items()
                               if key not in ("parameters", "reference_candidate")})
        if self.failures >= self.settings["maximum_consecutive_failures"]:
            raise ArchiveStop("consecutive_request_failures")
        self.sleep(self.settings["request_interval_seconds"])
        return payload

    def universe(self) -> list[tuple[str, int]]:
        securities = []
        seen = set()
        total = None
        page_size = self.settings["universe_page_size"]
        for page in range(1, self.settings["universe_max_pages"] + 1):
            payload = self.request(UNIVERSE_URL, {"pn": page, "pz": page_size, "po": 1,
                "np": 1, "fltt": 1, "invt": 2, "fid": "f12", "fs": UNIVERSE_FILTER,
                "fields": UNIVERSE_FIELDS}, "universe", str(page))
            if payload is None:
                raise ArchiveStop("universe_request_failed")
            data = payload["data"]
            if type(data.get("total")) is not int or data["total"] < 1:
                raise ArchiveStop("invalid_universe_total")
            if total is not None and data["total"] != total:
                raise ArchiveStop("universe_total_changed")
            total = data["total"]
            rows = data.get("diff")
            if not isinstance(rows, list) or not rows or len(rows) != min(page_size, total - len(securities)):
                raise ArchiveStop("incomplete_universe_page")
            for row in rows:
                if not isinstance(row, dict):
                    raise ArchiveStop("invalid_universe_row")
                code = row.get("f12")
                market = row.get("f13")
                if (not isinstance(code, str) or not re.fullmatch(r"[0-9]{6}", code)
                        or type(market) is not int or market not in (0, 1) or code in seen
                        or not code.startswith(("000", "001", "002", "003", "300", "301",
                                                "600", "601", "603", "605", "688", "689", "4", "8", "92"))
                        or market != (1 if code.startswith("6") else 0)):
                    raise ArchiveStop("duplicate_or_invalid_universe_security")
                seen.add(code)
                securities.append((code, market))
            if len(securities) == total:
                return securities
        raise ArchiveStop("universe_page_budget_exhausted")

    def collect(self, protocol_raw: bytes, approval: dict) -> dict:
        self.trade_date = validate_gate(self.protocol, protocol_raw, approval, self.now())
        if (self.root / "STOP").exists():
            raise ArchiveStop("operator_stop")
        self.root.mkdir(parents=True, exist_ok=True)
        lock = self.root / "collector.lock"
        descriptor = os.open(lock, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        self.deadline = self.monotonic() + self.settings["maximum_runtime_seconds"]
        manifest = {"trade_date": self.trade_date, "status": "partial", "licensed": False,
                    "protocol_sha256": digest(protocol_raw), "started_at": self.now().isoformat()}
        try:
            os.write(descriptor, str(os.getpid()).encode())
            day_root = self.root / "archives" / self.trade_date
            if day_root.exists():
                raise ArchiveStop("session_already_attempted_no_automatic_replay")
            self.run = day_root / ("run-" + uuid.uuid4().hex)
            self.run.mkdir(parents=True)
            self.log = self.root / "logs" / (self.trade_date + ".jsonl")
            self.log.parent.mkdir(parents=True, exist_ok=True)
            append_json(self.log, {"event": "started", **manifest})
            with (self.run / "protocol.json").open("xb") as stream:
                stream.write(protocol_raw)
            try:
                securities = self.universe()
                manifest["universe_count"] = len(securities)
                with (self.run / "universe.next").open("x", encoding="utf-8") as stream:
                    json.dump(securities, stream)
                    stream.flush()
                    os.fsync(stream.fileno())
                os.replace(self.run / "universe.next", self.run / "universe.json")
                for code, market in securities:
                    self.request(SNAPSHOT_URL, {"secid": f"{market}.{code}", "fltt": 1,
                                               "invt": 2, "fields": FIELDS}, "stock", code)
                self.check_stop(next_request=False)
                if self.snapshots == len(securities):
                    manifest["status"] = "complete_capture_not_acceptance"
                else:
                    manifest["stop_reason"] = "missing_stock_responses"
            except (ArchiveStop, OSError, ValueError, TypeError) as exc:
                manifest["stop_reason"] = str(exc) if isinstance(exc, ArchiveStop) else type(exc).__name__
            finally:
                manifest.update(requests=self.requests, received_snapshots=self.snapshots,
                                structurally_valid_limit_candidates=self.valid,
                                finished_at=self.now().isoformat())
                with (self.run / "manifest.json").open("x", encoding="utf-8") as stream:
                    json.dump(manifest, stream, ensure_ascii=False, indent=2)
                    stream.write("\n")
                append_json(self.log, {"event": "finished", **manifest})
            return manifest
        finally:
            os.close(descriptor)
            lock.unlink()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--protocol", type=Path, required=True)
    parser.add_argument("--approval", type=Path, required=True)
    args = parser.parse_args()
    try:
        protocol_raw = args.protocol.read_bytes()
        protocol = json.loads(protocol_raw)
        approval = json.loads(args.approval.read_bytes())
        collector = Collector(args.root, protocol)
        def stop(signum, frame):
            collector.interrupted = True
        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        manifest = collector.collect(protocol_raw, approval)
        print(json.dumps(manifest, ensure_ascii=False))
        return 0 if manifest["status"] == "complete_capture_not_acceptance" else 5
    except (ArchiveStop, OSError, KeyError, ValueError, TypeError) as exc:
        print(json.dumps({"status": "not_started", "reason": str(exc) if isinstance(exc, ArchiveStop)
                          else type(exc).__name__}, ensure_ascii=False))
        return 5


if __name__ == "__main__":
    raise SystemExit(main())
