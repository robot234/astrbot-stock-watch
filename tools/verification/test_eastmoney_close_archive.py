from __future__ import annotations

from datetime import datetime, timezone
import importlib.util
import json
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location("archive_eastmoney", ROOT / "tools/archive_eastmoney_close.py")
ARCHIVE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ARCHIVE)
NOW = datetime(2026, 10, 8, 8, 10, tzinfo=timezone.utc)


def protocol():
    result = json.loads((ROOT / "docs/FORMAL_RISK_FORWARD_ACCEPTANCE_20261004.json").read_text(encoding="utf-8"))
    result["status"] = "frozen_user_confirmed"
    result["collection"].update(universe_page_size=2, request_interval_seconds=0)
    raw = json.dumps(result).encode()
    approval = {"approved": True, "protocol_sha256": ARCHIVE.digest(raw),
                "confirmed_at": "2026-10-04T12:00:00+00:00"}
    return result, raw, approval


def stock(code="600000", **changes):
    data = {"f57": code, "f59": 2, "f43": 1000, "f60": 1000,
            "f51": 1100, "f52": 900, "f86": int(NOW.replace(hour=7).timestamp()),
            "f58": "普通股票", "f47": 10, "f48": 10000}
    data.update(changes)
    return {"rc": 0, "data": data}


def fixture_fetch(calls, *, bad_stock=False, stale=False, duplicate=False):
    def fetch(url, timeout, maximum_bytes):
        calls.append(url)
        params = parse_qs(urlparse(url).query)
        if "clist/get" in url:
            page = int(params["pn"][0])
            rows = [{"f12": "600000", "f13": 1}, {"f12": "000001", "f13": 0}]
            if page == 2:
                rows = [{"f12": "600000" if duplicate else "300750", "f13": 1 if duplicate else 0}]
            payload = {"rc": 0, "data": {"total": 3, "diff": rows}}
        else:
            if bad_stock:
                raise ConnectionError("fixture transport error")
            code = params["secid"][0].split(".")[1]
            payload = stock(code)
            if stale:
                payload["data"]["f86"] -= 86400
        return json.dumps(payload, indent=1).encode()
    return fetch


def collector(tmp_path, result, calls, **kwargs):
    return ARCHIVE.Collector(tmp_path / "archive", result,
                             fetch=fixture_fetch(calls, **kwargs), now=lambda: NOW, sleep=lambda _: None)


def test_draft_refuses_before_network_or_files(tmp_path):
    result, raw, approval = protocol()
    result["status"] = "draft_pending_user_confirmation"
    calls = []
    instance = collector(tmp_path, result, calls)
    with pytest.raises(ARCHIVE.ArchiveStop, match="not_approved"):
        instance.collect(raw, approval)
    assert calls == [] and not instance.root.exists()


@pytest.mark.parametrize("now", [
    datetime(2026, 10, 7, 8, 10, tzinfo=timezone.utc),
    datetime(2026, 10, 10, 8, 10, tzinfo=timezone.utc),
    datetime(2026, 10, 8, 6, 59, tzinfo=timezone.utc),
    datetime(2026, 10, 8, 9, 31, tzinfo=timezone.utc),
    datetime(2026, 11, 5, 8, 10, tzinfo=timezone.utc),
])
def test_declared_sessions_and_start_time_only(now):
    result, raw, approval = protocol()
    with pytest.raises(ARCHIVE.ArchiveStop):
        ARCHIVE.validate_gate(result, raw, approval, now)


def test_approval_must_match_exact_protocol_bytes():
    result, raw, approval = protocol()
    with pytest.raises(ARCHIVE.ArchiveStop, match="not_approved"):
        ARCHIVE.validate_gate(result, raw + b"\n", approval, NOW)


def test_capture_keeps_original_bytes_real_time_hash_and_all_pages(tmp_path):
    result, raw, approval = protocol()
    calls = []
    instance = collector(tmp_path, result, calls)
    manifest = instance.collect(raw, approval)
    assert manifest["status"] == "complete_capture_not_acceptance"
    assert manifest["universe_count"] == manifest["received_snapshots"] == 3
    assert manifest["requests"] == len(calls) == 5
    assert manifest["licensed"] is False
    records = [json.loads(line) for line in (instance.run / "index.jsonl").read_text(encoding="utf-8").splitlines()]
    for record in records:
        assert record["received_at"] == NOW.isoformat()
        raw_response = (instance.run / record["raw_file"]).read_bytes()
        assert record["raw_sha256"] == ARCHIVE.digest(raw_response)
        assert b"\n" in raw_response
    assert not (instance.root / "collector.lock").exists()
    with pytest.raises(ARCHIVE.ArchiveStop, match="already_attempted"):
        collector(tmp_path, result, []).collect(raw, approval)


def test_two_transport_failures_stop_and_keep_partial_log(tmp_path):
    result, raw, approval = protocol()
    calls = []
    instance = collector(tmp_path, result, calls, bad_stock=True)
    manifest = instance.collect(raw, approval)
    assert manifest["status"] == "partial"
    assert manifest["stop_reason"] == "consecutive_request_failures"
    assert len(calls) == 4
    assert "fixture transport error" not in instance.log.read_text(encoding="utf-8")
    assert (instance.run / "manifest.json").exists()


def test_stale_prices_are_archived_but_no_reference(tmp_path):
    result, raw, approval = protocol()
    instance = collector(tmp_path, result, [], stale=True)
    manifest = instance.collect(raw, approval)
    assert manifest["received_snapshots"] == 3
    assert manifest["structurally_valid_limit_candidates"] == 0


def test_duplicate_universe_stops_before_stock_requests(tmp_path):
    result, raw, approval = protocol()
    calls = []
    manifest = collector(tmp_path, result, calls, duplicate=True).collect(raw, approval)
    assert manifest["status"] == "partial"
    assert manifest["stop_reason"] == "duplicate_or_invalid_universe_security"
    assert len(calls) == 2


@pytest.mark.parametrize("changes", [
    {"f57": "000001"}, {"f59": 3}, {"f59": True}, {"f51": 0}, {"f52": 1001},
    {"f43": True}, {"f43": "NaN"}, {"f86": True},
    {"f86": int(NOW.replace(hour=6).timestamp())},
    {"f86": int(NOW.replace(hour=9).timestamp())},
])
def test_malformed_or_intraday_reference_remains_unknown(changes):
    reference = ARCHIVE.limit_reference(stock(**changes), "600000", "2026-10-08", NOW.isoformat())
    assert reference["valid"] is False
    assert reference["limit_up"] is None and reference["limit_down"] is None


def test_stop_marker_and_single_writer_lock(tmp_path):
    result, raw, approval = protocol()
    instance = collector(tmp_path, result, [])
    instance.root.mkdir()
    (instance.root / "STOP").touch()
    with pytest.raises(ARCHIVE.ArchiveStop, match="operator_stop"):
        instance.collect(raw, approval)
    (instance.root / "STOP").unlink()
    (instance.root / "collector.lock").write_text("another-writer")
    with pytest.raises(FileExistsError):
        instance.collect(raw, approval)
    assert (instance.root / "collector.lock").read_text() == "another-writer"


def test_budget_exhaustion_keeps_partial(tmp_path):
    result, raw, approval = protocol()
    result["collection"]["maximum_requests"] = 3
    raw = json.dumps(result).encode()
    approval["protocol_sha256"] = ARCHIVE.digest(raw)
    manifest = collector(tmp_path, result, []).collect(raw, approval)
    assert manifest["status"] == "partial"
    assert manifest["stop_reason"] == "request_budget_exhausted"


def test_stop_during_running_capture_is_safe(tmp_path):
    result, raw, approval = protocol()
    instance = collector(tmp_path, result, [])
    original = instance.fetch
    def stop_after_first_stock(url, timeout, maximum_bytes):
        raw_response = original(url, timeout, maximum_bytes)
        if "stock/get" in url:
            (instance.root / "STOP").touch()
        return raw_response
    instance.fetch = stop_after_first_stock
    manifest = instance.collect(raw, approval)
    assert manifest["status"] == "partial" and manifest["stop_reason"] == "operator_stop"
    assert manifest["received_snapshots"] == 1
    assert not (instance.root / "collector.lock").exists()


def test_exact_request_budget_can_finish(tmp_path):
    result, raw, approval = protocol()
    result["collection"]["maximum_requests"] = 5
    raw = json.dumps(result).encode()
    approval["protocol_sha256"] = ARCHIVE.digest(raw)
    manifest = collector(tmp_path, result, []).collect(raw, approval)
    assert manifest["status"] == "complete_capture_not_acceptance"


def test_low_disk_keeps_partial_without_network(tmp_path):
    result, raw, approval = protocol()
    result["collection"]["minimum_free_bytes"] = 2 ** 60
    raw = json.dumps(result).encode()
    approval["protocol_sha256"] = ARCHIVE.digest(raw)
    calls = []
    manifest = collector(tmp_path, result, calls).collect(raw, approval)
    assert manifest["status"] == "partial"
    assert manifest["stop_reason"] == "free_disk_below_minimum" and calls == []


def test_invalid_http_response_keeps_raw_hash(tmp_path):
    result, raw, approval = protocol()
    instance = collector(tmp_path, result, [])
    original = instance.fetch
    invalid = b'{"rc":-1,"data":null}'
    def invalid_stocks(url, timeout, maximum_bytes):
        return invalid if "stock/get" in url else original(url, timeout, maximum_bytes)
    instance.fetch = invalid_stocks
    manifest = instance.collect(raw, approval)
    assert manifest["status"] == "partial"
    records = [json.loads(line) for line in (instance.run / "index.jsonl").read_text(encoding="utf-8").splitlines()]
    assert records[-1]["raw_sha256"] == ARCHIVE.digest(invalid)
    assert (instance.run / records[-1]["raw_file"]).read_bytes() == invalid


@pytest.mark.parametrize("name,expected", [("*ST某某", True), ("ST某某", True),
    ("普通股票", False), ("某某退", None), ("", None), ("--", None), (None, None)])
def test_dated_name_independently_references_st(name, expected):
    candidate = ARCHIVE.limit_reference(stock(f58=name), "600000", "2026-10-08", NOW.isoformat())
    assert candidate["st"] is expected


@pytest.mark.parametrize("volume,amount,expected", [(10, 100, False),
    (0, 0, None), ("", "", None), (None, 100, None), (10, 0, None), (True, 100, None)])
def test_only_positive_turnover_references_not_suspended(volume, amount, expected):
    candidate = ARCHIVE.limit_reference(stock(f47=volume, f48=amount), "600000", "2026-10-08", NOW.isoformat())
    assert candidate["suspended"] is expected


def test_missing_limit_bounds_do_not_erase_valid_name_and_turnover():
    payload = stock(f58="*ST某某")
    del payload["data"]["f51"]
    candidate = ARCHIVE.limit_reference(payload, "600000", "2026-10-08", NOW.isoformat())
    assert candidate["limit_up"] is None and candidate["st"] is True
    assert candidate["suspended"] is False


def batch_row(code="000001", **changes):
    result = {"f12": code, "f13": 0, "f2": 1157, "f18": 1135, "f350": 1249,
              "f351": 1022, "f14": "平安银行", "f5": 1045357, "f6": 1205814857.64,
              "f124": int(NOW.replace(hour=7).timestamp())}
    result.update(changes)
    return result


def test_batch_supplied_bounds_are_not_derived_or_f51_f52():
    row = batch_row(f51=999999999, f52=888888888)
    reference = ARCHIVE.batch_reference(row, "000001", 0, "2026-10-08", NOW.isoformat())
    assert reference["upper"] == "12.49" and reference["lower"] == "10.22"
    assert reference["st"] is False and reference["suspended"] is False
    assert reference["limit_up"] is False and reference["limit_down"] is False
    assert reference["mapping_status"] == "probe_candidate_not_formal_acceptance"


@pytest.mark.parametrize("changes", [{"f350": "-"}, {"f351": None}, {"f350": True},
                                     {"f2": "-"}, {"f124": True}, {"f13": 1},
                                     {"f124": int(NOW.timestamp()) + 1}])
def test_unavailable_or_invalid_batch_fields_are_unknown(changes):
    reference = ARCHIVE.batch_reference(batch_row(**changes), "000001", 0, "2026-10-08", NOW.isoformat())
    assert reference["limit_up"] is None and reference["limit_down"] is None


def test_batch_records_each_stock_from_one_original_page_without_stock_get(tmp_path):
    result, raw, approval = protocol()
    calls = []
    def fetch(url, timeout, maximum_bytes):
        calls.append(url)
        assert "clist/get" in url and "f350" in parse_qs(urlparse(url).query)["fields"][0]
        return json.dumps({"rc": 0, "data": {"total": 2, "diff": [batch_row(), batch_row("000002")]}}).encode()
    instance = ARCHIVE.Collector(tmp_path, result, batch=True, fetch=fetch, now=lambda: NOW, sleep=lambda _: None)
    manifest = instance.collect(raw, approval)
    assert manifest["requests"] == len(calls) == 1 and manifest["received_snapshots"] == 2
    assert manifest["licensed"] is False and manifest["status"] == "complete_capture_not_acceptance"
    records = [json.loads(line) for line in (instance.run / "index.jsonl").read_text(encoding="utf-8").splitlines()]
    candidates = [record for record in records if record["kind"] == "batch_stock"]
    assert len(candidates) == 2 and candidates[0]["raw_sha256"] == candidates[1]["raw_sha256"]
    assert len(list(instance.run.glob("*.raw"))) == 1
