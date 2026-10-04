from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import types
from contextlib import contextmanager
from pathlib import Path

import pytest


SCRIPT = Path(__file__).with_name("capture_research_risk.py")
spec = importlib.util.spec_from_file_location("capture_research_risk", SCRIPT)
capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capture)


class BaoResult:
    error_code = "0"
    fields = ["date", "code", "close", "tradestatus", "isST"]

    def __init__(self, status="1"):
        self.status = status
        self.read = False

    def next(self):
        if self.read:
            return False
        self.read = True
        return True

    def get_row_data(self):
        return ["2026-09-23", "sh.600000", "8.98", self.status, "0"]


class Frame:
    def __init__(self, codes, prices=None):
        self.codes = codes
        self.prices = prices if prices is not None else [8.98] * len(codes)

    def __getitem__(self, key):
        assert key in ("代码", "最新价")
        return types.SimpleNamespace(tolist=lambda: self.codes if key == "代码" else self.prices)

    def to_json(self, **kwargs):
        return json.dumps(self.codes)


def setup_sources(monkeypatch, *, status="1", up=None, down=None, up_price=8.98):
    @contextmanager
    def fake_guard(max_calls, target_date="", on_first_send=None):
        yield types.SimpleNamespace(login=lambda client: client.login(), logout=lambda client: client.logout())
    monkeypatch.setattr(capture, "_guard_session", fake_guard)
    monkeypatch.setattr(capture, "_read_pool", lambda *args: (
        ("run-1", "2026-09-23", "batch-1"), [("600000", 8.98)]))
    monkeypatch.setitem(sys.modules, "baostock", types.SimpleNamespace(
        login=lambda: types.SimpleNamespace(error_code="0"),
        logout=lambda: None,
        query_history_k_data_plus=lambda *args, **kwargs: BaoResult(status)))
    monkeypatch.setitem(sys.modules, "akshare", types.SimpleNamespace(
        stock_zt_pool_em=lambda **kwargs: Frame(up or [], [up_price] * len(up or [])),
        stock_zt_pool_dtgc_em=lambda **kwargs: Frame(down or [])))


def test_capture_preserves_source_times_and_positive_only(monkeypatch, tmp_path):
    setup_sources(monkeypatch, up=["600000"])
    result = capture.capture(tmp_path / "db", tmp_path, expected_date="2026-09-23")
    bundle = json.loads((tmp_path / "2026-09-23.json").read_text())
    assert result["up_members"] == 1
    assert set(bundle["source_captured_at"]) == {"trading", "limit_up", "limit_down"}
    assert bundle["rows"]["600000"]["limit_down"] is None


def test_capture_rejects_conflict_without_publication(monkeypatch, tmp_path):
    setup_sources(monkeypatch, status="0", up=["600000"])
    with pytest.raises(RuntimeError, match="provider conflict"):
        capture.capture(tmp_path / "db", tmp_path)
    assert not (tmp_path / "2026-09-23.json").exists()


def test_capture_rejects_bad_pool_or_missing_daily_row(monkeypatch, tmp_path):
    setup_sources(monkeypatch, up=["600000.0"])
    with pytest.raises(RuntimeError, match="AKShare limit_up"):
        capture.capture(tmp_path / "db", tmp_path)
    setup_sources(monkeypatch, status="x")
    with pytest.raises(RuntimeError, match="BaoStock missing"):
        capture.capture(tmp_path / "db", tmp_path)
    assert not (tmp_path / "2026-09-23.json").exists()


def test_capture_rejects_cross_source_price_conflict(monkeypatch, tmp_path):
    setup_sources(monkeypatch, up=["600000"], up_price=9.20)
    with pytest.raises(RuntimeError, match="AKShare limit_up"):
        capture.capture(tmp_path / "db", tmp_path)
    assert not (tmp_path / "2026-09-23.json").exists()


def test_scheduled_capture_waits_for_today(monkeypatch, tmp_path):
    monkeypatch.setattr(capture, "_read_pool", lambda *args: (
        ("run-1", "2026-09-22", "batch-1"), []))
    assert capture.scheduled_capture(tmp_path / "db", tmp_path, "astrbot", "/evidence")["status"] == "waiting_for_today_freeze"


def test_scheduled_publication_links_only_matching_batch(monkeypatch, tmp_path):
    today = capture.datetime.now(capture.ZoneInfo("Asia/Shanghai")).date().isoformat()
    run = ("run-1", today, "batch-1")
    monkeypatch.setattr(capture, "_read_pool", lambda *args: (run, [("600000", 8.98)]))
    monkeypatch.setattr(capture, "_published_batch", lambda *args: None)
    monkeypatch.setattr(capture, "_container_file_exists", lambda *args: False)
    def fake_capture(db, out, **kwargs):
        (out / (today + ".json")).write_text(json.dumps({
            "trade_date": today, "batch_id": "batch-1", "failed_codes": [],
            "source_captured_at": {"trading": "now"}, "rows": {"600000": {}}}))
    monkeypatch.setattr(capture, "capture", fake_capture)
    calls = []
    real_run = subprocess.run
    def fake_run(args, **kwargs):
        calls.append(args)
        if args[:3] == ["docker", "exec", "astrbot"]:
            source, target = tmp_path / "remote.next", tmp_path / "remote.json"
            source.write_bytes((tmp_path / (today + ".json")).read_bytes())
            real_run([sys.executable, "-c", args[5], str(source), str(target),
                      today, "batch-1"], check=True)
            assert target.exists() and not source.exists()
    monkeypatch.setattr(capture.subprocess, "run", fake_run)
    result = capture.scheduled_capture(tmp_path / "db", tmp_path, "astrbot", "/evidence")
    assert result["status"] == "published"
    assert len(calls) == 2


def test_scheduled_failure_does_not_retry_same_batch(monkeypatch, tmp_path):
    today = capture.datetime.now(capture.ZoneInfo("Asia/Shanghai")).date().isoformat()
    monkeypatch.setattr(capture, "_read_pool", lambda *args: (("run-1", today, "batch-1"), [("600000", 8.98)]))
    monkeypatch.setattr(capture, "_published_batch", lambda *args: None)
    calls = []
    def fail_once(*args, **kwargs):
        calls.append(1)
        kwargs["before_request"]()
        kwargs["on_first_send"]()
        raise RuntimeError("source failed")
    monkeypatch.setattr(capture, "capture", fail_once)
    with pytest.raises(RuntimeError, match="source failed"):
        capture.scheduled_capture(tmp_path / "db", tmp_path, "astrbot", "/evidence")
    assert capture.scheduled_capture(tmp_path / "db", tmp_path, "astrbot", "/evidence")["status"] == "partial_no_automatic_retry"
    assert calls == [1]


@pytest.mark.parametrize("reason", ["lock_wait_timeout", "shared_daily_budget_insufficient", "connect_failed"])
def test_zero_request_stop_keeps_next_hour_available(monkeypatch, tmp_path, reason):
    today = capture.datetime.now(capture.ZoneInfo("Asia/Shanghai")).date().isoformat()
    monkeypatch.setattr(capture, "_read_pool", lambda *args: (("run-1", today, "batch-1"), [("600000", 8.98)]))
    monkeypatch.setattr(capture, "_published_batch", lambda *args: None)
    calls = []
    def stop_before_send(*args, **kwargs):
        calls.append(1)
        raise RuntimeError(reason)
    monkeypatch.setattr(capture, "capture", stop_before_send)
    for _ in range(2):
        with pytest.raises(RuntimeError, match=reason):
            capture.scheduled_capture(tmp_path / "db", tmp_path, "astrbot", "/evidence")
    assert calls == [1, 1]
    assert not list(tmp_path.glob(".attempt-*"))
