from contextlib import contextmanager
from datetime import datetime, date, timedelta
import json
from pathlib import Path
import sys
import types

import pytest


TOOLS = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(TOOLS))
import archive_risk_daily as daily


def setup_archive(tmp_path, kind="baostock", **kwargs):
    raw = (TOOLS.parent / "docs/FORMAL_RISK_FORWARD_ACCEPTANCE_20261004.json").read_bytes()
    protocol = json.loads(raw)
    protocol["status"] = "frozen_user_confirmed"
    raw = json.dumps(protocol).encode()
    hour = 18 if kind == "baostock" else 16
    now = lambda: datetime(2026, 10, 8, hour, 10, tzinfo=daily.CHINA)
    approval = {"approved": True, "protocol_sha256": daily.digest(raw), "confirmed_at": "2026-10-04T00:00:00+08:00"}
    archive = daily.DailyArchive(tmp_path, protocol, kind, now=now, **kwargs)
    return archive, raw, approval


def universe(tmp_path, raw, rows=None):
    run = tmp_path / "archives/2026-10-08/run-one"
    run.mkdir(parents=True)
    (run / "protocol.json").write_bytes(raw)
    (run / "universe.json").write_text(json.dumps(rows or [["000001", 0], ["688001", 1]]))


class Cursor:
    def __init__(self, rows):
        self.rows = iter(rows)
        self.error_code = "0"
        self.fields = list(rows[0]) if rows else []
    def next(self):
        self.row = next(self.rows, None)
        return self.row is not None
    def get_row_data(self):
        return list(self.row.values())


def fake_baostock(rows=None):
    target = date(2026, 10, 8)
    calendar = [{"calendar_date": (target - timedelta(days=offset)).isoformat(), "is_trading_day": "1"}
                for offset in range(46)]
    return types.SimpleNamespace(query_stock_basic=lambda: Cursor([{"code": "sz.000001", "status": "1"}]),
        query_trade_dates=lambda **kwargs: Cursor(calendar),
        query_history_k_data_plus=lambda **kwargs: Cursor(rows if rows is not None else [
            {"code": "sz.000001", "date": "2026-10-08", "volume": "", "tradestatus": "0"}]))


@contextmanager
def fake_guard(**kwargs):
    yield types.SimpleNamespace(login=lambda client: types.SimpleNamespace(error_code="0"),
                                logout=lambda client: None, run_calls=0,
                                snapshot=lambda: {"calls_today": 68, "run_calls": 0})


def test_capture_supported_universe_and_preserve_empty_volume(tmp_path):
    archive, raw, approval = setup_archive(tmp_path, session=fake_guard)
    universe(tmp_path, raw)
    result = archive.collect(raw, approval, fake_baostock())
    assert result["status"] == "complete_capture_not_acceptance" and result["licensed"] is False
    assert result["supported_count"] == 1 and result["missing_codes"] == []
    saved = json.loads(next(archive.run.glob("*-000001.table.json")).read_bytes())
    assert saved[0]["volume"] == ""
    assert result["shared_calls_before"] == 68


@pytest.mark.parametrize("rows", [[], [{"date": "2026-09-30", "code": "sz.000001"}],
                                [{"date": "2026-10-08", "code": "sz.000002"}]])
def test_missing_or_old_daily_is_partial_not_no_data(tmp_path, rows):
    archive, raw, approval = setup_archive(tmp_path, session=fake_guard)
    universe(tmp_path, raw)
    result = archive.collect(raw, approval, fake_baostock(rows))
    assert result["status"] == "partial" and result["missing_codes"] == ["000001"]
    assert result["stop_reason"] == "empty_stale_or_mismatched_daily"
    with pytest.raises(FileExistsError):
        archive.collect(raw, approval, fake_baostock())


def test_missing_universe_waits_bounded_then_does_not_login(tmp_path):
    clock = {"seconds": 0}
    def sleep(seconds):
        clock["seconds"] += seconds
    def forbidden_session(**kwargs):
        pytest.fail("login before complete universe")
    archive, raw, approval = setup_archive(tmp_path, monotonic=lambda: clock["seconds"],
                                         sleep=sleep, session=forbidden_session)
    result = archive.collect(raw, approval, fake_baostock())
    assert result["stop_reason"] == "complete_today_universe_unavailable"
    assert clock["seconds"] == 300


@pytest.mark.parametrize("reason", ["lock_wait_timeout", "shared_daily_budget_insufficient"])
def test_zero_request_daily_failure_stays_partial(tmp_path, reason):
    @contextmanager
    def stopped_guard(**kwargs):
        raise daily.ArchiveStop(reason)
        yield
    archive, raw, approval = setup_archive(tmp_path, session=stopped_guard)
    universe(tmp_path, raw)
    result = archive.collect(raw, approval, fake_baostock())
    assert result["status"] == "partial" and result["stop_reason"] == reason


@pytest.mark.parametrize("change", ["draft", "bad_hash", "stop", "before_open"])
def test_gate_blocks_before_creating_day_directory(tmp_path, change):
    archive, raw, approval = setup_archive(tmp_path)
    if change == "draft":
        archive.protocol["status"] = "draft"
    elif change == "bad_hash":
        approval["protocol_sha256"] = "bad"
    elif change == "stop":
        (tmp_path / "STOP").touch()
    else:
        archive.now = lambda: datetime(2026, 10, 8, 17, 30, tzinfo=daily.CHINA)
    with pytest.raises(daily.ArchiveStop):
        archive.collect(raw, approval)
    assert not (tmp_path / "baostock").exists()


def test_queue_past_latest_start_never_logs_in(tmp_path):
    archive, raw, approval = setup_archive(tmp_path)
    universe(tmp_path, raw)
    @contextmanager
    def delayed_guard(**kwargs):
        archive.now = lambda: datetime(2026, 10, 8, 20, 0, 1, tzinfo=daily.CHINA)
        yield types.SimpleNamespace(snapshot=lambda: {"calls_today": 0},
                                    login=lambda client: pytest.fail("too late"))
    archive.session = delayed_guard
    result = archive.collect(raw, approval, fake_baostock())
    assert result["status"] == "partial" and "latest_start_passed" in result["stop_reason"]


@pytest.mark.parametrize("row,reason", [
    ({"停牌时间": "2026-10-09"}, "future_start"),
    ({"预计复牌时间": "2026-10-08"}, "resumed_by_target_day"),
    ({"停牌期限": "盘中临时停牌"}, "intraday_or_temporary"),
    ({"停牌截止时间": "2026-10-07"}, "past_end"),
])
def test_suspension_uses_record_dates_not_query_date(row, reason):
    record = {"代码": "000001", "停牌时间": "2026-10-01", "停牌截止时间": "2026-10-10", "停牌期限": "连续停牌"}
    result = daily.suspension_candidate({**record, **row}, "2026-10-08")
    assert result["suspended"] is None and result["reason"] == reason


def test_unconfirmed_suspension_semantics_cannot_generate_positive():
    row = {"代码": "000001", "停牌时间": "2026-10-01", "停牌截止时间": "2026-10-10", "停牌期限": "连续停牌"}
    assert daily.suspension_candidate(row, "2026-10-08")["suspended"] is None
    assert daily.suspension_candidate(row, "2026-10-08", semantics_confirmed=True)["suspended"] is True


def test_calendar_rejects_missing_days():
    with pytest.raises(daily.ArchiveStop, match="calendar"):
        daily.verify_calendar([], date(2026, 9, 1), date(2026, 10, 8))


def test_new_service_writes_shared_owner_and_queue_timeout():
    text = (TOOLS / "operations/stock-watch-baostock-risk-archive.service").read_text()
    assert "ReadWritePaths=/home/pi/apps/stock-watch-risk-archive-20261008 /home/pi/apps/stock-fund-fetch-20260929" in text
    assert "TimeoutStartSec=14900" in text


def test_suspension_archive_keeps_raw_hash_and_excludes_future_records(tmp_path):
    archive, raw, approval = setup_archive(tmp_path, "suspensions")
    payload = {"success": True, "result": {"pages": 1, "data": [{"SECURITY_CODE": "000001"}]}}
    response = types.SimpleNamespace(content=json.dumps(payload).encode(), status_code=200,
        json=lambda: payload, raise_for_status=lambda: None)
    requests = types.SimpleNamespace(get=lambda url, **kwargs: response)
    original = requests.get
    rows = [{"代码": "000001", "停牌时间": "2026-10-09", "停牌期限": "全天"}]
    def fetch_table(**kwargs):
        for _ in range(2):
            requests.get("https://datacenter-web.eastmoney.com/api/data/v1/get", params={"pageNumber": 1})
        return types.SimpleNamespace(to_json=lambda **kwargs: json.dumps(rows))
    client = types.SimpleNamespace(__version__="test", stock_tfp_em=fetch_table)
    result = archive.collect(raw, approval, client, requests)
    assert result["status"] == "complete_capture_not_acceptance" and result["independent_positives"] == 0
    assert requests.get is original
    assert len(list(archive.run.glob("*.raw"))) == 2
    index = [json.loads(line) for line in (archive.run / "index.jsonl").read_text().splitlines()]
    assert index[0]["raw_sha256"] == daily.digest(response.content)
    candidate = json.loads(next(archive.run.glob("*-candidates.table.json")).read_bytes())[0]
    assert candidate["reason"] == "future_start"


def test_suspension_network_failure_not_retried(tmp_path):
    archive, raw, approval = setup_archive(tmp_path, "suspensions")
    calls = []
    def fail(url, **kwargs):
        calls.append(kwargs["timeout"])
        raise TimeoutError("network")
    requests = types.SimpleNamespace(get=fail)
    def fetch_table(**kwargs):
        requests.get("https://datacenter-web.eastmoney.com/api/data/v1/get")
    result = archive.collect(raw, approval, types.SimpleNamespace(stock_tfp_em=fetch_table), requests)
    assert result["status"] == "partial" and result["stop_reason"] == "TimeoutError"
    assert calls == [15] and requests.get is fail


def test_record_check_after_acquiring_shared_lock_prevents_duplicate(monkeypatch, tmp_path):
    import capture_research_risk as legacy
    today = legacy.datetime.now(legacy.ZoneInfo("Asia/Shanghai")).date().isoformat()
    monkeypatch.setattr(legacy, "_read_pool", lambda *args: (("run", today, "batch"), [("000001", 1)]))
    monkeypatch.setattr(legacy, "_published_batch", lambda *args: None)
    def queued_capture(*args, **kwargs):
        kwargs["on_first_send"]()
        with pytest.raises(RuntimeError, match="already sent"):
            kwargs["before_request"]()
        raise RuntimeError("another queued caller already sent")
    monkeypatch.setattr(legacy, "capture", queued_capture)
    with pytest.raises(RuntimeError, match="another queued caller"):
        legacy.scheduled_capture(tmp_path / "db", tmp_path, "astrbot", "/evidence")
    assert legacy.scheduled_capture(tmp_path / "db", tmp_path, "astrbot", "/evidence")["status"] == "partial_no_automatic_retry"
