"""Read-only Web fixes from the 2026-10-07 audit, checked against the current plugin schema."""
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import sqlite3

import pytest

from webapp.data import Dashboard
from webapp.demo import create_demo
from webapp.deploy.snapshot import publish, record_check


HOLIDAY = datetime(2026, 10, 7, 2, 30, tzinfo=timezone.utc)
ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def plugin_template(tmp_path_factory):
    from test_v0139_intraday_m2 import _imports
    _, _, Store = _imports()
    database = tmp_path_factory.mktemp("plugin") / "template.sqlite3"
    Store(database)
    db = sqlite3.connect(database)
    try:
        db.execute("INSERT INTO datasets(dataset_id,dataset_key,provider,created_at) VALUES('ds','tushare_daily','tushare','2026-09-30T08:00:00')")
        db.execute("INSERT INTO batches(batch_id,dataset_id,requested_date,actual_trade_date,status,quality,source,generation,"
                   "row_count,manifest_hash,created_at,published_at) VALUES('batch-21','ds','2026-09-30','2026-09-30','published',"
                   "'good','tushare',21,4,'digest','2026-09-30T08:00:00','2026-09-30T08:30:00+00:00')")
        for day, close in (("2026-09-29", 10.0), ("2026-09-30", 10.5)):
            partition = "p-" + day
            db.execute("INSERT INTO day_partitions(partition_id,dataset_id,trade_date,content_hash,source,row_count,created_at) "
                       "VALUES(?,?,?,?,?,?,?)", (partition, "ds", day, "h" + day, "tushare", 2, "2026-09-30T08:00:00"))
            for code in ("600857", "600000"):
                db.execute("INSERT INTO partition_bars(partition_id,trade_date,code,ts_code,name,open,high,low,close,pre_close,"
                           "pct_change,volume,amount,source) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                           (partition, day, code, code + ".SH", "", close, close + 0.2, close - 0.2, close, close - 0.5,
                            round(0.5 / (close - 0.5) * 100, 4), 1000, 10000, "tushare"))
            db.execute("INSERT INTO batch_days(batch_id,trade_date,partition_id,row_count) VALUES('batch-21',?,?,2)", (day, partition))
        db.execute("INSERT INTO active_generations(dataset_id,active_batch_id,generation,updated_at) VALUES('ds','batch-21',21,'2026-09-30T08:30:00')")
        db.executemany("INSERT INTO stock_symbols(code,name,normalized_name,source,updated_at) VALUES(?,?,?,?,?)", [
            ("600857", "宁波中百", "宁波中百", "stock_basic", "2026-09-30T08:00:00"),
            ("600999", "只有名称", "只有名称", "stock_basic", "2026-09-30T08:00:00")])
        db.execute("INSERT INTO daily_bars(code,trade_date,open,high,low,close,volume,amount,source,fetched_at,price_basis) "
                   "VALUES('600857','2026-08-28',9,9.2,8.8,9,1,1,'eastmoney','2026-08-28T08:00:00','unadjusted')")
        db.execute("INSERT INTO daily_snapshot_meta(trade_date,source,quality,complete,requested_date,fetched_at) "
                   "VALUES('2026-09-30','tushare','good',1,'2026-09-30','2026-09-30T09:30:00')")
        db.commit()
        db.execute("PRAGMA journal_mode=DELETE")
    finally:
        db.close()
    return database


@pytest.fixture
def plugin_db(plugin_template, tmp_path):
    database = tmp_path / "plugin.sqlite3"
    database.write_bytes(plugin_template.read_bytes())
    return database


def execute(database, sql, params=()):
    db = sqlite3.connect(database)
    try:
        db.execute(sql, params)
        db.commit()
    finally:
        db.close()


def test_headline_date_comes_from_active_raw_not_legacy_daily_bars(plugin_db):
    meta = Dashboard(plugin_db, now=lambda: HOLIDAY).query("overview")["meta"]
    assert meta["primary_source"]["dataset"] == "raw_active_generation"
    assert meta["primary_source"]["date"] == "2026-09-30"
    assert meta["primary_source"]["generation"] == 21
    legacy = [item for item in meta["sources"] if item["dataset"] == "daily_bars"]
    assert legacy and legacy[0]["date"] == "2026-08-28"


def test_stock_detail_reads_active_raw_and_separates_missing_data(plugin_db):
    dashboard = Dashboard(plugin_db, now=lambda: HOLIDAY)
    before = hashlib.sha256(plugin_db.read_bytes()).digest()
    data = dashboard.query("stocks/600857")["data"]
    assert data["name"] == "宁波中百"
    assert data["bar_source"]["kind"] == "active_raw" and data["bar_source"]["batch_id"] == "batch-21"
    assert [bar["date"] for bar in data["bars"]] == ["2026-09-29", "2026-09-30"]
    assert data["last_close"] == 10.5 and data["bar_date"] == "2026-09-30"
    assert data["bars"][-1]["data_evidence"]["quality"] == "active_published_raw"
    assert data["formal_status"] == "no_formal_candidate" and data["recommendation_status"] == "no_recommendation"
    symbol_only = dashboard.query("stocks/600999")["data"]
    assert symbol_only["bars"] == [] and symbol_only["bar_source"]["kind"] == "none"
    assert symbol_only["data_quality"]["missing_reason"] == "daily_bars_not_collected"
    assert dashboard.query("stocks/688999")["meta"]["reason"] == "stock_unavailable"
    assert hashlib.sha256(plugin_db.read_bytes()).digest() == before
    execute(plugin_db, "DELETE FROM active_generations")
    legacy = Dashboard(plugin_db, now=lambda: HOLIDAY).query("stocks/600857")["data"]
    assert legacy["bar_source"]["kind"] == "legacy_daily_bars"
    assert [bar["date"] for bar in legacy["bars"]] == ["2026-08-28"]


def test_latest_close_prefers_active_raw(plugin_db):
    dashboard = Dashboard(plugin_db, now=lambda: HOLIDAY)
    with dashboard.snapshot() as snapshot:
        raw, legacy_only = snapshot.attach_latest_closes([{"code": "600857"}, {"code": "000001"}])
    assert raw["close"] == 10.5 and raw["close_date"] == "2026-09-30" and raw["close_source"] == "active_raw"
    assert raw["prev_close"] == 10.0 and raw["pct_change"] == pytest.approx(5.0)
    assert legacy_only["close"] is None and legacy_only["close_source"] is None


def test_search_by_code_or_name_without_formal_candidates(plugin_db):
    dashboard = Dashboard(plugin_db, now=lambda: HOLIDAY)
    by_name = dashboard.query("search", {"q": "中百"})["data"]["items"]
    assert by_name[0] == {"code": "600857", "name": "宁波中百", "source": "stock_symbols"}
    by_code = [row["code"] for row in dashboard.query("search", {"q": "6000"})["data"]["items"]]
    assert by_code == ["600000"]
    assert dashboard.query("search", {"q": "%_"})["meta"]["reason"] == "search_query_invalid"


def test_provider_telemetry_and_rate_limit_deadlines_stay_separate(plugin_db):
    now = HOLIDAY.timestamp()
    with sqlite3.connect(plugin_db) as db:
        db.execute("INSERT INTO provider_health(provider,last_success_at,last_error_at,success_count,error_count,last_quality) "
                   "VALUES('tushare','2026-09-30T08:00:00',NULL,10,0,'good')")
        for name, blocked in (("stock_basic", now - 86400), ("daily", now + 3600), ("trade_cal", 0), ("index_daily", "garbage")):
            db.execute("INSERT INTO provider_api_state(api_name,blocked_until,updated_at) VALUES(?,?,'2026-10-07T01:00:00')",
                       (name, blocked))
    providers = {row["name"]: row for row in Dashboard(plugin_db, now=lambda: HOLIDAY).query("health")["data"]["providers"]}
    assert providers["tushare"]["telemetry"] == "provider_health"
    assert providers["tushare"]["success_at"].startswith("2026-09-30T08:00:00")
    expired, future = providers["stock_basic"], providers["daily"]
    assert expired["telemetry"] == "api_rate_limit_state" and expired["success_at"] is None
    assert expired["blocked_until"] == datetime.fromtimestamp(now - 86400, timezone.utc).isoformat()
    assert expired["blocked_active"] is False
    assert future["blocked_active"] is True
    assert providers["trade_cal"]["blocked_until"] is None and providers["trade_cal"]["blocked_active"] is False
    assert providers["index_daily"]["blocked_until"] is None and providers["index_daily"]["blocked_active"] is False
    assert expired["state_updated_at"].startswith("2026-10-07T01:00:00")


@pytest.mark.parametrize(("calendar", "now", "expected"), [
    (("2026-10-07", 0, "closed"), HOLIDAY, ("closed", "closed_day")),
    (("2026-10-08", 1, "open"), datetime(2026, 10, 8, 2, 0, tzinfo=timezone.utc), ("open", "trading")),
    (("2026-10-08", 1, "open"), datetime(2026, 10, 8, 4, 0, tzinfo=timezone.utc), ("open", "lunch_break")),
    (("2026-10-08", 1, "open"), datetime(2026, 10, 8, 12, 0, tzinfo=timezone.utc), ("open", "after_close")),
    (None, datetime(2026, 10, 9, 2, 0, tzinfo=timezone.utc), ("unknown", "unknown")),
    (("2026-10-09", 1, "closed"), datetime(2026, 10, 9, 2, 0, tzinfo=timezone.utc), ("unknown", "unknown")),
])
def test_session_uses_calendar_not_missing_quotes(plugin_db, calendar, now, expected):
    if calendar:
        execute(plugin_db, "INSERT INTO trading_calendar(trade_date,is_open,status,source,fetched_at) VALUES(?,?,?,'tushare','2026-10-01T00:00:00')",
                calendar)
    session = Dashboard(plugin_db, now=lambda: now).query("overview")["data"]["session"]
    assert (session["calendar"], session["phase"]) == expected


def test_coverage_breakdown_keeps_separate_denominators(plugin_db):
    diagnostics = {"input": 5561, "risk_tuple_complete": 0, "tradable": 0, "risk_confirmed": 0,
                   "indicator_targets": 0, "enriched": 0, "indicator_coverage": 0.0}
    with sqlite3.connect(plugin_db) as db:
        db.execute("INSERT INTO screen_runs(run_id,job_name,requested_date,actual_trade_date,started_at,status,quality,coverage,"
                   "candidate_count,diagnostics) VALUES('run-930','automatic_close','2026-10-02','2026-09-30','2026-10-02T07:00:00',"
                   "'completed','good',0,0,?)", (json.dumps(diagnostics),))
        db.execute("INSERT INTO market_contexts(as_of,payload,source,quality,fetched_at) VALUES('2026-09-30',?,'tushare','good','2026-09-30T09:00:00')",
                   (json.dumps({"advancing": 3000, "declining": 2400, "flat": 161}),))
    data = Dashboard(plugin_db, now=lambda: HOLIDAY).query("overview")["data"]
    breakdown = data["coverage_breakdown"]
    assert breakdown["market"]["count"] == 5561 and breakdown["market"]["complete"] is True
    assert breakdown["market"]["raw_batch_id"] == "batch-21"
    assert breakdown["risk"] == {"known": 0, "total": 5561}
    assert breakdown["indicator"]["status"] == "not_applicable" and breakdown["indicator"]["coverage"] is None
    assert breakdown["formal"] == {"risk_confirmed": 0, "candidates": 0, "visible": 0}
    assert data["coverage"] == 0  # legacy field kept for older clients


def test_research_freeze_parameters_are_exposed(plugin_db):
    parameters = {"deep_limit": 300, "primary_limit": 7, "radar_limit": 20, "price_min": 2.0, "price_max": 80.0,
                  "independent_of": ["price_min", "price_max", "deep_screen_limit"]}
    execute(plugin_db, "INSERT INTO research_pool_runs(run_id,trade_date,batch_id,source,basis,published_at,frozen_at,diagnostics,status) "
                       "VALUES('research:batch-21','2026-09-30','batch-21','tushare','unadjusted','2026-09-30T08:30:00+00:00',"
                       "'2026-09-30T10:00:00+00:00',?,'research_only')",
            (json.dumps({"selection_policy": "technical-research-v1", "parameters": parameters}),))
    research = Dashboard(plugin_db, now=lambda: HOLIDAY).query("candidates")["data"]["research"]
    assert research["selection_policy"] == "technical-research-v1"
    assert research["parameters"] == {"deep_limit": 300, "primary_limit": 7, "radar_limit": 20, "price_min": 2.0, "price_max": 80.0}
    assert research["independent_of"] == ["price_min", "price_max", "deep_screen_limit"]


def test_snapshot_check_distinguishes_unchanged_from_stopped_timer(tmp_path):
    source, target = tmp_path / "source.sqlite3", tmp_path / "snapshot.sqlite3"
    create_demo(source, tmp_path / "settings.json")
    published = publish(source, target)
    record_check(target, published)
    later = datetime.fromisoformat(published["captured_at"]) + timedelta(hours=30)
    unchanged = publish(source, target)
    record_check(target, unchanged, now=later - timedelta(minutes=5))
    snapshot = Dashboard(target, now=lambda: later).query("settings")["meta"]["snapshot"]
    assert snapshot["status"] == "stale"
    assert snapshot["check"]["status"] == "recent" and snapshot["check"]["result"] == "unchanged"
    assert snapshot["check"]["last_published_at"] == published["captured_at"]
    stopped = Dashboard(target, now=lambda: later + timedelta(hours=3)).query("settings")["meta"]["snapshot"]["check"]
    assert stopped["status"] == "stale"
    record_check(target, {"status": "failed"}, "TimeoutError", now=later)
    failed = Dashboard(target, now=lambda: later).query("settings")["meta"]["snapshot"]["check"]
    assert failed["status"] == "failed" and failed["failure_category"] == "TimeoutError"
    (tmp_path / "snapshot_status.json").unlink()
    assert Dashboard(target, now=lambda: later).query("settings")["meta"]["snapshot"]["check"]["reason"] == "snapshot_status_missing"
    assert not list(tmp_path.glob(".status-*"))


def test_research_catalog_binds_frozen_files_without_formal_promotion(plugin_db, tmp_path):
    dashboard = Dashboard(plugin_db, now=lambda: HOLIDAY)
    before = hashlib.sha256(plugin_db.read_bytes()).digest()
    payload = dashboard.query("research_catalog")
    assert payload["meta"]["status"] == "available"
    data = payload["data"]
    assert data["formal_tables_written"] is False
    assert data["stage_vocabulary"] == ["not_passed", "exploration", "forward_pending", "passed"]
    ultrashort, sector = data["entries"]
    assert ultrashort["id"] == "ULTRASHORT_REVERSAL_V1" and ultrashort["stages"] == ["not_passed", "forward_pending"]
    assert sector["id"] == "LLM_SECTOR_FIRST_EXP_V0" and sector["stages"] == ["exploration"]
    frozen = dashboard.catalog_dir / "ULTRASHORT_REVERSAL_V1_FROZEN.json"
    assert ultrashort["file_sha256"] == hashlib.sha256(frozen.read_bytes()).hexdigest()
    assert [row["code"] for row in ultrashort["items"]] == ["600857", "601086", "603221", "600487", "603823"]
    assert ultrashort["items"][0]["close"] == 17.22 and ultrashort["input_as_of"] == "2026-09-30"
    assert [row["code"] for row in sector["items"]] == ["300110", "600664", "301201", "600246", "301093"]
    assert sector["items"][0]["close"] == 3.17 and sector["items"][0]["sector"] == "C27医药制造业"
    assert all(entry["eligibility"] == "research_only" and entry["plugin_integrated"] is False for entry in data["entries"])
    assert hashlib.sha256(plugin_db.read_bytes()).digest() == before
    dashboard.catalog_dir = tmp_path
    (tmp_path / "LLM_SECTOR_FIRST_EXP_V0_FROZEN.json").write_text("{not json", encoding="utf-8")
    degraded = dashboard.query("research_catalog")
    assert degraded["meta"]["status"] == "partial" and degraded["data"]["entries"] == []
    assert {item["reason"] for item in degraded["data"]["unavailable"]} == {"frozen_file_missing", "frozen_file_unreadable"}
    package = (ROOT / "webapp" / "deploy" / "package.py").read_text(encoding="utf-8")
    assert "docs/research/ULTRASHORT_REVERSAL_V1_FROZEN.json" in package and "docs/research/LLM_SECTOR_FIRST_EXP_V0_FROZEN.json" in package


def test_version_reports_capabilities_and_build(plugin_db, tmp_path):
    dashboard = Dashboard(plugin_db, now=lambda: HOLIDAY)
    dashboard.build_info_path = tmp_path / "absent.json"
    unknown = dashboard.query("version")
    assert unknown["meta"]["status"] == "partial" and unknown["data"]["build"]["status"] == "unknown"
    assert {"research_pools", "stock_active_raw_bars", "provider_state_v2"}.issubset(unknown["data"]["capabilities"])
    assert unknown["data"]["database_schema_version"] == 24
    dashboard.build_info_path.write_text(json.dumps({"release": "web-20261007", "revision": "abc1234"}), encoding="utf-8")
    recorded = dashboard.query("version")["data"]["build"]
    assert recorded["status"] == "recorded" and recorded["revision"] == "abc1234"
