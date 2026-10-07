"""Load-time settings snapshot, the unpassable risk-gate stop, and job/acceptance explanations on the Web."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import importlib
import json
from pathlib import Path
import sqlite3
import types

from test_v0138_automatic_close import _automatic_reconciliation_main, _candidate, _imports
from webapp import data as web_data
from webapp.data import Dashboard

Main, ScreenScoreResult, core, StockStore = _imports()
main_module = importlib.import_module("astrbot_stock_watch.main")
policy = importlib.import_module("astrbot_stock_watch.formal_source_policy")
ROOT = Path(__file__).resolve().parents[2]
HOLIDAY = datetime(2026, 10, 7, 2, 30, tzinfo=timezone.utc)
JOB = "automatic_close:2026-09-09"
TOKEN_KEY, HIDDEN = "tushare_token", "fixture-hidden-value"


def _risk_blocked_main(tmp_path):
    """A complete close whose only gate failure is the four-field risk evidence."""
    store = StockStore(tmp_path / "automatic.sqlite3")
    main, _reads = _automatic_reconciliation_main(Main, ScreenScoreResult, core, store)
    with store._connect() as db:
        db.execute("UPDATE job_runs SET automatic_attempts=1 WHERE job_key=?", (JOB,))
    store.set_subscription("qq:group:1", True)
    sent = []

    async def send_message(origin, _chain):
        sent.append(origin)

    main.context = types.SimpleNamespace(send_message=send_message)
    fixture_score = main._score_quotes_result
    watch = _candidate(core, "600000")
    watch.risk_level = "unknown"

    async def score(quotes, limit, as_of="", **kwargs):
        scored = await fixture_score(quotes, limit, as_of, **kwargs)
        diagnostics = {**scored.diagnostics, "risk_tuple_complete": 0, "tradable": 0, "indicator_targets": 0,
                       "indicator_raw_batch": 0, "indicator_coverage": 0.0, "enriched": 0, "risk_confirmed": 0,
                       "raw_generation": 12}
        return ScreenScoreResult.build([], diagnostics, [watch])

    main._score_quotes_result = score
    return main, store, sent


def _execute(database, statements):
    db = sqlite3.connect(database)
    try:
        for sql, params in statements:
            db.execute(sql, params)
        db.commit()
    finally:
        db.close()


def test_unlicensed_fields_follow_the_accepted_scenarios(monkeypatch):
    assert policy.unlicensed_fields() == policy.RISK_FIELDS
    monkeypatch.setattr(policy, "ACCEPTED_NEGATIVE_SCENARIOS",
                        frozenset({("src", "v1", "suspended"), ("src", "v1", "st")}))
    assert policy.unlicensed_fields() == ("limit_up", "limit_down")
    monkeypatch.setattr(policy, "ACCEPTED_NEGATIVE_SCENARIOS", frozenset(("src", "v1", field) for field in policy.RISK_FIELDS))
    assert policy.unlicensed_fields() == ()


def test_gate_failures_split_into_unpassable_risk_and_retryable_data(monkeypatch):
    only_risk = Main._gate_failure_classes(["risk_evidence_missing"])
    assert only_risk["gate_unpassable"] == ["risk_evidence_missing"] and only_risk["gate_retryable"] == []
    assert only_risk["gate_dependency"] == "formal_risk_source_acceptance"
    assert only_risk["gate_unlicensed_risk_fields"] == list(policy.RISK_FIELDS)
    assert only_risk["gate_risk_policy_version"] == policy.POLICY_VERSION
    mixed = Main._gate_failure_classes(["risk_evidence_missing", "market_stats_unconfirmed"])
    assert mixed["gate_retryable"] == ["market_stats_unconfirmed"] and mixed["gate_unpassable"] == ["risk_evidence_missing"]
    assert Main._gate_failure_classes(["indicator_coverage"]) == {"gate_retryable": ["indicator_coverage"], "gate_unpassable": []}
    monkeypatch.setattr(policy, "unlicensed_fields", lambda: ())
    assert Main._gate_failure_classes(["risk_evidence_missing"])["gate_retryable"] == ["risk_evidence_missing"]


def test_unpassable_risk_gate_stops_the_job_on_the_first_attempt(tmp_path):
    main, store, sent = _risk_blocked_main(tmp_path)

    result = asyncio.run(main._run_automatic_close_job("2026-09-09", JOB))

    assert result["state"] == "fail_closed" and result["reasons"] == ["risk_evidence_missing"]
    assert result["terminal_reason"] == "formal_gate_unpassable:risk_evidence_missing"
    job = store.job_run(JOB)
    assert job["status"] == "missed" and job["automatic_attempts"] == 1
    assert job["automatic_terminal_reason"] == "formal_gate_unpassable:risk_evidence_missing"
    assert float(job["automatic_next_retry_at"] or 0) == 0
    assert "risk_evidence_missing" in job["error"]
    with store._connect() as db:
        gate = json.loads(db.execute("SELECT diagnostics_json FROM screen_gate_diagnostics WHERE job_key=?", (JOB,)).fetchone()[0])
    assert gate["gate_unpassable"] == ["risk_evidence_missing"] and gate["gate_retryable"] == []
    assert gate["gate_dependency"] == "formal_risk_source_acceptance" and gate["raw_generation"] == 12
    assert sent == ["qq:group:1"]
    again = store.claim_automatic_close_job(JOB, "2026-09-09")
    assert again["acquired"] is False and again["reason"] == "missed"


def test_risk_gate_keeps_retrying_once_every_field_has_a_licensed_source(tmp_path, monkeypatch):
    monkeypatch.setattr(policy, "unlicensed_fields", lambda: ())
    main, store, sent = _risk_blocked_main(tmp_path)

    result = asyncio.run(main._run_automatic_close_job("2026-09-09", JOB))

    assert result["state"] == "fail_closed" and result["terminal_reason"] is None
    job = store.job_run(JOB)
    assert job["status"] == "failed" and float(job["automatic_next_retry_at"]) > 0
    assert job["automatic_terminal_reason"] == ""
    assert sent == []


def test_terminal_reason_ends_a_failed_job_and_is_ignored_on_completion(tmp_path):
    store = StockStore(tmp_path / "jobs.sqlite3")
    for key in ("automatic_close:2026-10-08", "automatic_close:2026-10-09"):
        assert store.claim_automatic_close_job(key, key.split(":")[1])["acquired"]
    stopped = store.finish_automatic_close_job("automatic_close:2026-10-08", status="failed", error="x",
                                               terminal_reason="formal_gate_unpassable:risk_evidence_missing")
    assert stopped["status"] == "missed" and stopped["automatic_attempts"] == 1
    assert stopped["automatic_terminal_reason"] == "formal_gate_unpassable:risk_evidence_missing"
    done = store.finish_automatic_close_job("automatic_close:2026-10-09", status="completed", terminal_reason="ignored")
    assert done["status"] == "completed" and done["automatic_terminal_reason"] == ""


def test_plugin_snapshot_copies_numbers_and_allowlisted_strings_only(tmp_path):
    hidden = {**dict.fromkeys((TOKEN_KEY, "llm_api_key"), HIDDEN), "push_whitelist": "qq:group:123456",
              "llm_base_url": "https://internal.example/v1", "intraday_artifact_path": "/data/private.json"}
    config = {**dict(main_module._SCHEMA_DEFAULTS), **hidden, "max_concurrency": 7, "confirmation_enabled": True,
              "llm_model": "deepseek-chat", "price_max": "80"}
    stub = types.SimpleNamespace(config=config, deprecated_settings=["confirmation_enabled"])
    stub.public_settings_snapshot = lambda: Main.public_settings_snapshot(stub)

    snapshot = Main.public_settings_snapshot(stub)

    text = json.dumps(snapshot, ensure_ascii=False)
    for value in (HIDDEN, "123456", "internal.example", "private.json"):
        assert value not in text
    assert snapshot["values"]["max_concurrency"] == 7 and snapshot["values"]["confirmation_enabled"] is True
    assert snapshot["values"]["llm_model"] == "deepseek-chat"
    assert snapshot["configured"]["tushare_token"] == "custom" and snapshot["configured"]["news_rss_url"] == "empty"
    assert snapshot["configured"]["official_evidence_trusted_hosts"] == "default"
    assert snapshot["configured"]["price_max"] == "invalid_type" and "price_max" not in snapshot["values"]
    assert set(snapshot["values"]) | set(snapshot["configured"]) == set(main_module._SCHEMA_DEFAULTS)
    assert snapshot["deprecated_settings"] == ["confirmation_enabled"] and snapshot["plugin_version"] == "0.13.3"
    assert snapshot["code_sha256"] == hashlib.sha256(Path(main_module.__file__).read_bytes()).hexdigest()

    target = tmp_path / "plugin_data" / "public_settings.json"
    Main._write_public_settings(stub, target)
    assert json.loads(target.read_text(encoding="utf-8"))["values"]["max_concurrency"] == 7
    assert [path.name for path in target.parent.iterdir()] == ["public_settings.json"]

    stub.config = {**config, "max_concurrency": 10 ** 400}
    Main._write_public_settings(stub, target)
    assert list(target.parent.iterdir()) == []


def test_plugin_and_web_share_the_public_string_allowlist():
    assert main_module.PUBLIC_STRING_SETTINGS == web_data.PUBLIC_STRING_SETTINGS


def test_settings_page_reads_the_plugin_snapshot_and_never_shows_free_strings(tmp_path):
    database = tmp_path / "plugin.sqlite3"
    StockStore(database)
    artifact = tmp_path / "plugin_data" / "intraday_quotes.json"
    artifact.parent.mkdir()
    (artifact.parent / "public_settings.json").write_text(json.dumps({
        "schema_version": 1, "written_at": "2026-10-07T04:00:00+00:00", "plugin_version": "0.13.3",
        "code_sha256": "a" * 64, "schema_sha256": "b" * 64, "deprecated_settings": ["confirmation_enabled"],
        "values": {"max_concurrency": 7, "price_min": 2.0, "confirmation_enabled": True, "llm_model": "deepseek-chat",
                   TOKEN_KEY: HIDDEN, "news_rss_url": "https://private.example/rss"},
        "configured": {TOKEN_KEY: "custom", "llm_api_key": "empty", "news_rss_url": "bogus"},
    }), encoding="utf-8")
    build = tmp_path / "build_info.json"
    build.write_text(json.dumps({"release": "r1", "revision": "abc1234", "plugin_main_sha256": "a" * 64}), encoding="utf-8")
    dashboard = Dashboard(database, artifact_path=artifact, now=lambda: HOLIDAY)
    dashboard.build_info_path = build

    data = dashboard.query("settings")["data"]

    items = {item["key"]: item for item in data["items"]}
    text = json.dumps(data, ensure_ascii=False)
    assert HIDDEN not in text and "private.example" not in text
    assert len(items) == len(json.loads((ROOT / "_conf_schema.json").read_text(encoding="utf-8")))
    assert {key: items["max_concurrency"][key] for key in ("effective", "state", "differs", "source")} == {
        "effective": 7, "state": "shown", "differs": True, "source": "plugin_snapshot"}
    assert items["price_min"]["effective"] == 2.0 and items["price_min"]["differs"] is False
    assert items["tushare_token"]["effective"] is None and items["tushare_token"]["state"] == "custom"
    assert items["news_rss_url"]["state"] == "unknown" and items["news_rss_url"]["source"] == "effective_unknown"
    assert items["llm_api_key"]["state"] == "empty" and items["llm_api_key"]["differs"] is False
    assert items["llm_model"]["effective"] == "deepseek-chat"
    assert all(item["group"] == "common" for item in data["items"][:len(web_data.COMMON_SETTINGS)])
    snapshot = data["snapshot"]
    assert snapshot["status"] == "plugin_snapshot" and snapshot["matches_web_build"] is True
    assert snapshot["written_at"] == "2026-10-07T04:00:00+00:00" and snapshot["deprecated_settings"] == ["confirmation_enabled"]


def test_settings_stay_unknown_without_a_readable_snapshot(tmp_path):
    database = tmp_path / "plugin.sqlite3"
    StockStore(database)
    missing = Dashboard(database, artifact_path=tmp_path / "nowhere" / "intraday_quotes.json", now=lambda: HOLIDAY).query("settings")["data"]
    assert missing["snapshot"]["status"] == "missing" and missing["snapshot"]["matches_web_build"] is None
    assert all(item["effective"] is None and item["source"] == "effective_unknown" for item in missing["items"])
    assert Dashboard(database, now=lambda: HOLIDAY).query("settings")["data"]["snapshot"]["status"] == "not_configured"
    broken = tmp_path / "broken.json"
    broken.write_text("{not json", encoding="utf-8")
    assert Dashboard(database, settings=broken, now=lambda: HOLIDAY).query("settings")["data"]["snapshot"]["status"] == "unreadable"


def test_health_jobs_explain_attempts_stop_reason_and_gate_funnel(tmp_path):
    database = tmp_path / "plugin.sqlite3"
    store = StockStore(database)
    insert = ("INSERT INTO job_runs(job_key,job_name,trade_date,started_at,finished_at,status,error,automatic_attempts,"
              "automatic_first_started_at,automatic_next_retry_at,automatic_terminal_reason) VALUES(?,?,?,?,?,?,?,?,?,?,?)")
    retry_at = datetime(2026, 10, 9, 7, 16, tzinfo=timezone.utc).timestamp()
    _execute(database, [
        (insert, ("automatic_close:2026-10-08", "automatic_close", "2026-10-08", "2026-10-08T07:10:00", "2026-10-08T07:12:00",
                  "missed", "自动收盘报告校验失败：risk_evidence_missing", 1, "2026-10-08T07:10:00", 0,
                  "formal_gate_unpassable:risk_evidence_missing")),
        (insert, ("automatic_close:2026-09-30", "automatic_close", "2026-09-30", "2026-09-30T07:35:00", "2026-09-30T07:36:00",
                  "missed", "自动收盘报告校验失败：indicator_coverage", 6, "2026-09-30T07:10:00", 0,
                  "automatic close retry bounds exhausted")),
        (insert, ("automatic_close:2026-10-09", "automatic_close", "2026-10-09", "2026-10-09T07:10:00", "2026-10-09T07:11:00",
                  "failed", "当日完整收盘数据尚未就绪，等待重试", 2, "2026-10-09T07:05:00", retry_at, "")),
    ])
    store.record_screen_gate_diagnostics("automatic_close:2026-10-08", "2026-10-08", "fail_closed:risk_evidence_missing", {
        "input": 5561, "risk_tuple_complete": 0, "tradable": 0, "indicator_targets": 0, "enriched": 0, "raw_generation": 22,
        "raw_batch_id": "batch-22", "observation_universe_targets": 300, "observation_universe_enriched": 298,
        **Main._gate_failure_classes(["risk_evidence_missing"])})

    data = Dashboard(database, now=lambda: HOLIDAY).query("health")["data"]

    jobs = {job["key"]: job for job in data["jobs"]}
    blocked = jobs["automatic_close:2026-10-08"]
    assert blocked["stop"] == "gate_unpassable" and blocked["attempts"] == 1 and blocked["failure_codes"] == ["risk_evidence_missing"]
    assert blocked["gate"]["unpassable"] == ["risk_evidence_missing"] and blocked["gate"]["retryable"] == []
    assert blocked["gate"]["counts"]["input"] == 5561 and blocked["gate"]["counts"]["observation_universe_enriched"] == 298
    assert blocked["gate"]["generation"] == 22 and blocked["gate"]["dependency"] == "formal_risk_source_acceptance"
    exhausted = jobs["automatic_close:2026-09-30"]
    assert exhausted["stop"] == "retry_exhausted" and exhausted["attempts"] == 6 and exhausted["failure_codes"] == ["indicator_coverage"]
    assert exhausted["gate"] is None
    waiting = jobs["automatic_close:2026-10-09"]
    assert waiting["stop"] is None and waiting["failure_codes"] == ["waiting_snapshot"]
    assert waiting["next_retry_at"] == "2026-10-09T07:16:00+00:00"
    assert data["automatic_close_limits"] == {"max_attempts": None, "retry_seconds": None, "retry_window_seconds": None}


def test_acceptance_keeps_its_verdict_and_lists_later_runs_beside_it(tmp_path):
    database = tmp_path / "plugin.sqlite3"
    StockStore(database)
    acceptance = ("INSERT INTO daily_acceptance_runs(trade_date,checked_at,status,fingerprint,summary,findings_json,changed_at) "
                  "VALUES(?,?,?,?,?,?,?)")
    run = ("INSERT INTO screen_runs(run_id,job_name,requested_date,actual_trade_date,source,started_at,finished_at,"
           "quote_count,candidate_count,status,quality,report_version) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)")
    missing = json.dumps([{"code": "candidate_freeze_missing", "severity": "critical"}])
    _execute(database, [
        (acceptance, ("2026-09-30", "2026-09-30T07:40:00", "critical", "f30", "缺少候选冻结", missing, "2026-09-30T07:40:00")),
        (run, ("run-30a", "automatic_close", "2026-09-30", "2026-09-30", "tushare", "2026-09-30T07:10:00",
               "2026-09-30T07:30:00", 5561, 0, "failed", "good", 0)),
        (run, ("run-30b", "daily_screen", "2026-09-30", "2026-09-30", "tushare", "2026-10-02T01:00:00",
               "2026-10-02T01:03:00", 5561, 0, "completed", "good", 1)),
        (acceptance, ("2026-09-29", "2026-09-29T07:40:00", "critical", "f29", "缺少候选冻结", missing, "2026-09-29T07:40:00")),
        (run, ("run-29", "automatic_close", "2026-09-29", "2026-09-29", "tushare", "2026-09-29T08:00:00",
               "2026-09-29T08:05:00", 5561, 0, "completed", "good", 1)),
        ("INSERT INTO automatic_close_publications(publication_key,actual_trade_date,requested_date,run_id,invocation_id,payload,"
         "payload_hash,origins_json,outbox_prepared,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
         ("automatic_close:2026-09-29", "2026-09-29", "2026-09-29", "run-29", "job-29", "report", "h", "[]", 1,
          "2026-09-29T08:05:30", "2026-09-29T08:05:30")),
    ])
    before = hashlib.sha256(database.read_bytes()).digest()

    data = Dashboard(database, now=lambda: HOLIDAY).query("health")["data"]

    rows = {row["date"]: row for row in data["daily_acceptance"]}
    late = rows["2026-09-30"]
    assert late["status"] == "critical" and late["findings"] == ["candidate_freeze_missing"]
    assert late["review"] == "late_screen_not_formal" and late["publication"] is None
    assert [(item["run_id"], item["after_check"]) for item in late["runs"]] == [("run-30a", False), ("run-30b", True)]
    assert late["runs"][1]["candidates"] == 0 and late["runs"][1]["report_version"] == 1
    published = rows["2026-09-29"]
    assert published["status"] == "critical" and published["review"] == "late_publication"
    assert published["publication"] == {"run_id": "run-29", "created_at": "2026-09-29T08:05:30+00:00"}
    assert hashlib.sha256(database.read_bytes()).digest() == before
