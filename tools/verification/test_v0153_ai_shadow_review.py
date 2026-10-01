from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from astrbot_stock_watch.core import Candidate, Quote
from astrbot_stock_watch.main import Main
from astrbot_stock_watch.providers import OpenAICompatibleClient
from astrbot_stock_watch.storage import StockStore
from webapp.data import Dashboard
from webapp.demo import create_demo

from test_v0140_recommendation_tracking import _record


class _Response:
    def __init__(self, body):
        self._body = body

    def raise_for_status(self):
        return None

    def json(self):
        return self._body


class _Client:
    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error

    async def post(self, *_args, **_kwargs):
        if self.error:
            raise self.error
        return _Response(self.response)


class _Runtime:
    def __init__(self, client):
        self.client = client

    @asynccontextmanager
    async def slot(self):
        yield self.client

    async def close(self):
        return None


def _candidate(code, score=20):
    quote = Quote(
        code,
        code,
        10,
        rsi6=55,
        ma5=10,
        ma10=9.8,
        ma20=9.5,
        momentum5=3,
        momentum20=8,
        volume_ratio=1.2,
        history_days=60,
        indicator_last_date="2026-09-21",
    )
    return Candidate(quote, score, ["均线多头趋势+12"], base_score=score, risk_level="watch_only")


def test_schema_v17_review_batch_is_idempotent_and_complete(tmp_path):
    store = StockStore(tmp_path / "ai-review.sqlite3")
    assert store.schema_version() == 23
    _record(store, "r1", code="600000", version="v1")
    _record(store, "r2", code="600001", version="v1")

    first = store.begin_recommendation_ai_review(
        "run-v1", model="deepseek-flash", prompt_version="shadow-risk-v1", input_sha256="a" * 64
    )
    assert first["claimed"] is True and first["status"] == "pending" and len(first["records"]) == 2
    duplicate = store.begin_recommendation_ai_review(
        "run-v1", model="deepseek-flash", prompt_version="shadow-risk-v1", input_sha256="a" * 64
    )
    assert duplicate["claimed"] is False and duplicate["review_batch_id"] == first["review_batch_id"]

    assert store.finish_recommendation_ai_review(
        first["review_batch_id"],
        status="complete",
        model_returned="deepseek-flash",
        usage={"total_tokens": 123},
        decisions=[
            {"code": "600000", "decision": "keep", "score_adjustment": 0, "risk_tags": ["证据待确认"], "reason": "规则证据相对完整"},
            {"code": "600001", "decision": "watch", "score_adjustment": -2, "risk_tags": ["短期过热"], "reason": "需要继续观察"},
        ],
    ) is True
    saved = store.recommendation_ai_review("run-v1")
    assert saved["status"] == "complete" and len(saved["decisions"]) == 2
    assert json.loads(saved["usage_json"])["total_tokens"] == 123
    assert store.finish_recommendation_ai_review(first["review_batch_id"], status="failed") is False


def test_review_client_requires_full_strict_json_and_timeout_is_unknown():
    candidates = [_candidate("600000"), _candidate("600001", 18)]
    body = {
        "model": "deepseek-flash",
        "usage": {"total_tokens": 321},
        "choices": [{"message": {"content": json.dumps({"items": [
            {"code": "600000", "decision": "keep", "score_adjustment": 0, "risk_tags": ["证据待确认"], "reason": "趋势证据较完整"},
            {"code": "600001", "decision": "watch", "score_adjustment": -1, "risk_tags": ["动量有限"], "reason": "等待更多证据"},
        ]}, ensure_ascii=False)}}],
    }
    client = OpenAICompatibleClient("https://example.invalid", "key", "deepseek-flash", min_interval=0, http_runtime=_Runtime(_Client(response=body)))
    result = asyncio.run(client.review_candidates(candidates))
    assert result["status"] == "complete" and len(result["decisions"]) == 2
    assert result["usage"]["total_tokens"] == 321

    timeout = httpx.ReadTimeout("slow", request=httpx.Request("POST", "https://example.invalid"))
    client = OpenAICompatibleClient("https://example.invalid", "key", "deepseek-flash", min_interval=0, http_runtime=_Runtime(_Client(error=timeout)))
    result = asyncio.run(client.review_candidates(candidates))
    assert result["status"] == "unknown" and result["decisions"] == []


def test_web_exposes_ai_decisions_and_separate_groups(tmp_path):
    database = create_demo(tmp_path / "demo.sqlite3")
    with sqlite3.connect(database) as db:
        db.execute("ALTER TABLE recommendation_records ADD COLUMN run_id TEXT")
        db.execute("UPDATE recommendation_records SET run_id='demo-close'")
        db.executescript("""
            CREATE TABLE recommendation_ai_review_batches(
                review_batch_id TEXT PRIMARY KEY,run_id TEXT,recommended_date TEXT,model_requested TEXT,
                model_returned TEXT,prompt_version TEXT,input_sha256 TEXT,status TEXT,error TEXT,
                usage_json TEXT,requested_at TEXT,completed_at TEXT
            );
            CREATE TABLE recommendation_ai_reviews(
                review_batch_id TEXT,recommendation_id TEXT,code TEXT,decision TEXT,
                score_adjustment INTEGER,risk_tags_json TEXT,reason TEXT,created_at TEXT
            );
        """)
        db.execute(
            "INSERT INTO recommendation_ai_review_batches VALUES(?,?,?,?,?,?,?,?,?,?,?,?)",
            ("batch", "demo-close", "2026-09-11", "deepseek-flash", "deepseek-flash", "shadow-risk-v1", "a" * 64, "complete", "", "{}", "2026-09-11T08:00:00", "2026-09-11T08:00:10"),
        )
        rows = db.execute("SELECT recommendation_id,code FROM recommendation_records ORDER BY code").fetchall()
        for index, (recommendation_id, code) in enumerate(rows):
            decision = "keep" if index < 3 else "watch"
            db.execute(
                "INSERT INTO recommendation_ai_reviews VALUES(?,?,?,?,?,?,?,?)",
                ("batch", recommendation_id, code, decision, 0 if decision == "keep" else -1, "[]", "fixture review", "2026-09-11T08:00:10"),
            )

    dashboard = Dashboard(database, origin="demo", now=lambda: datetime(2026, 9, 12, 3, tzinfo=timezone.utc))
    candidates = dashboard.query("candidates")["data"]["items"]
    assert candidates and candidates[0]["ai_review"]["model"] == "deepseek-flash"
    performance = dashboard.query("performance", {"horizon": "5"})["data"]
    assert performance["ai_groups"]["keep"]["sample_count"] == 3
    assert performance["ai_groups"]["watch"]["sample_count"] == 5
    assert all(row["ai_decision"] in {"keep", "watch"} for row in performance["records"])


def test_incomplete_decisions_do_not_terminalize_batch(tmp_path):
    store = StockStore(tmp_path / "incomplete.sqlite3")
    _record(store, "r1", code="600000", version="v1")
    _record(store, "r2", code="600001", version="v1")
    batch = store.begin_recommendation_ai_review(
        "run-v1", model="deepseek-flash", prompt_version="shadow-risk-v1", input_sha256="b" * 64
    )
    with pytest.raises(ValueError, match="cover every"):
        store.finish_recommendation_ai_review(
            batch["review_batch_id"], status="complete",
            decisions=[{"code": "600000", "decision": "keep", "score_adjustment": 0, "risk_tags": [], "reason": "only one"}],
        )
    assert store.recommendation_ai_review("run-v1")["status"] == "pending"


def test_main_shadow_pipeline_persists_once_per_run(tmp_path):
    store = StockStore(tmp_path / "main-shadow.sqlite3")
    _record(store, "r1", code="600000", version="v1")
    _record(store, "r2", code="600001", version="v1")
    candidates = [_candidate("600000"), _candidate("600001", 18)]

    class Llm:
        model = "deepseek-flash"
        calls = 0

        @staticmethod
        def shadow_review_input(values):
            return OpenAICompatibleClient.shadow_review_input(values)

        async def review_candidates(self, values, **_kwargs):
            self.calls += 1
            return {
                "status": "complete",
                "model_returned": self.model,
                "usage": {"total_tokens": 88},
                "error": "",
                "decisions": [
                    {"code": value.quote.code, "decision": "keep" if index == 0 else "watch",
                     "score_adjustment": 0 if index == 0 else -1, "risk_tags": ["证据待确认"], "reason": "fixture"}
                    for index, value in enumerate(values)
                ],
            }

    main = Main.__new__(Main)
    main.store = store
    main.llm = Llm()
    main.config = {"llm_shadow_max_tokens": 8192, "llm_shadow_prompt_version": "shadow-risk-v1"}
    first = asyncio.run(main._run_recommendation_ai_shadow_review("run-v1", candidates))
    second = asyncio.run(main._run_recommendation_ai_shadow_review("run-v1", candidates))
    assert first["status"] == "complete" and second["status"] == "complete"
    assert main.llm.calls == 1
    saved = store.recommendation_ai_review("run-v1")
    assert saved["status"] == "complete" and len(saved["decisions"]) == 2
