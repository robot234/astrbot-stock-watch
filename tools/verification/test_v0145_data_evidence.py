import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from datetime import datetime, timezone
import json
import sqlite3

import httpx
import pytest

from test_v0139_intraday_m2 import _imports

_imports()
from astrbot_stock_watch import data_evidence as ev
from astrbot_stock_watch import providers as provider_module
from astrbot_stock_watch.providers import SinaQuoteProvider, TusharePermissionError


DATE = "2026-09-11"
KNOWN = "2026-09-12T12:00:00+00:00"


@pytest.fixture(autouse=True)
def fixed_clock(monkeypatch):
    class FixedDateTime(datetime):
        @classmethod
        def now(cls, tz=None):
            value = cls(2026, 9, 12, 1, tzinfo=timezone.utc)
            return value.astimezone(tz) if tz else value.replace(tzinfo=None)
    monkeypatch.setattr(ev, "datetime", FixedDateTime)
    monkeypatch.setattr(provider_module, "datetime", FixedDateTime)


def assertion(kind="st_flag", value=False):
    return ev.envelope("600000", DATE, kind, value, source="sse.com.cn",
                       evidence="https://www.sse.com.cn/disclosure/fixture.html",
                       announcement_date=DATE, collected_at="2026-09-12T01:00:00+00:00")


def test_point_in_time_and_conflict_contract():
    good = assertion()
    assert ev.resolve([good], "600000", DATE, KNOWN, "st_flag")["value"] is False
    for patch in ({"code": "600001"}, {"business_date": "2026-09-10"},
                  {"announcement_date": "2026-09-13"}, {"collected_at": "2026-09-13T00:00:00Z"},
                  {"quality": "permission_denied"}, {"evidence": ""}, {"value": 0}, {"source": "unknown"}):
        bad = {**good, **patch}
        assert ev.resolve([bad], "600000", DATE, KNOWN, "st_flag")["value"] is None
    conflict = {**good, "value": True, "source": "cninfo.com.cn"}
    assert ev.resolve([good, conflict], "600000", DATE, KNOWN, "st_flag")["reason"] == "source_conflict"
    financial = ev.envelope("600000", "2026-06-30", "roe", 10.0,
                           source="tushare:fina_indicator", evidence="fixture:roe",
                           announcement_date="2026-08-20", collected_at="2026-09-12T01:00:00Z")
    assert ev.validate(financial, "600000", DATE, KNOWN, exact_date=False)
    assert not ev.validate(financial, "600000", DATE, "2026-09-11T08:00:00Z", exact_date=False)


def test_legacy_claims_and_cash_ratio_cannot_bypass_evidence():
    row = ev.safe_factor_row({"roe": 18, "st_flag": False, "audit_flag": False,
                              "fundamental_score": 10}, "600000", DATE, KNOWN)
    assert all(row[k] is None for k in ("roe", "st_flag", "audit_flag", "fundamental_score"))
    financial = ev.envelope("600000", "2026-06-30", "roe", 18,
                           source="tushare:fina_indicator", evidence="fixture:roe",
                           announcement_date="2026-08-20", collected_at="2026-09-12T01:00:00Z")
    assert ev.safe_factor_row({"evidence_records": [financial]}, "600000", DATE, KNOWN)["roe"] == 18
    assert ev.safe_factor_row({"code": "600001", "evidence_records": [financial]}, "600000", DATE, KNOWN).get("roe") is None
    assert ev.safe_factor_row({"as_of": "2026-09-10", "evidence_records": [financial]}, "600000", DATE, KNOWN).get("roe") is None


class Runtime:
    max_concurrency = 8
    def __init__(self, handler=None):
        self.client_object = httpx.AsyncClient(transport=httpx.MockTransport(handler or (lambda request: httpx.Response(404))))
    async def client(self):
        return self.client_object
    @asynccontextmanager
    async def slot(self):
        yield self.client_object


class FactorGateway:
    def __init__(self, codes, defect=""):
        self.codes, self.defect, self.requests = codes, defect, []
    async def request_json(self, client, payload, **kwargs):
        api, params = payload["api_name"], payload["params"]
        self.requests.append((api, dict(params)))
        if self.defect == "permission":
            raise TusharePermissionError("fixture permission failure")
        fields = payload["fields"].split(",")
        if api == "daily_basic":
            rows = [{"ts_code": code + ".SH", "trade_date": "20260911", "pe": 12, "pb": 1.2} for code in self.codes]
            if self.defect == "daily_date":
                for row in rows:
                    row["trade_date"] = "20260910"
        else:
            rows = [{"ts_code": params["ts_code"], "ann_date": "20260820", "end_date": "20260630",
                     "roe": 12, "netprofit_yoy": 8, "ocf_to_or": 20}]
            if self.defect == "future_announcement":
                rows[0]["ann_date"] = "20260913"
            if self.defect == "wrong_code":
                rows[0]["ts_code"] = "600999.SH"
            if self.defect == "conflict":
                rows.append({**rows[0], "roe": 30})
        return {"code": 0, "data": {"fields": fields, "items": [[row.get(f) for f in fields] for row in rows]}}


def test_factor_100_symbols_and_field_semantics():
    async def run():
        codes = [f"{600000 + i:06d}" for i in range(100)]
        runtime, gateway = Runtime(), FactorGateway(codes)
        provider = SinaQuoteProvider(tushare_token="fixture-not-a-secret", http_runtime=runtime, gateway=gateway)
        try:
            rows = await provider.fetch_tushare_factors(codes, DATE)
            assert set(rows) == set(codes)
            assert {p["ts_code"][:6] for api, p in gateway.requests if api == "fina_indicator"} == set(codes)
            assert len(gateway.requests) == 101
            clean = ev.safe_factor_row(rows[codes[-1]], codes[-1], DATE, KNOWN)
            assert clean["profit_growth"] == 8
            assert clean["roe"] == 12 and clean["pe"] == 12
            assert clean["cash_quality"] is None
            assert all(e["collected_at"] for e in rows[codes[-1]]["evidence_records"])
        finally:
            await runtime.client_object.aclose()
    asyncio.run(run())


def test_quote_collection_is_not_backdated_to_exchange_timestamp():
    async def run():
        fields = [""] * 32
        fields[0], fields[2], fields[3], fields[8], fields[9] = "fixture", "10", "11", "100", "1100"
        fields[30], fields[31] = "2026-09-11", "15:00:00"
        runtime = Runtime(lambda request: httpx.Response(200, text='var hq_str_sh600000="' + ",".join(fields) + '";'))
        provider = SinaQuoteProvider(http_runtime=runtime)
        try:
            quotes = await provider.fetch_quotes(["600000"], remember_symbols=False)
            assert len(quotes) == 1
            assert quotes[0].provider_ts.date().isoformat() == "2026-09-11"
            assert quotes[0].fetched_at.date().isoformat() == "2026-09-12"
            assert quotes[0].fetched_at > quotes[0].provider_ts
            Main, _, _ = _imports()
            main = Main.__new__(Main)
            main.config = {}
            now = datetime(2026, 9, 12, 1, tzinfo=timezone.utc)
            assert main._fresh_quotes(quotes, now=now) == []
            assert main._intraday_signal_specs(quotes[0], {}, now=now)[1] == ["stale_quote"]
        finally:
            await runtime.client_object.aclose()
    asyncio.run(run())


@pytest.mark.parametrize("defect,field", [("permission", "roe"), ("future_announcement", "roe"),
                                        ("wrong_code", "roe"), ("conflict", "roe"), ("daily_date", "pe")])
def test_factor_failed_binding_is_unknown(defect, field):
    async def run():
        runtime, gateway = Runtime(), FactorGateway(["600000"], defect)
        provider = SinaQuoteProvider(tushare_token="fixture", http_runtime=runtime, gateway=gateway)
        try:
            rows = await provider.fetch_tushare_factors(["600000"], DATE)
            clean = ev.safe_factor_row(rows.get("600000", {}), "600000", DATE, KNOWN)
            assert clean[field] is None
        finally:
            await runtime.client_object.aclose()
    asyncio.run(run())


def descriptor():
    return {"url": "https://www.sse.com.cn/disclosure/fixture.html", "code": "600000",
            "business_date": DATE, "announcement_date": DATE, "kind": "st_flag", "value": False,
            "title": "测试夹具：撤销风险警示", "quote": "公司股票600000自2026-09-11起撤销其他风险警示，恢复正常交易。"}


def test_official_body_binding_and_negative_states():
    async def run():
        doc = descriptor()
        for defect in ("valid", "empty", "captcha", "permission", "pdf", "future", "wrong_code", "unreadable", "proposal"):
            item = deepcopy(doc)
            body, status, content_type = "<html>公告日期：2026-09-11 " + doc["quote"] + "</html>", 200, "text/html"
            if defect == "captcha": body = "验证码 captcha"
            if defect == "permission": status = 403
            if defect == "pdf": content_type = "application/pdf"
            if defect == "future": item["announcement_date"] = "2026-09-13"
            if defect == "wrong_code": body = body.replace("600000", "600001")
            if defect == "unreadable": body = "<html>正文加载中</html>"
            if defect == "proposal":
                item["quote"] = doc["quote"].replace("起撤销", "起申请撤销")
                body = "公告日期：2026-09-11 " + item["quote"]
            runtime = Runtime(lambda request: httpx.Response(status, text=body, headers={"content-type": content_type}))
            adapter = ev.OfficialEvidenceAdapter(runtime, documents=[] if defect == "empty" else [item], min_interval=.1)
            try:
                try:
                    result = await adapter.collect("600000", DATE)
                    outcome = ev.resolve(result["records"], "600000", DATE, KNOWN, "st_flag")
                except ev.EvidenceUnavailable:
                    outcome = {"value": None}
                assert outcome["value"] is (False if defect == "valid" else None), defect
            finally:
                await runtime.client_object.aclose()
    asyncio.run(run())


def test_search_contract_conflicts_and_cache_fencing(tmp_path):
    async def run():
        negative, positive = descriptor(), {**descriptor(), "url": "https://www.cninfo.com.cn/other.html",
                                             "value": True, "quote": "公司股票600000自2026-09-11起实施其他风险警示。"}
        hits = []
        def handler(request):
            hits.append(str(request.url))
            if request.url.path == "/search":
                return httpx.Response(200, json={"status": "complete", "items": [negative, positive]})
            body = "公告日期：2026-09-11 " + (positive["quote"] if "cninfo" in request.url.host else negative["quote"])
            return httpx.Response(200, text=body, headers={"content-type": "text/html"})
        runtime = Runtime(handler)
        adapter = ev.OfficialEvidenceAdapter(runtime, search_url="https://www.sse.com.cn/search", min_interval=.1)
        store = ev.EvidenceStore(tmp_path / "evidence.sqlite3")
        now = [datetime(2026, 9, 12, 8, tzinfo=timezone.utc).timestamp()]
        service = ev.RiskEvidenceService(store, adapter, clock=lambda: now[0])
        try:
            first, concurrent = await asyncio.gather(service.get("600000", DATE), service.get("600000", DATE))
            complete = first if first["status"] == "complete" else concurrent
            assert ev.resolve(complete["records"], "600000", DATE, KNOWN, "st_flag")["reason"] == "source_conflict"
            assert len(hits) == 3
            assert (await service.get("600000", DATE))["records"] == complete["records"]
            assert len(hits) == 3
            assert store.claim("stale", now[0] + 100)
            assert store.claim("successor", now[0] + 200)
            assert not store.finish("wrong", "stale", complete, now[0] + 201)
        finally:
            await runtime.client_object.aclose()
    asyncio.run(run())


def test_negative_cache_never_creates_false(tmp_path):
    async def run():
        hits = []
        runtime = Runtime(lambda request: (hits.append(str(request.url)) or httpx.Response(200, json={"status": "complete", "items": []})))
        adapter = ev.OfficialEvidenceAdapter(runtime, search_url="https://www.sse.com.cn/search")
        service = ev.RiskEvidenceService(ev.EvidenceStore(tmp_path / "empty.sqlite3"), adapter)
        try:
            first = await service.get("600000", DATE)
            again = await service.get("600000", DATE)
            assert first == again and first["status"] == "unknown" and not first["records"]
            assert len(hits) == 1
            with sqlite3.connect(service.store.path) as db:
                db.execute("UPDATE evidence_fetch_cache SET payload='[]'")
            assert (await service.get("600000", DATE))["reason"] == "cache_invalid"
            assert len(hits) == 1
            for url in ("http://www.sse.com.cn/a", "https://evil.example/a", "https://www.sse.com.cn@evil.example/",
                        "https://www.sse.com.cn/a?token=secret"):
                assert not adapter.allowed(url)
        finally:
            await runtime.client_object.aclose()
    asyncio.run(run())


def test_low_reward_risk_is_not_missing_data():
    from test_v0140_recommendation_tracking import _validated_plan
    from astrbot_stock_watch.core import Candidate, Quote, format_compact_candidate, price_plan_is_validated, risk_label
    plan = _validated_plan()
    plan.resistance = 10.2
    assert price_plan_is_validated(plan)
    candidate = Candidate(Quote("600099", "fixture", 10), 10, [], price_plan=plan)
    text = format_compact_candidate(candidate, 1)
    assert "情景空间不足：盈亏比未达到 1:1" in text
    assert "预计：数据不足" not in text
    assert risk_label(candidate.risk_level) in text.splitlines()[0]


def test_daily_quote_risk_flags_round_trip_only_explicit_values_and_block_unknown(tmp_path):
    _Main, core, Store = _imports()
    store = Store(tmp_path / "quotes.sqlite3")
    explicit = core.Quote("600000", "显式", 10, suspended=False, limit_up=True, limit_down=False)
    missing = core.Quote("600001", "缺失", 10, suspended=0, limit_up=None, limit_down=False)
    assert store.save_daily_quotes(DATE, [explicit, missing]) == 2
    loaded = {quote.code: quote for quote in store.daily_quotes(DATE)}
    assert (loaded["600000"].suspended, loaded["600000"].limit_up, loaded["600000"].limit_down) == (False, True, False)
    assert (loaded["600001"].suspended, loaded["600001"].limit_up, loaded["600001"].limit_down) == (None, None, False)
    assert not core.is_screenable(loaded["600000"])
    assert not core.is_screenable(loaded["600001"])

    quote = core.Quote("600002", "provider", 10)
    SinaQuoteProvider._apply_authoritative_risk_fields(
        [quote], [{"f12": "600002", "trade_status": "active", "f43": 10, "f51": 10, "f52": 9}]
    )
    assert (quote.suspended, quote.limit_up, quote.limit_down) == (None, True, None)
    SinaQuoteProvider._apply_authoritative_risk_fields(
        [quote], [
            {"f12": "600002", "trade_status": "active", "f43": 10, "f51": 10, "f52": 9},
            {"f12": "600002", "trade_status": "active", "f43": 10, "f51": 10, "f52": 9},
        ],
    )
    assert (quote.suspended, quote.limit_up, quote.limit_down) == (None, None, None)


def test_daily_risk_enrichment_batches_before_cache_persistence_and_keeps_unknowns_blocked(tmp_path):
    Main, core, Store = _imports()
    requests = []

    class Response:
        def __init__(self, rows):
            self.rows = rows

        def raise_for_status(self):
            return None

        def json(self):
            return {"data": {"diff": self.rows}}

    class Client:
        async def get(self, url, *, params):
            assert url == "https://push2.eastmoney.com/api/qt/ulist.np/get"
            assert params["fields"] == "f12,f43,f51,f52,f86,suspended,trade_status"
            requests.append(params["secids"])
            if params["secids"] == "0.300000":
                raise httpx.HTTPError("fixture companion failure")
            return Response([
                {"f12": "600000", "trade_status": "active", "f43": 10, "f51": 11, "f52": 9},
                # This legacy-looking field is intentionally not accepted.
                {"f12": "000001", "f_suspended": "0", "f43": 10, "f51": 11, "f52": 9},
            ])

    class Slot:
        async def __aenter__(self):
            return Client()

        async def __aexit__(self, exc_type, exc, traceback):
            return False

    class Http:
        def slot(self):
            return Slot()

    provider = SinaQuoteProvider.__new__(SinaQuoteProvider)
    provider.http = Http()
    quotes = [
        core.Quote("600000", "known", 10),
        core.Quote("000001", "unsupported", 10),
        core.Quote("300000", "failed", 10),
    ]
    summary = asyncio.run(provider.enrich_daily_risk_fields(quotes, batch_size=2))

    assert requests == ["1.600000,0.000001", "0.300000"]
    assert summary == {
        "requested": 3, "batches": 2, "transport_failed": 1,
        "invalid_response": 0, "matched_rows": 2, "complete": 0,
    }
    assert (quotes[0].suspended, quotes[0].limit_up, quotes[0].limit_down) == (None, None, None)
    assert quotes[1].suspended is None
    assert (quotes[2].suspended, quotes[2].limit_up, quotes[2].limit_down) == (None, None, None)

    store = Store(tmp_path / "daily-risk.sqlite3")
    assert store.save_daily_quotes(DATE, quotes) == 3
    loaded = {quote.code: quote for quote in store.daily_quotes(DATE)}
    assert not core.is_screenable(loaded["600000"])
    assert not core.is_screenable(loaded["000001"])
    assert not core.is_screenable(loaded["300000"])
    assert (loaded["600000"].suspended, loaded["600000"].limit_up, loaded["600000"].limit_down) == (None, None, None)

    cache_date = "2026-09-10"
    stale = core.Quote("688001", "cached", 10)
    assert store.save_daily_quotes(cache_date, [stale]) == 1

    class CacheProvider:
        calls = []

        async def enrich_daily_risk_fields(self, rows):
            self.calls.append([quote.code for quote in rows])
            SinaQuoteProvider._apply_authoritative_risk_fields(
                rows,
                [{"f12": "688001", "suspended": "0", "f43": 10, "f51": 11, "f52": 9}],
            )

    main = Main.__new__(Main)
    main.store, main.quotes, main.config = store, CacheProvider(), {}
    asyncio.run(Main._enrich_daily_snapshot_risk_fields(main, cache_date, store.daily_quotes(cache_date), persist=True))
    cached = {quote.code: quote for quote in store.daily_quotes(cache_date)}["688001"]
    assert main.quotes.calls == [["688001"]]
    assert (cached.suspended, cached.limit_up, cached.limit_down) == (None, None, None)
    assert not core.is_screenable(cached)


def test_factor_evidence_cutoff_preserves_point_in_time_validation_after_storage(tmp_path):
    Main, _core, Store = _imports()
    store = Store(tmp_path / "factors.sqlite3")
    cutoff = Main._factor_evidence_cutoff(DATE)
    timely = ev.envelope("600000", "2026-06-30", "roe", 12.0,
                         source="tushare:fina_indicator", evidence="fixture:roe",
                         announcement_date="2026-08-20", collected_at="2026-09-11T06:00:00+00:00")
    late = {**timely, "collected_at": "2026-09-12T06:00:00+00:00"}
    store.save_factor_snapshots(DATE, {"600000": {"code": "600000", "as_of": DATE,
                                                       "evidence_records": [timely]}}, "fixture", "good")
    cached = store.factor_snapshots(DATE)["600000"]
    assert ev.safe_factor_row(cached, "600000", DATE, cutoff)["roe"] == 12.0
    assert ev.safe_factor_row({"code": "600000", "as_of": DATE, "evidence_records": [late]},
                              "600000", DATE, cutoff)["roe"] is None


def test_price_plan_deep_pullback_is_explicit_not_a_nearby_attention_signal():
    _Main, core, _Store = _imports()
    quote = core.Quote("600003", "深回撤", 100, atr14=2, support20=80, resistance20=105,
                       history_days=20, indicator_last_date=DATE, indicator_last_close=100,
                       indicator_price_basis="unadjusted", indicator_source="tushare",
                       provider_ts=datetime(2026, 9, 11, 15, tzinfo=timezone.utc),
                       suspended=False, limit_up=False, limit_down=False)
    plan = core.build_price_plan_for_context(quote, DATE, context="daily_close")
    assert core.price_plan_is_validated(plan)
    assert plan.provenance["attention_kind"] == "deep_pullback_watch"
    assert plan.provenance["reference_observed_at"] == "2026-09-11T15:00:00+00:00"
    assert plan.invalidation < plan.attention_low <= plan.attention_high < plan.confirmation
    descriptor = core.price_plan_attention_descriptor(plan)
    assert "深回撤风险区间" in descriptor and "非即时操作指令" in descriptor and "19.5%" in descriptor
    item = core.Candidate(quote, 10, [], price_plan=plan, risk_level="eligible")
    assert "深回撤风险区间" in core.format_compact_candidate(item, 1)

    quote.support20 = 98
    nearby = core.build_price_plan_for_context(quote, DATE, context="daily_close")
    assert core.price_plan_is_validated(nearby)
    assert nearby.provenance["attention_kind"] == "setup_monitoring"
    assert "风险区间" in core.price_plan_attention_descriptor(nearby)


def test_current_schema_web_evidence_fixture(tmp_path):
    from web_evidence_fixture import create_fixture
    from webapp.data import Dashboard
    database = create_fixture(tmp_path / "schema.sqlite3")
    dashboard = Dashboard(database, origin="demo", now=lambda: datetime(2026, 9, 12, 3, tzinfo=timezone.utc))
    before = database.read_bytes()
    for route in ("overview", "signals", "candidates", "stocks/600000", "performance", "health", "settings"):
        response = dashboard.query(route)
        assert response["meta"]["status"] != "unavailable", (route, response)
        assert response["meta"]["dataset_kind"] == "synthetic_demo"
        assert response["meta"]["sources"]
    stock = dashboard.query("stocks/600000")["data"]
    assert stock["data_evidence"]["financial"]["roe"]["value"] == 12
    assert stock["data_evidence"]["risk"]["st_flag"]["value"] is False
    assert stock["announcements"]["status"] == "available"
    assert "夹具" in stock["announcements"]["items"][0]["title"]
    assert database.read_bytes() == before
    with sqlite3.connect(database) as db:
        db.execute("UPDATE data_evidence_records SET payload=json_set(payload,'$.announcement_date','2099-01-01')")
        db.execute("UPDATE factor_snapshots SET payload='{}'")
    invalid = dashboard.query("stocks/600000")["data"]
    assert invalid["announcements"]["items"] == []
    assert invalid["data_evidence"]["risk"]["st_flag"]["value"] is None
