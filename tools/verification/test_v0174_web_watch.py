"""Add-to-watchlist from the read-only Web: the page queues a request file, the plugin applies it to one scope."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import http.client
import importlib
import json
from pathlib import Path
import threading

from test_v0138_automatic_close import _imports
from webapp.data import Dashboard
from webapp.server import create_server
from webapp.watch_inbox import WatchInbox

Main, _ScreenScoreResult, _core, StockStore = _imports()
main_module = importlib.import_module("astrbot_stock_watch.main")
web_watch = importlib.import_module("astrbot_stock_watch.web_watch")
ROOT = Path(__file__).resolve().parents[2]
DEFAULTS = dict(main_module._SCHEMA_DEFAULTS)
ORIGIN = "aiocqhttp:FriendMessage:123456789"
OTHER = "aiocqhttp:GroupMessage:987654321"
NOW = datetime(2026, 10, 7, 11, 30, tzinfo=timezone.utc)


def _request(code="600857", request_id="a" * 32, **extra):
    return json.dumps({"v": 1, "action": "add", "code": code, "request_id": request_id, **extra}).encode()


def test_request_parsing_trusts_only_the_id_and_code():
    assert web_watch.parse_request(_request()) == {"request_id": "a" * 32, "code": "600857"}
    assert web_watch.parse_request(b"x" * 3000) == {"error": "too_large"}
    assert web_watch.parse_request(b"\xff") == {"error": "not_json"}
    assert web_watch.parse_request(json.dumps({"v": 1, "action": "remove"}).encode()) == {"error": "unsupported"}
    assert web_watch.parse_request(_request(request_id="../x"))["error"] == "invalid_request_id"
    assert web_watch.parse_request(_request(code="60085")) == {"error": "invalid_code", "request_id": "a" * 32}


def test_scope_resolution_never_guesses_between_sessions():
    assert web_watch.resolve_scope(" " + OTHER, {ORIGIN}, [ORIGIN]) == (OTHER, "configured")
    assert web_watch.resolve_scope("", {ORIGIN, OTHER, "*"}, [ORIGIN]) == (ORIGIN, "whitelisted_with_watchlist")
    assert web_watch.resolve_scope("", {ORIGIN, "*"}, []) == (ORIGIN, "only_whitelisted")
    assert web_watch.resolve_scope("", {"*"}, [OTHER]) == (OTHER, "only_watchlist_scope")
    assert web_watch.resolve_scope("", {ORIGIN, OTHER}, [ORIGIN, OTHER]) == (None, "ambiguous")
    assert web_watch.resolve_scope("", {ORIGIN, OTHER}, []) == (None, "ambiguous")
    assert web_watch.resolve_scope("", set(), []) == (None, "no_scope")
    label = web_watch.scope_label(ORIGIN)
    assert label == "aiocqhttp · 私聊 · 尾号 6789" and "123456789" not in label
    assert web_watch.scope_label(OTHER).endswith("群聊 · 尾号 4321") and web_watch.scope_label("odd") == "已配置的会话"


def test_inbox_is_applied_oldest_first_once(tmp_path):
    (tmp_path / "0000000000002-b.json").write_bytes(_request("000001", "b" * 32))
    (tmp_path / "0000000000001-a.json").write_bytes(_request("600857", "a" * 32))
    (tmp_path / ".0000000000003-c.json.tmp").write_bytes(_request("000002", "c" * 32))
    (tmp_path / "0000000000004-d.json").write_bytes(b"{")
    seen = []

    def apply(code):
        seen.append(code)
        if code == "000001":
            raise RuntimeError("boom")
        return {"status": "added", "name": "宁波中百"}

    results = web_watch.process_inbox(tmp_path, apply)
    assert seen == ["600857", "000001"]
    assert [(r["code"], r["status"]) for r in results] == [("600857", "added"), ("000001", "error"), (None, "invalid_request")]
    assert results[2]["reason"] == "not_json" and results[1]["reason"] == "apply_failed"
    assert sorted(p.name for p in tmp_path.iterdir()) == [".0000000000003-c.json.tmp"]
    assert web_watch.merge_results([{"a": 1}, "junk"], [{"b": 2}], keep=1) == [{"b": 2}]


class _Plugin:
    """The real Main methods over a real store; only chat delivery is captured."""

    def __new__(cls, tmp_path, **config):
        plugin = Main.__new__(Main)
        plugin.config = {**DEFAULTS, "push_whitelist": ORIGIN, **config}
        plugin.store = StockStore(tmp_path / "plugin.sqlite3")
        plugin.intraday_artifact_path = tmp_path / "plugin_data" / "intraday_quotes.json"
        plugin.intraday_artifact_path.parent.mkdir(exist_ok=True)
        plugin.pushed = []

        async def push(origin, text):
            plugin.pushed.append((origin, text))
            return True

        plugin._push = push
        with plugin.store._connect() as db:
            db.execute("INSERT INTO stock_symbols(code,name,normalized_name,source,updated_at) VALUES(?,?,?,?,?)",
                       ("600857", "宁波中百", "宁波中百", "fixture", "2026-10-07T00:00:00"))
        return plugin


def _pass(plugin, published_at=None):
    return asyncio.run(Main._web_watch_pass(plugin, published_at))


def _results(plugin):
    return json.loads(plugin.intraday_artifact_path.with_name("web_watch_results.json").read_text(encoding="utf-8"))


def test_plugin_adds_to_the_resolved_scope_and_reports_back(tmp_path):
    plugin = _Plugin(tmp_path)
    plugin.store.add_watch(ORIGIN, "600000", 100, 12.5, "浦发银行")
    inbox = plugin.intraday_artifact_path.with_name("web_watch_inbox")
    inbox.mkdir()
    (inbox / "0000000000001-x.json").write_bytes(_request())
    published = _pass(plugin)
    assert published is not None and list(inbox.iterdir()) == []
    assert plugin.store.list_watch(ORIGIN) == ["600000", "600857"]
    assert plugin.store.watch_cost(ORIGIN, "600000") == 12.5 and plugin.store.watch_cost(ORIGIN, "600857") is None
    body = _results(plugin)
    assert body["scope"] == {"status": "whitelisted_with_watchlist", "label": "aiocqhttp · 私聊 · 尾号 6789", "count": 2, "limit": 100}
    assert body["codes"] == ["600000", "600857"] and body["enabled"] is True and body["inbox"] == "ready"
    assert [(r["request_id"], r["status"], r["name"]) for r in body["results"]] == [("a" * 32, "added", "宁波中百")]
    raw = plugin.intraday_artifact_path.with_name("web_watch_results.json").read_text(encoding="utf-8")
    assert "123456789" not in raw and "12.5" not in raw
    assert plugin.pushed and plugin.pushed[0][0] == ORIGIN and "/自选 删除 600857" in plugin.pushed[0][1]

    (inbox / "0000000000002-y.json").write_bytes(_request(request_id="b" * 32))
    _pass(plugin, published)
    assert _results(plugin)["results"][-1]["status"] == "exists" and len(plugin.pushed) == 1


def test_plugin_refuses_when_disabled_ambiguous_or_full(tmp_path):
    plugin = _Plugin(tmp_path, web_watch_enabled=False)
    inbox = plugin.intraday_artifact_path.with_name("web_watch_inbox")
    inbox.mkdir()
    plugin.store.add_watch(ORIGIN, "600000", 100)
    (inbox / "1-a.json").write_bytes(_request())
    _pass(plugin)
    assert _results(plugin)["results"][-1]["status"] == "disabled" and plugin.store.list_watch(ORIGIN) == ["600000"]

    plugin.config.update(web_watch_enabled=True, push_whitelist=f"{ORIGIN},{OTHER}")
    plugin.store.add_watch(OTHER, "000001", 100)
    (inbox / "2-b.json").write_bytes(_request(request_id="b" * 32))
    _pass(plugin)
    last, body = _results(plugin)["results"][-1], _results(plugin)
    assert (last["status"], last["reason"]) == ("scope_unresolved", "ambiguous") and body["scope"]["label"] is None

    plugin.config.update(web_watch_scope=ORIGIN, watchlist_limit=1)
    (inbox / "3-c.json").write_bytes(_request(request_id="c" * 32))
    _pass(plugin)
    assert _results(plugin)["results"][-1]["status"] == "limit_reached" and plugin.pushed == []


def test_plugin_without_inbox_still_publishes_its_state(tmp_path):
    plugin = _Plugin(tmp_path)
    published = _pass(plugin)
    body = _results(plugin)
    assert body["inbox"] == "missing" and body["results"] == [] and body["scope"]["status"] == "only_whitelisted"
    assert _pass(plugin, published) == published  # no rewrite inside the heartbeat interval


def test_scope_setting_stays_out_of_the_public_snapshot():
    schema = json.loads((ROOT / "_conf_schema.json").read_text(encoding="utf-8"))
    assert {k: schema[k]["default"] for k in ("web_watch_enabled", "web_watch_scope", "web_watch_notify")} == {
        "web_watch_enabled": True, "web_watch_scope": "", "web_watch_notify": True}
    assert "web_watch_scope" not in main_module.PUBLIC_STRING_SETTINGS
    stub = type("Stub", (), {})()
    stub.config, stub.deprecated_settings, stub.setting_issues = {**DEFAULTS, "web_watch_scope": ORIGIN}, [], []
    snapshot = Main.public_settings_snapshot(stub)
    assert snapshot["configured"]["web_watch_scope"] == "custom" and ORIGIN not in json.dumps(snapshot)


def _dashboard(tmp_path):
    database = tmp_path / "web.sqlite3"
    store = StockStore(database)
    with store._connect() as db:
        db.execute("INSERT INTO stock_symbols(code,name,normalized_name,source,updated_at) VALUES(?,?,?,?,?)",
                   ("600857", "宁波中百", "宁波中百", "fixture", "2026-10-07T00:00:00"))
    return Dashboard(database, now=lambda: NOW)


def test_web_inbox_validates_limits_and_writes_plugin_readable_requests(tmp_path):
    clock = [NOW.timestamp()]
    inbox = WatchInbox(tmp_path / "web_watch_inbox", clock=lambda: clock[0])
    known = {"600857", "000001"}.__contains__
    assert inbox.submit("60085", known) == (400, {"status": "rejected", "reason": "invalid_code"})
    assert inbox.submit("600001", known)[0] == 404
    assert inbox.submit("600857", known) == (503, {"status": "rejected", "reason": "inbox_missing"})
    inbox.inbox.mkdir()
    status, body = inbox.submit("600857", known)
    assert status == 202 and body["status"] == "queued"
    written = list(inbox.inbox.iterdir())
    assert len(written) == 1 and web_watch.parse_request(written[0].read_bytes()) == {"request_id": body["request_id"], "code": "600857"}
    assert inbox.status(body["request_id"])["request"] == {"state": "pending"}
    for _ in range(9):
        assert inbox.submit("000001", known)[0] == 202
    assert inbox.submit("000001", known) == (429, {"status": "rejected", "reason": "rate_limited"})
    clock[0] += 61
    for _ in range(20):
        inbox.recent.clear()
        if inbox.submit("000001", known)[0] != 202:
            break
    assert inbox.submit("000001", known) == (503, {"status": "rejected", "reason": "inbox_full"})


def test_web_status_keeps_only_whitelisted_result_fields(tmp_path):
    inbox = WatchInbox(tmp_path / "web_watch_inbox", clock=lambda: NOW.timestamp())
    assert inbox.status()["problem"] == "results_missing"
    inbox.results_path.write_text(json.dumps({
        "written_at": "2026-10-07T11:29:00+00:00", "enabled": True, "inbox": "ready", "plugin_version": "0.13.3",
        "scope": {"status": "only_whitelisted", "label": "aiocqhttp · 私聊 · 尾号 6789\x07", "count": 2, "limit": 100},
        "codes": ["600857", "bad", 1, "000001"],
        "results": [{"request_id": "a" * 32, "code": "600857", "status": "added", "name": "宁波中百", "reason": "Bad Reason!",
                     "processed_at": "2026-10-07T11:29:00+00:00", "scope": ORIGIN},
                    {"request_id": "b" * 32, "status": "weird"}]}), encoding="utf-8")
    data = inbox.status("a" * 32)
    assert data["plugin"] == {"written_at": "2026-10-07T11:29:00+00:00", "age_seconds": 60, "fresh": True, "enabled": True,
                              "inbox": "ready", "version": "0.13.3"}
    assert data["scope"]["label"] == "aiocqhttp · 私聊 · 尾号 6789" and data["codes"] == ["000001", "600857"]
    assert data["request"] == {"state": "done", "status": "added", "reason": None, "code": "600857", "name": "宁波中百",
                               "processed_at": "2026-10-07T11:29:00+00:00"}
    assert inbox.status("b" * 32)["request"]["status"] == "error"
    assert inbox.status("../etc")["request"] == {"state": "invalid_id"}
    assert inbox.status("c" * 32)["request"] == {"state": "unknown"}
    assert ORIGIN not in json.dumps(data)
    stale = WatchInbox(tmp_path / "web_watch_inbox", clock=lambda: NOW.timestamp() + 600)
    assert stale.status()["plugin"]["fresh"] is False


def _serve(server):
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server.server_port


def _call(port, method, path, body=None, headers=None):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    connection.request(method, path, body=body, headers={"Host": f"127.0.0.1:{port}", **(headers or {})})
    response = connection.getresponse()
    payload = response.read()
    connection.close()
    return response.status, json.loads(payload or b"null")


def test_server_accepts_only_same_origin_json_adds(tmp_path):
    dashboard = _dashboard(tmp_path)
    (tmp_path / "web_watch_inbox").mkdir()
    server = create_server(dashboard, 0, "127.0.0.1", watch=WatchInbox(tmp_path / "web_watch_inbox"))
    port = _serve(server)
    try:
        good = {"Origin": f"http://127.0.0.1:{port}", "X-Stock-Watch": "add", "Content-Type": "application/json"}
        body = json.dumps({"code": "600857"})
        assert _call(port, "POST", "/api/watch/add", body, {k: v for k, v in good.items() if k != "Origin"})[0] == 403
        assert _call(port, "POST", "/api/watch/add", body, {**good, "Origin": "http://evil.example"})[0] == 403
        assert _call(port, "POST", "/api/watch/add", body, {k: v for k, v in good.items() if k != "X-Stock-Watch"})[0] == 403
        assert _call(port, "POST", "/api/watch/add", body, {**good, "Content-Type": "text/plain"})[0] == 415
        assert _call(port, "POST", "/api/watch/add", "x" * 2000, good)[0] == 413
        assert _call(port, "POST", "/api/watch/add", json.dumps({"code": "600001"}), good)[0] == 404
        status, queued = _call(port, "POST", "/api/watch/add", body, good)
        assert status == 202 and queued["code"] == "600857" and len(list((tmp_path / "web_watch_inbox").iterdir())) == 1
        assert _call(port, "PUT", "/api/watch/add", body, good) == (405, {"error": "read_only"})
        status, payload = _call(port, "GET", "/api/watch")
        assert status == 200 and payload["data"]["configured"] is True and payload["data"]["inbox"] == "ready"
    finally:
        server.shutdown()
        server.server_close()


def test_server_without_inbox_stays_read_only(tmp_path):
    server = create_server(_dashboard(tmp_path), 0, "127.0.0.1")
    port = _serve(server)
    try:
        headers = {"Origin": f"http://127.0.0.1:{port}", "X-Stock-Watch": "add", "Content-Type": "application/json"}
        assert _call(port, "POST", "/api/watch/add", json.dumps({"code": "600857"}), headers) == (405, {"error": "read_only"})
        status, payload = _call(port, "GET", "/api/watch")
        assert payload["data"] == {"configured": False} and payload["meta"]["reason"] == "watch_inbox_not_configured"
    finally:
        server.shutdown()
        server.server_close()


def test_web_request_round_trips_through_the_plugin(tmp_path):
    plugin = _Plugin(tmp_path)
    inbox_dir = plugin.intraday_artifact_path.with_name("web_watch_inbox")
    inbox_dir.mkdir()
    web = WatchInbox(inbox_dir)
    status, queued = web.submit("600857", _dashboard(tmp_path).known_code)
    assert status == 202
    _pass(plugin)
    data = web.status(queued["request_id"])
    assert data["request"]["state"] == "done" and data["request"]["status"] == "added"
    assert data["codes"] == ["600857"] and data["scope"]["label"].endswith("尾号 6789") and data["plugin"]["fresh"]
