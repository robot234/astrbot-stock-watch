from pathlib import Path

from storage import StockStore


def test_whitelist_round_trip(tmp_path: Path):
    store = StockStore(tmp_path / "stock.sqlite3")
    origin = "aiocqhttp:GroupMessage:123"
    assert not store.is_whitelisted(origin)
    store.set_whitelist(origin, True)
    assert store.is_whitelisted(origin)
    assert store.whitelist() == [origin]
    store.set_whitelist(origin, False)
    assert not store.is_whitelisted(origin)


def test_whitelist_supports_private_and_group_origins(tmp_path: Path):
    store = StockStore(tmp_path / "stock.sqlite3")
    values = ["aiocqhttp:FriendMessage:42", "aiocqhttp:GroupMessage:99"]
    for origin in values:
        store.set_whitelist(origin, True)
    assert store.whitelist() == sorted(values)
