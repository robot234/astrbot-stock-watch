"""Build a clearly synthetic fixture using the CURRENT complete plugin schema."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sqlite3
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from test_v0139_intraday_m2 import _imports
from webapp.demo import create_demo


def create_fixture(path):
    path = Path(path)
    if path.exists():
        raise FileExistsError("fixture output already exists")
    path.parent.mkdir(parents=True, exist_ok=True)
    _, _, Store = _imports()
    Store(path)
    from astrbot_stock_watch.data_evidence import EvidenceStore, envelope
    evidence_store = EvidenceStore(path)
    with tempfile.TemporaryDirectory() as directory:
        demo_path = create_demo(Path(directory) / "demo.sqlite3")
        source = sqlite3.connect(demo_path)
        source.row_factory = sqlite3.Row
        try:
            with sqlite3.connect(path) as target:
                target.execute("CREATE TABLE web_demo_metadata(key TEXT PRIMARY KEY,value TEXT)")
                target.execute("INSERT INTO web_demo_metadata VALUES('kind','synthetic_demo')")
                target.execute("INSERT INTO web_demo_metadata VALUES('schema_fixture','current_plugin_schema')")
                tables = [r[0] for r in source.execute("SELECT name FROM sqlite_master WHERE type='table'") if r[0] != "web_demo_metadata"]
                for table in tables:
                    columns = target.execute(f"PRAGMA table_info({table})").fetchall()
                    if not columns:
                        continue
                    for row in source.execute(f"SELECT * FROM {table}"):
                        values = {}
                        for _, name, kind, required, default, _ in columns:
                            if name in row.keys():
                                values[name] = row[name]
                            elif required and default is None:
                                values[name] = "[]" if name in ("risk_flags", "reasons") else 0 if kind in ("INTEGER", "REAL") else ""
                        # Fixture raw batches need dataset identity under the current schema.
                        if table == "batches":
                            continue
                        names = ",".join(values)
                        target.execute(f"INSERT OR REPLACE INTO {table}({names}) VALUES({','.join('?' for _ in values)})", tuple(values.values()))
                for table in ("daily_bars", "daily_quotes", "screen_candidates"):
                    columns = [r[1] for r in target.execute(f"PRAGMA table_info({table})")]
                    existing = target.execute(f"SELECT {','.join(columns)} FROM {table} WHERE code='DEMO01'").fetchall()
                    for original in existing:
                        record = dict(zip(columns, original))
                        record["code"] = "600000"
                        if "name" in record:
                            record["name"] = "结构夹具非真实标的"
                        target.execute(f"INSERT INTO {table}({','.join(columns)}) VALUES({','.join('?' for _ in columns)})", tuple(record.values()))
        finally:
            source.close()
    collected = "2026-09-12T01:00:00+00:00"
    record = envelope("600000", "2026-09-11", "st_flag", False,
                      source="fixture:sse.com.cn", evidence="fixture:reviewed-official-body",
                      announcement_date="2026-09-11", collected_at=collected)
    financial = envelope("600000", "2026-06-30", "roe", 12,
                         source="fixture:tushare:fina_indicator", evidence="fixture:roe",
                         announcement_date="2026-08-20", collected_at=collected)
    document = {**record, "quality": "readable", "title": "合成结构夹具公告，不是真实公告",
                "quote": "此内容只用于本地页面验收，不代表任何上市公司的实际风险状态。"}
    now = datetime.now(timezone.utc).timestamp()
    assert evidence_store.claim("fixture-writer", now)
    assert evidence_store.finish("fixture-cache", "fixture-writer",
                                 {"records": [record], "documents": [document], "status": "complete", "reason": ""}, now)
    with sqlite3.connect(path) as db:
        db.execute("INSERT INTO factor_snapshots(as_of,code,payload,source,quality,fetched_at) VALUES(?,?,?,?,?,?)",
                   ("2026-09-11", "600000", json.dumps({"evidence_records": [financial, record]}),
                    "synthetic_fixture", "partial", collected))
        # This disposable fixture can be read without any active plugin writer.
        db.commit()
        db.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    return path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--database", required=True, type=Path)
    print(create_fixture(parser.parse_args().database))
