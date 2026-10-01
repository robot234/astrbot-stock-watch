# Production research pools: read-only checkpoint, 2026-09-24

This is a dated Raspberry Pi observation, not a strategy result or permission
to mutate production. The host clock returned `2026-09-24T09:31:30+08:00`.
No credentials, configuration values, message bodies, or private origins were
read or saved. The user reported a `/观察池` reply; its browser text was not
independently captured in this checkpoint.

## Identity and rollback boundary

- Target: container `astrbot`, plugin `/AstrBot/data/plugins/astrbot_stock_watch`.
  `docker inspect` returned `running`, PID `3791832`, `RestartCount=0`.
  The mutable image tag was `m.daocloud.io/docker.io/soulter/astrbot:latest`;
  the bounded plugin load log reported version `0.13.3`.
- Installed SHA-256 matched the exact four-file staging manifest at
  `C:\Users\DIO\.codex\tmp\stock-watch-research-bytes-232132he\manifest.json`:

  | File | SHA-256 |
  | --- | --- |
  | `main.py` | `4eddf3d2b30f26aa4b6358327a8503a2d52d2fd449e5eec29e97f41f7434ce93` |
  | `storage.py` | `e0442773cad4f1b39752e9fe6d4e8c8c55e1237cdcacfe6f71566efed063abd8` |
  | `research_selector.py` | `0c603e68a3159e8b70aef8551fcccf501c1c69500a923884aca3f3ea047b44c0` |
  | `_conf_schema.json` | `e4b9be85d84dd38d9ff2316c499f8c49d316ad45d9284975c05b15effccce250` |

- The existing rollback directory
  `/AstrBot/data/plugin_backups/astrbot_stock_watch_research_20260923T143855Z`
  still contained a manifest and nonempty SQLite backup. This recheck proves
  presence, not a fresh restore drill or backup integrity check. Schema-19
  `storage.py` alone cannot be restored over a schema-20 database.
- Bounded `docker logs` for 2026-09-23 23:00:47 through 2026-09-24 00:00
  China time showed plugin loading at 23:00:47, paper/research mode at
  23:00:48, inbound WebChat `/观察选股` at 23:02:03, and `/观察池` at 23:04:24.
  Inbound logs do not prove reply contents or downstream delivery. A separate
  `/警戒池` browser result was not captured.

## Read-only SQLite observation

Used `docker exec -i astrbot python -`, SQLite URI
`file:/AstrBot/data/plugin_data/astrbot_stock_watch/stock_watch.sqlite3?mode=ro`,
and `PRAGMA query_only=ON`. Queried `PRAGMA quick_check`, `schema_meta`,
`research_pool_runs`, `research_pool_picks`, `active_candidate_runs`,
`screen_runs`, and `recommendation_records`; no write/migration was invoked.

- `quick_check=ok`, `schema_version=20`, exactly one research run:
  `research:batch-77e4361e93424f32a00a2d36211d0277`, trade date
  `2026-09-23`, source `tushare`, basis `unadjusted`, status `research_only`.
  Stored published time `2026-09-23T07:54:26.198501`, frozen time
  `2026-09-23T15:03:28.398973` (UTC-naive persisted text). Diagnostics:
  `universe=5209`, `liquid=4820`, `examined=300`, `ranked=185`, policy
  `technical-research-v1`. These are selector inputs, not proof of complete
  exchange coverage or 185 safe candidates.
- Primary has seven distinct codes in order: `000823`, `601872`, `301297`,
  `601086`, `600172`, `600641`, `002436`. Radar has 20 distinct codes:
  `600186`, `000002`, `002080`, `600246`, `301150`, `600707`, `300862`,
  `600552`, `688388`, `603067`, `001216`, `301132`, `600699`, `300725`,
  `301176`, `688678`, `002757`, `301366`, `002119`, `600059`. Every row has
  `risk_level=unknown`. The questioned `600641` and `002436` were primary
  ranks 6 and 7, not omitted.
- Formal `active_candidate_runs` still pointed to a completed `2026-09-16`
  run. The latest formal `screen_runs` row was also `2026-09-16` (completed,
  good, 30 candidates); `recommendation_records` held 150 historical rows.
  A join by research run ID against `screen_candidates` yielded zero rows.
  Existing recommendations/outcomes are not results of the new research pool.
- At the 09:31 China-time read, before the 15:00 close gate, no 2026-09-24
  research freeze existed. This does not prove or disprove later daily repeats.

## Still open

Successive close freezes, per-stock negative suspension/limit/ST/audit
evidence, publication chronology, radar alert delivery, T+N outcomes and
baselines, formal recommendation publication, and strategy accuracy remain
unverified. Keep `pending`, `unknown`, `unknown_order`, and `unknown_delivery`
distinct. Freeze new real sessions without rewriting the 2026-09-23 batch.

The authoritative local worktree has schema-20 code and focused
`test_v0155_research_pools.py`, but is uncommitted and not byte-identical to
the deployed package except for `research_selector.py`. The Desktop checkout
is another dirty source state; neither entire checkout is a release template.
