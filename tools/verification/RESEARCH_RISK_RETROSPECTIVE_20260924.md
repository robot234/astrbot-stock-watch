# Retrospective risk cross-check for the 2026-09-23 research pools

Read-only public-data probe on the Raspberry Pi, started at
`2026-09-24T01:45:23.516806+00:00`. This is **after** the immutable
`2026-09-23T15:03:28.398973` research freeze. It cannot prove that the
same observations were available to the selector at freeze time, and it
must not rewrite the frozen `risk_level=unknown` rows.

Source environment: isolated `/home/pi/apps/stock-watch-data-probe/.venv`,
BaoStock 0.9.4 and AKShare 1.18.97. Queried BaoStock unadjusted daily rows
(`adjustflag=3`) for exactly 2026-09-23, requesting `date,code,close,preclose,
volume,amount,tradestatus,isST`. Each of the 27 requests returned code `0`
with one matching dated row. All 27 returned `tradestatus=1,isST=0`; their
closes matched the corresponding persisted research-pool close to 0.0001.
These are retrospective provider observations, not a certification of every
intraday interval or the full exchange universe. Source response bytes and
per-row first-observed timestamps were not captured; do not backdate them.

AKShare `stock_zt_pool_em(date='20260923')` returned 51 up-pool rows,
including three frozen codes: `301150`, `603067`, `002119`. Its parsed
DataFrame JSON SHA-256 was
`df5e1809a141ceb78597abd267f7bef78dc24c7aa6d8e8c6ecd70b798307a9ab`.
`stock_zt_pool_dtgc_em(date='20260923')` returned 13 down-pool rows,
matching none of the frozen codes; parsed digest
`16f900c951631dee015036e7702b7bcb3f809db761084e279b10bce434d1211f`.
These digests identify this parsed retrieval, not original HTTP bytes.
Pool membership is positive evidence; absence alone does not prove an
explicit negative limit state. No individual limit prices or publication
timestamps for these 27 were captured in this run.

| Pool | Code | Bao close | Bao trading | Bao ST | AK up pool | AK down pool |
| --- | --- | ---: | --- | --- | --- | --- |
| Primary | 000823 | 27.20 | 1 | 0 | unknown | unknown |
| Primary | 601872 | 20.78 | 1 | 0 | unknown | unknown |
| Primary | 301297 | 49.69 | 1 | 0 | unknown | unknown |
| Primary | 601086 | 17.02 | 1 | 0 | unknown | unknown |
| Primary | 600172 | 16.74 | 1 | 0 | unknown | unknown |
| Primary | 600641 | 47.80 | 1 | 0 | unknown | unknown |
| Primary | 002436 | 45.90 | 1 | 0 | unknown | unknown |
| Radar | 600186 | 13.59 | 1 | 0 | unknown | unknown |
| Radar | 000002 | 3.92 | 1 | 0 | unknown | unknown |
| Radar | 002080 | 60.70 | 1 | 0 | unknown | unknown |
| Radar | 600246 | 14.78 | 1 | 0 | unknown | unknown |
| Radar | 301150 | 49.20 | 1 | 0 | member | unknown |
| Radar | 600707 | 9.80 | 1 | 0 | unknown | unknown |
| Radar | 300862 | 60.00 | 1 | 0 | unknown | unknown |
| Radar | 600552 | 18.56 | 1 | 0 | unknown | unknown |
| Radar | 688388 | 31.26 | 1 | 0 | unknown | unknown |
| Radar | 603067 | 39.86 | 1 | 0 | member | unknown |
| Radar | 001216 | 28.31 | 1 | 0 | unknown | unknown |
| Radar | 301132 | 74.56 | 1 | 0 | unknown | unknown |
| Radar | 600699 | 21.53 | 1 | 0 | unknown | unknown |
| Radar | 300725 | 54.35 | 1 | 0 | unknown | unknown |
| Radar | 301176 | 65.52 | 1 | 0 | unknown | unknown |
| Radar | 688678 | 31.43 | 1 | 0 | unknown | unknown |
| Radar | 002757 | 20.28 | 1 | 0 | unknown | unknown |
| Radar | 301366 | 75.15 | 1 | 0 | unknown | unknown |
| Radar | 002119 | 26.92 | 1 | 0 | member | unknown |
| Radar | 600059 | 11.33 | 1 | 0 | unknown | unknown |

`unknown` in the two pool columns means not proven by this query; it does
not assert absence. `isST=0` is only BaoStock's retrospective daily flag,
not independently time-attested issuer-risk evidence. The three up-pool
members are **not** candidates for a safe/eligible promotion. No audit
opinion, correction chronology, intraday halt, independent per-stock limit
price, real alert delivery, or T+N outcome was established. Production
plugin/database were not changed and no message was sent by this probe.

## Subsequent production integration (2026-09-24)

After this read-only probe, exactly `main.py` and new `research_risk.py`
were installed and the stock plugin alone was reloaded. The companion JSON
was installed under plugin data as `research_risk_evidence/2026-09-23.json`.
The bounded loader accepted 27/27 rows against the actual frozen batch;
`301150` displays retrospective suspended=no, ST=no, limit_up=member.
The container stayed running with unchanged PID 3791832 and restart count 0;
the plugin load log showed no exception. The previous `main.py` is backed up
at `/AstrBot/data/plugin_backups/astrbot_stock_watch_risk_20260924T0205Z`.
No database migration or frozen-risk rewrite occurred. An actual chat reply
was not captured after the reload. The subsequent 2026-09-24 scheduled
collector deployment is described below; freeze-time evidence is not yet
proven.

## Scheduled collection checkpoint (2026-09-24)

The Pi now has `stock-watch-research-risk.timer` enabled. It runs the
isolated BaoStock/AKShare collector at 18:30 through 23:30 China time on
weekdays, retrying hourly. The service reads the latest frozen pool through
`docker exec` in SQLite read-only mode. It waits if there is no freeze for
the current China date. It atomically publishes one date-and-batch-bound
sidecar into plugin data via a container-local hard link, without rewriting
the SQLite freeze or replacing an existing sidecar. Its first read-only
service invocation returned `waiting_for_today_freeze` and exit status 0;
the first actual scheduled network capture has not happened yet.

BaoStock supplies unadjusted close, daily trading status and ST. AKShare
supplies positive membership in its dated up/down pools only; absence stays
unknown. The collector rejects missing Bao rows, close mismatches, invalid
pool codes, mismatched AKShare hit prices, suspended/limit contradictions,
and changed freeze batches before publication. Each source step has a
capture timestamp; the AKShare parsed pool digests do not prove original
response bytes or independent publication times. The AKShare pool tables
do not return a trade-date column, so the requested date plus matching
per-stock close is a cross-check, not proof of response date. Failed jobs
remain in systemd logs and retry at the next scheduled slot; automatic
notification for an exhausted day is not yet connected.
