# Local Read-Only Web App

Independent Python standard-library HTTP server, SQLite projections and native
HTML/CSS/JavaScript. No frontend package install or plugin initialization.
Python 3.10+ is required. Lucide icons and their license are vendored locally.

Run from this repository's root:

```powershell
py -m webapp.demo --database .local_records/web-demo.sqlite3 --settings .local_records/web-demo-settings.json
py -m webapp.server --database .local_records/web-demo.sqlite3 --settings .local_records/web-demo-settings.json --origin demo --port 8765
```

Open `http://127.0.0.1:8765`. The demo generator refuses existing output files.
Its fixed July-September 2026 DEMO01-DEMO08 prices, calendars and performance
are synthetic fixtures, not real market evidence. Every page labels them.

For an authorized **local copy** of real plugin data, pass that file explicitly
with `--database`. There is no implicit production path and no migration.
The server binds only IPv4 loopback. Do not expose it with a proxy or tunnel:
remote hosting, authentication and production permissions need separate work.
Mobile acceptance here means a responsive browser viewport, not LAN access.

## Views And Data

- Overview: market date/quality, a coverage breakdown with separate denominators
  (quotes, four risk fields known, indicators computable, formal passes), today's
  trading-calendar session, candidates and recent runs.
- Monitor: fixed-origin outbox events, original plan identity and stored quotes.
  "Closed" is shown only when the stored calendar says so.
- Candidates: score/risk search, sorting, levels and stock links.
- Stock: unadjusted OHLC from the published active raw generation (legacy
  `daily_bars` only as a labelled fallback), volume, MA5/10/20, plan, research
  membership and event history. `/api/search?q=` finds codes by code or name.
- Performance: verified trading-day T+1/3/5/10 windows, gross close returns,
  MFE, close-series drawdown and distinct price/path denominators.
- Health: provider success telemetry and API rate-limit deadlines kept apart,
  batches, jobs and outboxes; records older than the current data date are folded.
- Settings: allowlisted public effective values and separate schema defaults.
- `/api/version`: API version, capability list, build release/revision (written
  into `webapp/build_info.json` by `deploy/package.py`) and database schema.
- `/api/research_catalog`: the allowlisted offline research freezes in
  `docs/research/*_FROZEN.json` (packaged with the Web release), each with its
  file SHA-256, freeze time, input date, registration commit and curated stage
  (`not_passed`, `exploration`, `forward_pending`, `passed`). Lists are shown
  separately on the research page and never written to formal tables.

The snapshot timer writes `snapshot_status.json` beside the snapshot on every
check (`published`, `unchanged` or `failed`). The dashboard reads it from next
to `--database` and reports it as `meta.snapshot.check`, so an unchanged but old
copy during a holiday is distinguishable from a timer that stopped checking.

Only public recommendations and the server's fixed `--origin` are visible.
The API ignores client attempts to change origin. Without `--origin`, private
events are not exposed. This is a local trust boundary, not user authentication.
Optional `--settings` must be an explicitly sanitized JSON file of the form
`{"values":{"min_score":20,"paper_trading_only":true}}`; never pass credentials.
Absent settings mean effective values are unknown, not equal to defaults.

Each API request opens SQLite with `mode=ro`, enables `query_only`, and reads
one transaction with a 3-second query budget. Missing files are never created.
Missing/legacy/locked data is partial or unavailable, never a healthy default.
The newest stored quote is not live; stale and future timestamps are checked.
Monitor polling reads the local database every 15 seconds without provider calls.

Performance is recomputed from the explicitly selected local snapshot. On real
data it requires exact code/date Tushare adj_factor evidence at the base and
every forward session, unchanged factors, complete daily snapshots and valid
unadjusted OHLC. Missing or changed evidence is unknown. Immature windows are
pending; absent calendar evidence is unknown. Daily target/invalidation order
collisions never enter path denominators. No fees or execution fills are assumed.
Same-window index benchmark remains unavailable; single-day market statistics
are never substituted. Announcements are displayed only from persisted,
body-bound evidence records; absent or unreadable evidence remains unavailable.
Acquisition is an opt-in plugin deep-screen backend, never a Web refresh action.
API lists are
bounded (500 candidates, 200 events, 1,000 recommendations, 120 bars per stock),
so aggregates describe the returned local sample, not unlimited lifetime history.

## Checks

```powershell
py -m pytest tools/verification/test_v0144_web_dashboard.py -q
node --check webapp/static/app.js
node tools/verification/web_app_logic.cjs
node tools/verification/web_dashboard_browser.cjs http://127.0.0.1:8765
py -m py_compile webapp/data.py webapp/server.py webapp/demo.py
py -m pytest -q
```

The browser check requires Playwright on Node's module path and installed
Microsoft Edge. In Codex's bundled environment, set `NODE_PATH` to the bundled
`dependencies/node/node_modules` folder before running it. It launches and closes
its own headless Edge instance, tests four viewport sizes and desktop/mobile
workflows, and writes screenshots/report below `.local_records/web-browser`.
It never attaches to the user's existing browser profile.

No application route writes settings, sends messages, triggers screening or
places trades. Deployment/reload, real-data acceptance and publication remain
outside the first-version scope.

## Update An Installed Release

`deploy/install.py` is first-install only. Later releases use `deploy/update.py`
on the Pi; it never touches the plugin directory or the AstrBot container.

```powershell
py webapp/deploy/package.py <release> <commit>   # packages committed bytes, not the CRLF checkout
scp .local_records/pi-web-deployment/<release><commit>/runtime.tar.gz webapp/deploy/update.py pi@192.168.124.6:/tmp/
ssh pi@192.168.124.6 "sudo /usr/bin/python3 -B /tmp/update.py /tmp/runtime.tar.gz <sha256> <release>"
```

It stages the release, probes a temporary `127.0.0.1:18767` instance against
the live snapshot, switches `current` with automatic rollback, switches the
snapshot helper, runs one snapshot check and writes
`/home/pi/apps/stock-watch-web/deployments/<release>/rollback.sh`. Server
arguments are read from the installed user unit. The snapshot helper removes
staging files left by killed runs before each check and cleans up on SIGTERM.

## Explicit Existing Database

Pass an already authorized, consistent **local** SQLite snapshot explicitly:

```powershell
py -m webapp.server --database "C:\approved-local-copy\stock_watch.sqlite3" --origin "fixed-authorized-origin" --port 8767
```

The path is an example, not a configured production location. Do not copy an
active WAL database's main file alone. The Web server performs no source
copying, provider requests, migration or credential loading. No automatic
Desktop-worktree synchronization is performed.

Every page has a source/date/collection-time disclosure; stock detail separates
technical history, financial coverage and risk evidence. Stored price
observations expose the common evidence fields but are labelled
`stored_observation`, not promoted to verified upstream or live evidence.
The new data contract, source audit, trusted adapter setup and local complete-
schema fixture are documented in `tools/verification/DATA_EVIDENCE_REFERENCE.md`.
