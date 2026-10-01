# Data Evidence Reference And Local Web Contract

Scope: `171e` worktree only, custom / Astra-direct. No production access,
credentials, deployments, reloads, messages, trades or Git publication.
This reference records code capabilities and synthetic/local evidence, not
current live account entitlements or measured market-wide coverage.

## Source Audit

| Domain | Current Code Path | Coverage / Remaining Gap |
| --- | --- | --- |
| Intraday quotes | Sina `hq.sinajs.cn`, batched `fetch_quotes` and whole-universe collector | Code/time checks and existing freshness gates. Actual collection is now separate from exchange quote time; fresh collection cannot make a stale exchange quote fresh. No per-stock search engine scraping. |
| Close quotes / daily OHLC | Tushare immutable raw batches, daily calendar and adj_factor gates; bounded Eastmoney unadjusted history fallback | Existing same-date/basis/calendar/generation validation retained. Web reads stored bars only. Real current account permission and market-wide coverage were not checked. |
| Current industry annotation | Eastmoney industry cache and final-pool fetch | Display annotation is separate from historical scoring; no historical announcement evidence is inferred. |
| Valuation | Tushare `daily_basic` exact ts_code/trade_date plus PE/PB | Malformed/wrong-date rows are excluded. Eastmoney untimestamped fields and legacy custom fields cannot bypass the new scoring evidence envelope. |
| Financials | Tushare `fina_indicator`, latest visible report and announcement for exact ts_code | ROE and `netprofit_yoy` retain report/announcement/collection dates. The former 50-symbol truncation is now the same bounded 500-symbol ceiling as the caller; a deterministic 100-symbol test checks every code. |
| Cash quality | Raw `ocf_to_or` ratio is retained in an unknown-quality evidence record | The official field description does not specify a normalization compatible with the legacy CNY cash-flow scoring scale. It is NOT silently converted or scored. Cash-quality coverage remains unknown until a declared unit/normalization contract is separately verified. |
| ST / removal of ST | No authoritative bulk ST status implementation in the current provider; new opt-in official-body evidence adapter | Name/keyword/no-result does not establish false. Removal requires an explicit implemented removal plus resumption of normal trading, exact entity/date and readable publication binding. Mixed or conflicting warnings remain unknown. |
| Audit | No live `fina_audit` integration claimed; narrow official-body assertions or configured evidence-bearing factor source | Explicit opinion text, code/date and publication proof required. Proposals, unreadable PDFs and ambiguous wording do not prove a clean audit. |
| Suspension / resumption | Existing `Quote.suspended` tri-state plus new stored official risk assertions | An announcement is not copied into a live quote's trading-state flags. Missing current quote suspension evidence still fails existing intraday gates. |
| Price limits | Existing `Quote.limit_up/limit_down` tri-state; no new bulk exchange-limit source is claimed | Search is not used to calculate limit prices. Missing explicit limit state remains unknown; no assumed 10%/20% substitute. |
| Announcements / delisting risk | Configured trusted search JSON adapter or explicit official document descriptors | No search-engine API credential or working live engine was supplied. The real backend contract and directed UTF-8 HTML/text fetching are implemented and mocked deterministically. Live coverage, CAPTCHA behavior and PDF extraction remain unverified/unavailable. |

Official financial field descriptions were read over public HTTPS on
2026-09-12 (no token/account request):
`https://tushare.pro/document/2?doc_id=79`.
The source lists `ann_date`, `end_date`, `roe`, `netprofit_yoy`, and
`ocf_to_or` (operating cash flow / revenue). No undocumented income endpoint
`revenue_yoy` is used as a proxy for profit growth.

Local coverage is explicitly synthetic: the complete-schema fixture has
stored bars and exactly one verified ROE and one ST assertion for its
600000 test entity; financial coverage is 1/5 and risk coverage 1/4.
The fixture announcement is visibly labelled synthetic, not an actual
issuer disclosure. Market-wide real coverage remains unknown.

## Point-In-Time Contract

`data_evidence.py` owns a common envelope:

```json
{
  "code": "600000",
  "business_date": "2026-06-30",
  "announcement_date": "2026-08-20",
  "collected_at": "2026-09-12T01:00:00+00:00",
  "source": "fixture:tushare:fina_indicator",
  "evidence": "fixture:exact-code-date-field",
  "kind": "roe",
  "value": 12,
  "quality": "verified"
}
```

The values above are a synthetic example. Daily values require the exact
business date. Financials require a valid report period no later than the
announcement, which must be visible by the requested business cutoff.
Collection must be no later than the decision's knowledge timestamp.
Timezone-free collection values are rejected. A historical date queried
today does NOT imply that today's collection existed at the historical
decision time. Risk action announcements may precede their effective date,
but this first version does not extrapolate safe risk status to later days.

Missing, invalid, permission-failed, future or conflicting assertions resolve
to unknown. Equal duplicated assertions are harmless; conflicting values
are not resolved by arbitrary source priority. Cache reuse goes through the
same resolver. Legacy bare `st_flag=false`, `audit_flag=false` or aggregate
factor scores do not count as verified evidence.

Stored quote/OHLC Web projections use the same envelope fields, but their
SQLite primary-key locators and `stored_observation` quality do not substitute
for upstream validation or prove a live feed. The existing intraday outbox
column `quote_fetched_at` retains its legacy quote-evidence/source-time meaning
for delivery freshness; actual collection time remains separate on Quote and
the stored-price evidence envelope. No old database timestamps are rewritten.

Auto enrichment now considers missing financial components even when a code
already has an Eastmoney annotation. Incoming and prior evidence lists are
combined, not replaced to hide source conflicts.

## Search And Fetch Contract

All new configuration defaults are inert: `official_evidence_enabled=false`,
empty `official_evidence_search_url`, empty descriptor list. Only the
deep-screen candidate subset can call this path; Web cannot.

An administrator may configure a trusted HTTPS adapter endpoint. It receives
GET parameters `code`, `as_of`, and `kinds`, and must return:

```json
{"status":"complete","items":[]}
```

Empty items mean unknown, never safe. A nonempty item has the following
shape (synthetic example only; not a real official URL):

```json
{
  "url": "https://www.sse.com.cn/disclosure/example.html",
  "code": "600000",
  "business_date": "2026-09-11",
  "announcement_date": "2026-09-11",
  "kind": "st_flag",
  "value": false,
  "title": "Synthetic descriptor example",
  "quote": "公司股票600000自2026-09-11起撤销其他风险警示，恢复正常交易。"
}
```

Alternatively place descriptors in the `official_evidence_documents` JSON
string. The adapter independently fetches each body. It requires the code,
effective business date, exact quote and an explicit publication-date label
matching the descriptor. Only narrow implemented-action/opinion expressions
are accepted. Intent/negation/uncertainty markers are rejected conservatively.
Body-readable documents may be displayed without asserting a risk value.

Default roots: cninfo.com.cn, sse.com.cn, szse.cn, bse.cn. Custom trusted
backend roots require explicit administrator configuration. HTTPS only,
no userinfo/nonstandard port, no redirects, no common credential query
parameters. The adapter cannot visit arbitrary search hits. UTF-8 HTML/text
and simple textual JSON only, at most 1 MB; unsupported PDFs, encrypted
bodies, CAPTCHA, access failure, missing code/date or excessive hit count
remain unknown. There is no LLM guessing from titles or browser dependency.
No claim is made that a public site currently offers this search API.

## Concurrency And Storage

- One persisted global official-risk lease per SQLite file; 90-second lease,
  unique owner token, fenced cache/evidence commit and expired-owner rejection.
- A query has a 45-second total timeout; each HTTP request has a 10-second
  timeout, byte bound, sequential adapter lock and minimum request interval.
- At most three document fetches per candidate; broader results require
  narrowing rather than silent result truncation.
- Success cache has configurable 60-86400 second TTL; empty/error cache is
  capped at 300 seconds. Expired entries are purged and cache size is capped
  at 2000. Configuration fingerprints prevent reusing another source setup.
- Additive tables: `data_evidence_records`, `evidence_fetch_cache`,
  `evidence_fetch_lease`. No plugin business-table migration is run by Web.
- Failed or abandoned owners cannot commit over a successor. Concurrent
  callers without the lease return unknown rather than duplicate fetches.

## Explicit Local Web Connection

Run from the authorized worktree. The path below is a placeholder for an
already authorized LOCAL snapshot, not a production path:

```powershell
py -m webapp.server --database "C:\approved-local-copy\stock_watch.sqlite3" --origin "fixed-authorized-origin" --port 8767
```

Do not paste a Tushare token, pass a credential file as `--settings`, or copy
an actively changing SQLite main file without its consistent WAL state.
Prepare any real consistent copy only with applicable data-access authority.
Browser state contains no provider credentials and never calls third parties.
The settings projection omits search URLs, descriptor bodies and credentials.
Missing/legacy/locked source data is partial/unavailable, not recreated.

Every page exposes source/business-date/collection-time/count information.
Stock detail separates technical history, financial field coverage and risk
coverage, with persisted announcement provenance. A readable connection does
not imply a live feed or a healthy provider. Demonstration markers remain.

## Reproducible Local Checks

```powershell
py -m pytest tools/verification/test_v0145_data_evidence.py tools/verification/test_v0144_web_dashboard.py -q
py tools/verification/web_evidence_fixture.py --database .local_records/new-evidence-fixture.sqlite3
py -m webapp.server --database .local_records/new-evidence-fixture.sqlite3 --origin demo --port 8766
# Requires bundled Playwright on NODE_PATH and installed Edge:
$env:WEB_EVIDENCE_FIXTURE='1'
node tools/verification/web_dashboard_browser.cjs http://127.0.0.1:8766 .local_records/web-evidence-browser
py -m pytest -q
node --check webapp/static/app.js
```

The fixture generator refuses existing output and imports only test stubs
for AstrBot, then creates a fresh complete-schema local database. It reads
no runtime configuration or credentials and does not connect to production.

## Authorization Boundary

W23 remains open: production paths/permissions, provider credentials and
live entitlements, real search backend acceptance, authentication, HTTPS,
external/mobile-network exposure, deployment and plugin-only reload.
No local test authorizes those operations or proves real strategy returns,
ST/audit safety, delivery receipts or real-time prices.

## Local Acceptance Result

2026-09-12: full pytest **253 passed**; Web/evidence focused **40 passed**;
**30 Python files** compiled; both JavaScript syntax checks, configuration JSON
parsing, default-off checks, `git diff --check` and new-file whitespace passed.
Existing deprecation/line-ending warnings remain.

Dedicated Edge/Playwright checked both local datasets at 1440x1000, 768x1024,
390x844 and 320x740: **56 layouts**, **26 desktop/mobile workflow groups**,
zero overflow/broken-icon/blank-canvas/console errors. The fixture API tests
also verify future announcement rejection and unchanged database bytes.
Evidence reports: `.local_records/web-browser/report.json` and
`.local_records/web-evidence-browser/report.json`.

No production read/write, credential access, send, trade, deployment, reload,
Desktop checkout modification, commit or push occurred. The assertions in the
complete-schema fixture are synthetic test data, not real issuer statements.
