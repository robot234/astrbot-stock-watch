# Stock Watch Implementation Plan

## Completed Foundations

- [x] P0: immutable per-invocation diagnostics and concurrent report isolation.
- [x] P1: report invariant validation and explicit technical/fundamental/risk coverage semantics.
- [x] P2: reliable market breadth statistics, deterministic ranking, and protected price/watch-range hints.
- [x] P3: isolated offline walk-forward evaluator with chronology and no-production-write tests.

P3 proves evaluator mechanics only. It does not establish that the production ranking strategy is effective.

## Milestone 1: Automatic Close Screening

- [x] Enforce an explicit close scheduler state machine: never scan before 15:00, require a verified open session, and fail closed for an unknown calendar.
- [x] Require a complete, good, same-date close snapshot; retain bounded retry/backoff when publication is delayed without labelling an older date as today.
- [x] Use one durable automatic-close job/invocation key per requested date, persist retry attempts/backoff, recover same-date stale/failed work after reload, terminalize unfinished prior-date jobs as observable `missed`, and close prior-date snapshot requests instead of fetching stale sessions.
- [x] Publish the candidate bundle idempotently once per actual trading date while keeping manual and automatic diagnostics isolated.
- [x] Snapshot allowed subscription destinations and create a durable per-destination delivery outbox whose reload preparation scans only a bounded pending set.
- [x] Retry only confirmed pre-send failures within attempt/time bounds; a timeout, partial chunk send, transport exception, expired in-flight send, or ACK ambiguity becomes `unknown_delivery` and is never auto-retried.
- [x] Expose delivery state and unknown-delivery review needs through logs/status without claiming absolute exactly-once delivery.
- [x] Cover duplicate scheduling, persisted reload backoff, date-rollover `missed`, manual/automatic concurrency, time/calendar states, publication delay, coverage failure, bounded retries, partial/timeout/TypeError delivery ambiguity, lease expiry, ACK ambiguity, and multiple recipients with deterministic tests.
- [x] Pass the complete pytest suite, compile checks, and `git diff --check`.

## Later Milestones

- [ ] Evaluate ranking quality on sufficiently long, point-in-time real datasets; do not tune production from incomplete or leaked samples.
- [ ] Add turnover-change and index/market divergence only after reliable comparable historical turnover and index-baseline interfaces exist.
- [ ] Add downstream delivery idempotency keys or receipt lookup if a platform exposes them; until then, sent-before-ACK crashes remain conservatively `unknown_delivery`.

## Milestone 3: Recommendation Review And Range Evaluation

- [x] Persist an immutable recommendation snapshot with trade date, code/name, candidate and confirmation reference, watch/invalidation/confirmation/target levels, plan version, market regime, source timestamp/freshness, source, caller identity, and price basis.
- [x] Evaluate T+1/T+3/T+5/T+10 only from persisted verified calendar open days and unadjusted daily bars not later than the evaluation date; missing maturity, calendar, suspension/quote, or uncertain price basis remains `pending`/`unknown`.
- [x] Persist close return, interval maximum gain, maximum drawdown, and confirmation/target/invalidation touch state. A same-day daily-bar collision is `unknown_order`, never an invented sequence.
- [ ] Connect a real upstream corporate-action evidence source to the daily-close producer. Until then, M3 comparability remains `unknown` and outcome evaluation fails closed; a producer declaration, provider name, or fixture is not evidence.
- [ ] Complete producer-to-render integration coverage for the explanatory ATR/resistance/risk-reward scenario range, including every fail-closed display branch.
- [ ] Complete command-level permission/concurrency coverage for `/推荐复盘` and `/策略表现`; storage isolation and aggregation are covered locally, but command adapter acceptance remains pending.
- [ ] Broaden deterministic fixtures to include transaction rollback and all producer/render boundary cases; current coverage proves calendar/session, price quality, ordering, visibility, and immutability only.
- [ ] Observe mature real-market outcomes after 2026-09-09. No real return, hit rate, or strategy claim is fabricated before the required trading-day windows have matured.

## Milestone 1 Deployment And Live Acceptance

- [x] Deploy the M1 runtime changes with plugin-only reload. On 2026-09-09, production `main.py` and `storage.py` were replaced from the verified local build after a minimal file/SQLite backup; the AstrBot container was not restarted.
- [x] Complete live automatic-scheduler and date-dedupe acceptance. After reload, the scheduler observed the existing completed `daily_screen:2026-09-09` guard, logged the conservative skip, and created no automatic job, publication, delivery, or duplicate message.
- [x] Complete live stale-request recovery. Eight unfinished prior-date snapshot requests became terminal with cleared leases while the complete 2026-09-08 and 2026-09-09 requests remained complete.
- [x] Verify the deployed durable outbox in an isolated disposable runtime database with no external send: two destinations were prepared, an ambiguous delivery became terminal `unknown_delivery`, and it was absent from the recoverable set.
- [ ] Observe the first eligible production automatic-close publication and delivery on a later trading date without a legacy completed guard. The 2026-09-09 acceptance intentionally exercised dedupe, so it did not fabricate or force a real automatic send.

## Milestone 2: Intraday Signals, Cooldown, And Risk Invalidation

- [x] Build each subscribed session's bounded target set from its watchlist, unexpired verified-calendar close candidates, and configured focus codes, retaining provenance, candidate run, expiry, and plan-version identity.
- [x] Atomically persist a plan-version-scoped FSM trigger and its outbox intent for attention entry, confirmed breakout, invalidation/reference-exit levels, abnormal volume, rapid moves, cost thresholds, and hard risk invalidation; an intent-write failure rolls back the trigger.
- [x] Persist debounce, consecutive confirmation, cooldown, hysteresis rearm, trigger sequence, and non-trigger reasons across plugin reloads; stale, missing, failed-batch, and hard-risk/omitted-signal evidence breaks confirmation continuity.
- [x] Emit one durable, fail-closed risk invalidation for stale/missing data and expired plans, preserving last verified plan/reference evidence without presenting it as a current quote.
- [x] Keep opportunity signals fail-closed for stale/invalid prices, unknown suspension or limit state, invalid plans, expired candidates, suspension, price limits, illiquidity, blocked candidate risk, and missing current whole-market evidence.
- [x] Apply strong/neutral/weak/risk-off live-market confirmation adaptation from a source-timestamped Sina cross-section over the active raw code universe. Evaluate timestamps after collection, reject actual future/stale/mixed-time rows, retain and revalidate the oldest/newest verified bounds for cached/send use, and persist transition confirmations/hysteresis; weak adds confirmation, risk-off suppresses opportunities, and hard price/liquidity/plan gates never relax. Local deterministic coverage only.
- [x] Render bounded event payloads with time, code/name, price, signal/condition/evidence, attention-confirmation-invalidation levels, regime, freshness, risk, provenance, plan version, and invocation identity.
- [x] Isolate `/盯盘状态`, `/暂停盯盘`, and `/恢复盯盘` by message origin and a durable intraday preference, without changing the broader automatic-close/news subscription state.
- [x] Deliver through a durable idempotent event outbox: retry only confirmed pre-send failures; mark timeout, partial-send, transport, expired in-flight, and ACK ambiguity as terminal `unknown_delivery` without resend; cancel queued opportunity events with stale quotes, replaced/expired plans, or unverified candidate state before any transport call.
- [x] Preserve full-market/manual diagnostics during concurrent intraday processing and expose bounded trigger/non-trigger and outbox state through logs/status.
- [x] Cover target provenance/expiry, atomic trigger rollback/restart recovery, threshold crossing/oscillation, cooldown/rearm/reload, plan versions, suspension/limits/liquidity, actual stale/missing partial cycles, hard-risk recovery, fail-closed live-market invalidation, session pause isolation, stale/replaced/expired outbox cancellation, ambiguity/idempotency, and concurrent diagnostics with deterministic tests.
- [x] Pass focused M1/M2 regressions, the complete pytest suite, compile checks, configuration parsing, and `git diff --check` locally.
- [x] Deploy M2 runtime/configuration changes with the verified prior-byte backup `/AstrBot/data/plugin_backups/astrbot_stock_watch_m2_20260909T203228+0800` and one plugin-only reload on 2026-09-09 20:46 +08:00. Production loaded the plugin, created the additive intraday tables, retained `RestartCount=0`, and did not restart the container.
- [x] Deploy the later whole-market live-regime runtime/configuration checkpoint on 2026-09-09 21:35 +08:00 with backup `/AstrBot/data/plugin_backups/astrbot_stock_watch_m2_regime_20260909T213312+0800`; the plugin-only WebUI reload logged a successful load, created `intraday_market_regime_state`, retained `RestartCount=0`, and did not restart the container. Only the eleven `intraday_market_*` schema entries were added to the prior production schema.
- [ ] Observe the first eligible real production intraday trigger and delivery on a future trading session. Local and isolated tests do not establish real-market signal quality or actual downstream delivery.
