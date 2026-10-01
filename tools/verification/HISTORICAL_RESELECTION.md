# Offline historical technical re-selection

Run the standalone CLI against a **consistent SQLite copy** (preferred) or a readable
database, and supply one explicit `published`/`good` Tushare batch covering both endpoints.
The process uses SQLite `mode=ro` and `query_only`; stdout is one deterministic JSON
object, stderr reports errors. It does not create tables, snapshots, or recommendation rows.

```
C:\Users\DIO\AppData\Local\Programs\Python\Python313\python.exe tools/verification/historical_reselection.py --db PATH_TO_CONSISTENT_COPY --batch-id PINNED_BATCH_ID --core core.py --as-of 2026-06-26 --through 2026-09-24
```

For stdin-based remote execution, pass an absolute `--core` path to the installed
plugin's `core.py`; do not assume the remote working directory. No network or
third-party packages are used by this script. Do not stream it to production
until the operator has independently validated a consistent read-only input and
the batch provenance; this tool neither contacts Pi nor chooses a batch.

The universe is the selected batch's trade-date cross section, never saved old
candidates. At each date the top 300 by amount (after the existing price-only
`is_tradable` check) receive existing indicators and `score_quote`. The top ten
technical scores at least ten are selected when history coverage is at least
80%. Scores are recalculated every date. `horizon=5` compares unadjusted closes
on the fifth subsequent session in the pinned batch. `pending` means the horizon
is beyond the requested endpoint; `unknown` means the target bar is missing.
Each day's cross section must have at least 4000 rows (override only in fixtures).
No intraday order or executable return is inferred.
`summary.observed_returns` gives the mean raw close-to-close percent return,
positive/negative/zero event counts, and their explicit observed-only denominator.
Pending and unknown events are excluded; repeated daily candidates count as
separate events, not independent stocks. Empty observed sets report a null mean.

**Not the live full strategy or point-in-time proof.** Historical universe,
ST, suspension, limit status, fundamental factors, announcements, and the batch's
historical publication timing are not proven at June 26. Risk remains `unknown`
without these status fields. The June 26 window starts with only 73 sessions
available since March 11, versus configured `raw_session_count=120`; the code's
indicator minimum is 20, but this does not supply a complete 120-session history.
Every result is explicitly labeled retrospective, technical-only research.
