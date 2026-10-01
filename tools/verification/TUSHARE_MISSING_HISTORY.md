# Isolated Tushare missing-session fetch

`fetch_missing_tushare_history.py` is a read-only bounded fetcher for the 64
sessions before the existing `2026-06-26` batch. It imports the repository's
`TushareBulkDailyProvider`, calls only `daily(trade_date=YYYYMMDD)`, keeps the
provider in memory, and never opens `StockStore`, Pi, or a production config.

The token is read only from the environment variable named by `--token-env`
(default `TUSHARE_TOKEN`). The value is never printed. If it is absent, the
tool exits with status 3 and records every target day as `unattempted`.

Bounded execution against one day:

```powershell
& 'C:\Users\DIO\AppData\Local\Programs\Python\Python313\python.exe' tools/verification/fetch_missing_tushare_history.py --start-index 0 --max-days 1
```

Use a preconfigured secure environment for the token; do not place it in the
command line, a file in this repository, or output. Increase `--max-days`
only after reviewing the previous JSON evidence. Each verified day records
the source, unadjusted basis, UTC observation time, page offsets, request
count, page digests, row count, unique-code count, market counts, and a
deterministic row digest.

`daily` omits suspended stocks. Therefore a row threshold passing day is a
whole-day bulk sample, while historical universe completeness remains
`unknown_without_bak_basic` unless separately authorized `bak_basic` evidence
is available. The tool never upgrades that status to full historical coverage.

The 2026-09-27 probe is recorded in
`tushare_missing_history_probe_20260927.json`: no safe local token was
available, so no Tushare request was attempted and no bar count or digest is
claimed.
