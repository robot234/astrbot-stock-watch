# Historical reasoning input freeze

This is **not** a completed news/risk inference or a point-in-time backtest.
It projects the retrospective technical report into one outcome-free JSON input
per selection date. The five-session future closes remain only in the source
report, never in the candidate packets. Each packet and the original inputs
are hashed in `manifest.json`. An existing output directory is not overwritten.

Run with the verified local Python interpreter:

```
C:\Users\DIO\AppData\Local\Programs\Python\Python313\python.exe tools/verification/freeze_historical_inputs.py --report PATH_TO_HISTORICAL_RESELECTION_JSON --index tools/verification/historical_news_index_20260927.json --output-dir NEW_EMPTY_OUTPUT_PATH
```

Only the first selection date has an official retrospective announcement
**index**, not original document text. Records with publication date on the
selection day are omitted because exact release times have not been proven.
The three `complete_reconciled` codes have duplicate rows in their persisted
record lists and fewer unique rows than their reconciled totals; the packets
mark those rows as partial instead of claiming full coverage. For all later
days the index status is `not_collected`. Historical publication availability,
corrections, ST/suspension/limit status, and issuer-news relevance are unproven.

`reasoning_status=not_run` and `reasoning_completed=0` are intentional. Before
independent outcome evaluation, obtain original documents and timestamped
as-of evidence for each day's candidates, perform and freeze actual per-day
reasoning without access to the future-price report, then audit the resulting
decisions and compare against outcomes in a separate step. Raw unadjusted
close changes must not be described as executable returns.
