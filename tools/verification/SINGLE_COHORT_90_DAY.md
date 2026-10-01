# One selection, 90-day retrospective comparison

Selection was frozen on 2026-06-26; 90 calendar days end on 2026-09-24.
This is one cohort, never 65 daily reselections.

Provisional hit: the unadjusted 90-day end close exceeds the selection close.
The report gives counts, hit rates and mean raw change without a pass/fail
accuracy target. Unknown endpoints do not count as wins. The additional
next-session-open comparison is informational, not proof of executable fills.

Run with the consistent offline copy and a new output path:

```
C:\Users\DIO\AppData\Local\Programs\Python\Python313\python.exe tools/verification/evaluate_single_three_month_cohort.py --packet FROZEN/2026-06-26.json --manifest FROZEN/manifest.json --db COPY/synthetic-batch.sqlite3 --batch-id synthetic-archive-plus-pinned-20260927 --holding-days 90 --output NEW_OUTPUT.json
```

Local result: ten frozen technical picks, ten known endpoint bars, zero positive
raw price changes and ten nonpositive changes; observed-only mean -33.9104%.
Raw unadjusted price changes omit dividends, corporate
actions, transaction costs and tradability. Its historical universe, ST,
suspension, limit state, announcement visibility and risk decisions are not
proven. This is not a completed combined news/risk strategy or a buy signal.

One ten-stock cohort cannot establish generalizable strategy accuracy. Any
different filter designed after seeing this endpoint is exploratory. Additional
earlier validated history is needed to derive rules on past-only inputs, freeze
independent selections, and compare them on a separate later holdout. Do not
retrofit the June 26 list to reach a desired percentage.

## Announcement evidence audit (2026-09-27)

The official-index staging manifest contains 30 pre-cutoff PDF originals across
the ten frozen codes: three for 600367 in `staged-originals-600367` and 27 in
`staged-originals-remaining`. The latter 27 PDF digests, page counts, and
extracted text artifacts were independently checked against the manifest and
fresh `pypdf` extraction. Twenty-six of those texts are nonblank; the remaining
eight-page 000100/1225381618 is scanned. A rendering of pages 1, 2, and 7
identifies it as a lawyer's opinion on TCL's second extraordinary shareholders'
meeting, but the full eight pages have not been audited.

Two native `custom/gpt-6-luna` high-effort agents reviewed disjoint groups,
without access to the future-price result. Group A's 15 text summaries lack
page-level PDF review; group B read 11 of 12 texts. SHA-256 confirms identity,
not full contents or historical visibility. Their outputs remain separate from
the frozen technical picks and do not constitute completed news/risk selection.
The three most recent announcements per code do not establish complete news,
correction, ST, suspension, or tradability coverage as of 2026-06-26.

The 0/10 result is the **technical-only baseline**, not the accuracy of a
combined strategy. To test a revised rule without choosing winners in hindsight,
obtain older verified bars, adjustments, and point-in-time evidence, define the
hit metric and rule before viewing an independent holdout, then evaluate all
eligible selections (including abstentions and unknown outcomes). No strategy
accuracy has been validated by this one cohort.
