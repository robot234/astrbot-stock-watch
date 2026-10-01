# Source probe baseline, 2026-09-23

Read-only Raspberry Pi observations after the 2026-09-23 close. The
2026-09-22 queries are retrospective, not historical freezes. No
production plugin, service, database, or credentials were changed.

## Two genuine closes

BaoStock 0.9.4 returned unadjusted 09-22/09-23 closes:

| Board/control | Code | 09-22 | 09-23 | Status on both days |
| --- | --- | ---: | ---: | --- |
| Shanghai main | 600000 | 9.04 | 8.98 | trading 1, ST 0 |
| Shenzhen main | 000001 | 11.71 | 11.60 | trading 1, ST 0 |
| ChiNext | 300750 | 304.63 | 301.00 | trading 1, ST 0 |
| STAR | 688981 | 122.41 | 121.39 | trading 1, ST 0 |
| ST control | 600053 | 7.72 | 7.43 | trading 1, ST 1 |
| suspension control | 601059 | 15.56 | 15.56 | trading 0, ST 0; volume/amount empty |
| limit control | 000756 | 16.14 | 14.53 | trading 1, ST 0 |

AKShare 1.18.97 up/down pool counts were 63/3 on 09-22 and 51/13
on 09-23. Code 000756 appeared in the 09-22 up pool; code 001234
appeared in the 09-23 up pool. Absence from a pool proves nothing.

The Pi's 09-23 per-stock Eastmoney `f43/100` matched BaoStock's closes
for four board samples. Code 001234 had `f43=f51=3218` (up), 000756
had `f43=f52=1453` (down), while suspended 601059 had `f43=0` (not
a valid trading price). Two quote rounds agreed where responses arrived,
but the first 600000 request failed with `ConnectionError`. Per-code
`f86` timestamps differed. Neither collection time nor pool absence
proves a fresh negative risk state.

The separate production Tushare raw generation published a good,
unadjusted 09-23 partition of 5,556 rows at 15:54 China time. This
does not imply a completed selector freeze: the automatic close job
was `missed` and acceptance `critical` (`candidate_freeze_missing`).

## Official announcement index

The CNINFO HTTPS issuer map binds 600053 to `gssh0600053`. The Pi first
observed a complete 59/59 two-page issuer-scoped index for
2026-04-29..2026-09-23 at 2026-09-23T12:29:07Z. Response SHA-256
page digests: `0aabf57e0c95c27801de1e6b022b7c658da5932ea21a87ba48f23045cae37b31`
and `7d749abae19543dbc3d6aa62410b68599618d4a1ee7639ee47173826107959ea`.
The index's `announcementTime` is normalized to midnight on the date;
it is not a proven publication minute. HTTP `Last-Modified` also
does not prove first public availability.

| Index date | ID | Document / relationship |
| --- | --- | --- |
| 2024-04-16 | 1219623060 | Original 2023 annual report |
| 2025-04-25 | 1223285789 | Original 2024 annual report |
| 2026-04-29 | 1225250216 | 2025 annual report |
| 2026-04-29 | 1225250289 | Separate 2025 audit report |
| 2026-04-29 | 1225250293 | Corrected 2023 audit report |
| 2026-04-29 | 1225250222 | Corrected 2024 audit report |
| 2026-04-29 | 1225250266 | Prior-period accounting-error correction |
| 2026-04-29 | 1225250914 | Delisting-risk and suspension notice |
| 2026-07-01 | 1225400529 | Reply to 2025 annual report inquiry |

These are report-period correction candidates, not a proven one-to-one
old-PDF-to-new-PDF supersession. The 2025 annual PDF was 1,489,076
bytes, SHA-256 `022ef30353c4bc2debf905b3004c202cf753130676bd12843849e552a57080eb`.
Pi `pdftotext` on its first four pages identified the code, year and
standard-unqualified-opinion phrase. The risk-notice PDF was 139,249
bytes, SHA-256 `17d33245d035384f743dcad7593766e4b968d70dc86149450f964a4663a2a63b`.
The standalone audit PDFs were about 84-88 MB and were not parsed in
this bounded run; a large download timed out. Audit and ST are separate.

Repeat on each genuine close with first-observed/source times,
response digests, complete index pagination and explicit negative
suspension/limit evidence for shortlist codes. Unknown stays unknown;
no risk evidence was injected into the selector or used for accuracy.
