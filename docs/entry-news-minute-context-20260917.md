# News and minute-level entry context - 2026-09-17

Baseline: `85b3e9c5323ab7e4c7f9276261082f1736b41990`, continuing PR #44.
This slice uses actual provider context in entry admission. It does not claim
that its new timing hypothesis is a profitable strategy.

## Native collection and causality

Both Alpaca data adapters now collect symbol-filtered news and a separate raw
one-minute price/volume stream before capturing the final quotes. The existing
35-hourly-bar strategy context is not replaced. Native adapters require the
new entry_context capsule; failed or missing native context cannot authorize
an order. Replay fixtures and historical snapshots without it remain explicitly
not_recorded, not evidence that the new policy was exercised.

News uses the documented GET /v1beta1/news interface, a fixed 24-hour window,
50 records per page and at most three pages. Pagination exhaustion/cycles,
invalid source identity, duplicate conflicting versions, failed authentication
and provider errors become unavailable data, not zero-news clearance. Original
article IDs, headlines, source URLs, created/updated timestamps and exclusions
are retained. Content versions updated after a historical cutoff cannot be
substituted for their unavailable earlier versions. Later-created articles and
other-symbol articles are excluded. Headline text cannot create a direction.

Minutes use GET /v2/stocks/bars, explicit 1Min/raw/asof=-/feed fields, a 45-minute
request window and bounded pages. Six consecutive completed current-session
minute bars are required for the candidate rule. Future/uncompleted bars and
previous-session substitutes cannot manufacture confirmation.

## Fixed candidate policy: news_minute_trigger.v1

A bullish trigger requires the latest minute close above the preceding three
bars' high and above its own open. A bearish trigger requires the symmetric
break below the preceding three lows and its own open. Latest volume must be
at least the positive median of the preceding five minute volumes. This is
observed volume on the declared feed, not total-market relative volume.

The trigger bar must start at or after the latest retained relevant news
version. Otherwise the entry waits for post-news price evidence. The rule does
not classify a headline as good/bad, predict an earnings surprise, or infer that
an article caused a move. News articles from one provider are not a complete
scheduled earnings/macro calendar.

At both captured and refreshed admission, a direct-symbol quote must still hold
the original minute breakout level. Completed-bar age includes refresh latency
and is capped at 90 seconds. Proxy/index price units remain identified; raw
proxy prices are not numerically compared to an index-scaled quote. Hourly
selection and existing live-thesis/quote/account guards remain active.

These are new, deliberately fixed entry hypotheses, not parameters fitted to
INTC losses or selected to maximize a historical result. Excluded recoveries,
missed winners, unavailable data and fewer trades must stay visible in a valid
market comparison. Study manifests identify this context policy and coverage;
admission statuses show whether recorded context was actually confirmed.

## Evidence and limits

Two baseline regressions reproduced the missing context path: provider context
was discarded, and a context failure did not block an otherwise eligible fake
entry. New tests exercise data failures, paging, exact version/cutoff binding,
news timing, minute gaps/volume/direction, refresh-price rejection, preserved
primary/exit metadata, native adapter requests and replay of retained evidence.
Full exact-commit counts are recorded in the PR checkpoint after execution.

An authenticated read-only provider smoke at approximately 20:36:49 UTC returned
11 SPY articles and 11 minute bars, and 8 QQQ articles and 25 minute bars. The
candidate rejected both directions because the observation was outside its
regular-session window. That proves data connectivity and after-hours rejection,
not profitable selections. No broker endpoints or orders were used in the smoke.

Only native data collection, admission, study context reporting and supporting
tests/docs change. Exits, broker submissions, risk limits, accounting journals,
option selection and fixed opportunity/drawdown/persistence/holding rules remain
unchanged. No runtime settings or production order permissions were changed.

Important remaining limits: a scheduled earnings/macro calendar and sector
ranking are not implemented here. The legacy blackout boolean is not claimed
as calendar verification; explicit event labels and calendar-status fields
retain that uncertainty. Native data reads use the existing provider transport;
extra API latency and retry behavior require hosted cadence acceptance before
production. Page counts are bounded, but this is not a hard wall-clock budget.

Required independent review, protected runtime acceptance and an accepted paper
release remain separate. Isolated software tests and provider connectivity do
not establish a profitable advantage: entry_advantage_established=false.

## Primary API references checked during implementation

- https://docs.alpaca.markets/us/reference/news-3
- https://docs.alpaca.markets/us/docs/historical-news-data
- https://docs.alpaca.markets/us/reference/stockbars
