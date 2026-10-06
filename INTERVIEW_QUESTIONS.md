# Interview Questions

A running Q&A doc, in my own reasoning voice, updated as each layer is built.
Includes the uncomfortable questions on purpose.

## General / scope

**Q: Why did you build this instead of another AWS or Kafka project?**
A: I already have an AWS-heavy project (Airflow/Lambda/S3/Glue) and a
Kafka/RAG project in my portfolio. This one is deliberately cloud-agnostic and
focused on core distributed data engineering fundamentals - identity
resolution, SCD2 history tracking, and cohort analytics - rather than
cloud-specific plumbing.

**Q: Why do your two data sources cover different year ranges (2023-2025 vs.
2021-2024)?**
A: Open Payments and Part D Prescriber data have genuinely different
publication lags (~6 months vs. ~18-24 months). Forcing them into the same
window would mean using stale Open Payments data or unavailable Part D data.
Using each source's actual most-current release is more honest than a
symmetric-looking but artificial alignment. Part D was extended from an
original 2021-2023 lock to 2021-2024 once CY2024 was verified live (real,
filterable rows returned, not just present in a catalog listing) - see
`DECISIONS.md`.

**Q: How did you get historical NPI data if CMS doesn't archive it?**
A: NPPES itself only publishes a current full file plus weekly deltas - no
historical downloads. NBER independently archives monthly NPPES snapshots at
data.nber.org/npi/YYYY/MM. I pulled two snapshots (May 2023 and a late-2025
month) and joined each one's core + taxonomy files on NPI to get real
before/after attribute data. Verified this archive actually has both years
before designing Layer 2 around it, rather than assuming.

## Layer 1 - Identity Resolution

**Q: Why didn't you use ML for the matching?**
A: The comparison space at this data volume (blocking pools of a few
hundred to ~1,300 golden-roster candidates) doesn't need it - Jaro-Winkler
plus explicit blocking is fully explainable line by line, which matters
more here than squeezing out marginal accuracy with a trained model I'd
then have to justify and validate separately. It's also the same reasoning
behind keeping the scoring logic as plain Python rather than a distributed
Spark UDF - right-sized to the actual problem, not the biggest tool
available.

**Q: Why cardiology, and why California + Montana?**
A: A deliberate high-volume vs. low-volume contrast. It shows up directly
in Layer 1: CA's blocking pools (a few hundred to ~1,300 golden-roster
candidates per block) are large enough that best-in-block matching finds
*some* candidate almost every time, which is exactly what produced the
false positives described below - a smaller-population contrast like
Montana wouldn't have surfaced that. (Layer 3's cohort-size fallback logic
is the other reason this pairing was chosen - see `DECISIONS.md`.)

**Q: How do you know your matching logic is correct?**
A: I didn't trust the first design and it showed: a blended last+first-name
score with single thresholds (0.90 auto / 0.75 review) produced 3,178
"review" pairs (49% of unmatched records) and let an exact-surname/
different-given-name pair auto-match ("PATEL MANDIRA" vs "PATEL NARMISHA").
Switching to independent last/first thresholds fixed the blend problem but
still let "SPIEGEL DAVID"/"SIEGEL DAVID" and "HUSSAIN SYED"/"HUSAIN SYEDA"
through at 0.95/0.90 - both checked by hand since 28 auto-matches is few
enough to fully review, not sample. Tightened to 0.98/0.98, which also
surfaced a many-to-one collision (two different NPIs both matching the same
golden identity) that a separate dedicated check catches. Final: 11
auto_match / 1,220 review / 5,279 no_match out of 6,510 unmatched-source
candidates. See `DECISIONS.md` "Layer 1: threshold validation found real
false positives" for the full three-round writeup.

**Q: What happens to pairs you're not confident about?**
A: They go to `identity_review_queue` with a `status` column - never
silently merged or dropped. A mechanical first-pass (exact rules, not
judgment calls - e.g. same near-exact surname + clearly different first
name + different city = `confirmed_no_match`) resolved 514 to
`confirmed_no_match` and 9 to `confirmed_match` automatically; the rest
went to a 75-pair stratified sample (oversampling the score boundaries) for
me to actually adjudicate by hand - see `data/layer1_validation_sample.csv`.
Of that sample's 61 genuinely ambiguous pairs, I confirmed 5 as matches and
45 as non-matches; the remaining 11 I left `pending` on purpose (see next
question) rather than guess. Final queue state: 559 `confirmed_no_match`,
11 `confirmed_match`, 650 still `pending` (most of the 650 were never
manually reviewed at all - only the 61-pair sample was). The `confirmed_match`
count is 11, not the originally mechanical 14, because 3 rows belonging to
a many-to-one collision (two different NPIs both claiming golden identity
"Lee, David") were forced back to `pending` once that collision check became
a standing pipeline invariant - see `DECISIONS.md`.

**Q: Give me an example of a pair you genuinely couldn't resolve.**
A: "Chu, James" (Orange), "Chao, James" (Pleasanton), and "Chun, James"
(San Francisco) - three different Open Payments/Part D records, three
different California cities - all best-matched to the same golden-roster
entry, "Chou, James" in Panorama City. Jaro-Winkler alone can't tell me
whether that's one physician named Chou who's shown up under three
name-entry variants and three different practice addresses across sources
and years, or three entirely different physicians who all happen to have
similar-sounding surnames and the same first name. Without a date of birth
or a second independent identifier, that's not a judgment call I'm willing
to make from name text alone - it's flagged `pending` in
`identity_review_queue.csv` with a note explaining why, not silently
resolved either direction. Same shape for a few other pairs (e.g. "Li,
Walter" vs. "Lieu, Dale," which recurs identically across both Open
Payments and Part D) - see `DECISIONS.md`, "Manual adjudication of the
61-pair validation sample," for the full list of 11.

## Layer 2 - History Tracking

**Q: What happens if two sources disagree on the same fact?**
A: The precedence rule is by attribute type, not by source. For
identity/profile facts (name, address, specialty), the NPI Registry always
wins - even if an Open Payments or Part D record looks more recent, because
NPI Registry is the authoritative source on *who a provider is*, not the
most-recently-updated one. For transactional facts (payment amount/date,
claim quantity), the source that owns that transaction always wins for its
own records - there's no cross-source override, since Open Payments has no
basis to override Part D's claim counts or vice versa. I rejected two
simpler options: a single source always winning everything (conflates
identity authority with transactional authority - NPI Registry has no
transactional data to be authoritative over) and picking whichever
timestamp is latest across all sources (NPI Registry's dates are snapshot-
pull proxies, not true change dates, so they're not actually comparable in
confidence to a transactional source's exact timestamp). The honest
tradeoff: "NPI wins for identity" can mean Layer 2 prefers a stale NPI value
over a genuinely more current payer-reported address - see `LIMITATIONS.md`.

**Q: Your row-level data quality checks wouldn't catch a pagination bug
that silently truncates a whole pull. What would?**
A: A separate check category I added specifically for that: a pipeline-level
volume/row-count sanity check, distinct from row-level `warn`/`expect_or_drop`/
`expect_or_fail` gates. It groups each bronze table by its own period column
and compares each period's row count to the prior comparable period's,
failing the whole pipeline run if the deviation exceeds ~50%. That threshold
isn't arbitrary - this project's own real corrected multi-year data moves
under 1% (Open Payments) to under 3.5% (Part D) year over year, and I tested
the check against the real historical data from two incidents this project
actually hit: it would have caught the Open Payments 2024 truncation
(~2,500 rows vs. an expected ~78K, a 96.8% deviation) immediately, on the
first appearance of the bad year. The Part D 2021-2023 undercount is a more
interesting case: because all three years were bad by a similar factor, the
2022-vs-2021 comparison alone stays under the 50% threshold - the check only
trips on 2023-vs-2022. Since this pipeline ingests all configured years in
one batch per run rather than incrementally, that still means the bug would
have been caught in the same run that first ingested it, but it's an honest
limitation: this check detects deviation from the *prior period*, not from
ground truth, and a run of periods that are all wrong by a similar stable
factor can partially evade it. I'd rather state that limitation than imply
the check is a complete answer to "did this pull work."

**Q: Why `expect_or_fail` (halt everything) instead of just a loud warning
a human reviews?**
A: Because unlike the one place this project does use `warn` (Open Payments
business-key duplicates, where it's genuinely unresolved whether flagged
rows are real repeated events or a pull artifact), there's no equivalent
ambiguity here. A >50% swing in a comparable period has never once been a
legitimate reporting change in this project's real data - only ever a
pipeline bug, both times it's actually happened. Continuing to build silver
history on top of a batch that's off by that much is exactly the kind of
silent-corruption risk this project already refuses to take elsewhere (the
`stg_golden_roster` `expect_or_fail` checkpoint exists for the identical
reason, just for identity collisions instead of batch shape).

## Layer 3 - Behavioral Analytics

**Q: What's your smallest cohort size, and why didn't that break your
analysis?**
A: N=10 minimum, with a two-tier fallback (specialty-only, then
`insufficient_cohort_data`). Real run result: California never falls back -
even its smallest specialty+state cell (interventional cardiologists) has
17-18 members. Montana always falls back to specialty-only, but the
fallback pools CA+MT by specialty, so Montana's handful of providers get
absorbed past N=10 by California's much larger same-specialty population
every time - `insufficient_cohort_data` never actually fires on this real
data, 0 of 327 provider-year rows. It didn't "break" the analysis; it
revealed something more specific - only 2 of Montana's 31 golden
cardiologists ever have Part D history that resolves to a golden identity
at all, a ~6% resolution rate compounding Layer 1's identity-resolution
funnel on top of Montana's already-small population.

**Q: How do you know that 6% is a real low-prescribing-volume finding and
not Layer 1 quietly under-matching Montana specifically?**
A: I checked, rather than assumed - Montana's 65 unmatched Part D
prescribers (of 67 distinct MT Part D NPIs across 2021-2024, 2 of which
exact-match) went through the identical blocking + Jaro-Winkler scoring
every other unmatched record in this project gets, blocked against all 31
golden MT cardiologists. All 65 land in `no_match`; zero land in `review`.
If Layer 1 were under-matching Montana, I'd expect at least some of those
65 to land in the review band the way CA's unmatched records do (1,220
review-tier rows out of 6,314 CA candidates) - none do, and the scores
aren't close misses either (best combined scores 0.0-0.76, well under the
0.85/0.55 review floor). The committed `identity_review_queue.csv` confirms
this independently: of its 1,220 rows, zero are Montana. Same result for
Open Payments (131 of 131 unmatched MT NPIs also land in `no_match`). See
`DECISIONS.md`, "MT's ~6% Part D resolution rate investigated, not just
reported."

**Q: If the fallback tier never actually fires, how do you know the
`insufficient_cohort_data` logic is correct?**
A: Honestly: it's logically verified (the guard conditions were checked
against real data - e.g. the divide-by-zero protection for a zero-stddev
cohort fires 0 times, confirming it doesn't misfire, not that it's needed)
but not observed firing. I'd rather state that distinction plainly than
imply it's been proven by real data when it hasn't - the same standard
this project already applies to the Layer 2 volume-check's synthetic
replay against real preserved incident data, since that check hadn't
organically fired either.

**Q: Why total Part D claim volume for the z-score instead of payment
amount?**
A: Payment amount (Open Payments) is reserved for the separate,
out-of-scope compound-flag stretch goal ("payment spike") - using it here
too would blur a distinction the project's own docs already draw. Drug
cost was also considered and rejected for the same reason cost share was
rejected for HHI: list-price outliers would dominate. Claim count keeps
both Layer 3 metrics on a consistent basis.

**Q: Why generic name instead of brand name for HHI's drug identity?**
A: Found a real fragmentation bug while building this - a single generic
drug (e.g. "Diltiazem Hcl") can appear as up to 6 different `Brnd_Name`
rows for the same provider-year in Part D. Grouping by the raw row grain
would treat each brand as a separate drug, artificially inflating apparent
variety and understating true concentration. Aggregating to generic name
first fixes it - verified afterward that every real single-drug
provider-year has HHI exactly 1.0, which wouldn't hold if brand
fragmentation were still silently splitting a single drug's claims.

**Q: Is your compound flag (HHI shift + payment spike) a fraud detector?**
A: It's not built, and I made a final call on 2026-07-31 not to build it -
it's out of scope for this project, not a "maybe later." Even as designed,
it would only ever have been a screening heuristic, not a fraud detector:
Open Payments (2023-2025) and Part D (2021-2024) only overlap in PY2023 and
PY2024, two data points, checkable for two-year consistency but not a
trend or a verdict. I closed it out rather than build it because it would
need its own real threshold-validation pass (what counts as a "shift" or a
"spike" - neither is defined, and this project's standard, from Layer 1's
three real validation rounds, is not to ship a threshold unchecked), the
two-year window is thin evidence to validate anything against, and this
project has consistently chosen depth over breadth when the two trade off
(the same reasoning as not adding oncology alongside cardiology). See
`DECISIONS.md`, "Compound flag (HHI shift + payment spike) - final call,
declared out of scope."

## Dashboard

**Q: Does your dashboard read live data, or is it a static demo?**
A: Both, on purpose. The review queue panel reads the committed
`data/identity_review_queue.csv` directly, so it always renders even with
no Databricks connection. The other three panels (quarantine counts,
cohort fallback-tier breakdown, pipeline event log) query the live
Databricks workspace - the same SQL warehouse and Lakeflow pipeline this
project's Layer 2/3 work already runs on - via plain REST calls, using the
same `~/.databrickscfg` token. Without a live connection it degrades to
showing the last known real numbers with an explicit "needs a live
connection" note, rather than a blank panel or a crash - verified both
paths with `streamlit.testing.v1.AppTest` (a real live connection, and a
forced-unreachable one), not just eyeballed in a browser once. See
`DECISIONS.md`, "Dashboard built and run for real."
