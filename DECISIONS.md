# Decisions

Every non-trivial design decision, logged in the same session as the code
that implements it. Format: what was decided, alternatives considered, why.

## Scope: Cardiology, California + Montana

**Decided:** Filter all three sources to cardiology providers in California
and Montana only.

**Alternatives considered:** A single large state (e.g. just California) or a
national sample.

**Why:** A single state doesn't force the Layer 3 cohort-size fallback logic
to actually run - with enough California cardiologists, cohort z-scores would
almost always hit the N=10 minimum and the fallback tiers would be dead code.
Pairing a high-volume state (CA) with a genuinely small-population state (MT)
means the fallback and `insufficient_cohort_data` paths are demonstrable, not
theoretical. Oncology is a possible stretch addition, but only after
cardiology is fully working end to end - starting it early would split focus
across two specialties before either is solid.

## Year ranges: Open Payments 2023-2025, Part D 2021-2023 (deliberately offset)

**Decided:** Use each source's genuinely most-current available data rather
than forcing both into the same calendar window.

**Alternatives considered:** Restricting both sources to their overlapping
years only, or picking an arbitrary shared 3-year window regardless of actual
availability.

**Why:** Open Payments publishes with a ~6-month lag (PY2025 is the current
latest release: 17.07M records, $14.67B). Part D Prescriber data publishes
with an ~18-24 month lag (CY2023, released Dec 2024, is the most recent
provider-level file available - CY2024 isn't published yet and CY2025 isn't
expected until late 2026). Forcing symmetric ranges would mean either using
stale Open Payments data or waiting on Part D data that doesn't exist yet.
Using each source's real current release is more honest than a
same-looking-but-artificial alignment. This project will not claim symmetric
"2-3 years across both sources" anywhere in docs.

**Known consequence:** The only calendar year both sources overlap is 2023.
The Layer 3 compound flag (HHI shift + payment spike in the same year), if
attempted as a stretch goal, can only be meaningfully demonstrated for
PY2023 - one data point, not a multi-year pattern. This is accepted scope
going in, not a surprise discovered mid-build.

## NPI temporal snapshots: NBER archive, verified before building on it

**Decided:** Pull two independent NPPES snapshots from NBER's archive
(`data.nber.org/npi/YYYY/MM/`) to get genuine before/after NPI attribute data
for SCD2, instead of relying on a single static current snapshot.

**Alternatives considered:** (1) Use only the current NPPES full file and
treat NPI attributes as never-changing - rejected because it gives Layer 2
nothing real to demonstrate for NPI-sourced facts. (2) Simulate synthetic NPI
changes - rejected as a first choice because faking data undermines the
project's credibility; only used, and always clearly labeled synthetic, if
the archive approach had failed.

**Why this works:** NPPES itself only distributes a current full file plus
weekly deltas - no historical downloads. NBER independently archives monthly
snapshots. Verified directly (not assumed from docs) by fetching the actual
directory listings:
- `data.nber.org/npi/2023/5/` (May 2023): `core_20235.csv` (name, address,
  entity type) + `ptaxcode_20235.csv` (specialty taxonomy codes), joinable by
  NPI, plus `desc_*.txt` field descriptions. Also available in dta/sas7bdat.
- `data.nber.org/npi/2025/07/` (2025 monthly structure continues through at
  least October 2025): `core_202507.*` + `byvar/ptaxcode_202507.*` in csv,
  dta, parquet, and sas7bdat.

Plan: use May 2023 as snapshot 1 and a late-2025 month as snapshot 2 (~2.5
year gap) so real drift has time to accumulate, rather than picking two
snapshots close together and hoping for visible change. Note: NBER's server
returns 403 to bare `curl`/bot-pattern requests without a browser-style
User-Agent header - relevant if a Databricks notebook downloads these files
directly rather than via a pre-downloaded local copy.

**Fallback (not needed - documented per the original spec anyway):** if the
archive hadn't worked out, SCD2 demonstration would have scoped primarily
around Open Payments' correction/dispute mechanic and Part D's
year-over-year facts, with NPI attributes treated as a slowly-changing
dimension snapshotted once now and once later - and any simulated NPI change
would have been explicitly labeled synthetic.

## Layer 1 vs. Layer 2: hand-written PySpark vs. SDP declarative primitives

**Terminology, defined once here since it recurs throughout this file:**
"SDP" is Spark Declarative Pipelines, the `pyspark.pipelines` API this
project's code imports directly (`from pyspark import pipelines as dp` -
see `silver_history_scd2.py`) - an open-source Apache Spark feature, not a
Databricks-only one. "Lakeflow Declarative Pipelines" (used in `DECISIONS.md`
and `SESSION_LOG.md` entries about actually deploying/running Layer 2) is
Databricks' own product name for the managed service that runs SDP
pipelines - the two terms refer to the API layer this project's code is
written against vs. the platform layer it's deployed and run on top of,
not two different or competing frameworks.

**Decided:** Layer 1 (identity resolution) is written as plain, explicit
PySpark. Layer 2 (history tracking) uses SDP's declarative primitives
(`AUTO CDC INTO`, expectations, streaming tables/materialized views).

**Why:** SDP has no built-in fuzzy-matching primitive - blocking on
specialty+zip and scoring name similarity with Jaro-Winkler is exactly the
kind of candidate-generation logic that has no natural declarative shape.
Forcing it into SDP's mold would obscure the matching logic rather than
clarify it, and it's the logic most likely to come up in interview
questions - it needs to read as plain PySpark I can explain line by line.
SCD2, by contrast, is precisely what `AUTO CDC INTO` was built for - writing
it by hand as manual merge logic would just be reimplementing what SDP
already does declaratively, with more room for bugs and less clarity about
intent. This is a deliberate split by what each layer's problem actually
looks like, not an inconsistency in approach.

## Layer 1: threshold validation is manually labeled by me

**Decided:** Validation sample of ~75-100 candidate pairs, labeled match/
no-match by me as the sole ground-truth labeler, oversampling boundary cases
(similar name + different specialty, same name + different address).

**Why:** This is a portfolio project with no team to label data
independently. Stating that plainly here (and in `INTERVIEW_QUESTIONS.md`)
is more credible than implying an automated or independently-validated
ground truth set. Thresholds (auto-match vs. flag-for-review) are chosen from
precision/recall against this sample, not intuition - detail to follow once
Layer 1 candidate generation exists to score.

## Layer 1: review queue, not silent merge or drop

**Decided:** Pairs that fall in the flag band go into a persistent
`identity_review_queue` table with a `status` column
(`pending` / `confirmed_match` / `confirmed_no_match`), never silently
merged or dropped.

**Why:** Silent merging risks combining two distinct providers; silent
dropping loses a record entirely. A queue with explicit status makes the
uncertainty visible and auditable. A sample of 10-20 flagged records will be
manually adjudicated to demonstrate the workflow functions end to end; in
production this would route to a human review team - documented as such
rather than pretending the manual step scales.

## Layer 3: HHI over the full drug universe, claim-count share

**Decided:** Compute each provider's yearly HHI over every distinct drug they
prescribed that year (no top-N truncation), using claim-count share rather
than cost share.

**Why:** Top-N truncation silently changes what the metric means for
providers with different total drug-count variety - a provider with 8 drugs
and one with 80 aren't comparable if both get truncated to the same N.
Claim-count share avoids the metric being dominated by list-price outliers
that cost share would be sensitive to. Scale: 0-1 (sum of squared shares),
stated explicitly rather than left ambiguous between that and the 0-10,000
convention.

**Real bug found while building this, not a hypothetical edge case:**
`silver_part_d_history`'s natural row grain is `(golden_npi, data_year,
Brnd_Name, Gnrc_Name)`. Grouping HHI shares directly by that grain (i.e.
treating each brand-name row as its own "drug") would silently fragment a
single generic drug across multiple brand rows - queried the real data and
found, e.g., "Diltiazem Hcl" split across up to 6 different `Brnd_Name`
values for the same provider-year, each individually claim-small. Left
ungrouped, this would understate concentration (more, smaller shares ->
lower HHI) purely as an artifact of branding, not real prescribing
diversity - a correctness bug, not a style choice. **Fixed:** drug identity
for HHI is `Gnrc_Name` (generic name) - `drug_claims` aggregates
`sum(Tot_Clms)` by `(golden_npi, data_year, Gnrc_Name)` before computing
shares, collapsing brand-name fragmentation first. Pinned with
`tests/test_hhi_calculation.py` - a local pandas reimplementation of the
same formula (sum of squared claim-count shares), checked in isolation
without a Spark session, the same local-validation pattern
`identity_matching_core.py`'s functions already use for Layer 1 - including
a regression test for this exact brand-fragmentation bug, so a future
change to the aggregation grain can't silently reintroduce it without a
local test failing first, not just a real-run sanity check catching it
after the fact.

**Built and run for real, 2026-07-31** (`notebooks/03_analytics.py` Part
B, same run as the cohort z-score - see the tier-split entry above for the
shared population). `provider_intelligence.gold.hhi_part_d_drug_concentration`,
**327 rows** (same population as the cohort z-score, since both read the
same resolved-Part-D-history population). Real sanity checks, run against
the actual written table rather than asserted:
- **Zero rows with HHI outside `(0, 1]`.**
- **Zero single-drug provider-years with HHI != 1.0** exactly (4 real
  examples found: NPIs prescribing only one generic drug that year, 11-16
  total claims each, HHI = 1.0 exactly, confirming the share^2 math is
  correct at the boundary).
- **`corr(n_distinct_drugs, hhi) = -0.32`** - the expected direction (more
  drug variety, lower concentration), not asserted as a hard per-row rule
  but a real aggregate confirmation the metric behaves sensibly.
- Real range: **min 0.0146** (golden NPI `1003914227`, 220-226 distinct
  drugs across years, 33k-35k total claims/year - a genuinely diversified
  high-volume prescriber) to **max 1.0** (several low-volume, single-drug
  providers, e.g. 11-16 total claims/year).

## Layer 3: minimum cohort size N=10, with fallback tiers

**Decided:** Require N=10 providers in a (specialty + geography) cohort
before computing a z-score. Below N=10, fall back to specialty-only
(dropping geography); if still below N=10, flag `insufficient_cohort_data`
rather than compute a z-score on too few peers.

**Why:** N=10 is a pragmatic choice given the filtered data volume (cardiology
+ CA/MT only), not a statistically rigorous minimum. The fallback tiers exist
specifically because Montana's low provider count is expected to hit them -
that's the point of the CA/MT contrast (see scope decision above), not a bug
to engineer around. The proportion of providers landing in each tier
(full cohort / specialty-only / insufficient-data) will be logged and
reported as a real number in the README once Layer 3 runs.

**Real run, 2026-07-31 (`notebooks/03_analytics.py`, on Databricks Free
Edition) - actual tier split, not projected:** 327 provider-year rows
total (a provider-year exists only if that golden NPI has resolved Part D
history that `data_year` - see the cohort z-score metric entry below).
**CA: 323/323 (100%) land in `full_cohort`** - even CA's smallest
(specialty, state) cell, interventional cardiologists, has 17-18 members
per year, comfortably over N=10, so CA never needs a fallback at all.
**MT: 4/4 (100%) land in `specialty_only`** - and **`insufficient_cohort_data`
never fires anywhere in this real dataset, 0 of 327 rows.**

**This is a real, more specific (and more interesting) finding than the
"Montana will hit the fallback tiers" expectation stated above -** the
fallback mechanism is real and exercised, but it never bottoms out to the
third tier, for a reason the original framing didn't anticipate: **only 2
of Montana's 31 golden cardiologists ever have a Part D record that
resolves to a golden identity at all**, across all four years combined
(NPI `1003865171`, interventional, active 2021 only; NPI `1023407806`,
general_cardiology, active 2022-2024) - a ~6% resolution rate for MT
against the golden roster, far below the overlap this project's other
numbers would suggest. Both providers land in `specialty_only` because
Montana alone never has 10 same-specialty peers - but the *specialty-only*
pool is CA+MT combined, not MT alone, and CA's much larger same-specialty
population (61-67 general_cardiology providers with Part D history in a
given year; 17-18 interventional) absorbs Montana's lone provider past
N=10 immediately, every time. **Verified this isn't a bug**, not just
assumed: checked directly against `silver_part_d_history` that these are
in fact the only 2 distinct MT `golden_npi`s ever present, and that the
`specialty_only` cohort sizes (67, 64, 61, 18) match CA's same-specialty
population plus exactly one MT member, not some unrelated number.

**Honestly flagged, not oversold:** the `insufficient_cohort_data` tier's
logic is real, was code-reviewed against its own guard conditions (the
zero-stddev divide-by-zero guard fires 0 times on real data - checked, not
assumed safe), and is exercised in the sense that its condition is
evaluated for every row - but it has never actually been *triggered* by
this project's real data, because the specialty-only fallback's
cross-state pooling turns out to be strong enough on this data's actual
shape that the hardest tier is never reached. This project holds itself to
distinguishing "built and logically verified" from "observed firing on
real data" elsewhere (e.g. the volume/row-count check's synthetic-replay
testing against real preserved incident data, since it hadn't organically
fired either) - stated the same way here rather than implied otherwise.

## Layer 2: partitioning by state, knowingly skewed

**Decided:** Partition history tables by state.

**Why:** State is a natural query dimension here and matches how the
dashboard and analytics slice data. Explicitly acknowledged: California's
cardiology provider count vastly exceeds Montana's, so this partitioning is
meaningfully skewed, not balanced. That skew is accepted as a reasonable
tradeoff given the two states were chosen for contrast, not for even data
distribution - not presented as a clean partitioning scheme it isn't.

## Open question: CMS's data catalog now shows a CY2024 Part D file

**Not yet decided - flagged for the project owner.** While verifying Part D
download URLs (2026-07-26), CMS's dataset catalog (`data.cms.gov/data.json`)
showed `Medicare Part D Prescribers - by Provider and Drug` has a CY2024 file
(`MUP_DPR_RY26_P04_V10_DY24_NPIBN.csv`) published 2026-05-21. The locked
scope above states "CY2024 is not yet published" - true when the spec was
written, no longer true as of today.

This wasn't acted on unilaterally: the CA/MT/cardiology pull for this project
still only covers 2021-2023 as originally locked. Extending to 2021-2024
would need to be a deliberate re-decision, not a silent scope change, because
it also affects the Layer 3 compound-flag reasoning above (currently
justified as "PY2023 is the only overlap year" - a CY2024 Part D file
would add a second overlap year with Open Payments' 2023-2025 range,
which changes that limitation's framing).

## Layer 2: conflict resolution across sources

**Decided:** Attribute-type-based precedence, not source-based:

- **Identity/profile attributes** (name, address, specialty) - **NPI
  Registry always wins**, regardless of how recent a conflicting Open
  Payments or Part D value looks. Effective date = the NPI snapshot pull
  date, documented explicitly as a **proxy** for the true change date, not
  the true change date itself - NPPES gives point-in-time snapshots, not a
  change log, so a detected address change between the May 2023 and July
  2025 snapshots could have actually happened anywhere in that 26-month
  window.
- **Transactional facts** (payment amount/date, claim quantity) - the
  respective transactional source always wins for its own records; there is
  no cross-source override on transactional data, ever (Open Payments
  doesn't know Part D's claim counts and shouldn't be able to overwrite
  them, or vice versa).
- **Narrow fallback:** if the NPI Registry has no snapshot covering a
  provider for a given window, fall back to the most recent transactional
  source's embedded identity value, flagged with a lower-confidence marker.
  This is an exception path for providers missing from both NPI snapshots,
  not a parallel general precedence system - it doesn't apply when an NPI
  snapshot exists but simply disagrees with a transactional source.

**Alternatives considered:** (1) Single-source-always-wins (e.g. "NPI
Registry wins everything") - rejected because it conflates identity
authority with transactional authority; NPI Registry has no transactional
data to be authoritative over, so a blanket rule would be meaningless for
half the fact types Layer 2 tracks. (2) Most-recent-by-date across all
sources, source-agnostic - rejected because the dates aren't comparable in
confidence: NPI dates are snapshot-pull proxies (see above), while
transactional dates are exact, so a naive "latest date wins" would let a
low-confidence proxy date beat a high-confidence exact one.

**Why attribute-type, not source, is the right axis:** the three sources
aren't competing authorities on the same kind of fact - NPI Registry is
authoritative on *who a provider is*, and Open Payments/Part D are each
authoritative on *what happened in their own transactions*. Picking one
source to trust unconditionally, or picking whichever timestamp is
numerically latest, both ignore that distinction. This mirrors the same
reasoning already applied elsewhere in this project (e.g. claim-count share
over cost share for HHI, independent last/first thresholds over a blended
score) - match the resolution method to what the data actually
distinguishes, not to what's simplest to implement.

**Known limitation** (also recorded in `LIMITATIONS.md`): "NPI wins for
identity" is a pragmatic convention based on source authority, not a claim
that NPI Registry is always more current in real-world terms - a provider
could plausibly update their address with a payer before ever updating
NPPES, and this rule would still prefer the (possibly stale) NPI value.

**Unblocks:** `silver_history_scd2.py`'s `AUTO CDC INTO` logic, flagged as
blocked on this decision in "Layer 2: bronze layer scaffolded, silver/gold
deliberately not started" below - that entry's blocking condition is now
resolved. Writing the actual merge logic is still a separate step, not
done as part of locking this rule.

## Layer 2: data quality gate tiers

**Decided:** Row-level gates, tiered by consequence:

- **`warn` (row survives, flagged, not blocked):**
  - `valid_npi` (already implemented, `transformations/bronze.py`) - NPI
    non-null and non-empty on ingest into `bronze_npi_registry` /
    `bronze_open_payments` / `bronze_part_d`.
  - **Business-key duplicates** (new): rows sharing NPI + payer/prescriber +
    amount + date + payment nature, with a distinct `record_id` and
    `change_type == "NEW"` (i.e. not a correction/dispute reissue of an
    existing record, which is expected to carry a different `change_type`).
    Flagged and counted - a `duplicate_flag` column plus a per-batch count -
    **not deduplicated**. See "why warn, not drop" below.
- **`expect_or_drop` (quarantined, doesn't reach silver):** none locked yet.
  No check in this project currently rises to "the row itself is unusable,
  quarantine it but don't fail the whole load" - left genuinely open rather
  than inventing a gate to fill the tier.
- **`expect_or_fail` (stops the pipeline):** none at the row level yet, same
  reasoning. The new pipeline-level volume/row-count check (next entry) is
  this project's first `expect_or_fail`-tier gate, but it's deliberately not
  a row-level check - see below for why that has to be its own category.

**Explicitly excluded from every tier - not a gate at all:** "Part D vs NPI
specialty mismatch" (~96-97% of Part D records carry a specialty label that
doesn't literally string-match their NPI Registry entry's taxonomy text -
Open Payments shows the same shape). This is the base rate that motivates
Layer 1's existence as rule-based identity resolution rather than a
straight join, not a data quality issue Layer 2 should flag - a gate here
would fire on effectively every row, which is a sign it's not a data
quality gate's job. See `README.md`'s framing-statistic callout.

**Why warn, not drop, for business-key duplicates:** it is genuinely
unresolved whether these are real repeated events (e.g. a payer legitimately
issuing the same NPI two payments of the same amount, same nature, same
date, in two different transactions) or a reporting/pagination artifact of
the same general shape as the Part D and Open Payments pulls this project
already found and fixed elsewhere in this file. Dropping rows on an
unresolved question risks silently discarding real data; keeping them with a
visible flag and count keeps the ambiguity honest and auditable instead of
resolving it by fiat. See `LIMITATIONS.md` for this stated as an open
question, not a settled one.

**Why the tiers are otherwise minimal:** every check actually locked here
maps to a concrete, already-observed failure mode in this project's own
data (missing NPIs, the business-key duplicate pattern) rather than a
speculative one - the same standard this project has held itself to
elsewhere (see the Part D pagination bug and Open Payments truncation bug
below). `expect_or_drop`/`expect_or_fail` at the row level stay empty until
a real failure mode calls for them, not filled preemptively.

## Layer 2: pipeline-level volume/row-count sanity checks (new expectation category)

**Decided:** Add a pipeline-level check, distinct from the row-level
`warn`/`expect_or_drop`/`expect_or_fail` tiers above: once per ingested
batch (not once per row), compare the new pull's row count against the
prior comparable period's row count for the same source + filter (same
dataset, same state/specialty filter, prior program/calendar year). A
deviation beyond **~50%** in either direction triggers `expect_or_fail`.

**`expect_or_fail`, not warn - resolved, not left open:** the original
framing of this entry left both tiers on the table ("`expect_or_fail` for a
fully unattended run, or at minimum a loud warn where a human is in the
loop"). Settled in favor of `expect_or_fail`, unconditionally, once tested
against the concrete question this project has used to settle every other
gate-tier call: would this check, as designed, have caught the actual bug?
Yes, immediately, for the Open Payments 2024 case, and yes (though only
once the third bad year arrived - see the implementation entry below) for
the Part D 2021-2023 case. The `warn` tier elsewhere in this project
(business-key duplicates - see "Layer 2: data quality gate tiers" above)
exists specifically because there's a genuinely unresolved question of
whether the flagged rows are real data or an artifact - warn keeps that
ambiguity visible without discarding possibly-real data. There is no
equivalent ambiguity here: this project's own real, corrected multi-year
data shows normal year-over-year movement staying under ~3.5% (Part D) and
under ~1% (Open Payments) - see the threshold reasoning below - so a
deviation actually large enough to cross a 50% threshold has never once
been a legitimate reporting swing in this project's data, only ever a
pipeline bug. Continuing to build silver history (and eventually Layer 3
cohort stats) on top of a batch that's off by that much is exactly the
"silently build on corrupted state" failure this project's other
defensive checkpoint (`stg_golden_roster`'s `expect_or_fail`, in
`silver_history_scd2.py`) already exists to prevent for identity
collisions - this is the same reasoning applied to batch shape instead of
row identity.

**Why this needs its own category, not just another row-level check:**
row-level expectations (`valid_npi`, business-key duplicates) each evaluate
one row in isolation - they cannot catch "this whole batch is 97% too
small," because every individual surviving row can look perfectly valid on
its own. That is exactly the shape of the Open Payments 2024 bug this
project already hit: the puller's pagination loop silently gave up after
repeated connection errors and returned **~2,500 rows instead of the
expected ~78K** - with exit code 0, no error raised, and no row-level
anomaly (every one of those ~2,500 rows was individually well-formed). See
`DATA_SOURCES.md`'s "Lesson learned mid-pull" entry for the full incident.
No row-level check, no matter how well designed, would have caught this -
the failure is about *how many* rows arrived, not what's in them.

**Threshold, and why ~50%:** proposed against this project's own observed
year-over-year stability once pulls are actually complete - Part D's four
corrected years land within an ~83,700-86,700 row band (about a 3.5%
band-width) and Open Payments' three years land within ~78,000-78,600
(under 1%) - see `DATA_SOURCES.md`. A 50% threshold sits far outside that
normal year-to-year noise band but comfortably inside the shortfalls both
of this project's real incidents actually produced - the Open Payments 2024
truncation (~2,500 vs ~78K, a ~97% shortfall) and the original Part D
2021-2023 pagination bug (a 36-135x undercount, i.e. >95% shortfall) would
both have tripped this threshold immediately. Calibrated against two real,
already-observed failures in this project, not a guess.

**Implemented and run for real, 2026-07-31 - see "Layer 2: volume/row-count
sanity check implemented and run for real" below** for the mechanism
actually used (reusing SDP's row-level `expect_or_fail` to express a
pipeline-level assertion, since SDP has no first-class pipeline-level
primitive), the real Databricks run, and two real bugs it surfaced along
the way.

## 2026-07-30: Part D range extended to 2021-2024 (supersedes the original 2021-2023 lock)

**Decided:** Extend the Part D Prescriber range from 2021-2023 (as locked in
the "Year ranges" decision above) to **2021-2024**. This entry supersedes
that lock and resolves the "Open question" entry above - it does not edit
or delete either.

**What changed:** One additional year (CY2024) added to the Part D slice.
Open Payments remains 2023-2025, unchanged.

**Why:** The "Open question" entry above (logged 2026-07-26) flagged that
CMS's catalog showed a CY2024 file but explicitly did not act on it pending
a decision. That verification was redone independently on 2026-07-30,
this time going past the catalog metadata to pull actual rows:

- Raw `data.json` entry for **"Medicare Part D Prescribers - by Provider
  and Drug"** (the dataset this project actually uses - confirmed by
  matching its column schema, including `Brnd_Name`/`Gnrc_Name`, against
  the already-committed 2021-2023 files) shows a CY2024 distribution:
  `modified: 2026-05-21`, `temporal: 2024-01-01/2024-12-31`, dataset id
  `d5aa71a8-dcc0-4570-8bcf-bd39deac69fe`.
- Its `resourcesAPI` lists a live 604,500,065-byte CSV
  (`MUP_DPR_RY26_P04_V10_DY24_NPIBN.csv`), not a placeholder.
- The `data-api/v1/dataset/{id}/data` endpoint, queried directly with
  `filter[Prscrbr_State_Abrvtn]` + `filter[Prscrbr_Type]`, returned real
  CY2024 rows for both CA and MT cardiology providers (e.g. NPI
  `1003017666`, Nitzel, CA, drug-level rows for Amlodipine/Atorvastatin/
  etc.; NPI `1023407806`, Taylor, MT, rows including newer drugs like
  Camzyos/Mavacamten - itself a sign this is genuinely current data, not a
  stale extract). This is stronger evidence than the catalog metadata
  alone: an empty or missing result would have settled the question the
  other way, and it didn't come back empty.

This is no longer "the catalog lists it" but "the data is live, filterable,
and returns real CA/MT cardiology rows" - the standard this project holds
itself to elsewhere (see the NPI temporal snapshot verification above).

**Downstream effect:** Layer 3's compound-flag stretch goal (HHI shift +
payment spike in the same year) was scoped as PY2023-only, because 2023 was
the only calendar year where the original 2021-2023 Part D range and the
2023-2025 Open Payments range overlapped - one data point, explicitly
accepted as a limitation, not a trend. With Part D extended to 2021-2024,
**the overlap is now two years: 2023 and 2024.** The compound flag can now
be checked for consistency (or inconsistency) across two years instead of
asserted from a single year - a materially stronger version of the same
stretch goal, not just more raw data. `LIMITATIONS.md` updated accordingly.

**Also discovered during this verification, not yet acted on:** the
committed `partd_2023_ca_mt_cardiology.csv` (all of CA+MT, all three
cardio types combined) has **2,356 rows / 57 distinct NPIs**. Fully
re-paginating the *same* CY2023 dataset directly via `data-api/v1`, with
the identical state + `Prscrbr_Type` filters used for the committed file,
returns **84,822 rows / 2,232 distinct NPIs** - a ~36x difference, same
year, same dataset, same filters. (Breakdown: CA/Cardiology 70,938,
CA/Interventional Cardiology 10,071, CA/Advanced Heart Failure and
Transplant Cardiology 2,135, MT/Cardiology 1,424, MT/Interventional
Cardiology 254, MT/Advanced Heart Failure and Transplant Cardiology 0.)
This ~36x gap is essentially identical in magnitude to the 2024-vs-
recorded-2021-2023 comparison above, which rules out "2024 was just an
unusually large year" - the original 2021-2023 pull under-captured by
roughly the same factor across the board. Root cause not yet confirmed -
the `DATA_SOURCES.md` write-up shows the actual reproducible code
(`00_data_exploration.py`'s `stream_filter_part_d`) uses a proper
`csv.DictReader` stream, not naive comma-splitting, so a parsing bug is
not the obvious explanation. A silently-truncated multi-GB streaming
download (the same class of failure already caught and fixed for Open
Payments elsewhere in this file - a paginated/streamed pull "succeeding"
with exit code 0 while actually cut short) is a more likely candidate,
but this is not yet confirmed. Flagged here rather than silently fixed:
re-pulling 2021-2023 would change already-committed, already-decided-on
data and is a call for the project owner, not something to do
unilaterally while extending 2024.

## 2026-07-30: Part D 2021-2023 re-pulled with the corrected pagination method

**Decided:** Re-pull CY2021, CY2022, and CY2023 using the same paginated
`data-api/v1` method already proven for CY2024 (not the bulk-CSV stream the
original 2021-2023 pull used), replacing the undercounted committed files.
The originals are kept as `partd_{year}_ca_mt_cardiology_ORIGINAL_UNDERCOUNTED.csv`
in `data/raw/` (gitignored either way) rather than deleted outright.

**Why now, not left flagged:** The prior entry above flagged this as "not
yet acted on" because changing already-committed data unilaterally would
have been a scope decision, not an execution detail. This is exactly the
kind of already-locked-scope change this session's ground rules call for
verifying properly (raw pulled rows, not just re-reading the same suspect
file) before deciding - done below - rather than leaving it as an open
question indefinitely.

**Verification:** Found the exact per-year dataset IDs from
`data.cms.gov/data.json`'s "Medicare Part D Prescribers - by Provider and
Drug" catalog entry (one distribution per year, 2013-2024) - CY2021:
`f68114ed-f854-4ffc-9c6e-ed78b5e2f8d0`, CY2022:
`b101b457-ffa4-49bb-8fd9-27c1266086e2`, CY2023:
`e54db557-cd82-4e91-a0fe-61aad5865d69` (a newer resource ID than previously
referenced - CMS refreshed the CY2023 distribution since the last check;
confirmed same dataset by title and temporal range). Pulled all four years
(2021-2024) fresh through the identical paginated method for full
consistency, not just the two missing years.

**Final, corrected row/NPI counts, CA+MT, 3 cardiology `Prscrbr_Type`
values, all four years now via the same method:**

| Year | Rows | Distinct NPIs | CA NPIs | MT NPIs |
|------|------|----------------|---------|---------|
| 2021 | 83,752 | 2,158 | 2,103 | 55 |
| 2022 | 83,747 | 2,176 | 2,119 | 57 |
| 2023 | 84,822 | 2,232 | 2,176 | 56 |
| 2024 | 86,698 | 2,254 | 2,194 | 60 |

CY2023 and CY2024 reproduced *exactly* the counts already recorded above
(84,822/2,232 and 86,698/2,254) - the same query run twice, independently,
returns the same result, which is itself a useful confirmation the method
is deterministic and correct. All four years now land in the same
~83,700-86,700 row / ~2,150-2,260 NPI range - a stable, plausible shape
year-over-year, unlike the original 2021-2023 files' 1,272 / 719 / 2,356
rows and 33 / 16 / 57 NPIs.

**This confirms the root cause is a systematic pagination bug in the
original bulk-CSV ingestion, not a per-year anomaly:** the ~36x undercount
factor now appears independently in three different years (2021: 2,158 vs.
an implied ~60 under the old method's pattern; 2022: 2,176 vs. 16; 2023:
2,232 vs. 57 - each roughly 36-135x), all pulled via the same
`stream_filter_part_d` bulk-CSV code path that the CY2024/re-pulled years
did not use. The `data-api/v1` paginated method, by contrast, has now
produced consistent, stable, reproducible counts across all four years
independently. Root cause of the *original* bug still not fully isolated
(the streaming `csv.DictReader` code looks correct on inspection - see the
prior entry), but no longer needs to be to move forward: the corrected data
is verified correct by cross-method reproduction (CY2023/CY2024 matching
across two independent runs each), which is a stronger standard than
explaining why the old method failed.

**What changed on disk:** `data/raw/part_d/partd_{2021,2022,2023,2024}_ca_mt_cardiology.csv`
now hold the corrected data. `data/sample/` slices and `00_data_exploration.py`'s
profiling numbers are updated in the same pass - see the corresponding
`DATA_SOURCES.md` update and the following entries.

## Layer 1: blocking + similarity scoring in plain Python, not a Spark UDF

**Decided:** The blocking-key assignment and Jaro-Winkler scoring logic
(`normalize_name`, `name_similarity`, `specialty_bucket_from_*`, `classify`)
is plain, Spark-free Python. `01_identity_resolution.py` still uses PySpark
DataFrames to load the sources, build the golden table, and write the review
queue - only the per-pair scoring step itself is plain Python, called via a
regular Python UDF (not a `pandas_udf`) inside the Spark job.

**Alternatives considered:** Writing the scoring as a `pandas_udf` for
distributed execution, matching how a large-scale production system would do
this.

**Why:** The blocked comparison space here is small - the golden NPI roster
is 1,369 individual CA/MT cardiologists (`entity == "Individual"`), and each
block (state x specialty-bucket) caps the pool further. Distributing a
comparison space this size across a Spark cluster is the kind of premature
scale-engineering this project's constraints explicitly rule out - a plain
Python function called from a regular UDF is simpler, easier to unit-test in
isolation (see `tests/test_identity_matching.py`), and just as correct at
this data volume. The same function is imported and run directly against the
real pulled CSVs with pandas (no Spark session needed) to generate the
candidate pairs used for threshold validation below - one implementation,
two callers, not a divergent "local version" vs. "real version." At a larger
data volume this would be a real limitation worth reconsidering, and is
noted as such in `LIMITATIONS.md`.

## Layer 1: specialty bucketing is a verified-partial taxonomy mapping

**Decided:** For blocking, each source's own specialty signal is reduced to
one of two buckets: `interventional` or `general_cardiology` (the NPI
roster's `heart_failure_transplant` distinction collapses into
`general_cardiology` for blocking purposes - see below).

**Why:** Of the 29 distinct `207R%` taxonomy codes observed in the NBER
ptaxcode file, only `207RI0011X` (Interventional Cardiology) has been
independently confirmed against its NUCC description (matching the
DATA_SOURCES.md writeup). Building a confident label for the other 28 codes
would require pulling the full NUCC taxonomy code table, which wasn't
available to verify directly this session - guessing at labels for codes
this project hasn't checked would violate the "never silently fake data"
rule as surely as fabricating rows would. All unconfirmed codes default to
`general_cardiology` rather than a guessed subspecialty label. Open Payments
and Part D, which carry their specialty as free text, *can* distinguish
"heart failure/transplant" reliably by substring match - but blocking keys
must agree across sources to find candidates, so the coarser 2-bucket split
is what's actually used for blocking; the 3-way bucket is kept only as
descriptive metadata on candidate rows, not as part of the block key.
**Flagged as needs review**, not a full stop: if Layer 1's match rates look
suspicious for interventional-vs-general cardiologists specifically, revisit
by pulling the full NUCC code table rather than continuing to guess.

## Layer 1: matching core is a separate module, not inline

**Decided:** The blocking/scoring functions live in
`notebooks/identity_matching_core.py`, an ordinarily-importable plain Python
file (not Databricks-notebook-formatted), imported by both
`01_identity_resolution.py` and `tests/test_identity_matching.py`.

**Alternatives considered:** Defining the functions inline in
`01_identity_resolution.py` (like `00_data_exploration.py`'s download
functions), with tests importing them via `importlib.util` against the
notebook file directly.

**Why:** Tried the inline approach first. `01_identity_resolution.py`, like
`00_data_exploration.py`, has unconditional `spark.read.csv(...)` calls at
module level once past the function definitions - real Databricks notebook
files execute top to bottom and rely on Databricks injecting a `spark`
global, so this is consistent with the existing project convention, not a
new problem. But it means importing the *whole file* (via `importlib`, since
`01_identity_resolution` isn't a valid dotted-import name) to reach the pure
functions would execute those Spark calls too and crash immediately outside
Databricks - there's no clean way to import "just the top part" of a single
notebook file. Splitting the pure functions into their own real module
sidesteps this entirely: `tests/test_identity_matching.py` does a normal
`from identity_matching_core import ...`, no `importlib` hack needed, and
the notebook imports the same module rather than duplicating the functions.
One implementation, two callers - same principle as the plain-Python-not-
Spark-UDF decision above, just solving the "how do you actually test it"
half of that problem.

## Layer 1: threshold validation found real false positives, not just theoretical ones

**Decided:** Final thresholds - auto-match requires both name components
>=0.98 (last name AND first name independently, not a blended average);
review band is last name >=0.85 AND first name >=0.55; a many-to-one
collision check runs after classification (see below). Locked after two
rounds of real validation against the actual pulled data, not chosen from
intuition and left unchecked - see `LIMITATIONS.md` for how this is framed
there.

**Round 1 - blended score, single threshold (0.90 auto / 0.75 review):**
Produced 3,178 "review"-tier pairs out of 6,474 unmatched source records
(49%) and several incorrect auto-matches. Root cause: taking the single
best-scoring candidate across a large blocking pool (CA general-cardiology
alone has ~900 golden-roster candidates) means *some* wrong candidate will
incidentally score high just from common-surname collisions - real examples
found: "KIM JUYONG" auto-matched-band-scored against "KIM YONG-SIK" (0.8984,
different people), "GANDHI HARESH" against "GANDHI NAINESH" (0.8984), and a
blended score let a perfect surname match paper over a wrong first name:
"PATEL MANDIRA" scored 0.9032 against "PATEL NARMISHA."

**Round 2 - independent last/first thresholds (0.95/0.90 auto):** Fixed the
blended-score compensation problem, cut the review tier to 1,199 (18.5%) -
a much more plausible distribution. But re-checking all 28 resulting
auto-match pairs by hand (small enough to fully review, not just sample)
found two more real false positives even at 0.95/0.90: "SPIEGEL DAVID"
matched "SIEGEL DAVID" (different surname, last_score 0.9571) and "HUSSAIN
SYED" matched "HUSAIN SYEDA" (first_score 0.96 - Syed/Syeda is a male/
female name-suffix distinction in this naming convention, almost certainly
different people). Six more pairs in that same batch (Singh Harpreet/
Harjeet, Kim Hyungjin/Hyun, Nguyen Minh/Minhthao, Singh Raman/Ram, Nguyen
Tuan/Tan, Nguyen Thanh/Thang) had the same shape: one component in the
0.90-0.97 range, not fully exact.

**Round 3 - tightened to 0.98/0.98:** Left only 14 pairs, all with both
components at essentially 1.0. A separate check then found a many-to-one
collision even among these: NPIs `1225276652` and `1619003860` (two
different Part D prescribers, both "Lee, David") both auto-matched to the
same golden NPI `1003832346` - demoted both to review rather than letting
either silently claim the golden identity. **Final: 11 auto_match, 1,220
review, 5,279 no_match**, out of 6,510 total unmatched-source candidate
records (4,040 from Open Payments, 2,470 from Part D).

**Why this matters beyond the specific numbers:** every round found real,
concrete false positives, not hypothetical ones - validating the actual
pulled data against actual thresholds (as opposed to fixing thresholds from
intuition and hoping) caught three different failure modes: blended-score
compensation, near-exact-but-not-quite name collisions, and many-to-one
golden-identity conflicts. This is the strongest evidence available in this
project for why `LIMITATIONS.md`'s stated false-positive risk (no DOB/
gender/other disambiguators) is real and not just a disclaimer.

## Layer 1: review queue first-pass status, mechanical rules only

**Decided:** Of the 1,220 `identity_review_queue` rows, a mechanical
first-pass rule set (`first_pass_status` in `identity_matching_core.py`)
resolves the unambiguous ends: **514 `confirmed_no_match`** (near-identical
surname, clearly different first name, different city - the common-surname-
collision pattern found in Round 1 above, e.g. Kim/Kim, Chen/Chen, Patel/
Patel, Nguyen/Nguyen, Singh/Singh with different given names and different
cities) and **9 `confirmed_match`** (near-exact name, or near-exact surname
plus a plausible spelling/nickname variant corroborated by matching city).
The remaining **697 stay `pending`** - genuinely ambiguous under these
rules, not guessed at.

**Note added 2026-07-30, after the fact:** 3 of the 9 `confirmed_match`
rows above were the "Lee, David" many-to-one collision - the mechanical R1
rule ("both components >=0.98") doesn't know about collisions, so it
confirmed them the same as any other near-exact pair. See "Layer 1:
many-to-one collision check is a standing pipeline invariant" below - the
genuine mechanical-only count is 6 `confirmed_match` / 700 `pending`, not
9/697.

**Why mechanical rules, not full manual labeling, for the whole queue:**
This session's ground rules call for "a first-pass labeling using clearly
stated, consistent rules... put every genuinely ambiguous pair into a
clearly marked 'needs my review' section instead of guessing." Applying
consistent, auditable rules to the full 1,220-row queue (not just a sample)
is different from *guessing* at the ambiguous ones - every `confirmed_*`
row here can be traced to exactly which rule fired and why. A separate,
smaller **75-pair stratified sample** (oversampling the score boundaries
rather than drawn uniformly) was pulled from the review tier specifically
for threshold sanity-checking, independent of the queue's bulk status
assignment - see `SESSION_LOG.md` for that sample's breakdown and the
specific pairs left for the project owner's own judgment.

## Layer 2: bronze layer scaffolded, silver/gold deliberately not started

**Decided:** Set up the SDP pipeline project structure
(`notebooks/pipelines/pipeline.yml`) and write real bronze-layer ingestion
(`transformations/bronze.py` - streaming reads of all three sources' raw
CSVs, unioned across years/snapshots, with a `valid_npi` expectation and an
`_ingested_at` column). Did **not** write `silver_history_scd2.py`'s
`AUTO CDC INTO` logic or any conflict-resolution merge code.

**Why stop here:** This session's ground rules explicitly call this out -
`AUTO CDC INTO` needs a decided conflict-resolution rule for when NPI
Registry, Open Payments, and Part D disagree on the same fact at
overlapping times, and that rule doesn't exist yet (see the still-open
"Layer 2: conflict resolution across sources" entry above, unchanged since
before this session). Writing merge logic ahead of that decision would
mean guessing at the one design choice Layer 2 most depends on - the kind
of design fork this session's rules say to flag rather than resolve
unilaterally.

**Update (2026-07-31): no longer blocked.** The "Layer 2: conflict
resolution across sources" entry above is now decided (attribute-type-based
precedence - NPI Registry wins identity/profile facts, transactional
sources always win their own facts). `silver_history_scd2.py`'s
`AUTO CDC INTO` logic is unblocked but still **not yet written** - locking
the precedence rule and actually implementing the merge logic are two
separate steps, and this entry only closes out the first one.

**Also flagged, not resolved (at the time):** `silver_identity_resolution.py`
already existed as an empty stub before this session, predating the "Layer
1 vs. Layer 2" decision above that locked identity resolution as plain
PySpark, not an SDP step. Left in place with a comment explaining the
conflict rather than deleted, with an explicit removal condition stated in
that comment: remove it once it's confirmed Layer 2's silver layer reads
Layer 1's already-published `golden_provider_table` / `identity_review_queue`
directly, rather than re-deriving identity resolution inside the pipeline.

**Resolved and removed, 2026-07-31 (documentation consistency pass):** that
condition was met - `silver_history_scd2.py` (written 2026-07-31, see
below) does read `golden_provider_table` and `identity_review_queue`
directly via `dp.read(...)`/`spark.read.table(...)`, never re-deriving
identity resolution inside the SDP pipeline. The stub's own stated removal
criterion had already been satisfied for a full session without the file
actually being removed - a real staleness gap this consistency pass caught,
not just a cosmetic one, since the file's comment still read as an open
question after the question itself had been answered elsewhere in this
document. Deleted `notebooks/pipelines/transformations/silver_identity_resolution.py`
rather than continuing to carry it as a stub with nothing left to point to.

**Next to discuss together:** (1) the conflict-resolution precedence rule
itself - candidates include "most recently published source wins,"
"NPI Registry wins for demographic facts, claims sources win for
behavioral facts," or a per-field rule; (2) whether Layer 2's silver
layer should read Layer 1's already-published `golden_provider_table` /
`identity_review_queue` Delta tables directly (probably yes - re-deriving
identity resolution inside the SDP pipeline would duplicate Layer 1's
logic in a place the project's own architecture decision says it doesn't
belong); (3) the data quality gate tiers (`warn` / `expect_or_drop` /
`expect_or_fail`) mapped to specific checks, still an open entry above.

## 2026-07-30: Manual adjudication of the 61-pair validation sample

**Decided:** Of the 61 `needs_my_review` pairs sampled for threshold
validation (see `SESSION_LOG.md` for the full list), the project owner
adjudicated: **5 `confirmed_match`**, **45 `confirmed_no_match`**, and
**11 left `pending`** as genuinely hard cases. Applied to both
`data/identity_review_queue.csv` (the full 1,220-row table) and
`data/layer1_validation_sample.csv`, matched by `(source, source_npi,
candidate_npi)` - not by name text (see the note on a caught mistake
below) - with a `note` column added recording the adjudication and its
date.

**Stated rule, as given:** Match requires near-exact identity - an
exact/near-exact first name plus a plausible surname spelling variant. A
city difference between the source record and the golden-roster candidate
is *not* by itself disqualifying (providers can have multiple practice
addresses across sources/years). Common-surname or common-given-name
coincidences (this data has many: Nguyen, Singh, Patel, Lee, Chen family
collisions) default to no-match without further distinguishing evidence.

**The 5 confirmed_match:** Caffarelli/Capparelli Anthony, Daniels/Danze
Lori, Gong/Ong Timothy, Kong/Kwong Jeremy-Jerry, Spiegel/Siegel David.

**The 11 left `pending` (flagged as hard cases, not adjudicated from name
similarity alone - see `identity_review_queue.csv`'s `note` column, filter
for "hard case"):** the Chu/Chao/Chun vs. Chou James cluster (3 rows, all
three different-city source records converging on the same golden NPI -
"Chou, James," Panorama City); Li Walter vs. Lieu Dale (2 rows, recurs
identically across both Open Payments and Part D); Nguyen Minh vs. Nguyen
Minhthao (2 rows, also recurs across both sources); Nguyen Tuan vs. Nguyen
Tan; Levi Daniel vs. Levine David; Lerner David vs. Lee David; Raz Dan vs.
Razi David. These are the project's concrete example of identity
resolution that name-similarity scoring genuinely cannot settle - the
James cluster in particular (one golden identity, three different source
records, three different cities, none matching the others' city either) is
a good interview example of exactly the "no DOB/gender/other
disambiguator" limitation stated in `LIMITATIONS.md`.

**The remaining 45** default to `confirmed_no_match` per the stated rule
(no evidence beyond a coincidental name resemblance).

**Note on a mistake caught and fixed before it stuck:** the first attempt
at applying this matched rows by name text (`source_last`/`source_first`/
`candidate_last`/`candidate_first`), not by NPI. That accidentally applied
the same adjudication to *other* rows elsewhere in the 1,220-row queue
that happened to share an identical name pair but were never part of the
61-row sample - inflating the no-match count to 47 instead of 45 in the
full queue. Caught by checking the resulting counts against the expected
61-row total before treating the update as done, not discovered later.
Fixed by regenerating both files fresh from the original candidate data
(`all_candidates.csv`, still on disk from the original run) and re-applying
the adjudication keyed on `(source, source_npi, candidate_npi)` instead -
the actual unique identifier for each row, not a text field that can
legitimately collide across genuinely different people.

**Reconciling the counts against the request:** the request's summary
counts (5 match / 49 no-match / "7 pairs" open) don't arithmetically
reconcile against 61 total (5+49+7=61 only if "7 pairs" means 7 rows, but
the named list under "these 7 pairs" enumerates 11 actual rows - a 3-row
cluster plus two 2-row recurring cases plus four 1-row cases). Resolved by
following the explicit named list (unambiguous) rather than the summary
tally (which doesn't reconcile either way): 5 confirmed_match, 45
confirmed_no_match, 11 pending-as-hard-case. Flagged here rather than
silently picking whichever total the summary implied.

## 2026-07-30: Layer 1: many-to-one collision check is a standing pipeline invariant

**Decided:** The many-to-one collision check (Step 5 of
`01_identity_resolution.py`) is a permanent, tested part of the pipeline,
not a one-off script that happened to catch the "Lee, David" collision
during this session's validation run. Concretely: (1) the detection logic
is extracted into `find_many_to_one_collisions` in
`identity_matching_core.py` - the same plain-Python, unit-tested module the
rest of Layer 1's matching logic already lives in, not inline notebook
code - and covered by a new test in `tests/test_identity_matching.py`; (2)
rows demoted by this check are now marked with a `collision_flag` column and
forced to `status = "pending"` in the `identity_review_queue` build (Step
7), rather than falling through to the mechanical `first_pass_status`
rules.

**Why the Step 7 change is a real bug fix, not just a refactor:** a
collision-demoted row cleared the 0.98/0.98 auto-match threshold on both
name components by definition - that's *why* it was `auto_match` before
Step 5 demoted it to `review`. Without the override added here,
`first_pass_status`'s R1 rule ("both components >=0.98 -> confirmed_match")
would silently re-confirm exactly the collision Step 5 just flagged,
completely undoing the check. This was not a hypothetical: all three review
rows tied to the real "Lee, David" collision (source NPIs `1225276652` -
appearing once via Open Payments and once via Part D, the expected/non-
collision case - and `1619003860`, the actual second distinct source
identity) were sitting in the committed `identity_review_queue.csv` marked
`confirmed_match`, put there by the mechanical rule exactly as described
above. Corrected in place: those 3 rows are now `pending` with a `note`
explaining why. Queue totals updated project-wide accordingly - **559
`confirmed_no_match`, 11 `confirmed_match`, 650 `pending`** (previously
recorded as 559/14/647; the 3-row shift is entirely this fix, no other
adjudication changed) - see `LIMITATIONS.md` and `INTERVIEW_QUESTIONS.md`.

**Why this matters beyond these 3 rows:** the collision check existed in
code already, but its output was only as strong as what happened to rows
after it ran - a check that flags a conflict and then lets a *different*,
unrelated rule silently reverse the flag isn't actually protecting against
the many-to-one failure mode it was built for. This is the same class of
lesson as the Part D pagination bug and Open Payments truncation bug
elsewhere in this file: a component working correctly in isolation isn't
the same as the pipeline as a whole behaving correctly end to end.

## 2026-07-31: `silver_history_scd2.py` written - AUTO CDC INTO merge logic

**Decided:** Implement Layer 2's silver layer now that the conflict-
resolution precedence rule is locked. Three silver tables, not one, because
the precedence rule itself splits cleanly along attribute type:

- **`silver_provider_identity_history`** (SCD2) - identity/profile
  attributes (name, address), fed *only* from `bronze_npi_registry`, joined
  to Layer 1's golden roster. Open Payments and Part D identity fields are
  never read here at all - the precedence rule isn't applied as a per-row
  "pick a winner" comparison, it's implemented as "this table structurally
  cannot be influenced by the losing sources." `snapshot_month` is the
  `sequence_by` column, explicitly carried as a proxy for the true change
  date (see the conflict-resolution entry above) via an
  `effective_date_is_proxy` column, not silently indistinguishable from a
  real transactional timestamp downstream.
- **`silver_open_payments_history`** and **`silver_part_d_history`** (both
  SCD2) - transactional facts, each fed only from their own bronze table,
  scoped to rows whose source NPI resolves to a golden identity (exact
  match or Layer 1 `confirmed_match`). No cross-source merge logic exists
  between these two tables or with the identity table - "no cross-source
  override, ever" is enforced by these simply never joining on anything but
  their own source's data plus the golden-NPI resolution map.

**The two conflict-resolution fallback triggers were separated into
distinct, explicitly-named code paths, not one generic fallback branch**
(re-examining this while writing the code, not just restating the original
decision):

- **Trigger A - a golden NPI with no NPI Registry snapshot row at all** -
  considered and found **structurally unreachable** in this pipeline as
  built, not implemented. `golden_provider_table` is constructed directly
  from the July 2025 NPI Registry extract (`01_identity_resolution.py` Step
  2), so every `golden_npi` has at least one NPI Registry row by
  construction - there is no code path that could produce a golden identity
  without one. Documented as a comment in `silver_history_scd2.py` rather
  than built as dead code that could never execute; if Layer 1's
  golden-roster construction ever changes to admit identities from a
  non-NPI source, both that comment and this entry need revisiting.
- **Trigger B - a golden NPI present in the 2025-07 snapshot but absent
  from the 2023-05 one** - the real, reachable case (a provider who
  received their NPI, or was first captured by NBER's archive, sometime in
  the ~26-month gap between the two pulls). Implemented as
  `stg_identity_fallback_backfill`: the earliest transactional record
  (across both Open Payments and Part D) for that golden NPI stands in as a
  single low-confidence identity row, sequenced with a sentinel
  `snapshot_month = "0000-00"` so it sorts before any real NPI Registry
  row, and marked `source_of_truth = "transactional_fallback_low_confidence"`
  so it's never confused with a real NPI Registry-sourced row downstream.

**Business-key duplicate check (Layer 2's `warn`-tier gate, locked
previously) implemented on a staging table, not directly on
`bronze_open_payments`:** the original tier decision said this would
"most naturally" live on the bronze table, but the business key includes
`golden_npi`, which doesn't exist until after identity resolution is
joined in - so it's computed in `stg_open_payments_resolved` (a window
count over NPI+payer+amount+date+nature, restricted to `change_type ==
"NEW"`) via `@dp.expect("no_unexplained_business_key_duplicate", ...)`,
matching the locked behavior exactly: flagged and counted, every row still
passes through to `silver_open_payments_history` regardless.

**Defensive checkpoint added, not originally scoped, found necessary while
writing this:** `stg_golden_roster` re-reads `golden_provider_table` and
computes `is_duplicate_golden_npi` (a window count per `golden_npi`) behind
`@dp.expect_or_fail`. This is the concrete implementation of "confirm
Layer 1's many-to-one collision check ran upstream before this data feeds
SCD2 processing" - rather than trusting that invariant silently, Layer 2
re-verifies it and fails loudly if it's ever violated (e.g. Layer 1's
check regresses, or golden_provider_table is republished by something that
bypasses it). Every downstream table in this file reads through
`stg_golden_roster`, not `golden_provider_table` directly, so this check
can't be accidentally skipped by a table that forgets to call it.

**Not implemented in this pass:** the pipeline-level volume/row-count
sanity check (separate category, mechanism still open - see "Layer 2:
pipeline-level volume/row-count sanity checks" above) and the still-not-
built gold layer (`gold_analytics.py`, Layer 3's cohort z-scores/HHI).

**Not run against real data:** like the rest of this project's SDP pipeline
code, this was written and reasoned through without a Databricks cluster
available in this environment (no Spark/Java installed here - see
`SESSION_LOG.md`, 2026-07-30 17:36 entry) - syntax-checked
(`python -m py_compile`) but not executed. Flagged here rather than
presented as validated, consistent with this project's standard for every
other piece of code that couldn't be run against real infrastructure.

## Layer 2: run for real on Databricks Free Edition, 2026-07-30/31

**Decided:** Get a real Databricks workspace and run Layer 2 against it
end to end, rather than continuing to reason about the pipeline from
`py_compile` alone.

**"Community Edition" no longer exists:** it retired 2026-01-01, replaced
by **Databricks Free Edition** - verified against Databricks' own docs
before signing up, not assumed. Free Edition is serverless-only compute
(no cluster config), but does support Unity Catalog and Lakeflow
Declarative Pipelines (quota: one active pipeline per pipeline type - this
project only defines one, so no constraint in practice). `requirements.txt`'s
comment referencing "Community Edition" is now stale wording, corrected
here rather than silently left wrong.

**Infrastructure stood up via the Databricks CLI** (installed via `winget`,
authenticated with a workspace personal access token the project owner
generated and handed over - never committed to the repo, stored only in
`~/.databrickscfg` outside the project directory): catalog
`provider_intelligence`, schema `bronze` (pipeline default, per
`pipeline.yml`) and schema `raw`, and a Unity Catalog volume at
`/Volumes/provider_intelligence/raw/source_files` matching `bronze.py`'s
hardcoded `RAW_VOLUME` exactly. Catalog creation via the CLI's own
`catalogs create` REST call failed ("Metastore storage root URL does not
exist") even though Default Storage is enabled account-wide; creating it
via a SQL statement (`CREATE CATALOG ...` through the Statement Execution
API against the workspace's serverless SQL warehouse) picked up default
storage correctly where the bare REST call didn't - a real quirk of this
workspace's Free Edition metastore config, not something documented
anywhere obvious. The exact files `bronze.py` reads (not the full
`data/raw/` directory - `core_*`, `ptaxcode_*`, and the `_ORIGINAL_UNDERCOUNTED`
Part D files are excluded) were uploaded into the volume in matching
subfolders.

**A prerequisite gap found before Layer 2 could even start:**
`golden_provider_table` and `identity_review_queue` - the two tables
`silver_history_scd2.py` reads via `spark.read.table(...)` - had never
actually been persisted anywhere. `01_identity_resolution.py`'s closing
`.write.format("delta").saveAsTable(...)` calls were written but never
executed against real infrastructure (same reason Layer 2 itself was
unrun); only a CSV snapshot of `identity_review_queue` existed. Ran
`01_identity_resolution.py` for real first, specifically so Layer 2 would
have real tables to read - not originally scoped as part of "run Layer 2,"
but structurally required by it.

**Real bugs found running `01_identity_resolution.py`, in the order hit -
none of these were catchable by `py_compile`:**

1. **`BASE_PATH = "/Workspace/Repos/provider-intelligence-platform/data"`
   assumed a Databricks Repos checkout that was never set up.** This
   project's actual raw-file convention (matching `bronze.py`) is the
   Unity Catalog volume, not Repos. Replaced `BASE_PATH` with a
   `RAW_VOLUME` constant identical in spirit to `bronze.py`'s, and fixed
   every `spark.read.csv(...)` call to match. A second, smaller version of
   the same gap: `ptaxcode_202507_cardiology.csv` (needed for specialty
   bucketing) had never been uploaded at all, since only the exact files
   `bronze.py` reads were uploaded initially - added once this notebook's
   actual read list was known for real, not guessed in advance.
2. **`from identity_matching_core import (...)` failed with
   `NotebookImportException: ... Importing notebooks directly is not
   supported.`** Uploading a `.py` file via `databricks workspace import`
   with `--language PYTHON --format SOURCE` always creates a **notebook**
   object, even without the `# Databricks notebook source` header - and
   Python cannot `import` one notebook from another with a plain `from X
   import ...`. Fixed by re-importing `identity_matching_core.py` as a
   plain **workspace file** instead (target path keeps its `.py`
   extension, `--format AUTO`, no `--language` flag) - Databricks
   distinguishes "Files in Workspace" from notebooks by exactly this
   combination, and only a file (not a notebook) is importable as an
   ordinary Python module.
3. **`dbutils.library.restartPython()` after `%pip install jellyfish`
   silently wiped `RAW_VOLUME`**, defined in an earlier cell, producing
   `NameError: name 'RAW_VOLUME' is not defined` several cells later - a
   real ordering gotcha (restart resets the whole Python process, so
   anything defined before it is gone after, even though the notebook
   reads top-to-bottom). Fixed by moving `RAW_VOLUME`'s definition to
   after the install cell, and dropped the `restartPython()` call
   entirely once it was clear a fresh session doesn't need it (nothing had
   `jellyfish` imported yet to invalidate).
4. **`spark.sparkContext.broadcast(golden_by_block)` failed with
   `[JVM_ATTRIBUTE_NOT_SUPPORTED]`** - Free Edition's serverless compute
   blocks direct SparkContext access entirely; broadcast variables in the
   classic sense aren't available outside a classic single-user cluster.
   Fixed by dropping the explicit broadcast and letting the `best_candidate`
   UDF close over `golden_by_block` as a plain Python global instead -
   PySpark still ships it to executors via normal closure pickling, and at
   this data volume (1,369 rows across ~4 blocks) the efficiency loss from
   not broadcasting explicitly is immaterial.
5. **`.cache()` on `all_candidates` failed with
   `[NOT_SUPPORTED_WITH_SERVERLESS] PERSIST TABLE is not supported on
   serverless compute`** - another serverless-specific gap, `.cache()`
   lowers to a table-persist operation serverless rejects outright.
   Dropped; it was a performance hint, not a correctness dependency, and
   irrelevant at ~6,510 candidate rows.

**Real result, matching what the earlier pandas-validated run predicted:**
`golden_provider_table` - exactly **1,369** rows, matching the golden
roster size documented in `LIMITATIONS.md`. `identity_review_queue`'s
mechanical first-pass split (this notebook's own logic, not the separately
committed manually-adjudicated CSV snapshot) - **6 `confirmed_match`, 511
`confirmed_no_match`, 703 `pending`**, summing to 1,220 exactly as
predicted. The real PySpark execution reproduced the pandas-validated
numbers, which is real evidence the ported logic is correct, not just that
it compiles.

**Real bugs found running `silver_history_scd2.py`, after Layer 1's
tables existed for real:**

6. **`databricks pipelines deploy` needs a Databricks Asset Bundle
   wrapper** (`databricks.yml`) - `pipeline.yml` alone isn't deployable;
   the CLI error was explicit about this (`databricks.yml not found`).
   Tried referencing `pipeline.yml` via a bundle-level `include:` first -
   deployed without error but silently created **no pipeline resource at
   all**, only "unknown field" warnings for every one of `pipeline.yml`'s
   keys (`name`, `catalog`, `schema`, `libraries`, `configuration`) -
   because a plain `include:` merges the file's keys as if they were
   bundle-root config, and none of those keys are valid there. Renaming
   the file to `pipeline.pipeline.yml` (matching Databricks' own
   `<project>.pipeline.yml` naming convention for standalone pipeline
   projects) didn't change the outcome on this CLI version - so the fix
   actually used is a `databricks.yml` that defines the pipeline directly
   under `resources.pipelines.<key>`, mirroring `pipeline.pipeline.yml`'s
   content rather than including it. `pipeline.pipeline.yml` (renamed from
   `pipeline.yml`) remains the readable source of truth for the pipeline's
   logical definition; `databricks.yml` is deploy plumbing layered on top,
   not a replacement.
7. **`AnalysisException: Cannot have multiple queries named
   ...silver_provider_identity_history ... Additional queries on that
   table must be named.`** Two `dp.create_auto_cdc_flow(...)` calls target
   the same `silver_provider_identity_history` table (one from
   `stg_npi_identity_attributes`, one from `stg_identity_fallback_backfill`
   - both needed, per the conflict-resolution precedence rule's fallback
   design). Lakeflow requires each flow into a shared target to have an
   explicit, distinct `name=` once there's more than one - without it,
   both default to the target table's own name and collide. Fixed by
   adding `name="npi_identity_attributes_flow"` and
   `name="identity_fallback_backfill_flow"` respectively. This is exactly
   the kind of "syntactically valid, wrong against real execution
   semantics" bug `py_compile` structurally cannot catch.

**Real result, run against the full CA/MT filtered dataset directly (not a
small slice first - each full pipeline run completed in under 3 minutes,
fast enough that a slice-then-full-dataset iteration loop wasn't needed to
move quickly):**

- `silver_provider_identity_history`: **2,591** rows across **1,369**
  distinct `golden_npi` (matching the golden roster exactly - every golden
  identity has history). **2,579** rows `source_of_truth = "npi_registry"`,
  **12** rows `source_of_truth = "transactional_fallback_low_confidence"` -
  real, non-zero confirmation that fallback trigger (B) (a golden NPI
  present in the 2025-07 snapshot but absent from 2023-05) actually fires
  on real data, not just in the abstract.
- `silver_open_payments_history`: **6,443** rows. The `warn`-tier
  business-key-duplicate expectation flagged **8** rows
  `is_business_key_duplicate = true` - flagged and counted, not
  deduplicated, exactly as designed; still genuinely unresolved whether
  these are real repeated payments or a reporting artifact (see
  `LIMITATIONS.md`).
- `silver_part_d_history`: **13,671** rows.
- The defensive `stg_golden_roster` `expect_or_fail` checkpoint
  (`no_duplicate_golden_npi`) passed without firing, as expected - Layer
  1's own many-to-one collision check had already run cleanly upstream.

## Layer 2: volume/row-count sanity check implemented and run for real, 2026-07-31

**Decided:** Close out the one remaining open item from Layer 2's design
(see "Layer 2: pipeline-level volume/row-count sanity checks" above,
including the `expect_or_fail`-vs-warn resolution added there) before
starting Layer 3.

**Mechanism:** `notebooks/pipelines/transformations/volume_sanity_check.py`,
one small table per source (`stg_volume_check_npi_registry`,
`stg_volume_check_open_payments`, `stg_volume_check_part_d`). Each groups
its bronze table by its own partition column (`snapshot_month` /
`program_year` / `data_year`), producing one row per period carrying that
period's `row_count` and - via `F.lag(...)` over a window ordered by the
partition value itself - the prior comparable period's row count and the
resulting `pct_deviation`. `@dp.expect_or_fail("volume_within_tolerance",
...)` is then applied to those period-rows, not to the underlying source
records. This deliberately repurposes SDP's row-level expectation
mechanism to express a pipeline-level assertion, since SDP has no
first-class pipeline-level primitive - the "rows" being checked are
periods, not source records. Runs after bronze (`dp.read("bronze_...")`)
and before silver reads the same bronze tables directly, matching the
"post-ingestion validation step" option from the original design entry.

**Placement caveat, found empirically, not assumed:** an `expect_or_fail`
violation aborts the *update* (Lakeflow marks the whole run `FAILED`, not
just the offending table) - confirmed for real below. It does **not**
create an explicit DAG dependency forcing silver to wait on the volume
check: `stg_open_payments_resolved` reads `bronze_open_payments` directly,
not through `stg_volume_check_open_payments`, so the two run concurrently.
In the real failing run captured below, `silver_open_payments_history`
logged `has COMPLETED` (its streaming microbatch write committed) *before*
the update was reported `FAILED` two seconds later. So this design
guarantees the pipeline **run** ends in a visibly failed state - which is
what an orchestrator or a human watching the run would act on - but does
not guarantee zero downstream rows were written in that same run before
the abort registered. Flagged in `LIMITATIONS.md` as a real, observed gap,
not a theoretical one; a stronger design would make silver explicitly
depend on the volume-check table's output as a true DAG barrier, not
implemented now since there's no downstream consumer of silver yet to
protect (see `pipeline.pipeline.yml`'s own "safe to reset while iterating"
comment) - worth revisiting once Layer 3 starts reading silver for real.

**Closed out 2026-07-31, ahead of Layer 3 starting to read silver:**
stated the two concrete production-fix shapes explicitly in
`LIMITATIONS.md` rather than leaving "worth revisiting" open-ended - either
run the pipeline as a single atomic transaction (a failed volume check
rolls back every silver write from that update, not just halts before the
next one), or gate downstream consumption on an overall
pipeline-run-succeeded flag instead of assuming per-table completeness.
Lakeflow doesn't expose the first as something this project controls, so
the second is the realistic path. **Not implemented** - Layer 3's gold
layer (below) reads silver tables directly, inheriting this same
trust-per-table-completeness assumption; stated as the known mitigation
path, not built.

**Real run, current corrected data (2026-07-31, full-refresh, Databricks
Free Edition) - confirms no false positive on legitimate year-over-year
movement:**

| Source | Period | row_count | prior_period_count | pct_deviation |
|---|---|---|---|---|
| npi_registry | 2023-05 | 1,548 | - | - |
| npi_registry | 2025-07 | 1,716 | 1,548 | 10.9% |
| open_payments | 2023 | 78,327 | - | - |
| open_payments | 2024 | 78,617 | 78,327 | 0.37% |
| open_payments | 2025 | 77,998 | 78,617 | 0.79% |
| part_d | 2021 | 83,752 | - | - |
| part_d | 2022 | 83,747 | 83,752 | 0.006% |
| part_d | 2023 | 84,822 | 83,747 | 1.28% |
| part_d | 2024 | 86,698 | 84,822 | 2.21% |

All real deviations land well inside the 50% threshold, consistent with
the ~1-3.5% year-over-year bands the threshold was originally calibrated
against.

**Two real bugs found running this against real data, neither of which
`py_compile` could have caught:**

1. **The first full-refresh run failed for a reason unrelated to the
   check's own logic first: a stale streaming checkpoint.**
   `silver_open_payments_history` failed with
   `DELTA_SOURCE_TABLE_IGNORE_CHANGES` - its upstream source table had been
   overwritten (an `Overwrite`-mode write, version 3) since the last run,
   which a streaming source can't tolerate incrementally. Unrelated to this
   session's change; fixed with `--full-refresh-all`, consistent with
   `pipeline.pipeline.yml`'s own note that a full reset is safe pre-gold.
2. **The next run failed for real, on the check's own logic - but not on
   the scenario it was built for.** `stg_volume_check_open_payments`
   raised `EXPECTATION_VIOLATION`: `{"period":"2023","row_count":78324,
   "prior_period_count":6,"pct_deviation":13053.0}`. Querying
   `bronze_open_payments` directly (via `databricks api post
   /api/2.0/sql/statements` against the workspace's serverless SQL
   warehouse) found **6 rows with a null `program_year`**, forming a
   spurious smallest-sorting "period" that corrupted the real 2023 row's
   `prior_period_count`. Tracing those 6 rows (`SELECT record_id,
   covered_recipient_npi, ...`) showed them arriving in pairs: one row
   with a real NPI but every later column null, paired with one row
   holding fragments like `"Cardiology"` / `"No"` landing in the wrong
   columns entirely. **Root cause: 3 real Open Payments records have an
   embedded newline inside a quoted free-text field** (e.g. a manufacturer
   name), and `bronze.py`'s `spark.read.csv(...)` calls never set
   `multiLine=True` - Spark's default single-line CSV reader splits each
   such record across two physical rows, corrupting both. Python's
   `csv.DictReader` (used earlier in this project to validate real counts
   by hand) parses embedded newlines correctly, which is exactly why this
   was never visible until a check grouped by `program_year` specifically.
   **Fixed at the root:** added `multiLine=True` to all three sources'
   `spark.read.csv(...)` calls in `bronze.py`, not just Open Payments where
   it was actually observed - all three are CMS-published free-text-bearing
   CSVs pulled the same way. **Also hardened the check itself** to
   `.filter(F.col(partition_col).isNotNull())` before grouping, so a
   different future null-partition cause can't silently corrupt the window
   ordering again the same way.
   - **A second, smaller finding from the same bug:** `DATA_SOURCES.md`'s
     recorded Open Payments 2023 count (78,330, "final and verified") is
     itself inflated by these same 3 embedded-newline records - it was
     effectively a physical-line count (matching `wc -l`, which counts the
     3 embedded newlines as extra lines), not a true logical-row count.
     The corrected, `multiLine=True`-parsed count is **78,327**, which
     exactly matches `csv.DictReader`'s independent count on the same raw
     file. `DATA_SOURCES.md` corrected accordingly, with this note.

**Would this check, as designed, have caught the original bugs it was
motivated by?** Tested against the real preserved data for both incidents,
not asserted:

- **Open Payments 2024 truncation** (2023 good -> 2024 truncated to
  ~2,500 -> 2025 good): simulated with the real corrected 2023/2025 counts
  and the real reported truncated count. `2024 vs 2023`: deviation 96.8%,
  **trips immediately** - the very first appearance of the bad year fails
  the check, because a good baseline (2023) already existed in the
  comparison. Pipeline would have aborted before any of the truncated
  batch reached silver.
- **Part D 2021-2023 pagination undercount:** simulated with the real
  preserved `data/raw/part_d/partd_{2021,2022,2023}_ca_mt_cardiology_ORIGINAL_UNDERCOUNTED.csv`
  row counts (1,272 / 719 / 2,356 - all three years undercounted by a
  similar ~36-135x factor, per `DATA_SOURCES.md`). Result, honestly mixed:
  `2021` has no prior, passes trivially (nothing to compare against - not
  a gap in the check, just an inherent limit of any period-over-period
  comparison). `2022 vs 2021`: deviation **43.5%, under the 50% threshold
  - does NOT trip**, because both years are undercounted by a similar
  magnitude, so the pairwise ratio between two similarly-broken years
  stays inside the tolerance band even though both are ~1.5% of the true
  count. `2023 vs 2022`: deviation 227.7%, **trips**. Because
  `bronze_part_d` unions every configured year into one table build per
  run (not one new year appended per run - see `bronze.py`), all three
  bad years would have been evaluated together in a single pipeline
  update, and that update would have failed overall on the `2023` row -
  so **the bug would have been caught in the same run that first ingested
  it**, just not because of the earliest bad year specifically. Flagged
  as a real, non-obvious limitation in `LIMITATIONS.md`: this check
  detects a *deviation from the immediately preceding comparable period*,
  not a deviation from ground truth - a run of periods that are all wrong
  by a similar, stable factor can partially evade it, and only gets caught
  once a period breaks that pattern (here, 2023's pull happened to be
  undercounted by a different factor than 2021/2022's, which is what
  actually tripped it, not the mere fact that all three were wrong).

## Layer 3: `golden_provider_table` was missing specialty - real bug found starting Layer 3, not a hypothetical gap

**Found while building, not designed around in advance:** Layer 3's own
locked spec (cohort z-scores grouped by specialty + geography) and
`LIMITATIONS.md`'s already-written Layer 3 section (the specialty-taxonomy
cross-reference entry) both assumed a per-provider specialty signal would
be available downstream of Layer 1. It wasn't. `01_identity_resolution.py`
Step 2 computes `bucket` (`interventional` / `general_cardiology`, the
verified-partial NUCC mapping - see "Layer 1: specialty bucketing is a
verified-partial taxonomy mapping") and uses it for blocking, but the
`golden_table` persisted to `golden_provider_table` in Step 8 never
selected it - confirmed by `DESCRIBE provider_intelligence.bronze.golden_provider_table`
returning only `golden_npi, plname, pfname, plocstatename, ploccityname,
source_of_truth`, no specialty column at all, and by re-reading Step 8's
`golden_table = golden_roster.select(...)` call directly. Geography
(`plocstatename`) made it through; specialty didn't - an asymmetric
oversight, not a deliberate scoping choice, since the whole point of the
`bucket` computation upstream was to feed blocking, not to be discarded
after.

**Fixed:** added `F.col("bucket").alias("specialty_bucket")` to
`golden_table`'s select in `01_identity_resolution.py` Step 8 - the
minimal change, reusing the column Layer 1 already computes rather than
re-deriving specialty in the gold layer from raw ptaxcode data (which
isn't even in `bronze.py`'s ingestion set - only `bronze_npi_registry`'s
core file is, not the taxonomy file). This keeps "golden roster is Layer
1's one published source of provider attributes" true, matching how Layer
2 already reads `golden_provider_table` rather than re-deriving identity
resolution.

**A second, real bug surfaced immediately by re-running with the fix:**
the first re-run attempt (and its automatic retry) both failed with
`DELTA_METADATA_MISMATCH` - `golden_table.write.format("delta").mode("overwrite").saveAsTable(...)`
does not implicitly widen an existing Delta table's schema, even in
`overwrite` mode; Spark requires an explicit
`.option("overwriteSchema", "true")` to add a column to an already-existing
table, which the original Step 8 write never needed before because no
prior run had ever changed the schema. Added the option, with a comment
explaining why it's there (not a stylistic default) so a future schema
change doesn't hit the same failure blind. Re-ran a third time after this
fix - succeeded.

**Real, reproduced result confirms the fix is correct, not just that it
runs:** `identity_review_queue`'s status split reproduced **exactly** the
prior run's numbers (6 `confirmed_match`, 511 `confirmed_no_match`, 703
`pending`, summing to 1,220) - deterministic re-run on unchanged source
data, same standard used to validate the Part D re-pull earlier in this
project. `golden_provider_table` - still exactly **1,369** rows, now with
real `specialty_bucket` values: **general_cardiology** 1,311 CA / 30 MT;
**interventional** 27 CA / 1 MT. This is itself a real, useful number for
Layer 3's own documentation: interventional cardiologists are a small
minority of the golden roster (28 of 1,369, ~2%) - directly relevant to
how often the N=10 cohort minimum forces a fallback, see the cohort
z-score entries below.

**Why this belongs in `DECISIONS.md`, not just fixed silently:** this
project's own stated standard (flag genuine design ambiguities/bugs found
while building rather than silently resolving them) applies here exactly
- a downstream layer's locked spec depending on data the upstream layer
never actually published is a real integration gap, the same category as
the Layer 2 conflict-resolution rule being locked before the merge code
could be written, just discovered by trying to build against the real
published table instead of caught by design review beforehand.

## Layer 3: hand-written PySpark, not SDP declarative primitives

**Decided:** Layer 3 (`notebooks/03_analytics.py`) is plain PySpark -
`groupBy`/window-function SQL logic - reading Layer 2's published silver
tables directly via `spark.read.table(...)`, the same pattern
`01_identity_resolution.py` already established for Layer 1. Not
implemented inside the Lakeflow Declarative Pipeline
(`notebooks/pipelines/transformations/gold_analytics.py`, which predates
this decision as an empty stub).

**Why, same reasoning as the Layer 1 vs. Layer 2 split:** cohort-relative
z-scores (conditional group-by with a three-tier fallback:
full-cohort -> specialty-only -> `insufficient_cohort_data`) and HHI
(per-provider-year concentration over the full drug universe) are both
ordinary analytical aggregation and window-function logic - there's no
SDP declarative primitive (`AUTO CDC INTO`, streaming tables,
expectations) that naturally expresses "fall back to a coarser grouping
if the finer one doesn't have enough members" or "sum of squared shares
within a group." Forcing this into SDP's mold would mean either faking a
CDC-shaped problem that doesn't exist here, or writing the same
`groupBy`/window logic inside a `@dp.table` decorator for no benefit over
a plain notebook - style over substance. This mirrors exactly the
Layer 1 reasoning ("no SDP declarative primitive fits analytical
computation like this") applied to the same category of problem again;
`gold_analytics.py` is left in place as a stub with a comment pointing to
this decision and to `03_analytics.py` - unlike `silver_identity_resolution.py`,
which was left flagged rather than deleted when Layer 1's plain-PySpark
decision first made it stale, but was later actually deleted once its own
stated removal condition was satisfied (see "Layer 2: bronze layer
scaffolded, silver/gold deliberately not started" above). `gold_analytics.py`
stays a stub deliberately, not provisionally - Layer 3 living outside SDP
is this project's final answer, not a pending question the way Layer 1's
placement inside Layer 2's pipeline once was.

## Layer 3: cohort z-score metric is total yearly Part D claim volume, not payment amount

**Decided:** The cohort-relative z-score's underlying metric is each
provider's total yearly Part D claim volume - `sum(Tot_Clms)` across every
drug they prescribed that `data_year`, from `silver_part_d_history`.

**Alternatives considered:** (1) Total Open Payments amount received per
year - rejected for the core z-score specifically because it's already
earmarked as the *other* half of the explicitly out-of-scope compound-flag
stretch goal ("HHI shift + payment spike," see `LIMITATIONS.md` and
`INTERVIEW_QUESTIONS.md`) - using it here too would blur a distinction
this project's own docs already draw between the core Layer 3 build and
the stretch goal. (2) Total Part D drug cost (`Tot_Drug_Cst`) - rejected
for the same reason cost share was rejected for HHI below: list-price
outliers would dominate a cost-based z-score in a way a claim-count-based
one avoids, and this project has already committed to claim-count over
cost once (see "Layer 3: HHI over the full drug universe, claim-count
share" above) - using cost here would be an inconsistent basis between the
two Layer 3 metrics for no stated reason. (3) `Tot_Benes` (beneficiary
count) - rejected because it's subject to real, confirmed privacy
suppression (blank, not zero, below CMS's count-of-11 threshold - see
`docs/schema_notes.md`), which would require exclusion/imputation logic
`Tot_Clms` doesn't need. **Verified for real, not assumed:** queried
`silver_part_d_history` directly - `Tot_Clms` has zero null and zero
non-positive values across all 13,671 rows, confirming it carries none of
`Tot_Benes`/`GE65_*`'s suppression behavior and needs no special handling
for this metric.

## 2026-07-31: Dashboard built and run for real - event log, quarantine counts, review queue size, cohort fallback-tier breakdown

**Decided:** Build `dashboard/app.py` (previously a comment-only stub) per
the locked spec - event log, quarantine counts, review queue size, cohort
fallback-tier breakdown - as a real Streamlit app, and actually run it
against real data rather than leaving it written-but-unverified.

**Two data sources, not one:** the review queue panel reads the committed
`data/identity_review_queue.csv` directly (always available, carries the
manual adjudication - see "Manual adjudication of the 61-pair validation
sample"). The other three panels (quarantine counts, cohort fallback
tiers, pipeline event log) need a live connection: quarantine counts and
cohort tiers via the SQL Statement Execution API against the same
serverless SQL warehouse this project's Layer 2/3 verification queries
already used, and the event log via the Pipelines REST API
(`/api/2.0/pipelines/{id}/events`) against the deployed
`provider_intelligence_history_tracking` pipeline. Same `~/.databrickscfg`
token this project's other Databricks work already authenticates with (see
"Layer 2: run for real on Databricks Free Edition") - plain `urllib` REST
calls, not the `databricks-sdk` package, to avoid a new dependency for four
read-only queries.

**A real, useful discrepancy surfaced by putting both sources on the same
page:** the live `identity_review_queue` Delta table (Layer 1's own
mechanical first-pass output) shows 6 `confirmed_match` / 511
`confirmed_no_match` / 703 `pending`, while the committed CSV (which
carries the manual adjudication and the many-to-one collision-check fix)
shows 11 / 559 / 650. This isn't a bug in the dashboard - it's the real,
previously-documented fact that the manual adjudication was applied only
to the exported CSV, never re-persisted to the Databricks table (see
`SESSION_LOG.md`, 2026-07-30 "later" entry) - surfaced explicitly in the
dashboard's own UI (an expander comparing the two) rather than silently
picking one source and hiding the gap.

**Verified running, not just written - real numbers pulled live during
this session, 2026-07-31:**
- **Review queue:** 1,220 rows (650 `pending`, 559 `confirmed_no_match`,
  11 `confirmed_match`), from the CSV.
- **Quarantine (`warn`-tier business-key duplicates):** 8 of 6,443
  `silver_open_payments_history` rows flagged, live.
- **Cohort fallback-tier breakdown:** CA 323/323 `full_cohort`, MT 4/4
  `specialty_only`, 0/327 `insufficient_cohort_data` - live from
  `gold.cohort_zscore_part_d_claims`, exactly reproducing the numbers
  already documented in the Layer 3 cohort-size entry above.
- **Pipeline event log:** 25 real events from the deployed pipeline's most
  recent update, live from the Pipelines REST API.

**A real bug caught and fixed by actually running it, not just reading the
code:** `streamlit.testing.v1.AppTest` (runs the script server-side without
a browser, the same harness Streamlit's own test suite uses) surfaced a
real `TypeError` on first run - the SQL Statement Execution API returns
every column as a plain JSON string (`data_array` is text, not typed), so
the cohort-tier count column (`n`) was still a string when
`pivot_table(..., fill_value=0)` tried to fill a missing state/tier
combination with the integer `0`, which pandas' Arrow-backed string dtype
rejects outright. Fixed by casting `tiers["n"] = tiers["n"].astype(int)`
before pivoting. Exactly the kind of bug that only shows up by executing
the real code against a real API response shape, not by reading it - the
same lesson this project has already drawn from its Databricks/Lakeflow
bugs (`py_compile` can't catch it either). Verified clean afterward with
`AppTest` under two conditions: a real live Databricks connection (zero
exceptions, all four panels populated with the real numbers above) and a
forced-unreachable connection via bad `DATABRICKS_HOST`/`DATABRICKS_TOKEN`
env vars (zero exceptions, degrades to the local-only CSV-backed review
queue panel with explicit "needs a live connection" messaging on the other
three, not a silent blank or a crash).

## 2026-07-31: MT's ~6% Part D resolution rate investigated, not just reported - genuine low linkage, not Layer 1 under-matching

**Question:** `LIMITATIONS.md`'s "only 2 of Montana's 31 golden cardiologists
(~6%) ever have a Part D record that resolves to a golden identity" is a
striking number on its own, but on its own it can't distinguish two very
different explanations: (a) Montana genuinely has almost no Part D
prescribing volume among its golden cardiologists (a real, honest
data-availability finding), or (b) Montana's Part D prescribers who *should*
match a golden identity are stuck unresolved because Layer 1's blocking or
thresholds under-match MT specifically (a Layer 1 defect masquerading as a
data finding). Investigated by checking what fraction of MT's full
`identity_review_queue` - not just the golden-roster resolution rate -
links to Part D, per this session's instruction not to just report the
6% figure without digging into it.

**Method:** Reproduced Layer 1's exact candidate-generation logic (same
`identity_matching_core.py` functions: `normalize_name`,
`component_scores_normalized`, `block_bucket_from_text`, `block_key`,
`classify_components`) locally in pandas against the real raw CSVs, scoped
to Montana's unmatched-by-exact-NPI records specifically - the same
local-pandas-reproduction pattern this project has used elsewhere to
validate PySpark logic before/alongside a real Databricks run (see "Layer
1: blocking + similarity scoring in plain Python, not a Spark UDF"). Not
re-run on Databricks itself this time - this is investigative analysis
confirming an existing published result, not a pipeline code change, so the
local reproduction is disclosed as the standard of evidence here rather than
presented as a fresh cluster run.

**Real numbers, Part D, 2021-2024, pipeline-consistent dedup (drop rows with
a null NPI, then one row per distinct `Prscrbr_NPI`):**

| | Golden MT roster | Distinct MT Part D NPIs | Exact-NPI match to golden roster | Unmatched (enter blocking/scoring) |
|---|---|---|---|---|
| Count | 31 | 67 | **2** (the already-published 6% figure) | 65 |

Ran those 65 unmatched MT Part D prescribers through the identical
blocking + Jaro-Winkler scoring used for every other unmatched record in
this project (blocked against all 31 MT golden cardiologists - MT's only
two blocks, `general_cardiology` (30) and `interventional` (1)): **all 65
land in the `no_match` tier. Zero land in `review`.** Best combined scores
across the 65 range from 0.0 to 0.76 (last-name component 0-0.85, first-name
component 0-1.0 individually, but never both simultaneously clearing the
review floor of last>=0.85 AND first>=0.55) - not close misses sitting just
under the threshold, genuinely low similarity to any of the 31 golden MT
names. Cross-checked directly against the committed
`data/identity_review_queue.csv`: of its 1,220 rows, **zero have
`source_state == "MT"`** - every single review-queue row is CA. Ran the same
check for Open Payments (133 distinct MT NPIs, 2 exact match, 131
unmatched) for a second, independent confirmation: same result, all 131
land in `no_match`, zero in `review`.

**Conclusion: this is the low-prescribing/low-linkage explanation, not a
Layer 1 under-matching artifact.** If Layer 1 were under-matching Montana
specifically - too-tight thresholds, a blocking-key mismatch, or a state
filter bug - at least some of MT's 65 unmatched Part D prescribers would be
expected to land in `review` (a plausible but unconfirmed name variant of
one of the 31 golden cardiologists), the same way CA's unmatched records
produce 1,220 review-tier pairs out of 6,314 unmatched candidates. None do.
The 65 MT Part D prescribers whose specialty label is cardiology-adjacent
but who never resolve to a golden identity are, by name, simply not the
same 31 people - consistent with this project's already-documented
~96-97% Part D-vs-NPI-taxonomy specialty mismatch base rate (see
`README.md`'s framing statistic) landing especially starkly in Montana:
its small golden population (31) leaves little room for the kind of
coincidental surname overlap that inflates CA's review queue to 1,220 rows.
**The identity_review_queue's per-state split being exactly 0% MT / 100% CA
is itself worth stating explicitly** (not just left implicit in the
golden-roster stat) - on its own, "zero MT rows in the review queue" could
look like a state-filtering bug in queue construction; it isn't, it's the
predictable result of MT's unmatched records scoring too low to ever enter
the review band in the first place.

**Updated in `LIMITATIONS.md`** (Layer 3's MT resolution-rate entry, now
cross-referencing this investigation and its real numbers rather than
stating the 6% figure without the fraction it was checked against).

## Layer 3: cohort geography sourced from `golden_provider_table` (static), not per-snapshot SCD2 history

**Decided:** Each provider's state (the "geography" half of the cohort
key) comes from `golden_provider_table` (the single July 2025 NPI
Registry snapshot Layer 1 publishes), not from re-deriving it per
`data_year` against `silver_provider_identity_history`'s SCD2 rows.

**Checked, not assumed, that this is safe:** queried
`silver_provider_identity_history` for any `golden_npi` whose
`plocstatename` differs across its two NPI Registry snapshot rows
(2023-05 vs. 2025-07) - **zero** such providers in this project's real
CA/MT cardiology data. Since Part D's claim years (2021-2024) fall inside
or near that snapshot window, and no provider's recorded state actually
changed between the two snapshots, using the static "current" state for
every `data_year`'s cohort assignment produces the same result a
time-varying join would have, for this data. Documented as a checked fact
about this specific dataset, not a general guarantee - if a future data
refresh introduces a real state change between snapshots, this
simplification would need revisiting (a provider's early-year claims
would be grouped by their *later* recorded state, not their state at the
time of the claim).

## 2026-07-31: Compound flag (HHI shift + payment spike) - final call, declared out of scope

**Decided:** The compound-flag stretch goal is **not built, and is now
formally out of scope for this project**, not left as "deferred" or
"next in the queue." Every prior mention of it in this project's docs
(the "Year ranges" decision, the CY2024 Part D extension entry,
`LIMITATIONS.md`, `INTERVIEW_QUESTIONS.md`) called it a stretch goal
contingent on core Layer 3 being done first - core Layer 3 (cohort z-score
+ HHI, both built and run for real against Databricks Free Edition,
2026-07-31) is now done, which makes this the point where "deferred until
X" either gets converted into real work or gets closed out as a deliberate
scope decision. Closing it out, not building it, per this session's explicit
instruction to make an explicit, final call rather than leave it ambiguous.

**Why not build it, given the core work is done and the overlap window
improved:** three reasons, not one:

1. **It would need its own real threshold-validation pass, and this
   project holds a high bar for that** - Layer 1's auto-match threshold
   went through three real validation rounds against actual data before
   being trusted (see "Layer 1: threshold validation found real false
   positives"), specifically because a first-guess threshold shipped
   without checking it against real pairs produced real false positives
   twice. A compound flag needs two new, currently undefined thresholds -
   what counts as a "shift" in a provider's year-over-year HHI, and what
   counts as a "spike" in their year-over-year Open Payments total - with
   no existing precedent in this project to draw either from. Defining
   both from intuition and shipping them unchecked would violate the exact
   standard this project has held itself to everywhere else; validating
   them properly (checking real flagged provider-years by hand, the way
   Layer 1's thresholds were checked) is a real, non-trivial piece of new
   work, not a small addition on top of already-built tables.
2. **The overlap window is still structurally thin.** Part D's extension
   to 2021-2024 (see the entry above) improved the compound flag from a
   single data point (PY2023 only) to two (PY2023, PY2024) - a real
   improvement, but two consecutive years is still a before/after
   comparison, not a trend robust to a single noisy year. A "spike"
   detector validated against only one year-over-year transition has
   less real signal to validate against than any other threshold this
   project has built (Layer 1's had thousands of candidate pairs; the
   volume-check's had four full years of Part D and three of Open
   Payments to calibrate against - see `DECISIONS.md`'s threshold
   entries). This was accepted going in as a known limitation, not
   discovered late, but it's a real reason the stretch goal is weaker
   ground to build confidently on than the rest of this project.
3. **This project has already consistently chosen depth over breadth when
   the two trade off**, and this is the same trade again. The CA/MT scope
   decision explicitly rejected adding oncology "before cardiology is
   fully working end to end - starting it early would split focus across
   two specialties before either is solid" (see "Scope: Cardiology,
   California + Montana" above). The three core layers are now built, run
   for real, and validated against real data, with a strong set of actual
   findings (the Part D pagination bug, the Open Payments truncation bug,
   three rounds of Layer 1 false positives, the MT identity-resolution
   funnel investigated above, the business-key-duplicate ambiguity). Adding
   a fourth feature under the same rigor this project has held itself to
   is a real scope increase, not a small bonus - and a compound flag built
   to a lower standard than the rest of this project (unvalidated
   thresholds, a two-point trend) would be a worse addition than not
   building it at all.

**What this means concretely:** `notebooks/03_analytics.py` and
`gold_analytics.py` are not extended with compound-flag logic.
`LIMITATIONS.md`'s compound-flag entry is updated from "deferred" language
to a final "declared out of scope" statement, cross-referencing this entry.
If a future session wants to revisit this, the two real prerequisites are
unchanged from what this entry already identifies: a validated shift/spike
threshold pair, and an honest framing of a two-year window as a screening
heuristic, not a trend - not a new investigation, just work that was never
started.

## 2026-08-11: Formal precision/recall validation of the 0.98/0.98 thresholds - attempted, not completed

**Decided:** Stopping this work incomplete rather than finishing it under
time pressure or fabricating labels to make it look done - out of time
before an interview. Recorded here so this isn't mistaken for either
"never attempted" or "quietly finished."

**What this was:** A follow-up to the round-based validation above (Round
1/2/3, "Layer 1: threshold validation found real false positives"),
intended to add a proper precision/recall measurement instead of reusing
the review-tier sample. Methodology, locked and partly executed:

- **Precision set:** the 14 pairs that cleared 0.98/0.98 in Round 3
  (before the many-to-one collision check demoted 3 of them), as the
  labeled positive-prediction set.
- **Recall set:** a new sample pulled from the full scored candidate pool
  (regenerated locally in pandas from `identity_matching_core.py` -
  verified to reproduce the documented 14/1,220/5,279 tier split exactly,
  see `data/all_candidates_scored.csv`, 6,510 rows, every candidate's
  score, not just the review tier), restricted to pairs scoring
  last_score >=0.85 AND first_score >=0.85 (both components at or above
  `REVIEW_LAST_THRESHOLD` - a principled band, not one picked to hit a
  target count) and below the auto-match threshold. **56 distinct
  `(source_npi, candidate_npi)` pairs** at that band, deduplicated across
  Open Payments/Part D duplicates of the same source identity - the full
  set, not a further subsample, since the band itself is already small.

**What's actually done:** the candidate pool regeneration and its
verification against documented numbers, and the band selection and its
rationale (a tighter >=0.90/>=0.90 band was considered and rejected as the
sampling frame - only 13 distinct pairs, and marginal one-component-only
bands are dominated by the common-surname-collision failure mode, a
different thing than a genuine near-miss).

**What's not done:** none of the 56 recall-sample pairs are labeled. Of
the 14-pair precision set, 11 are clean (no issues found); the remaining 3
are the "Lee, David" many-to-one collision (source NPIs `1225276652` and
`1619003860`, both scoring 1.0/1.0 against golden NPI `1003832346`) -
distinguishing detail was pulled directly from the raw source files
(different middle names - "M" vs "Patrick" - different stable practice
addresses 350 miles apart across 5 years of Open Payments and 4 years of
Part D, different OP-reported subspecialty) strongly suggesting two
distinct physicians, but which one (if either) is the true match to the
golden identity was never adjudicated. No precision or recall number, no
TP/FP/FN count, was computed - stopping here rather than filling those in
without real labels.

**Left in place, not reverted:** `data/all_candidates_scored.csv` (the
regenerated full candidate pool - real, reproducible, independently
useful beyond this specific validation attempt),
`data/layer1_precision_set_round3.csv` (14 rows, 11 pre-filled
`confirmed_match`, 3 blank/flagged `UNRESOLVED`),
`data/layer1_recall_sample_near_threshold.csv` (56 rows, `label` and
`note` columns blank), and `data/near_threshold_pool_preview.csv` (an
earlier, tighter-band exploratory pull, superseded by the 56-row file but
kept rather than deleted, same reasoning as this project's other
superseded-not-deleted artifacts, e.g. the original undercounted Part D
files). None of these have fabricated content - blank label columns are
blank, not filled with guessed values, so anyone picking this back up
gets an honest, exact starting point rather than a completed-looking
artifact that silently isn't.

**Why the round-based validation remains this project's primary
documented evidence for the 0.98/0.98 thresholds:** Rounds 1-3 above
already found and fixed three real, concrete failure modes (blended-score
compensation, near-exact-but-not-quite name collisions, many-to-one
collisions) against the actual pulled data - that validation is complete
and stands on its own. This formal precision/recall attempt was meant to
add a second, more rigorous layer on top of it, not to replace it - its
incompleteness does not weaken the round-based validation already
documented, it just means the additional rigor isn't there yet. If
resumed: the concrete next steps are (1) label the 56-pair recall sample,
(2) adjudicate the 3 collision rows in the precision set, (3) compute
exact TP/FP/FN and precision/recall with formulas shown, (4) explicitly
caveat a 56-pair recall estimate as small-sample given how rare true
matches are in this data - none of that is guesswork, it's exactly where
this entry leaves off.
