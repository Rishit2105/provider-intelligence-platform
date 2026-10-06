# Limitations

Honest known failure modes. This list is maintained alongside the code that
introduces each limitation, not written retroactively at the end.

## Cross-cutting - data source integrity

Not tied to one layer's algorithm - these are limitations in the underlying
pulled data itself, inherited by whichever layer consumes it.

- **National NPI count anomaly, unresolved:** the *national* (pre-state-filter)
  count of distinct NPIs carrying any `207R%` cardiology taxonomy code was
  **105,491** in the May 2023 NBER snapshot vs. **66,547** in the July 2025
  snapshot - a ~37% drop that is not plausible as real physician attrition
  over 2.5 years. Both files share an identical column schema, so this isn't
  a parsing mismatch between the two pulls. Two candidate explanations, not
  yet distinguished: (1) the 2023 file is drawn from a differently-scoped
  extract than the 2025 monthly series (e.g. cumulative/historical vs.
  active-only), or (2) NUCC taxonomy code revisions between 2023 and 2025
  moved some cardiology providers to a sibling code outside `207R%`. **Does
  not block this project's CA/MT slice** - the state-filtered counts (1,548
  -> 1,716) move in a realistic direction and magnitude, which is a
  different and much smaller effect than the national anomaly - but it is
  genuinely unresolved, not explained away, and is flagged here for a future
  re-check rather than silently ignored. See `DATA_SOURCES.md`, "Anomaly
  worth flagging honestly."
- **Part D pagination undercount, corrected but not fully explained:** the
  originally committed 2021-2023 Part D data undercounted rows by roughly
  36-135x (a systematic pagination/streaming bug in the original bulk-CSV
  puller, not a per-year anomaly - the same factor appeared independently in
  three separate years). The corrected 2021-2024 data now used throughout
  this project was accepted **on reproducibility grounds** - CY2023 and
  CY2024 each independently reproduced their already-recorded counts exactly
  on a second, separate pull run, and all four corrected years land in the
  same stable ~83,700-86,700 row band. That is a strong practical standard,
  but it is not the same as fully isolating *why* the original bulk-CSV
  method failed - the `csv.DictReader`-based streaming code looked correct
  on inspection, and the root cause of the original bug was never
  conclusively identified, only worked around by switching to a different
  (paginated API) access method entirely. See `DECISIONS.md`, "Part D
  2021-2023 re-pulled with the corrected pagination method," for the full
  verification writeup.

## Layer 1 - Identity Resolution

Built and run against real data 2026-07-30 - see `DECISIONS.md` for the full
threshold-validation narrative. Numbers below are real output, not
projections.

- **False positives are real, not hypothetical - found three separate
  times during validation, not once:** (1) a blended last+first name score
  let an exact-surname/different-given-name pair auto-match ("PATEL
  MANDIRA" vs "PATEL NARMISHA" scored 0.90+ on the blend alone); (2) even
  after switching to independent last/first thresholds at 0.95/0.90,
  "SPIEGEL DAVID" matched "SIEGEL DAVID" and "HUSSAIN SYED" matched "HUSAIN
  SYEDA" (a male/female name-suffix distinction - almost certainly
  different people); (3) even at the final 0.98/0.98 threshold, two
  different Part D NPIs (`1225276652`, `1619003860`) both auto-matched to
  the same golden NPI ("Lee, David") - a many-to-one collision, caught by a
  dedicated check, not by the name-similarity threshold at all. Jaro-Winkler
  on name plus state/specialty blocking has no date-of-birth, gender, or
  other disambiguator to fall back on - these are the concrete, observed
  shape of that gap, not a disclaimer.
- **The review queue is large relative to the golden roster:** 1,220
  `identity_review_queue` rows against a 1,369-person golden roster. This
  is a real consequence of the data volumes involved (CA cardiology alone
  has hundreds of unmatched Open Payments/Part D records competing for a
  best-in-block match against ~900-1,300 golden candidates per block), not
  a sign the blocking design is broken - see `DECISIONS.md`'s "Layer 1:
  threshold validation found real false positives" for the full reasoning.
  Of these, 514 were mechanically resolved to `confirmed_no_match` and 9 to
  `confirmed_match` by clearly-stated rules (of that 9, 3 were later found to
  be the "Lee, David" many-to-one collision rows, mechanically re-confirmed
  by the same rules that should have left them flagged - corrected to 6
  genuine mechanical `confirmed_match` rows, see below); a 61-pair
  stratified sample of
  the remaining 697 was manually adjudicated on 2026-07-30 (5 confirmed
  match, 45 confirmed no-match, 11 left `pending` as genuinely hard cases -
  see `DECISIONS.md`). Current state: 559 `confirmed_no_match`, **11**
  `confirmed_match`, **650 `pending`** - most of which were never manually
  reviewed at all (only the 61-pair sample was), and were not guessed at.
  (Adjusted from an earlier 14/647 split: 3 rows tied to the "Lee, David"
  many-to-one collision were forced back to `pending` once the collision
  check became a standing pipeline invariant that overrides the mechanical
  first-pass rules for exactly this case - see `DECISIONS.md`, "Layer 1:
  many-to-one collision check is a standing pipeline invariant.")
- **False negatives:** a provider who changed both name and address between
  source snapshots may fall below even the flag-for-review threshold and go
  entirely unmatched, rather than landing in the review queue. Not directly
  measured this session (would require independent ground truth on who's
  missing entirely, which this project doesn't have).
- **Specialty taxonomy bucketing is a verified-partial mapping:** of the 29
  distinct `207R%` taxonomy codes actually observed in the NBER ptaxcode
  file, only **1** (`207RI0011X`, Interventional Cardiology) is
  independently confirmed against its NUCC description; the other 28 default
  to `general_cardiology` rather than a guessed subspecialty label. Blocking
  collapses to a 2-way split (interventional vs. everything else) rather than
  the 3-way split (interventional / heart-failure-transplant / general) the
  raw data could otherwise support - see `DECISIONS.md`, "Layer 1: specialty
  bucketing is a verified-partial taxonomy mapping." **This also affects
  Layer 3's cohort granularity** - any cohort defined by specialty (e.g. for
  the N=10 minimum cohort size / z-score fallback tiers) inherits this same
  coarse 2-way split, so a heart-failure/transplant specialist's cohort peers
  will include general cardiologists rather than being isolated to their true
  subspecialty. See the matching cross-reference in the Layer 3 section
  below.
- **Golden roster is not the full NPI universe:** matching candidates are
  only drawn from the already CA/MT + `207R%`-taxonomy-filtered NPI extract
  (1,369 individual providers), not the full national NPI Registry. A
  source record whose true match exists in NPPES but fell outside this
  project's filter (e.g., a genuinely different taxonomy code entirely) can
  never be found, only ever land in `no_match`. This is a scope
  consequence, not a bug - downloading the full national registry was
  explicitly out of scope for this project (see `DATA_SOURCES.md`).
- **Ground truth is one person (mostly mechanical, during this session
  an unattended AI agent):** the ~75-100-pair validation sample and the
  review queue's first-pass status were produced by clearly-stated
  mechanical rules, not full human judgment, during an unattended work
  session - genuinely ambiguous pairs were left `pending` rather than
  guessed. Final adjudication of those pairs is still a single person (the
  project owner), not an independent team - disclosed here and in
  `DECISIONS.md`/`INTERVIEW_QUESTIONS.md`, not hidden behind "manual
  validation."

## Layer 2 - History Tracking

Bronze and silver (`silver_history_scd2.py`) written 2026-07-31, **executed
for real against the full CA/MT cardiology filtered dataset on Databricks
Free Edition on 2026-07-31** (see `DECISIONS.md`, "Layer 2: run for real on
Databricks Free Edition"). Gold (Layer 3's cohort z-scores/HHI) not yet
built.

- **Verified, not just syntax-checked:** the AUTO CDC INTO merge behavior,
  the identity fallback backfill, and the business-key duplicate flag have
  all run against real data, not just `python -m py_compile`. Real result:
  `silver_provider_identity_history` - 2,591 rows across all 1,369 golden
  NPIs (2,579 `source_of_truth = "npi_registry"`, 12
  `"transactional_fallback_low_confidence"` - confirming the narrow
  fallback trigger genuinely fires on real data, not just in the abstract).
  `silver_open_payments_history` - 6,443 rows, of which 8 were flagged
  `is_business_key_duplicate` at the `warn` gate tier (flagged and counted,
  not deduplicated, per design - see the business-key-duplicates entry
  below, still genuinely unresolved whether these are real repeated events
  or a reporting artifact). `silver_part_d_history` - 13,671 rows. The
  defensive `stg_golden_roster` `expect_or_fail` checkpoint passed without
  firing, as expected, since Layer 1's own many-to-one collision check had
  already run cleanly upstream.
- **Getting a clean run required six real fixes, not zero** - a stale
  path convention, a notebook-vs-workspace-file import distinction, an
  execution-ordering bug, two Databricks Free Edition serverless-compute
  API gaps (`spark.sparkContext.broadcast(...)` and `.cache()` are both
  unavailable outside a classic cluster), and an `AUTO CDC INTO` flow
  naming collision (two flows into one target table need distinct
  `name=`, not just distinct sources) - none of which `py_compile` could
  have caught. Full bug-by-bug detail in `DECISIONS.md`. This is presented
  as a feature of actually running the code, not a defect list: it is
  exactly the category of bug real execution exists to find.
- **"NPI wins for identity" is a source-authority convention, not a
  recency claim:** the conflict-resolution precedence rule (NPI Registry
  wins identity/profile facts, transactional sources always win their own
  transactional facts - see `DECISIONS.md`, "Layer 2: conflict resolution
  across sources") means a provider who updates their address with a payer
  before ever updating NPPES will have Layer 2 prefer the (possibly stale)
  NPI Registry value. This is a deliberate tradeoff for a clear, auditable
  rule over a genuinely-most-current one - NPI-derived effective dates are
  themselves only proxies for the true change date (NPPES snapshots, not a
  change log), so "most recent" isn't even well-defined enough to use as
  the primary rule.
- **Partitioning skew:** partitioning by state means California holds far more
  data than Montana - a known, accepted imbalance given the deliberate
  two-state contrast, not a balanced scheme.
- **Business-key duplicates, unresolved:** rows sharing NPI + payer/prescriber
  + amount + date + payment nature, with a distinct `record_id` and
  `change_type == "NEW"`, are flagged and counted at the `warn` gate tier but
  **not deduplicated** - see `DECISIONS.md`, "Layer 2: data quality gate
  tiers." It is genuinely unresolved whether these represent real repeated
  events (a payer legitimately issuing multiple matching payments) or a
  reporting/pagination artifact of the same general shape as the Part D and
  Open Payments pulling bugs already found and fixed elsewhere in this
  project (see the cross-cutting section above). Left unresolved and visible
  rather than silently deduplicated, which risks discarding real data, or
  silently accepted, which risks double-counting in downstream Layer 3
  analytics.
- **Volume/row-count sanity check - implemented and run for real
  2026-07-31** (see `DECISIONS.md`, "Layer 2: volume/row-count sanity check
  implemented and run for real"). Two real limitations found while
  building and running it, not assumed away:
  - **Catches a failed *run*, not a guaranteed-clean *silver* write.**
    `expect_or_fail` aborts the whole pipeline update, but doesn't create a
    DAG dependency forcing silver to wait on the volume check - in the real
    failing run this session hit, `silver_open_payments_history` had
    already completed its streaming write before the update was reported
    failed two seconds later. An orchestrator or human watching run status
    would correctly see the run as failed, but the design doesn't currently
    guarantee zero downstream rows were written in that same run. No
    downstream consumer of silver exists yet to be exposed to this (Layer 3
    isn't built), but it's worth revisiting once one does.
    **Mitigation path, stated not implemented:** a production fix would
    take one of two shapes - either run the pipeline as a single atomic
    transaction (so a failed volume check rolls back every silver write
    from the same update, not just halts before the next one), or drop the
    all-writes-are-safe assumption entirely and gate downstream consumption
    (Layer 3's gold reads, in this project) on an overall
    pipeline-run-succeeded flag rather than trusting that "table X exists
    and has rows" means "table X's latest run was clean." Lakeflow doesn't
    expose the first option directly (`expect_or_fail` is documented,
    observed-here behavior, not a transaction boundary this project
    controls), so the second is the more realistic near-term fix. Neither
    is built in this project - Layer 3 reads silver tables directly,
    trusting the same per-table-completeness assumption this gap describes,
    consistent with every other place in this project where a known gap is
    disclosed rather than silently worked around.
  - **Detects a deviation from the prior period, not from ground truth.**
    Tested against the real preserved Part D 2021-2023 undercounted files:
    the check would NOT have tripped on `2022 vs 2021` (43.5% deviation,
    under the 50% threshold), because both years were undercounted by a
    similar ~36-135x factor - two similarly-broken consecutive periods can
    stay inside tolerance of each other. It does trip on `2023 vs 2022`
    (227.7%), and because `bronze_part_d` unions all configured years into
    one table build per run rather than appending one year per run, all
    three bad years would in fact have been evaluated in the same single
    update and that update would have failed - so the bug would still have
    been caught before reaching silver, just not at the earliest possible
    year. A pipeline that ingested one new year per run, with each new
    year's undercount similar in magnitude to the previous run's, could
    evade this check for longer than a single run.
  - **Surfaced, and led to fixing, a real unrelated bronze-layer bug:**
    running this check against real data found 6 rows with a null
    `program_year` in `bronze_open_payments`, traced to 3 real records
    with an embedded newline in a quoted field that Spark's CSV reader
    (no `multiLine=True`) was silently splitting into corrupted row pairs.
    Fixed in `bronze.py` for all three sources; also corrected
    `DATA_SOURCES.md`'s Open Payments 2023 count (78,330 -> 78,327), which
    had the same embedded-newline records inflating a physical-line count.
    See `DECISIONS.md` for the full trace.

## Layer 3 - Behavioral Analytics

Cohort z-score and HHI both built and run for real against Databricks Free
Edition, 2026-07-31 (`notebooks/03_analytics.py`) - see `DECISIONS.md` for
the full real-run writeups. Core Layer 3 scope is done; the compound flag
(HHI shift + payment spike) stretch goal is not.

- **HHI's drug identity is generic name, not the raw brand+generic row
  grain - a real bug found and fixed, not a hypothetical:** a single
  generic drug (e.g. "Diltiazem Hcl") can be split across up to 6 different
  brand-name rows for the same provider-year in `silver_part_d_history`.
  Left ungrouped, HHI would have silently understated concentration
  (artificial fragmentation -> more, smaller shares -> lower HHI) as a
  branding artifact, not real prescribing diversity. Fixed by aggregating
  claim counts to `(golden_npi, data_year, Gnrc_Name)` before computing
  shares. See `DECISIONS.md` for the full writeup and the real sanity
  checks run against the written table (HHI always in `(0,1]`, exact 1.0
  for real single-drug provider-years, real min 0.0146 for a 220+-drug
  prescriber).

- **Compound flag (HHI shift + payment spike) - formally declared out of
  scope, 2026-07-31, not left as "deferred."** It would have covered
  PY2023 and PY2024, the two calendar years where Open Payments
  (2023-2025) and Part D (2021-2024, extended 2026-07-30 - see
  `DECISIONS.md`) overlap - a screening heuristic demonstrable as a
  two-year comparison, not a longer multi-year trend or a fraud verdict.
  Now that core Layer 3 (cohort z-score + HHI) is done, this is a final
  call, not an open item: **not built, and not planned as a follow-on to
  this project.** Full reasoning in `DECISIONS.md`, "Compound flag (HHI
  shift + payment spike) - final call, declared out of scope": it would
  need its own real threshold-validation pass (a "shift" and a "spike"
  threshold, neither currently defined, and this project's own standard -
  see Layer 1's three-round threshold validation - is not to ship an
  unchecked threshold); the two-year overlap window is a before/after
  comparison, not a trend robust to a single noisy year; and this project
  has consistently chosen depth over breadth when the two trade off (see
  the CA/MT-not-CA+oncology scope decision) - a fourth feature built to a
  lower validation standard than the rest of this project would be a worse
  addition than not building it.
- **N=10 minimum cohort size** is a pragmatic choice given filtered data
  volume, not a statistically rigorous threshold. **Real result, not
  projected:** CA never needs a fallback (100% `full_cohort`, 323/323
  provider-year rows - even CA's smallest specialty+state cell has 17-18
  members). MT always needs the specialty-only fallback (100%,
  4/4 rows) - but **`insufficient_cohort_data` never actually fires, 0 of
  327 real provider-year rows**, because the specialty-only pool combines
  CA+MT, and CA's much larger same-specialty population absorbs Montana's
  handful of providers past N=10 every time. The three-tier fallback logic
  is real and its guard conditions (e.g. divide-by-zero protection for a
  zero-stddev cohort) were checked against real data, but the hardest tier
  has only been logically verified, not observed firing - see
  `DECISIONS.md` for the full honesty framing on this distinction.
- **A real, sharper finding than "Montana has fewer providers": Montana's
  golden-roster-to-Part-D-resolution rate is very low.** Only 2 of
  Montana's 31 golden cardiologists ever have a Part D record that
  resolves to a golden identity across all four pulled years (2021-2024) -
  a ~6% resolution rate, discovered while building the z-score's input
  population, not assumed going in. This is a compounding consequence of
  Layer 1's identity-resolution funnel (see the Layer 1 section above) on
  top of Montana's already-small raw provider count - the cohort-size
  fallback tiers exist for the *statistical* small-N problem, but this is
  a *data-availability* problem underneath it: most Montana golden
  providers simply never appear in Part D silver history at all, and so
  never even reach the cohort-size question for the z-score specifically.
  **Investigated, not just reported, 2026-07-31 - genuinely a low-linkage
  finding, not a Layer 1 under-matching artifact:** checked what fraction
  of MT's full `identity_review_queue` (not just the golden-roster
  resolution rate) links to Part D. Of MT's 67 distinct Part D prescriber
  NPIs (2021-2024), 2 exact-match the golden roster (the 6% figure above)
  and the other 65 were run through Layer 1's exact blocking + Jaro-Winkler
  scoring - **all 65 land in `no_match`, zero in `review`** (best scores
  0.0-0.76, not close misses under the threshold). The committed
  `identity_review_queue.csv` confirms this independently: of its 1,220
  rows, zero have `source_state == "MT"`. If Layer 1 were under-matching
  Montana specifically, at least some of those 65 would be expected to
  land in the review band the way CA's unmatched records do (1,220
  review-tier rows out of 6,314 CA unmatched candidates); none do. See
  `DECISIONS.md`, "MT's ~6% Part D resolution rate investigated, not just
  reported," for the full numbers, including the same confirmation run
  independently against Open Payments (131 of 131 unmatched MT NPIs also
  land in `no_match`).
- **Cohort granularity inherits Layer 1's specialty taxonomy limitation:**
  specialty-defined cohorts (used for the N=10 minimum / fallback tiers
  above) can only distinguish `interventional` from `general_cardiology`,
  because only 1 of 29 observed `207R%` NUCC codes is independently
  confirmed - see the Layer 1 section's "Specialty taxonomy bucketing is a
  verified-partial mapping" entry for the full reasoning. A true
  heart-failure/transplant specialist's cohort peers will include general
  cardiologists rather than being isolated to their actual subspecialty,
  which coarsens (but doesn't invalidate) the cohort-relative z-score. In
  the real golden roster, this 2-way split lands as 1,311 CA /30 MT
  `general_cardiology` vs. 27 CA / 1 MT `interventional` - interventional
  cardiologists are a small minority (28 of 1,369, ~2%) of the roster this
  cohort logic groups by.
- **Cohort geography is the static July 2025 NPI snapshot, not a
  per-`data_year` lookup against SCD2 history** - checked, not assumed,
  that this doesn't distort results on this project's real data: zero
  golden providers have a different recorded state across the two NPI
  Registry snapshots. See `DECISIONS.md` for the full reasoning and what
  would need revisiting if a future data refresh changed that fact.
- **The cohort z-score's underlying metric is Part D claim volume, not
  payment amount or drug cost** - see `DECISIONS.md`, "Layer 3: cohort
  z-score metric is total yearly Part D claim volume, not payment amount,"
  for why, and for the real, checked confirmation that `Tot_Clms` carries
  none of `Tot_Benes`/`GE65_*`'s privacy-suppression behavior.
- **`golden_provider_table` was missing specialty entirely until this
  session** - a real integration gap between Layer 1's locked spec (Layer
  3 needs specialty for cohorts) and what Layer 1 actually persisted, only
  discovered by starting to build against the real published table. Fixed
  by adding `specialty_bucket` to `golden_table`'s Step 8 select and
  re-running `01_identity_resolution.py` - see `DECISIONS.md` for the full
  bug writeup, including a second real bug (`DELTA_METADATA_MISMATCH`,
  needing `overwriteSchema=true`) hit while fixing the first.
