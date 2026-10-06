# Session Log

## Summary (read this first)

**What got done:** Completed everything in the "immediate priority" queue
and most of the "build queue." In order:

1. **Corrected Part D re-pull, all 4 years (2021-2024)**, via the paginated
   API method already proven for CY2024. Final counts: 2021: 83,752 rows /
   2,158 NPIs. 2022: 83,747 / 2,176. 2023: 84,822 / 2,232 (reproduced the
   already-recorded count exactly). 2024: 86,698 / 2,254 (also reproduced
   exactly). The same ~36-135x undercount factor showed up independently in
   three different years via the old bulk-CSV method - confirms a
   systematic pagination bug, not per-year anomalies. Original files kept
   as `_ORIGINAL_UNDERCOUNTED.csv`, not deleted.
2. `DECISIONS.md` and `DATA_SOURCES.md` updated with final numbers for all
   four years and the full verification writeup.
3. All four Part D `data/sample/` files regenerated as consistent 500-row
   slices - also fixed a real issue where the CY2024 sample was actually
   the full 86,698-row file committed to git in full.
4. `INTERVIEW_QUESTIONS.md`'s stale "2021-2023" references fixed (two
   places, not just the one flagged).
5. `00_data_exploration.py` finished end to end: corrected Part D download
   code, refreshed profiling numbers, explicitly cross-checked that the low
   null-NPI rate isn't masking a "too clean" slice (it isn't - Layer 1's
   real match rates show the actual messiness).
6. **Layer 1 (Identity Resolution) built and run against real data** -
   `notebooks/01_identity_resolution.py` + `notebooks/identity_matching_core.py`.
   This was the biggest piece of work this session and went through three
   real validation rounds, each catching genuine false positives (not
   hypothetical ones) - see the 18:00 log entry and `DECISIONS.md` for the
   full detail. Final: **11 auto_match, 1,220 review, 5,279 no_match** out
   of 6,510 unmatched-source candidates.
7. `identity_review_queue` built as both real PySpark logic in the notebook
   and a committed CSV snapshot (`data/identity_review_queue.csv`) - 514
   `confirmed_no_match`, 9 `confirmed_match`, 697 `pending` via mechanical
   first-pass rules.
8. Layer 2 SDP bronze-layer scaffolding (`notebooks/pipelines/pipeline.yml`,
   `transformations/bronze.py`) - stopped there as instructed, did not
   write `AUTO CDC INTO` or conflict-resolution logic.
9. `tests/test_identity_matching.py` written for real (13 tests, all
   passing) and `LIMITATIONS.md`/`INTERVIEW_QUESTIONS.md`/`README.md`
   updated with real Layer 1 findings throughout.

**What's flagged for your review** (not decided unilaterally - genuinely
ambiguous, or a design fork with real consequences):

- **61 specific candidate pairs need your actual judgment** - a stratified
  sample (oversampling the score boundaries) from the 697 `pending` rows in
  `identity_review_queue`, saved at `data/layer1_validation_sample.csv`
  with `rule_label == "needs_my_review"`. Format: `source record -> best
  golden-roster candidate (last-name score / first-name score)`.

  | Source | Name (city) | vs. Golden roster | Name (city) | last / first |
  |---|---|---|---|---|
  | part_d | Asghar, Fayaz (San Jose) | → | Afshar, Payam (La Jolla) | 0.90 / 0.73 |
  | open_payments | Bai, Charlotte (South San Francisco) | → | Bakhit, Caroline (Riverside) | 0.87 / 0.78 |
  | open_payments | Caffarelli, Anthony (Newport Beach) | → | Capparelli, Anthony (Thousand Oaks) | 0.89 / 1.00 |
  | open_payments | Chan, Ellen (San Francisco) | → | Channick, Colleen (Santa Monica) | 0.90 / 0.79 |
  | open_payments | Chang, Michael (Los Angeles) | → | Cheng, Michael (Orange) | 0.89 / 1.00 |
  | open_payments | Chen, Andrew (San Gabriel) | → | Chen, Anne (Fairfield) | 1.00 / 0.80 |
  | open_payments | Cheng, Wen (West Hollywood) | → | Chen, Wei (San Jose) | 0.96 / 0.82 |
  | open_payments | Choo, Daniel (Hacienda Heights) | → | Choi, Damon (Pasadena) | 0.88 / 0.76 |
  | open_payments | Chu, James (Orange) | → | Chou, James (Panorama City) | 0.93 / 1.00 |
  | part_d | Chao, James (Pleasanton) | → | Chou, James (Panorama City) | 0.87 / 1.00 |
  | part_d | Chou, Eric (Fontana) | → | Cho, Jessica (Los Angeles) | 0.94 / 0.73 |
  | part_d | Chun, James (San Francisco) | → | Chou, James (Panorama City) | 0.87 / 1.00 |
  | open_payments | Daniels, Lori (San Diego) | → | Danze, Lori (Concord) | 0.85 / 1.00 |
  | open_payments | Gong, Timothy (Long Beach) | → | Ong, Timothy (San Jose) | 0.92 / 1.00 |
  | open_payments | Hashmi, Aijaz (Palm Springs) | → | Shahim, Zahra (Laguna Niguel) | 0.89 / 0.60 |
  | open_payments | Hussain, Syed (Modesto) | → | Husain, Syeda (Anaheim) | 0.97 / 0.96 |
  | open_payments | Hyun, David (San Jose) | → | Huynh, Dat (San Jose) | 0.86 / 0.69 |
  | part_d | Hoffman, Richard (Beverly Hills) | → | Shiffman, Roger (Monterey) | 0.87 / 0.56 |
  | open_payments | Khan, Gauhar (Modesto) | → | Kang, Guson (Stanford) | 0.85 / 0.58 |
  | open_payments | Khanal, Sanjaya (Lancaster) | → | Khan, Sara (Upland) | 0.93 / 0.78 |
  | open_payments | Kim, Paul (La Jolla) | → | Kim, Pius (Fullerton) | 1.00 / 0.67 |
  | open_payments | Klein, Liviu (San Francisco) | → | Klein, Calvin (Santa Monica) | 1.00 / 0.70 |
  | open_payments | Kong, Jeremy (San Jose) | → | Kwong, Jerry (Los Angeles) | 0.94 / 0.88 |
  | part_d | Kung, Grace (Los Angeles) | → | Ku, Gregory (San Francisco) | 0.87 / 0.68 |
  | part_d | Kwan, Stephen (Los Angeles) | → | Kang, Esther (Los Angeles) | 0.85 / 0.78 |
  | open_payments | Lee, Anthony (Lynwood) | → | Lee, Sandy (La Palma) | 1.00 / 0.68 |
  | open_payments | Lee, Joseph (Glendale) | → | Le, Stephanie (Sacramento) | 0.91 / 0.70 |
  | open_payments | Lee, Sang (San Jose) | → | Lee, Sandy (La Palma) | 1.00 / 0.85 |
  | open_payments | Levi, Daniel (Los Angeles) | → | Levine, David (San Diego) | 0.93 / 0.76 |
  | open_payments | Li, Walter (San Francisco) | → | Lieu, Dale (San Diego) | 0.87 / 0.75 |
  | open_payments | Liu, Yu Chuan (Salinas) | → | Liu, Conan (Sacramento) | 1.00 / 0.66 |
  | part_d | Lai, Hoang (Murrieta) | → | Lai, Yu Kuang (Stanford) | 1.00 / 0.66 |
  | part_d | Lerner, David (Santa Barbara) | → | Lee, David (Elk Grove) | 0.87 / 1.00 |
  | part_d | Li, Walter (San Francisco) | → | Lieu, Dale (San Diego) | 0.87 / 0.75 |
  | open_payments | Maskatia, Shiraz (Palo Alto) | → | Marfatia, Apurva (Mountain View) | 0.87 / 0.56 |
  | open_payments | Mathur, Nitin (Auburn) | → | Arthur, Kristine (Fountain Valley) | 0.89 / 0.77 |
  | open_payments | Messele, Lydia (Sacramento) | → | Meserve, Leslie (Newport Beach) | 0.87 / 0.58 |
  | part_d | Manshadi, Ramin (Stockton) | → | Man, Alan (Santa Clara) | 0.85 / 0.63 |
  | part_d | Morrissy, Stephen (Sacramento) | → | Morrissey, Ellen (Berkeley) | 0.98 / 0.68 |
  | open_payments | Nguyen, Hoang (San Jose) | → | Nguyen, Thang (Sun City) | 1.00 / 0.87 |
  | open_payments | Nguyen, Minh (Long Beach) | → | Nguyen, Minhthao (Westminster) | 1.00 / 0.90 |
  | open_payments | Nguyen, Thai (Westminster) | → | Nguyen, Thang (Sun City) | 1.00 / 0.85 |
  | part_d | Nguyen, Minh (Long Beach) | → | Nguyen, Minhthao (Westminster) | 1.00 / 0.90 |
  | part_d | Nguyen, Tuan (Fountain Valley) | → | Nguyen, Tan (Sunnyvale) | 1.00 / 0.93 |
  | part_d | O'Neill, Timothy (Los Alamitos) | → | Oei, Tommy (Fontana) | 0.85 / 0.74 |
  | open_payments | Patel, Viral (Sacramento) | → | Patel, Giribala (Fullerton) | 1.00 / 0.77 |
  | open_payments | Phillips, Paul (San Diego) | → | Phillips, Julie (San Diego) | 1.00 / 0.63 |
  | open_payments | Raz, Dan (Duarte) | → | Razi, David (Inglewood) | 0.94 / 0.69 |
  | open_payments | Rodriguez Ruiz, Christina (Stanford) | → | Rodrigues, Brianna (Santa Monica) | 0.89 / 0.67 |
  | open_payments | Si, Ming-Sing (Los Angeles) | → | Shih, Chin Shun (Monterey Park) | 0.85 / 0.65 |
  | open_payments | Silva Enciso, Jorge (La Jolla) | → | Silvers, Jeffrey (San Leandro) | 0.87 / 0.68 |
  | open_payments | Singh, Karan (Oxnard) | → | Sinha, Kairav (Los Angeles) | 0.91 / 0.86 |
  | part_d | Shah, Kevin (San Diego) | → | Shih, Evan (Orange) | 0.87 / 0.78 |
  | part_d | Singh, Harvinder (Sacramento) | → | Singh, Harjeet (Bakersfield) | 1.00 / 0.67 |
  | open_payments | Singh, Kabir (Larkspur) | → | Sinha, Kairav (Los Angeles) | 0.91 / 0.86 |
  | part_d | Spiegel, David (Vista) | → | Siegel, David (Mather) | 0.96 / 1.00 |
  | open_payments | Thaker, Kartik (Lakewood) | → | Thakkar, Bhavik (Long Beach) | 0.91 / 0.67 |
  | open_payments | Tsao, Amanda (Los Angeles) | → | Tsai, Isaac (Corona) | 0.88 / 0.58 |
  | open_payments | Wilkinson, Michael (La Jolla) | → | Wilson, Lisha (Oakland) | 0.88 / 0.68 |
  | part_d | Weiss, Jamie (Lynwood) | → | Weiss, Michael (Walnut Creek) | 1.00 / 0.68 |
  | open_payments | Zimmerman, Dayna (Los Angeles) | → | Zimmer, Raymond (Beverly Hills) | 0.93 / 0.68 |

  Full detail (including candidate NPIs) in `data/layer1_validation_sample.csv`.
  Also worth a look separately: the 697 `pending` rows in
  `data/identity_review_queue.csv` (not just this 61-pair sample) if you
  want the complete picture rather than the stratified slice.
- **Design fork, easiest-to-reverse option chosen, flagged:** blocking
  bucket collapsed to 2-way (interventional vs. general) instead of the
  richer 3-way split the raw specialty text could support, because only
  one NUCC taxonomy code is independently confirmed. See `DECISIONS.md`
  "Layer 1: specialty bucketing is a verified-partial taxonomy mapping."
  Reversible by pulling the full NUCC code table later.
- **Layer 2 conflict-resolution rule** - still not decided (was already
  open before this session), and now blocks `silver_history_scd2.py`'s
  `AUTO CDC INTO` logic specifically. See the 18:26 log entry and
  `DECISIONS.md`'s "Layer 2: bronze layer scaffolded, silver/gold
  deliberately not started."

**What's next:** Once the 61 pairs above (or at least their boundary cases)
are adjudicated, update `identity_review_queue.status` accordingly and
re-run `01_identity_resolution.py`'s golden-table build. Then: decide the
Layer 2 conflict-resolution rule (three candidate approaches listed in the
18:26 entry) and write `silver_history_scd2.py`'s `AUTO CDC INTO` logic
together. Layer 3 (behavioral analytics) not started this session -
next in the queue after Layer 2.

---

## 2026-07-30 17:36 - Session start, oriented on existing state

Read `README.md`, `DECISIONS.md`, `LIMITATIONS.md`, `INTERVIEW_QUESTIONS.md`,
`DATA_SOURCES.md`, and all notebook/pipeline/test stubs. Confirmed: Layers
1-3 not yet started (all notebook files are 2-line stubs), data exploration
(`00_data_exploration.py`) exists but its markdown docs need corrected Part D
numbers once the re-pull lands. Confirmed no Spark/Java available in this
environment - `01_identity_resolution.py` will be written as real PySpark
(matching the locked architecture), but the blocking + Jaro-Winkler scoring
logic is validated locally first with pandas against the actual pulled CSVs
(no Databricks cluster available outside the notebook environment itself).

Verified network access to `data.cms.gov`'s `data-api/v1` (flaky on the
first few attempts, reliable afterward - consistent with what `DATA_SOURCES.md`
already documented about this endpoint).

## 2026-07-30 17:41 - Started corrected Part D 2021-2024 re-pull

Found the exact per-year dataset IDs for "Medicare Part D Prescribers - by
Provider and Drug" from `data.cms.gov/data.json` (the catalog entry lists
one distribution per year, 2013-2024):
- CY2021: `f68114ed-f854-4ffc-9c6e-ed78b5e2f8d0`
- CY2022: `b101b457-ffa4-49bb-8fd9-27c1266086e2`
- CY2023: `e54db557-cd82-4e91-a0fe-61aad5865d69` (a newer resource ID than the
  one referenced in the existing `DECISIONS.md` write-up - CMS appears to
  have refreshed the CY2023 distribution since the last verification pass;
  same dataset, confirmed by matching the "Medicare Part D Prescribers - by
  Provider and Drug : 2023-12-31" title and `2023-01-01/2023-12-31` temporal
  range)
- CY2024: `d5aa71a8-dcc0-4570-8bcf-bd39deac69fe` (already used for the
  existing CY2024 pull)

Launched a background re-pull of all 4 years via the same paginated
`data-api/v1` method already proven for CY2024 (CA + MT x 3 cardiology
`Prscrbr_Type` values, 6,500-row pages). Running as of this entry - see the
next entry for results.

## 2026-07-30 17:50 - Layer 1 groundwork: plain-Python matching core

While the Part D re-pull ran in the background, started Layer 1 (identity
resolution) design work in parallel. Wrote the blocking + Jaro-Winkler
scoring logic as a small, Spark-free Python module first, so it could be
validated against real data locally before finalizing the PySpark notebook
- see the new `DECISIONS.md` entry "Layer 1: blocking + similarity scoring
in plain Python, not a Spark UDF" for the full reasoning (small comparison
space at this data volume, not a production-scale problem).

Caught and fixed a real bug before it produced output: the golden NPI
roster can only independently confirm one cardiology taxonomy code
(`207RI0011X`, Interventional Cardiology) against the NUCC description - all
other ~28 observed `207R%` codes default to a `general_cardiology` bucket
rather than a guessed label (see `DATA_SOURCES.md`). My first draft of the
blocking key used a *3-way* specialty split (interventional / heart-failure-
transplant / general) sourced from Open Payments' and Part D's own free-text
specialty fields, but only a *2-way* split from the NPI roster's taxonomy
codes - meaning any true heart-failure/transplant specialist in Open
Payments or Part D would never share a block key with their actual match in
the golden roster (which had defaulted to `general_cardiology`), and would
silently never be found. Caught this by re-reading my own logic before the
first full run finished, not by observing bad output. Fixed by making both
sides agree on the coarser 2-way bucket for the actual block key, and kept
the 3-way label only as descriptive metadata on candidate rows. Documented
as its own `DECISIONS.md` entry ("Layer 1: specialty bucketing is a
verified-partial taxonomy mapping") since it's a real, load-bearing
simplification, not an implementation detail.

Also updated `docs/schema_notes.md` with real schema quirks found while
building this: Part D's suppression flags (`GE65_Sprsn_Flag` etc. - blank
`Tot_Benes` means "suppressed for privacy," not zero, which matters for
Layer 3's HHI/z-score math), the NPI roster's `entity` field being a text
label requiring an `Individual`-only filter for person-matching, and Open
Payments' wide sparse multi-specialty schema.

Wrote `tests/test_identity_matching.py` for real (was a stub) - imports the
scoring functions directly from `01_identity_resolution.py` via
`importlib` (the notebook file isn't a valid Python module name), covering
name normalization, exact/near/far similarity scores, the block-bucket
collapse behavior, and threshold classification.

First local run of the candidate-generation loop (naive `iterrows`-over-
`iterrows`) was too slow to finish in reasonable time against the real
unmatched-record counts (~4,000 Open Payments + ~2,400 Part D NPIs not
found in the golden roster - see next entry for what that gap means).
Rewrote it to normalize names once per record instead of once per pairwise
comparison, and to use plain tuples for the golden-roster blocks instead of
pandas `Series` - both are meaningful at this comparison volume. Re-running
now; see next entry for results once both this and the Part D re-pull land.

## 2026-07-30 18:00 - Threshold validation: three real rounds, not one

First full run (blended score, thresholds 0.90 auto / 0.75 review) produced
3,178 "review" pairs out of 6,474 unmatched records (49%) - too many to be
a sane review-queue size. Diagnosed the cause before assuming the queue
size itself was fine: the blocking pool for CA general-cardiology is ~900
golden-roster candidates, so taking the single best-scoring match across a
pool that large means *some* wrong candidate incidentally scores high just
from common-surname collisions - confirmed with real examples ("KIM JUYONG"
best-matched "KIM YONG-SIK" at 0.8984; "GANDHI HARESH" matched "GANDHI
NAINESH" at 0.8984).

Switched to independent last-name/first-name component thresholds instead
of a blended average (a blended score lets an exact surname match paper
over a wrong first name - "PATEL MANDIRA" vs "PATEL NARMISHA" scored 0.90+
blended despite a first-name score under 0.76). This cut review-tier pairs
to 1,199 (18.5%) - much more plausible. But rather than trust the 28
resulting auto-match pairs because the count looked small enough to be
safe, checked all 28 by hand (small enough to fully review). Found two more
real false positives even at 0.95/0.90 thresholds: "SPIEGEL DAVID" matched
"SIEGEL DAVID" (different surname) and "HUSSAIN SYED" matched "HUSAIN
SYEDA" (a male/female name-suffix distinction - almost certainly different
people). Six more pairs in that batch had the same near-miss shape.

Tightened auto-match to 0.98/0.98 (effectively exact on both components).
Left 14 clean-looking pairs - but a separate check (grouping auto-matches
by golden candidate NPI) found even among these a many-to-one collision:
two different Part D NPIs (`1225276652`, `1619003860`) both auto-matched to
the same golden NPI ("Lee, David"). Added a dedicated collision check that
demotes both sides to review rather than letting either claim the golden
identity silently.

**Final, after all three rounds: 11 auto_match, 1,220 review, 5,279
no_match**, out of 6,510 unmatched-source candidates (4,040 Open Payments +
2,470 Part D). Documented as its own `DECISIONS.md` entry ("Layer 1:
threshold validation found real false positives, not just theoretical
ones") because this is the strongest evidence in the project for why
`LIMITATIONS.md`'s stated false-positive risk (no DOB/gender disambiguator)
is real, not a disclaimer nobody checked.

## 2026-07-30 18:12 - Part D re-pull for all 4 years completed

CY2021: 83,752 rows / 2,158 NPIs. CY2022: 83,747 rows / 2,176 NPIs. CY2023:
84,822 rows / 2,232 NPIs (reproduced the already-recorded count exactly).
CY2024: 86,698 rows / 2,254 NPIs (also reproduced exactly). All four years
land in the same ~83,700-86,700 row / ~2,150-2,260 NPI range - a stable,
plausible shape, unlike the original 2021-2023 files' 1,272/719/2,356 rows
and 33/16/57 NPIs. This is the same ~36-135x undercount factor showing up
independently in three different years pulled via the same old bulk-CSV
method, which is what actually confirms a systematic pagination bug rather
than per-year anomalies - a single bad year could be a fluke, three
different years failing by the same factor via the same code path isn't.

Replaced the committed `data/raw/part_d/partd_{year}_ca_mt_cardiology.csv`
files with the corrected data; kept the originals as
`_ORIGINAL_UNDERCOUNTED.csv` rather than deleting them. Re-ran the identity
matching pipeline against the corrected 4-year union (this is what produced
the final 6,510-candidate numbers in the entry above - the collision-check
round happened after this re-pull landed).

## 2026-07-30 18:15 - Wrote 01_identity_resolution.py and finalized Layer 1

Split the matching core out of the notebook into its own real module,
`notebooks/identity_matching_core.py` - discovered while writing
`tests/test_identity_matching.py` that importing the whole notebook file
(needed since `01_identity_resolution` isn't a valid dotted-import name)
would also execute its unconditional `spark.read.csv(...)` calls and crash
outside Databricks. A separate ordinarily-importable module sidesteps this
- `01_identity_resolution.py` imports it, tests import it directly, no
`importlib` workaround needed. Documented as its own `DECISIONS.md` entry.
All 13 tests in `tests/test_identity_matching.py` pass, covering the exact
false-positive cases found during validation (Spiegel/Siegel, Hussain/
Husain-Syed/Syeda, Patel/Patel blended-score compensation) as regression
tests, not just happy-path cases.

Built and committed `data/identity_review_queue.csv` (1,220 rows: 514
`confirmed_no_match`, 9 `confirmed_match`, 697 `pending` - via the
mechanical first-pass rules in `identity_matching_core.py`'s
`first_pass_status`) and `data/layer1_validation_sample.csv` (the 75-pair
stratified sample used for threshold sanity-checking, oversampling the
score boundaries rather than drawn uniformly).

Filled in the real (not "to be filled in") sections of `LIMITATIONS.md` and
`INTERVIEW_QUESTIONS.md` for Layer 1, and updated `README.md`'s status line.

## 2026-07-30 18:23 - Data hygiene: sample files, 00_data_exploration.py

Found `data/sample/partd_2024_ca_mt_cardiology.csv` was actually the full
86,698-row file, committed to git in full - not trimmed, despite the
project's own stated convention (small illustrative slices only, full
pulls stay in gitignored `data/raw/`). Regenerated all four Part D sample
files (2021-2024) as consistent 500-row head slices from the corrected raw
data, matching the existing Open Payments sample convention (which is also
a naive first-500 slice, confirmed by checking - not stratified by state,
but that's the existing precedent, not a new gap introduced here).

Updated `00_data_exploration.py`: replaced the old bulk-CSV-stream Part D
download code (kept, renamed, and clearly marked SUPERSEDED rather than
deleted, since it's a real record of what the original approach was) with
the corrected paginated method for all four years; refreshed the Step 3
profiling markdown with real, freshly-recomputed numbers from the
corrected data (null NPI rate confirmed still ~0% across all 4 Part D years
and all 3 Open Payments years - checked, not assumed, and cross-referenced
against Layer 1's real match-rate numbers to confirm the low null-NPI rate
isn't masking a "too clean" slice: Layer 1's actual 11/1,220/5,279 tier
split shows the real messiness in this data is identity-matching ambiguity,
not missing NPIs).

## 2026-07-30 18:26 - Layer 2 bronze scaffolding, then stopped as instructed

Set up `notebooks/pipelines/pipeline.yml` and wrote real bronze-layer
ingestion in `transformations/bronze.py` (streaming reads of all three
sources' raw files, unioned across years/snapshots, `valid_npi`
expectations, `_ingested_at` column). Did **not** write
`silver_history_scd2.py`'s `AUTO CDC INTO` logic or any conflict-resolution
merge code, per this session's explicit instruction - that needs a decided
precedence rule for when sources disagree, which isn't decided yet (see
the still-open "Layer 2: conflict resolution across sources" entry in
`DECISIONS.md`, unchanged since before this session). Also flagged (not
deleted) `silver_identity_resolution.py` as a stub that predates and now
contradicts the locked "Layer 1 is plain PySpark, not SDP" decision.
Logged as its own `DECISIONS.md` entry with the specific open questions to
discuss next.

## 2026-07-30 (later) - Project owner adjudicated the 61-pair sample

Applied the project owner's adjudication: 5 `confirmed_match`, 45
`confirmed_no_match`, 11 left `pending` as genuinely hard cases (name
similarity alone can't settle them - e.g. three different source records
in three different cities all best-matching one golden identity, "Chou,
James"). Updated `data/identity_review_queue.csv` and
`data/layer1_validation_sample.csv` with a new `note` column recording the
adjudication.

First attempt at this matched rows by name text and accidentally applied
the same adjudication to *other* queue rows sharing an identical name pair
but never part of the 61-row sample - caught by checking the resulting
counts against the expected 61 before treating it as done (47 no-match
rows changed instead of the expected 45). Fixed by regenerating both files
fresh from the original candidate data and re-matching on `(source,
source_npi, candidate_npi)` instead of name text.

Also caught and flagged: the request's summary counts ("5 match / 49
no-match / 7 pairs open") don't reconcile against 61 total under a literal
reading - the named list under "7 pairs" actually enumerates 11 rows (a
3-row cluster plus two recurring 2-row cases plus four single rows).
Followed the explicit named list rather than guessing which total was
intended - documented in `DECISIONS.md` rather than silently picking one.

Final queue state: 647 `pending`, 559 `confirmed_no_match`, 14
`confirmed_match`. Updated `LIMITATIONS.md`, `INTERVIEW_QUESTIONS.md` (new
Q&A using the James cluster as a concrete "genuinely couldn't resolve"
example), and `01_identity_resolution.py`'s markdown to note the
adjudication happened downstream of the notebook's own code, not folded
back into the mechanical rules.

**Superseded by the 2026-07-30 (later still) entry below - the 14
`confirmed_match` count above includes 3 rows that turned out to be
mis-marked by an interaction bug between the collision check and the
mechanical first-pass rules, fixed in that entry.**

## 2026-07-30 (later still) - Layer 2 gate tiers locked; Layer 1 collision check made a standing invariant

Locked the Layer 2 data quality gate tiers with several findings folded in:
"Part D vs NPI specialty mismatch" (~96-97% base rate) is explicitly
excluded from every gate tier and documented instead as a `README.md`
framing statistic - it's the reason Layer 1 exists, not a data quality
defect. Business-key duplicates (same NPI+payer+amount+date+nature, distinct
`record_id`, `change_type=NEW`) are locked as a `warn`-tier check - flagged
and counted, deliberately not deduplicated, since it's genuinely unresolved
whether they're real repeated events or a reporting artifact (documented as
such in `LIMITATIONS.md`, not softened). Added a new pipeline-level
volume/row-count sanity check category, distinct from the row-level tiers -
compare each new pull's row count against the prior comparable period for
the same source+filter, ~50% deviation triggers `expect_or_fail` or a loud
warn - directly motivated by the Open Payments 2024 silent-truncation bug
(~2,500 rows vs. an expected ~78K, exit code 0) already recorded in
`DATA_SOURCES.md`. All of this is in `DECISIONS.md`'s "Layer 2: data quality
gate tiers" and "Layer 2: pipeline-level volume/row-count sanity checks"
entries; not yet implemented in pipeline code (Layer 2's silver layer is
still blocked on the separate, still-open conflict-resolution precedence
decision).

Also added a new permanent Layer 1 post-match invariant: the many-to-one
collision check (`01_identity_resolution.py` Step 5) already ran on every
notebook execution, but its detection logic was inline Spark code, not
tested, and - this is the real finding - its output was silently undone
downstream. A collision-demoted row cleared the 0.98/0.98 auto-match
threshold by definition, so Step 7's mechanical `first_pass_status` rule
(R1: both components >=0.98 -> `confirmed_match`) would re-confirm exactly
the row Step 5 had just flagged as a conflict. Confirmed this wasn't
theoretical: the committed `identity_review_queue.csv` had all 3 rows tied
to the real "Lee, David" collision (source NPIs `1225276652` and
`1619003860` both claiming golden NPI `1003832346`) marked
`confirmed_match`. Fixed by (1) extracting the check into
`find_many_to_one_collisions` in `identity_matching_core.py`, tested like
the rest of Layer 1's matching core (3 new tests in
`tests/test_identity_matching.py`, all passing - `jellyfish` couldn't be
installed in this environment due to an SSL/system-clock cert error, so the
full suite was verified with a stubbed-in `jaro_winkler_similarity`, not the
real package); (2) adding a `collision_flag` column and forcing those rows'
`status` to `pending` in Step 7, bypassing the mechanical rule for exactly
this case; (3) correcting the 3 already-committed rows in
`identity_review_queue.csv` and the matching row in
`layer1_validation_sample.csv` from `confirmed_match` to `pending`, each
with a `note` explaining why. **Queue totals changed project-wide: 559
`confirmed_no_match`, 11 `confirmed_match` (was 14), 650 `pending` (was
647)** - updated in `LIMITATIONS.md` and `INTERVIEW_QUESTIONS.md`. New
`DECISIONS.md` entry: "Layer 1: many-to-one collision check is a standing
pipeline invariant."

Also added: `LIMITATIONS.md`'s new "Cross-cutting - data source integrity"
section (national NPI count anomaly 105,491 -> 66,547, unresolved but
doesn't block the CA/MT slice; Part D pagination undercount, corrected on
reproducibility grounds with the original bug's root cause never fully
isolated), and extended the existing specialty-taxonomy-bucketing limitation
with an explicit Layer 3 cohort-granularity cross-reference in both
directions (Layer 1's entry now notes the Layer 3 impact; Layer 3's stub
section now has its own entry pointing back).

## 2026-07-31 - Layer 2 conflict-resolution rule locked

The last item on this project's own "next to discuss together" list (see
the summary at the top of this file, and `DECISIONS.md`'s "Layer 2: bronze
layer scaffolded" entry) was the cross-source conflict-resolution rule
blocking `silver_history_scd2.py`'s `AUTO CDC INTO` logic. Locked it:
**attribute-type-based precedence, not source-based.** Identity/profile
facts (name, address, specialty) - NPI Registry always wins, with its
effective date documented explicitly as a snapshot-pull proxy, not a true
change date. Transactional facts (payment amount/date, claim quantity) -
the owning transactional source always wins for its own records, no
cross-source override, ever. A narrow fallback (most recent transactional
source's embedded value, flagged low-confidence) applies only when no NPI
snapshot covers a provider at all - not a parallel general rule. Rejected
single-source-always-wins (conflates identity authority with transactional
authority) and most-recent-by-date-across-sources (NPI dates are proxies,
not exact - not actually comparable in confidence to a transactional
source's timestamp).

Filled in the previously-stub "Layer 2: conflict resolution across sources"
entry in `DECISIONS.md` with the full rule, alternatives considered, and
why; added the honest tradeoff ("NPI wins" can mean preferring a stale NPI
value over a more current payer-reported one) to `LIMITATIONS.md`'s Layer 2
section; answered the previously-stubbed conflict-resolution Q&A in
`INTERVIEW_QUESTIONS.md`; and updated the "still blocked" language in
`bronze.py`, `silver_history_scd2.py`, and `pipeline.yml`'s header comments
to reflect that the rule is decided, even though the merge logic itself
still isn't written - locking the rule and implementing `AUTO CDC INTO` are
two separate steps, and only the first one is done here.

**Still not done:** `silver_history_scd2.py`'s actual `AUTO CDC INTO` logic.
Now unblocked, but writing real merge code is a bigger step than locking
the rule - worth confirming before doing it, not assumed as an automatic
follow-on.

## 2026-07-31 (later) - `silver_history_scd2.py` written

Confirmed (proceed with writing the merge logic now), with specific
implementation requirements: the attribute-type split implemented
literally (not just structurally implied), NPI-sourced effective dates
visibly marked as proxy dates, the two conflict-resolution fallback
triggers handled as distinct code paths, and confirmation that Layer 1's
many-to-one collision check runs upstream of SCD2 processing.

Wrote three silver tables: `silver_provider_identity_history` (SCD2, fed
only by `bronze_npi_registry` - Open Payments/Part D identity fields are
never read here at all, which is how "NPI wins identity, always" is
actually enforced, not just stated) and `silver_open_payments_history` /
`silver_part_d_history` (SCD2, each fed only by its own bronze table, no
cross-source merge). Re-examined the "narrow fallback" case while writing
real code and found it splits into two triggers, only one of which is
actually reachable: a golden NPI with **no** NPI Registry row at all is
structurally impossible given how `golden_provider_table` is built (every
golden NPI comes from the July 2025 NPI extract by construction) - not
implemented, documented as unreachable rather than built as dead code. The
real case - a golden NPI missing from the **earlier** (2023-05) snapshot
specifically - is implemented as `stg_identity_fallback_backfill`, using
the earliest transactional record as a low-confidence stand-in, tagged and
sequenced to sort first.

Added a defensive `@dp.expect_or_fail` checkpoint (`stg_golden_roster`)
that Layer 1's request specifically asked for: re-verifies
`golden_provider_table` has no duplicate `golden_npi` before any silver
table reads it, rather than trusting the upstream invariant silently. Also
implemented the previously-locked business-key-duplicate warn check, moved
from bronze (as originally described) to a staging table, since the
business key needs `golden_npi`, which doesn't exist until after identity
resolution is joined in.

Fixed one implementation mistake before it shipped: the first draft of
`stg_golden_roster` paired a placeholder `@dp.expect_or_fail("...", "1 =
1")` decorator with a manual Python `raise ValueError` inside the function
body - technically would have worked but bypasses the pipeline's own
expectation-tracking/metrics, and the placeholder predicate was actively
misleading to leave in real code. Replaced with a proper row-level
`is_duplicate_golden_npi` boolean column (a window count per NPI) so the
check runs through `@dp.expect_or_fail` idiomatically, same pattern as
every other expectation in this file.

Also corrected internal references while reviewing the draft: sibling
tables within this same SDP pipeline (`bronze_*`, `stg_golden_roster`,
`stg_npi_identity_attributes`) need `dp.read(...)` to register properly in
the pipeline's dependency graph, not plain `spark.read.table(...)` - which
is still correct, and used, for `golden_provider_table` /
`identity_review_queue`, since those are genuinely external tables written
by Layer 1's separate notebook, not part of this pipeline's own DAG.

Syntax-checked with `python -m py_compile` (passes) - not run against real
data, no Spark/Databricks cluster available in this environment. Full
reasoning in `DECISIONS.md`, "`silver_history_scd2.py` written - AUTO CDC
INTO merge logic"; `LIMITATIONS.md` and `pipeline.yml`/`bronze.py`'s header
comments updated accordingly.

**Still not built:** the pipeline-level volume/row-count sanity check
(mechanism still open) and the gold layer (Layer 3's cohort z-scores/HHI,
`gold_analytics.py`).

## 2026-07-31 (later still) - Layer 2 run for real on Databricks Free Edition

Priority for this session: stop reasoning about Layer 2 from `py_compile`
alone and actually run it. Confirmed no Databricks workspace was connected
in this environment (no CLI, no `~/.databrickscfg`, no `DATABRICKS_*` env
vars) and no local Java/Spark either, consistent with the 17:36 entry
above. Also found: "Databricks Community Edition" retired 2026-01-01,
replaced by **Databricks Free Edition** - verified via Databricks' own
docs before signing up, not assumed from the project's existing (now
stale) wording.

Project owner created a Free Edition account and workspace (browser +
email verification - not something this session could do on their
behalf) and handed over a workspace URL and personal access token.
Installed the Databricks CLI via `winget`, stored the token in
`~/.databrickscfg` (outside the repo, never committed). Verified auth
(`databricks current-user me`), then stood up the missing infrastructure:
catalog `provider_intelligence`, schemas `bronze`/`raw`, and a Unity
Catalog volume at `/Volumes/provider_intelligence/raw/source_files`
matching `bronze.py`'s hardcoded path - catalog creation via the CLI's
`catalogs create` REST call failed on this workspace ("Metastore storage
root URL does not exist") despite Default Storage being enabled
account-wide; creating it via a `CREATE CATALOG` SQL statement against the
serverless SQL warehouse worked, a real quirk not obviously documented.
Uploaded the exact raw files `bronze.py` reads into matching subfolders.

Found before Layer 2 could even start: `golden_provider_table` and
`identity_review_queue` had never actually been persisted anywhere -
`01_identity_resolution.py`'s `saveAsTable(...)` calls were written but
never executed (same reason Layer 2 itself was unrun). Ran
`01_identity_resolution.py` for real first. Hit and fixed five real
runtime bugs in the process - a stale `BASE_PATH` assuming a Databricks
Repos checkout that was never set up (switched to the same Unity Catalog
volume convention `bronze.py` uses), a missing `ptaxcode_202507_cardiology.csv`
upload, a notebook-vs-file import distinction (`identity_matching_core.py`
has to be uploaded as a workspace **file**, not a notebook, to be
`import`-able), a `dbutils.library.restartPython()` ordering bug that
wiped a variable defined before it, and two serverless-compute-specific
API gaps (`spark.sparkContext.broadcast(...)` and `.cache()` are both
unavailable on Free Edition's serverless-only compute) - full detail and
exact fixes in `DECISIONS.md`, "Layer 2: run for real on Databricks Free
Edition." **Real result matched the earlier pandas-validated prediction
exactly:** `golden_provider_table` = 1,369 rows; `identity_review_queue`'s
mechanical first pass = 6 `confirmed_match` / 511 `confirmed_no_match` /
703 `pending` (1,220 total). Real PySpark execution reproducing the
pandas-predicted numbers is real evidence the ported logic is correct.

Deployed `silver_history_scd2.py` as an actual Lakeflow Declarative
Pipeline - needed a Databricks Asset Bundle wrapper (`databricks.yml`)
that `pipeline.yml` alone doesn't provide; a plain `include:` reference
silently produced zero pipeline resources (only "unknown field" warnings
merging the file's keys into bundle-root config), so `databricks.yml`
instead defines the pipeline directly under `resources.pipelines`.
Renamed `pipeline.yml` to `pipeline.pipeline.yml` (Databricks' naming
convention for standalone pipeline projects) as part of investigating
this, though the working fix ended up being the explicit `resources.pipelines`
block, not the rename itself. First real run failed with
`AnalysisException: Cannot have multiple queries named
...silver_provider_identity_history...` - the two `AUTO CDC INTO` flows
feeding that one target table (NPI Registry attributes, and the fallback
backfill) both defaulted to the target's own name with no explicit
`name=`, and Lakeflow requires distinct names once a target has more than
one flow. Fixed by naming both flows explicitly. Exactly the kind of bug
`py_compile` structurally cannot catch - valid syntax, wrong against real
execution semantics.

**Ran directly against the full CA/MT filtered dataset, not a small slice
first** - each full pipeline run completed in under 3 minutes end to end,
fast enough that iterating on a slice first wasn't necessary to move
quickly; a deliberate deviation from the original plan once it was clear
iteration speed wasn't the bottleneck. Pipeline completed cleanly:
`silver_provider_identity_history` - 2,591 rows across all 1,369 golden
NPIs (2,579 `npi_registry`, 12 `transactional_fallback_low_confidence` -
real, non-zero confirmation the narrow fallback trigger actually fires on
real data). `silver_open_payments_history` - 6,443 rows, 8 flagged
`is_business_key_duplicate` (flagged and counted, not deduplicated, as
designed). `silver_part_d_history` - 13,671 rows. The defensive
`stg_golden_roster` `expect_or_fail` checkpoint passed without firing, as
expected.

`LIMITATIONS.md`'s Layer 2 "not yet run against real data" caveat updated
to reflect what was actually verified, with the date and real numbers
above. Full bug-by-bug detail in `DECISIONS.md`.

**Still not done:** the pipeline-level volume/row-count sanity check
(mechanism still open, was explicitly deferred again this session per
the project owner's instructions) and the gold layer (Layer 3).

## 2026-07-31 (later still) - Volume/row-count sanity check implemented and run for real

Priority for this session: close the one remaining open item from Layer
2 before starting Layer 3 - the pipeline-level volume/row-count sanity
check whose threshold and comparison basis were locked in `DECISIONS.md`
but never implemented or placed anywhere.

**Resolved the one open design question first:** the original entry left
`expect_or_fail` vs. a loud warn undecided. Settled on `expect_or_fail`,
unconditionally - tested against the same standard this project has used
for every other gate-tier call (would this design have caught the actual
bug?), and unlike the business-key-duplicate `warn` case, there's no
genuine ambiguity here to preserve: this project's own real corrected data
never moves more than ~3.5% year over year, so a >50% deviation has never
once been a legitimate reporting swing, only ever a bug. Full reasoning in
`DECISIONS.md`.

**Implemented** as `notebooks/pipelines/transformations/volume_sanity_check.py`
- one small table per source, each grouping its bronze table by its own
period column and comparing each period's row count to the prior
comparable period's via a window lag, with `expect_or_fail` applied to
those period-rows. This deliberately reuses SDP's row-level expectation
mechanism to express a pipeline-level assertion, since SDP has no
first-class pipeline-level primitive - documented as a deliberate choice,
not a workaround stumbled into.

**Deployed and run for real on Databricks Free Edition** (CLI reinstalled
via `winget` after a stale `PATH`; the workspace and stored
`~/.databrickscfg` token from the prior session were still live). First
run failed on an unrelated stale streaming checkpoint
(`DELTA_SOURCE_TABLE_IGNORE_CHANGES` - an upstream table had been
overwritten since the last run); fixed with `--full-refresh-all`, which
`pipeline.pipeline.yml` already documents as safe pre-gold. Second run
failed for a real reason, on the check's own logic: 6 rows in
`bronze_open_payments` had a null `program_year`, which is not the
scenario the check was designed to catch, and worth chasing down rather
than just special-cased away. Traced (via direct SQL queries against the
workspace's serverless SQL warehouse) to 3 real Open Payments records with
an embedded newline in a quoted field, which Spark's default single-line
CSV reader (`bronze.py` never set `multiLine=True`) was silently
splitting into corrupted row pairs - a real, previously-undiscovered
bronze-layer bug, invisible to every check that existed before this one
because `csv.DictReader` (used earlier in this project to hand-verify
counts) parses embedded newlines correctly and never showed it. Fixed at
the root: added `multiLine=True` to all three sources' CSV reads in
`bronze.py`, and hardened the check itself to filter null partition values
before grouping regardless. Also corrected `DATA_SOURCES.md`'s Open
Payments 2023 count (78,330 -> 78,327 - the original figure was itself a
physical-line count inflated by the same 3 records).

**Third run completed cleanly end to end**, real numbers pulled directly
from the check tables via SQL: Open Payments moved 0.37-0.79% year over
year, Part D 0.006-2.21%, NPI Registry 10.9% across the ~2-year snapshot
gap - all comfortably inside the 50% threshold, confirming no false
positive on this project's actual legitimate data.

**Tested against both real historical incidents this check was motivated
by, not just asserted to work:** simulated with the real preserved row
counts (the actual truncated Open Payments 2024 figure, and the real
`_ORIGINAL_UNDERCOUNTED` Part D files still sitting in `data/raw/`). Open
Payments' 2024 truncation trips the check immediately (96.8% deviation,
first appearance of the bad year, since a good 2023 baseline already
existed). Part D's 2021-2023 undercount is a more honest, mixed result:
`2022 vs 2021` alone stays under the 50% threshold (both years undercounted
by a similar factor), and only `2023 vs 2022` trips (227.7%) - but since
`bronze_part_d` unions all configured years into one table build per run,
all three bad years would have been evaluated in the same single update,
so the bug would still have been caught before ever reaching silver, just
not at the earliest possible year. Documented as a real, non-obvious
limitation in `LIMITATIONS.md` - this check detects deviation from the
*prior period*, not from ground truth.

**A second real, empirically-observed limitation, not just theoretical:**
in the run that failed on the null-`program_year` bug, `silver_open_payments_history`
had already logged `has COMPLETED` (its streaming write committed) two
seconds *before* the overall update was reported `FAILED`. `expect_or_fail`
halts the pipeline **run**, but there is no explicit DAG dependency forcing
silver to wait on the volume check specifically, so a downstream flow can
still commit data in the same run before the abort registers. No
downstream consumer of silver exists yet (Layer 3 isn't built) to be
exposed to this, but it's a real gap worth revisiting once one does, not
assumed away. Documented in `DECISIONS.md` and `LIMITATIONS.md`.

All four project docs updated: `DECISIONS.md` (full entry, resolved
design question, real run numbers, both bug traces), `LIMITATIONS.md`
(the two check-design limitations plus the bronze bug), `DATA_SOURCES.md`
(corrected count), `INTERVIEW_QUESTIONS.md` (two new Q&As on the
expect_or_fail choice and the "would this have caught it" test).

**Next: Layer 3 (Behavioral Analytics)** - cohort-relative z-scores and
HHI, per the locked spec. Not started this session.

## 2026-07-31 (new session) - Layer 2 close-out, then Layer 3 cohort z-score built and run for real

Priority for this session: one small close-out item on Layer 2's already-
documented `expect_or_fail`-doesn't-guarantee-a-clean-silver-write gap
(state the concrete mitigation path explicitly, not implement it - it's
still blocked on no downstream consumer existing to protect), then start
Layer 3 per the locked spec, cohort z-score first.

**Close-out:** Added an explicit "mitigation path, stated not implemented"
note to both `LIMITATIONS.md` and `DECISIONS.md`'s volume-check entries -
either run the pipeline as one atomic transaction, or gate downstream
consumption on an overall pipeline-success flag rather than trusting
per-table completeness. Neither built; Layer 3's gold layer (below) reads
silver directly, inheriting the same assumption, disclosed rather than
silently carried forward.

**Real bug found immediately on starting Layer 3, before any gold-layer
code was written:** `golden_provider_table` - the one table Layer 3's own
locked spec depends on for specialty grouping - never actually persisted
a specialty column. Layer 1 computes `bucket` for blocking but drops it
before writing `golden_table` in Step 8. Fixed by adding
`specialty_bucket` to that select and re-running `01_identity_resolution.py`
for real on Databricks. Hit a second real bug doing so:
`DELTA_METADATA_MISMATCH` - `mode("overwrite")` alone doesn't widen an
existing Delta table's schema; needed an explicit
`.option("overwriteSchema", "true")`, added with a comment. Third re-run
attempt succeeded: `identity_review_queue`'s status split reproduced
exactly (6/511/703), `golden_provider_table` still 1,369 rows, now with
real `specialty_bucket` values (general_cardiology: 1,311 CA / 30 MT;
interventional: 27 CA / 1 MT). Full writeup in `DECISIONS.md`, "Layer 3:
`golden_provider_table` was missing specialty."

**Built `notebooks/03_analytics.py` as plain PySpark, not SDP** - same
reasoning as Layer 1 vs. Layer 2's declarative-vs-handwritten split, see
`DECISIONS.md`. `notebooks/pipelines/transformations/gold_analytics.py`
left in place as a stub pointing to this decision, same pattern as
`silver_identity_resolution.py`.

**Cohort z-score built and run for real, first (simpler dependency
chain, per instructions) - not accumulated untested alongside HHI.**
Metric: total yearly Part D claim volume (`sum(Tot_Clms)`), not payment
amount (reserved for the out-of-scope compound-flag stretch goal) or drug
cost (list-price outlier sensitivity, same reasoning already applied to
HHI's claim-count-share choice) - verified `Tot_Clms` itself carries none
of `Tot_Benes`/`GE65_*`'s suppression behavior (zero null/non-positive
across 13,671 rows) before relying on it. Cohort key: `(specialty_bucket,
plocstatename)` from `golden_provider_table` per `data_year`, N=10 minimum,
specialty-only fallback, `insufficient_cohort_data` otherwise - checked
first that using the static July 2025 snapshot state (rather than
re-deriving per-year against SCD2 history) is safe on this data: zero
golden providers have a different recorded state across the two NPI
snapshots.

Deployed via `databricks jobs submit` (one-time notebook run, not a
persisted job) against the same workspace/warehouse this project's Layer 2
session already stood up. First submission failed twice (both attempts hit
the `golden_provider_table` bugs above, before they were fixed) - fixed,
re-submitted, succeeded on the retry.

**Real result, `provider_intelligence.gold.cohort_zscore_part_d_claims`,
327 provider-year rows:** CA 323/323 (100%) `full_cohort` - even CA's
smallest cell (interventional) has 17-18 members, well over N=10. MT 4/4
(100%) `specialty_only`. **`insufficient_cohort_data` never fires, 0 of
327 rows** - a real, more specific finding than expected: only 2 of
Montana's 31 golden cardiologists ever have Part D history that resolves
to a golden identity across all four years (a ~6% resolution rate,
compounding Layer 1's identity-resolution funnel on Montana's already-small
population), and the specialty-only fallback pools CA+MT, so California's
much larger same-specialty pool absorbs Montana's handful of providers past
N=10 every time - the fallback machinery is real and exercised, but its
hardest tier has only been logically verified (guard conditions checked
against real data, zero misfires), not observed firing. Full numbers and
the honesty framing on that distinction in `DECISIONS.md` and
`LIMITATIONS.md`.

`LIMITATIONS.md` and `INTERVIEW_QUESTIONS.md` updated with the real numbers
and two new Q&As. `gold_analytics.py`/`03_analytics.py` header comments
cross-reference the relevant `DECISIONS.md` entries.

**HHI built and run immediately after, same notebook, same session -
also not accumulated untested.** Full drug universe (no top-N truncation),
claim-count share, scale 0-1. Found a real bug while building, before
running: `silver_part_d_history`'s row grain is `(golden_npi, data_year,
Brnd_Name, Gnrc_Name)`, and a single generic drug can be split across up
to 6 brand-name rows for the same provider-year (checked directly against
real data, e.g. "Diltiazem Hcl") - grouping HHI by the raw row grain would
have silently fragmented one drug into several, understating concentration.
Fixed by aggregating to `(golden_npi, data_year, Gnrc_Name)` first.

Uploaded and ran via the same `databricks jobs submit` one-time-run
pattern as the cohort z-score - succeeded on the first submission this
time (no re-run needed). **Real result,
`provider_intelligence.gold.hhi_part_d_drug_concentration`, 327 rows**
(same population as the cohort z-score): zero rows with HHI outside
`(0,1]`, zero single-drug provider-years with HHI != 1.0 (4 real examples,
each exactly 1.0), `corr(n_distinct_drugs, hhi) = -0.32` (expected
direction), real range 0.0146 (a 220+-distinct-drug prescriber) to 1.0
(several low-volume single-drug providers). `DECISIONS.md`,
`LIMITATIONS.md`, `INTERVIEW_QUESTIONS.md` all updated with the real
numbers and a new Q&A on the generic-vs-brand fix.

**Core Layer 3 scope (cohort z-score + HHI) is done, built and run for
real, not just written.** The compound-flag stretch goal (HHI shift +
payment spike) remains explicitly out of scope, per this session's
instructions - not started.

## 2026-07-31 (new session) - MT linkage investigated, dashboard built and run, doc consistency pass, compound flag closed out

Four items this session, all closed out for real rather than left as
written-but-unverified or ambiguous:

1. **Investigated the MT/Part D linkage finding rather than just reporting
   it.** Reproduced Layer 1's exact blocking + Jaro-Winkler scoring locally
   in pandas against the real raw CSVs, scoped to Montana's 65 unmatched
   Part D prescribers (of 67 distinct MT Part D NPIs, 2021-2024, 2 of which
   exact-match the golden roster). Result: all 65 land in `no_match`, zero
   in `review` - confirmed independently against the committed
   `identity_review_queue.csv` (0 of 1,220 rows are MT) and cross-checked
   against Open Payments (131 of 131 unmatched MT NPIs also `no_match`).
   This rules out "Layer 1 under-matching Montana" as the explanation for
   the previously-reported ~6% (2/31) MT Part D resolution rate - it's a
   genuine low-linkage finding, not a matching defect. Documented in
   `DECISIONS.md`, `LIMITATIONS.md`, `README.md`, and `INTERVIEW_QUESTIONS.md`.
2. **Built `dashboard/app.py` for real** (previously a comment-only stub) -
   event log, quarantine counts, review queue size, cohort fallback-tier
   breakdown. Review queue reads the committed CSV; the other three panels
   query live Databricks (SQL Statement Execution API + Pipelines REST API)
   via the same `~/.databrickscfg` token this project's other Databricks
   work uses. Actually ran it, twice: once live (all four panels populated
   with real numbers, zero exceptions) and once with a forced-unreachable
   connection (clean degrade to a local-only mode, not a crash) - both
   verified with `streamlit.testing.v1.AppTest`, which caught and let me
   fix a real bug (`pivot_table(fill_value=0)` on a string-typed column
   from the REST API's all-string JSON response) that reading the code
   alone wouldn't have surfaced. Surfaced, and left visible rather than
   hidden, a real known discrepancy: the live `identity_review_queue`
   table (6/511/703, mechanical-only) vs. the committed CSV (11/559/650,
   includes manual adjudication) - documented, not reconciled, since the
   gap itself is already-documented project history.
3. **Documentation consistency pass** across all five docs plus the SDP
   pipeline files. Found and fixed: a duplicated, half-filled-in interview
   question (`INTERVIEW_QUESTIONS.md` had two "why cardiology, CA+MT"
   entries, one still reading "fill in once Layer 1 results are in" long
   after Layer 1 was done); `README.md`'s "How to run" section, still
   literally "TBD" and referencing "Community Edition" by name after two
   full sessions of actually running this project on Databricks Free
   Edition - rewritten with the real run order and setup steps;
   `pipeline.pipeline.yml`'s header comment claiming gold/Layer 3 "is not
   yet written" after it had been; `silver_identity_resolution.py`, a stub
   whose own stated removal condition ("once Layer 2 is confirmed to read
   Layer 1's tables directly") had been satisfied for a full session
   without the file being removed - deleted, with the removal logged in
   `DECISIONS.md` the same way every other change in this project is;
   `tests/test_hhi_calculation.py` existing but never referenced in any
   doc - cross-referenced from `DECISIONS.md`'s HHI entry; the "SDP" vs
   "Lakeflow Declarative Pipelines" naming used inconsistently with no
   definition anywhere - defined once, in `DECISIONS.md`. Also caught and
   fixed a real arithmetic slip in this session's own new writing (cited
   "6,410 CA unmatched candidates" in three places; the real number, 6,510
   total minus MT's 196, is 6,314) - caught by checking the number against
   its stated source rather than trusting a first pass.
4. **Compound-flag stretch goal - final call: declared out of scope, not
   left as "deferred."** Core Layer 3 is done, which was the stated
   precondition for revisiting this; reasoned explicitly why not to build
   it now rather than defaulting to "later" - it would need its own
   unvalidated shift/spike thresholds (against this project's own standard
   of validating thresholds against real data before trusting them), the
   overlap window is still only two years, and this project has
   consistently chosen depth over breadth when the two trade off (the same
   reasoning as the original CA+MT-not-CA+oncology scope decision). Full
   reasoning in `DECISIONS.md`; `LIMITATIONS.md`, `README.md`, and
   `INTERVIEW_QUESTIONS.md` updated from "deferred" to "closed out"
   language throughout.

Full test suite re-run after all changes: 21 passed (16 identity-matching,
5 HHI), unaffected by anything this session touched.
