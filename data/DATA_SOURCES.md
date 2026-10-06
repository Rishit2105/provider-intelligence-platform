# Data Sources

Links, download instructions, confirmed year ranges, filtering criteria, and
the NBER archive URLs used for temporal NPI snapshots. Everything below was
verified directly (actual HTTP requests, not docs-reading) on 2026-07-26, and
the CA+Montana+cardiology slices described here were actually pulled and are
sitting in `data/raw/` (gitignored) and `data/sample/` (small, committed).

## 1. NPI Registry (NPPES), via NBER archive

NPPES itself only distributes a current full file plus weekly incrementals -
no historical downloads. NBER independently archives monthly NPPES snapshots,
which is what makes a genuine before/after comparison possible for Layer 2.

Directory listings require a browser-style `User-Agent` header - bare
`curl`/bot requests get a 403 from NBER's server. Every download below used:
`Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like
Gecko) Chrome/120.0 Safari/537.36`.

**Snapshot 1 - May 2023** (`https://data.nber.org/npi/2023/5/`):
- `core_20235.csv` - one row per NPI: name, entity type, mailing + practice
  location address (`plocstatename` is the field used to filter by state).
- `ptaxcode_20235.csv` - one or more rows per NPI: `npi, seq, ptaxcode,
  pprimtax, ptaxgroup`.
- Also available as `.dta` and `.sas7bdat` (no parquet for this snapshot).

**Snapshot 2 - July 2025** (`https://data.nber.org/npi/2025/07/`), pinned
here deliberately instead of the latest available month - see `DECISIONS.md`
for the reasoning (already-verified file beats squeezing out a few extra
months of drift):
- `core_202507.csv` - same schema as the 2023 core file.
- `byvar/ptaxcode_202507.csv` - same schema as the 2023 ptaxcode file
  (confirmed by inspecting both directly, not assumed from the folder name).
  Also available as `.dta`, `.sas7bdat`, and `.parquet`.

**Cardiology taxonomy filter:** all NUCC cardiology taxonomy codes share the
`207R` prefix (Allopathic & Osteopathic Physicians > Internal Medicine >
Cardiovascular Disease and subspecialties - e.g. `207RC0000X` Cardiovascular
Disease, `207RI0011X` Interventional Cardiology, `207RA0002X` Adult
Congenital Heart Disease). Filtering `ptaxcode LIKE '207R%'` is more robust
than listing individual subspecialty codes by hand, since it doesn't require
knowing every subspecialty code in advance.

**Filtering + join result (both snapshots filtered to `plocstatename` in
(CA, MT), joined to cardiology-taxonomy NPIs):**
- May 2023: 1,548 CA/MT cardiology providers matched.
- July 2025: 1,716 CA/MT cardiology providers matched.

**Anomaly worth flagging honestly:** the *national* (pre-state-filter) count
of distinct NPIs with any `207R%` taxonomy code was 105,491 in the May 2023
file vs. 66,547 in the July 2025 file - a large drop that is not plausible as
real-world physician attrition over 2.5 years. Both files share the identical
column schema (verified by inspecting raw rows from each), so this isn't a
parsing mismatch. Two most likely explanations, not yet distinguished: (1)
the 2023 file is drawn from a differently-scoped extract than the monthly
2025 series (e.g. cumulative/historical vs. active-only), or (2) NUCC taxonomy
code revisions between 2023 and 2025 shifted some cardiology providers to a
sibling code outside `207R%`. This does **not** block the project - the
CA/MT-filtered counts (1,548 -> 1,716) move in a realistic direction and
magnitude - but it's logged here rather than silently ignored, and is worth
re-checking if Layer 1 match rates look off.

## 2. CMS Open Payments

**Year range: 2023-2025** (PY2025 is the current latest release: 17.07M
records, $14.67B - confirmed against actual publication status, not assumed).

**Access method:** openpaymentsdata.cms.gov runs on DKAN, which exposes a
queryable datastore API - no need to download the full national bulk CSV
(14.7M+ rows for 2023 alone) to get a 2-state slice:

```
GET https://openpaymentsdata.cms.gov/api/1/datastore/query/{dataset_id}/0
    ?limit=500&offset=0
    &conditions[0][property]=recipient_state&conditions[0][value]=CA&conditions[0][operator]==
    &conditions[1][property]=covered_recipient_specialty_1&conditions[1][value]=%Cardio%&conditions[1][operator]=like
```

- **`limit` caps at 500** - requesting 1000+ returns HTTP 400 (confirmed by
  testing 500/1000/2000 directly). Paginate with `offset`.
- The API groups conditions with AND, and OR-across-states isn't a
  documented option, so CA and MT are pulled as two separate paginated
  passes and concatenated - simpler than relying on undocumented syntax.
- Dataset IDs (General Payment Data, one per program year):
  - 2023: `fb3a65aa-c901-4a38-a813-b04b00dfa2a9`
  - 2024: `e6b17c6a-2534-4207-a4a1-6746a14911ff`
  - 2025: `fb0b1734-1410-429d-92f6-3f4b35218e5e`
- Row counts pulled (CA + MT, `covered_recipient_specialty_1 LIKE '%Cardio%'`),
  final and verified: **2023: 78,327 / 2024: 78,617 / 2025: 77,998** - all
  three years land in the same ~78K ballpark, which is the expected shape.
  (2023 corrected from an originally-recorded 78,330 on 2026-07-31: the
  original count was effectively a physical-line count, inflated by 3 real
  2023 records with an embedded newline inside a quoted free-text field -
  found while building and running the Layer 2 volume/row-count sanity
  check, which grouped by `program_year` and required `bronze.py`'s CSV
  reads to add `multiLine=True`. See `DECISIONS.md`, "Layer 2: volume/
  row-count sanity check implemented and run for real.")

**Lesson learned mid-pull, worth keeping honest here:** the first attempt at
the 2024 pull silently returned only ~2,500 rows. The pagination loop gave up
after 3 failed retries on a connection error and moved on to the next state
*without raising an error* - so the script "succeeded" (exit code 0) with a
badly truncated result that looked superficially plausible (open payments
data legitimately varies year to year) until compared side-by-side against
2023/2025's ~78K counts. Fixed by rewriting the puller to be resumable
(checkpoint file per state + offset, retry from where it left off) and by
treating a >10x count mismatch between comparable years as a signal to
re-check rather than accept. Applies generally: a "successful" run of a
paginated API puller is not proof of completeness - compare against a
sanity baseline (a similar year, a known total) before trusting the count.

## 3. CMS Part D Prescriber data

**Year range: 2021-2024** (extended from the original 2021-2023 lock on
2026-07-30 - see `DECISIONS.md` for the full reasoning and evidence).

**Resolution of the 2026-07-26 open question:** the CY2024 file flagged
then (`MUP_DPR_RY26_P04_V10_DY24_NPIBN.csv`, published 2026-05-21) was
verified past the catalog metadata on 2026-07-30 - actual CA/MT cardiology
rows were pulled from `data-api/v1/dataset/d5aa71a8-dcc0-4570-8bcf-bd39deac69fe/data`
and returned real data (not empty), so the range was extended.

**Access method, all four years (as of 2026-07-30 - see below for the
original 2021-2023 method, superseded):** paginated, filtered
`data-api/v1/dataset/{id}/data` requests, filtered to `Prscrbr_State_Abrvtn`
in (CA, MT) x `Prscrbr_Type` in (`Cardiology`, `Interventional Cardiology`,
`Advanced Heart Failure and Transplant Cardiology`). The endpoint caps each
page at **6,500 rows regardless of the requested `size`**; paginated with
`offset` until a page came back short. Per-year dataset IDs (from
`data.cms.gov/data.json`'s "Medicare Part D Prescribers - by Provider and
Drug" catalog entry):
  - CY2021: `f68114ed-f854-4ffc-9c6e-ed78b5e2f8d0`
  - CY2022: `b101b457-ffa4-49bb-8fd9-27c1266086e2`
  - CY2023: `e54db557-cd82-4e91-a0fe-61aad5865d69`
  - CY2024: `d5aa71a8-dcc0-4570-8bcf-bd39deac69fe`

**Final row/NPI counts, CA+MT, 3 cardiology `Prscrbr_Type` values, all four
years via this method (confirmed 2026-07-30 - see `DECISIONS.md` for the
full verification writeup):**

| Year | Rows | Distinct NPIs | CA NPIs | MT NPIs |
|------|------|----------------|---------|---------|
| 2021 | 83,752 | 2,158 | 2,103 | 55 |
| 2022 | 83,747 | 2,176 | 2,119 | 57 |
| 2023 | 84,822 | 2,232 | 2,176 | 56 |
| 2024 | 86,698 | 2,254 | 2,194 | 60 |

All four years land in the same ~83,700-86,700 row / ~2,150-2,260 NPI
range - a stable, plausible year-over-year shape.

**Original 2021-2023 method (superseded 2026-07-30 - kept here for the
record, not as current guidance):** `data.cms.gov`'s own `data-api/v1`
query endpoint timed out repeatedly on every attempt in the original pull
- worked around at the time by streaming the static bulk CSV directly
(`curl <bulk_csv_url> | awk -F',' 'NR==1 || ($5=="CA" || $5=="MT") &&
tolower($7) ~ /cardio/'`, using `Prscrbr_State_Abrvtn`/`Prscrbr_Type`
columns). That method produced **1,272 / 719 / 2,356 rows** and only
**33 / 16 / 57 distinct NPIs** for 2021/2022/2023 - roughly **36-135x
undercounts** versus the corrected numbers above, confirmed by
independently reproducing CY2023 and CY2024 through the paginated API
method (both reproduced their already-recorded counts exactly on a second,
independent run). The same ~36x-scale gap appearing in three different
years pulled via the same bulk-CSV code path, resolved by switching to
paginated API requests, points to a systematic pagination/streaming bug in
that method rather than a per-year data anomaly - see `DECISIONS.md`,
"Part D 2021-2023 re-pulled with the corrected pagination method," for the
full reasoning. The original undercounted files are kept in `data/raw/`
(gitignored) as `partd_{year}_ca_mt_cardiology_ORIGINAL_UNDERCOUNTED.csv`
rather than deleted.

## Filtering checkpoint

After filtering all three sources to cardiology + CA/MT, verify the slice
still contains the messy cases Layer 1 exists to handle: null/missing NPIs,
name mismatches between sources, and address drift. Initial profiling (see
`00_data_exploration.py`) confirms real messiness survived filtering - not
re-checked here field-by-field, but the raw extracts are in `data/raw/` for
inspection.
