# Provider Intelligence Platform

A cloud-agnostic data engineering pipeline that resolves provider identity, tracks
history, and surfaces behavioral signals across three real CMS healthcare datasets
that never perfectly agree with each other.

**Status:** Layers 1-3 (Identity Resolution, History Tracking, Behavioral
Analytics) all built and run against real data on Databricks Free Edition.
The dashboard (`dashboard/app.py`) is built and verified running - event
log, quarantine counts, review queue size, cohort fallback-tier breakdown,
all real numbers pulled live. The compound-flag stretch goal (HHI shift +
payment spike) is formally out of scope - a final decision, not a "not
built yet" - see `DECISIONS.md`.

**A real Layer 3 number, not projected:** of the golden cardiologists with
resolved Part D history in a given year, **100% of California's** land in
a full `(specialty, state)` cohort of at least 10 peers - even its
smallest cell (interventional cardiologists) has 17-18 members. **100% of
Montana's** fall back to the specialty-only tier (peers pooled across both
states) - but that fallback is enough: `insufficient_cohort_data` never
actually fires on this data, because California's much larger
same-specialty population absorbs Montana's handful of providers past N=10
every time. The sharper finding underneath that: only **2 of Montana's 31
golden cardiologists** (~6%) ever have a Part D record that resolves to a
golden identity at all, across all four pulled years - investigated, not
just reported: Montana's full identity-resolution review queue also shows
zero Part D linkage (0 of 1,220 review-queue rows are MT), confirming this
is a genuine low-linkage finding and not Layer 1 under-matching Montana
specifically - see `DECISIONS.md` and `LIMITATIONS.md` for the full
writeup.

## The problem

NPI Registry, CMS Open Payments, and CMS Part D Prescriber data all describe the
same physicians, but:
- They don't always agree on identity (name variants, missing NPIs, address drift).
- Each source publishes on its own schedule, so "current" means a different year
  depending which file you're looking at.
- Provider attributes and payment/prescribing behavior change over time, and a
  naive pipeline overwrites history instead of tracking it.

**The scale of the identity gap, concretely:** ~96-97% of Part D Prescriber
records carry a specialty label (free text, e.g. `Cardiology`) that doesn't
literally string-match the NPI Registry's own taxonomy text for the same
provider (a NUCC code, e.g. `207RC0000X`) - Open Payments' free-text specialty
field shows the same shape of disagreement. This is a base rate, not a data
quality defect: it's *why* Layer 1 exists as rule-based identity resolution
(NPI-first, blocking + Jaro-Winkler fallback) rather than a straight join on
specialty text, and it's deliberately excluded from Layer 2's data quality
gates - see `DECISIONS.md`'s "Layer 2: data quality gate tiers." Gating on it
would mean failing the normal case, not the exceptional one.

This project builds three layers to deal with that:

1. **Identity Resolution** - rule-based matching (NPI where present, blocking +
   Jaro-Winkler fallback where not) into one golden provider table.
2. **History Tracking** - SCD Type 2 via Spark Declarative Pipelines' (SDP)
   `AUTO CDC INTO`, deployed and run on Databricks as a Lakeflow Declarative
   Pipeline, with tiered data quality gates and a documented
   conflict-resolution rule - see `DECISIONS.md` for how the two names relate.
3. **Behavioral Analytics** - cohort-relative z-scores and HHI drug-mix
   concentration, computed honestly (minimum cohort sizes, full-universe HHI).

## Scope (locked)

- **Specialty:** Cardiology, California + Montana (see `DECISIONS.md` for why
  this state pair was chosen deliberately, not arbitrarily).
- **Open Payments:** 2023-2025. **Part D Prescriber:** 2021-2024 (extended
  from the original 2021-2023 lock on 2026-07-30 once CY2024 was verified
  live - see `DECISIONS.md`). These ranges are intentionally offset, not
  forced to align - see `DECISIONS.md`.

## Repo layout

See the tree below. `DECISIONS.md`, `INTERVIEW_QUESTIONS.md`, and
`LIMITATIONS.md` are living documents updated alongside the code that
motivates each entry, not written retroactively.

```
provider-intelligence-platform/
├── data/                # raw (gitignored), sample slices, DATA_SOURCES.md
├── notebooks/            # one notebook per layer + SDP pipeline project
├── dashboard/            # Streamlit app
├── tests/                 # small, targeted tests
└── docs/                  # schema notes
```

## How to run

**Workspace:** [Databricks Free Edition](https://www.databricks.com/learn/free-edition)
(replaced "Community Edition," retired 2026-01-01) - serverless-only compute,
no cluster config needed. Install the CLI (`winget install Databricks.DatabricksCLI`
on Windows), then authenticate with a personal access token generated from the
workspace's own settings, stored in `~/.databrickscfg` (never committed - see
`.gitignore`). Full setup detail, including two real workspace-specific quirks
(catalog creation needing a `CREATE CATALOG` SQL statement rather than the
CLI's REST call, and uploading `identity_matching_core.py` as a workspace
**file**, not a notebook), in `DECISIONS.md`, "Layer 2: run for real on
Databricks Free Edition."

**Run order:**
1. `notebooks/00_data_exploration.py` - pulls and profiles all three raw
   sources (see `data/DATA_SOURCES.md` for access methods and dataset IDs).
2. `notebooks/01_identity_resolution.py` - Layer 1, writes
   `golden_provider_table` and `identity_review_queue`.
3. Deploy and run the Lakeflow Declarative Pipeline (Layer 2) via
   `databricks bundle deploy` + `databricks bundle run` against
   `databricks.yml` (which wraps `notebooks/pipelines/pipeline.pipeline.yml`
   - a plain `include:` doesn't work, see `DECISIONS.md`) - writes the three
   `silver_*_history` tables.
4. `notebooks/03_analytics.py` - Layer 3, writes the two gold tables
   (`gold.cohort_zscore_part_d_claims`, `gold.hhi_part_d_drug_concentration`).
5. `dashboard/app.py` - `streamlit run dashboard/app.py`, locally, reading
   the committed `data/identity_review_queue.csv` plus live queries against
   the workspace above (same `~/.databrickscfg` token) for the quarantine
   counts, cohort fallback-tier breakdown, and pipeline event log. Runs in a
   reduced local-only mode (review queue panel only) without a live
   connection - see the file's own comments.

Real run numbers for every step above are documented alongside the code that
produced them, not just asserted here - see `SESSION_LOG.md` for the
chronological account and `DECISIONS.md`/`LIMITATIONS.md` for the numbers
themselves.
