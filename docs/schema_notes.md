# Schema Notes

Source schema quirks discovered during exploration, and how schema evolution
is handled (Delta `mergeSchema`). Filled in during Layer 0/2 work.

## NPI Registry (NBER core + ptaxcode files)

- `entity` is a text label (`Individual` / `Organization`), not the raw NPPES
  numeric code (1/2) - confirmed by inspecting `core_202507.csv` directly.
  Layer 1 matching only makes sense against `Individual` rows: `Organization`
  rows have no `plname`/`pfname`, only `porgname`, and Open Payments/Part D
  are both physician-level (individual) sources. Filtering to `Individual`
  drops 347 of the 1,716 CA/MT cardiology rows in the July 2025 snapshot.
- `ptaxcode` is a separate one-or-more-rows-per-NPI file (a provider can hold
  multiple taxonomy codes). Only `207RI0011X` (Interventional Cardiology) is
  independently confirmed against its NUCC description; the other ~28
  observed `207R%` codes are not individually mapped to a display label here
  - see `DECISIONS.md` "Layer 1: blocking + scoring in plain Python" for how
  this is handled (default to a `general_cardiology` bucket rather than
  guessing at unverified code labels).

## CMS Part D Prescriber data - suppression flags, not true nulls

- Small counts (Medicare privacy rule: counts under 11) are suppressed, not
  reported as zero. `Tot_Benes` blank + `GE65_Bene_Sprsn_Flag` (or the
  claim-level `GE65_Sprsn_Flag`) set to `#`/`*` means "value withheld for
  privacy," not "no beneficiaries." Confirmed by inspecting real rows (e.g.
  `partd_2021_ca_mt_cardiology.csv` row 1: `Tot_Benes` blank, `GE65_Sprsn_Flag`
  = `#`). **This matters for Layer 3**: HHI and cohort z-scores must not
  silently coerce a blank/suppressed field to 0 - that would understate a
  provider's actual concentration/volume rather than correctly excluding the
  suppressed metric or flagging it as unknown. **Handled, 2026-07-31:** both
  Layer 3 metrics (cohort z-score's claim-volume metric, HHI's claim-count
  shares) are built on `Tot_Clms`, not `Tot_Benes`/`GE65_*` - verified for
  real that `Tot_Clms` itself has zero null/non-positive values across all
  13,671 `silver_part_d_history` rows, so it carries none of this
  suppression behavior and needed no coercion-avoidance logic. See
  `DECISIONS.md`.
- `Prscrbr_Type` is a **claims-derived** specialty label (self-reported /
  billing-derived), independent of NPPES's taxonomy-code-derived specialty.
  The two disagree in real, non-trivial numbers for this project's slice
  (see `DATA_SOURCES.md`) - this is Layer 1's actual reason to exist, not a
  hypothetical edge case.

## CMS Open Payments

- Wide, mostly-sparse schema: up to 6 repeated `covered_recipient_specialty_N`
  / `covered_recipient_primary_type_N` / product columns per record (a single
  payment can cite multiple specialties/products). Only `_1` is used for
  Layer 1's specialty bucketing here - a deliberate simplification, not an
  oversight, since a payment's *primary* listed specialty is the relevant
  signal for blocking.
- `covered_recipient_specialty_1` is a pipe-delimited full taxonomy path
  (e.g. `Allopathic & Osteopathic Physicians|Internal Medicine|Interventional
  Cardiology`), not a short code - bucketing matches on substrings
  (`"interventional"`, `"heart failure"`, `"transplant"`) rather than exact
  equality.
