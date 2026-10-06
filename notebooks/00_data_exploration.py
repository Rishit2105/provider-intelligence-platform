# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 0: Data Exploration
# MAGIC
# MAGIC First look at NPI Registry, CMS Open Payments, and CMS Part D Prescriber
# MAGIC data, filtered to cardiology + California/Montana. Goal: confirm the
# MAGIC filtered slice still contains the messy cases Layer 1 needs to handle
# MAGIC (null NPIs, name mismatches, address drift) before designing any matching
# MAGIC logic against it.
# MAGIC
# MAGIC **Design note (see `DECISIONS.md` for full reasoning):** the CA/MT +
# MAGIC cardiology filtering for all three sources was already verified and run
# MAGIC once outside this notebook (full national files are multi-GB; re-running
# MAGIC that filter on every notebook execution would be slow and wasteful for a
# MAGIC personal Community Edition cluster). This notebook documents that
# MAGIC download+filter step exactly as it was run (`DOWNLOAD_DATA = True` below
# MAGIC reproduces it), then does the actual exploration against the already
# MAGIC filtered files. Default is `False` - re-run the download only if you
# MAGIC don't already have `data/raw/` populated.

# COMMAND ----------

DOWNLOAD_DATA = False  # set True to re-run the full download+filter from scratch

# Adjust to wherever this repo's data/ folder lands in your workspace -
# e.g. a Databricks Repos checkout path, or a Unity Catalog volume you've
# copied data/raw into.
BASE_PATH = "/Workspace/Repos/provider-intelligence-platform/data"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Download + filter (only runs if `DOWNLOAD_DATA = True`)
# MAGIC
# MAGIC All three sources are pulled already filtered to California + Montana +
# MAGIC cardiology, instead of downloading full national files and filtering in
# MAGIC Spark afterward - the NPI and Part D national files are several GB each,
# MAGIC and Open Payments' API makes state-level filtering free (see below).

# COMMAND ----------

import csv
import io
import os
import time
import urllib.request
from urllib.parse import urlencode

# NBER's server 403s bare/bot User-Agents - a normal browser UA is required.
BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)


def http_get_text(url, headers=None, timeout=120):
    req = urllib.request.Request(url, headers=headers or {"User-Agent": BROWSER_UA})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read().decode("utf-8")


# COMMAND ----------

# MAGIC %md
# MAGIC ### 1a. NPI Registry - two snapshots from NBER's archive
# MAGIC
# MAGIC NPPES itself only publishes a current file plus weekly deltas - no
# MAGIC historical downloads. NBER independently archives monthly snapshots at
# MAGIC `data.nber.org/npi/YYYY/MM/`, which is what makes a genuine before/after
# MAGIC comparison possible for Layer 2's SCD2 tracking.
# MAGIC
# MAGIC Snapshot dates: **May 2023** and **July 2025** (pinned to an
# MAGIC already-confirmed-present month rather than "whatever's latest" - see
# MAGIC `DECISIONS.md`). ~2.25 years apart, long enough for real attribute drift.
# MAGIC
# MAGIC All cardiology taxonomy codes share the `207R` prefix (NUCC:
# MAGIC Allopathic & Osteopathic Physicians > Internal Medicine > Cardiovascular
# MAGIC Disease and subspecialties), so filtering `ptaxcode LIKE '207R%'` catches
# MAGIC every subspecialty without hand-listing each one.

# COMMAND ----------

NPI_SNAPSHOTS = {
    "2023-05": {
        "core_url": "https://data.nber.org/npi/2023/5/core_20235.csv",
        "ptax_url": "https://data.nber.org/npi/2023/5/ptaxcode_20235.csv",
    },
    "2025-07": {
        "core_url": "https://data.nber.org/npi/2025/07/core_202507.csv",
        "ptax_url": "https://data.nber.org/npi/2025/07/byvar/ptaxcode_202507.csv",
    },
}


def filter_npi_core_to_ca_mt(csv_text):
    """Keep only rows whose practice-location state is CA or MT."""
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = [row for row in reader if row["plocstatename"] in ("CA", "MT")]
    return rows


def filter_ptaxcode_to_cardiology(csv_text):
    """Keep only taxonomy rows under the cardiology code family (207R*)."""
    reader = csv.DictReader(io.StringIO(csv_text))
    rows = [row for row in reader if row["ptaxcode"].startswith("207R")]
    return rows


def join_npi_snapshot(core_rows, ptax_rows):
    """One row per CA/MT provider who has at least one cardiology taxonomy code."""
    cardiology_npis = {row["npi"] for row in ptax_rows}
    return [row for row in core_rows if row["npi"] in cardiology_npis]


if DOWNLOAD_DATA:
    npi_snapshots_joined = {}
    for label, urls in NPI_SNAPSHOTS.items():
        core_text = http_get_text(urls["core_url"])
        ptax_text = http_get_text(urls["ptax_url"])
        core_rows = filter_npi_core_to_ca_mt(core_text)
        ptax_rows = filter_ptaxcode_to_cardiology(ptax_text)
        joined = join_npi_snapshot(core_rows, ptax_rows)
        npi_snapshots_joined[label] = joined
        print(f"{label}: {len(core_rows)} CA/MT providers (all specialties), "
              f"{len(joined)} matched to cardiology")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1b. CMS Open Payments - via the datastore query API
# MAGIC
# MAGIC openpaymentsdata.cms.gov runs on DKAN, which exposes a queryable API -
# MAGIC no need to download the full national bulk CSV (14.7M+ rows for 2023
# MAGIC alone) just to get a 2-state slice.
# MAGIC
# MAGIC Confirmed by testing directly: **`limit` caps at 500** per request
# MAGIC (1000+ returns HTTP 400) - paginate with `offset`. The API ANDs
# MAGIC conditions together and OR-across-states isn't a documented option, so
# MAGIC CA and MT are pulled as two separate paginated passes and concatenated.

# COMMAND ----------

OPEN_PAYMENTS_DATASET_IDS = {
    2023: "fb3a65aa-c901-4a38-a813-b04b00dfa2a9",
    2024: "e6b17c6a-2534-4207-a4a1-6746a14911ff",
    2025: "fb0b1734-1410-429d-92f6-3f4b35218e5e",
}


def pull_open_payments_state_year(dataset_id, state, page_size=500):
    """Paginate through one state's cardiology-specialty payment records for a program year."""
    all_rows = []
    offset = 0
    while True:
        params = {
            "limit": page_size,
            "offset": offset,
            "conditions[0][property]": "recipient_state",
            "conditions[0][value]": state,
            "conditions[0][operator]": "=",
            "conditions[1][property]": "covered_recipient_specialty_1",
            "conditions[1][value]": "%Cardio%",
            "conditions[1][operator]": "like",
        }
        url = (f"https://openpaymentsdata.cms.gov/api/1/datastore/query/{dataset_id}/0"
               f"?{urlencode(params)}")
        for attempt in range(3):
            try:
                body = http_get_text(url, timeout=60)
                break
            except Exception as e:
                print(f"  retry {attempt} ({state}, offset {offset}): {e}")
                time.sleep(3)
        else:
            print(f"  giving up on {state} offset {offset}")
            break
        import json
        page = json.loads(body).get("results", [])
        if not page:
            break
        all_rows.extend(page)
        offset += page_size
        if len(page) < page_size:
            break
    return all_rows


if DOWNLOAD_DATA:
    open_payments_by_year = {}
    for year, dataset_id in OPEN_PAYMENTS_DATASET_IDS.items():
        rows = []
        for state in ("CA", "MT"):
            rows.extend(pull_open_payments_state_year(dataset_id, state))
        open_payments_by_year[year] = rows
        print(f"{year}: {len(rows)} CA/MT cardiology-specialty payment records")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1c. CMS Part D Prescriber data - paginated `data-api/v1` (all 4 years)
# MAGIC
# MAGIC **2026-07-30 - superseded from the original bulk-CSV-stream approach:**
# MAGIC the original 2021-2023 pull used `stream_filter_part_d` below (streaming
# MAGIC the static bulk CSV and filtering as it downloaded, because the
# MAGIC `data-api/v1` query endpoint timed out on every attempt at the time).
# MAGIC That method under-captured by roughly **36-135x** - confirmed by
# MAGIC independently re-pulling all four years through the paginated API
# MAGIC method below, which reproduced CY2023/CY2024's already-recorded counts
# MAGIC exactly on a second run each. See `DECISIONS.md`, "Part D 2021-2023
# MAGIC re-pulled with the corrected pagination method," for the full
# MAGIC verification. All four years now use the same method for consistency.
# MAGIC `stream_filter_part_d` is kept below only as a record of the original
# MAGIC (superseded) approach - `DOWNLOAD_DATA` never calls it.

# COMMAND ----------

PART_D_BULK_URLS_SUPERSEDED = {
    2021: "https://data.cms.gov/sites/default/files/2024-05/43359391-e7fa-40b9-9bd4-5dc295e18712/MUP_DPR_RY24_P04_V10_DY21_NPIBN.csv",
    2022: "https://data.cms.gov/sites/default/files/2024-05/18f82097-61a6-4889-9941-9a0b6ad7523c/MUP_DPR_RY24_P04_V10_DY22_NPIBN.csv",
    2023: "https://data.cms.gov/sites/default/files/2025-04/0d5915ce-002c-4d87-bde8-24ffb08bb6cc/MUP_DPR_RY25_P04_V10_DY23_NPIBN.csv",
}


def stream_filter_part_d(url):
    """SUPERSEDED - kept for the record only, not called. Streams the
    national bulk file and filters to CA/MT cardiology rows; this is the
    method that under-captured 2021-2023 by ~36-135x - see DECISIONS.md."""
    req = urllib.request.Request(url, headers={"User-Agent": BROWSER_UA})
    rows = []
    with urllib.request.urlopen(req, timeout=600) as resp:
        text_stream = io.TextIOWrapper(resp, encoding="utf-8", newline="")
        reader = csv.DictReader(text_stream)
        for row in reader:
            if row["Prscrbr_State_Abrvtn"] in ("CA", "MT") and "cardio" in row["Prscrbr_Type"].lower():
                rows.append(row)
    return rows


# MAGIC %md
# MAGIC #### Corrected method: paginated `data-api/v1`, all four years
# MAGIC
# MAGIC Per-year dataset IDs from `data.cms.gov/data.json`'s "Medicare Part D
# MAGIC Prescribers - by Provider and Drug" catalog entry (one distribution per
# MAGIC year). The endpoint caps pages at 6,500 rows regardless of the
# MAGIC requested `size` - paginate with `offset` until a page comes back short.

# COMMAND ----------

PART_D_DATASET_IDS = {
    2021: "f68114ed-f854-4ffc-9c6e-ed78b5e2f8d0",
    2022: "b101b457-ffa4-49bb-8fd9-27c1266086e2",
    2023: "e54db557-cd82-4e91-a0fe-61aad5865d69",
    2024: "d5aa71a8-dcc0-4570-8bcf-bd39deac69fe",
}
PART_D_API_PAGE_SIZE = 6500
PART_D_CARDIO_TYPES = (
    "Cardiology",
    "Interventional Cardiology",
    "Advanced Heart Failure and Transplant Cardiology",
)


def pull_part_d_via_api(dataset_id, state, prscrbr_type, page_size=PART_D_API_PAGE_SIZE):
    """Paginate the data-api endpoint for one (year's dataset, state, Prscrbr_Type) slice."""
    import json
    from urllib.parse import urlencode

    base = f"https://data.cms.gov/data-api/v1/dataset/{dataset_id}/data"
    rows = []
    offset = 0
    while True:
        params = {
            "filter[Prscrbr_State_Abrvtn]": state,
            "filter[Prscrbr_Type]": prscrbr_type,
            "size": page_size,
            "offset": offset,
        }
        url = f"{base}?{urlencode(params)}"
        for attempt in range(4):
            try:
                body = http_get_text(url, headers={"User-Agent": BROWSER_UA}, timeout=90)
                break
            except Exception as e:
                print(f"  retry {attempt} ({state}, {prscrbr_type}, offset {offset}): {e}")
                time.sleep(2 * (attempt + 1))
        else:
            raise RuntimeError(f"giving up on {state}/{prscrbr_type} offset {offset}")
        page = json.loads(body)
        rows.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return rows


if DOWNLOAD_DATA:
    part_d_by_year = {}
    for year, dataset_id in PART_D_DATASET_IDS.items():
        year_rows = []
        for state in ("CA", "MT"):
            for prscrbr_type in PART_D_CARDIO_TYPES:
                rows = pull_part_d_via_api(dataset_id, state, prscrbr_type)
                year_rows.extend(rows)
                print(f"{year} {state}/{prscrbr_type}: {len(rows)} rows")
        part_d_by_year[year] = year_rows
        print(f"{year} total: {len(year_rows)} CA/MT cardiology provider-drug rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Load the filtered extracts into Spark
# MAGIC
# MAGIC Reads from the already-filtered CSVs in `data/raw/` and `data/sample/`
# MAGIC (small illustrative slices are committed to `data/sample/` - the full
# MAGIC filtered extracts in `data/raw/` are gitignored).

# COMMAND ----------

npi_2023 = spark.read.csv(f"{BASE_PATH}/sample/npi_cardiology_ca_mt_20235.csv", header=True)
npi_2025 = spark.read.csv(f"{BASE_PATH}/sample/npi_cardiology_ca_mt_202507.csv", header=True)

part_d_2021 = spark.read.csv(f"{BASE_PATH}/sample/partd_2021_ca_mt_cardiology.csv", header=True)
part_d_2022 = spark.read.csv(f"{BASE_PATH}/sample/partd_2022_ca_mt_cardiology.csv", header=True)
part_d_2023 = spark.read.csv(f"{BASE_PATH}/sample/partd_2023_ca_mt_cardiology.csv", header=True)
part_d_2024 = spark.read.csv(f"{BASE_PATH}/sample/partd_2024_ca_mt_cardiology.csv", header=True)

# Open Payments extracts are large enough (tens of thousands of rows) that
# they live in data/raw/ rather than the committed data/sample/ folder.
open_payments_2023 = spark.read.csv(f"{BASE_PATH}/raw/open_payments/op_2023_ca_mt_cardiology.csv", header=True)
open_payments_2024 = spark.read.csv(f"{BASE_PATH}/raw/open_payments/op_2024_ca_mt_cardiology.csv", header=True)
open_payments_2025 = spark.read.csv(f"{BASE_PATH}/raw/open_payments/op_2025_ca_mt_cardiology.csv", header=True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Profile each source
# MAGIC
# MAGIC Checking the specific messiness Layer 1 will need to handle: null NPIs,
# MAGIC distinct name spellings per NPI, and provider counts across sources.
# MAGIC Real numbers from running this once already (see `DATA_SOURCES.md` for
# MAGIC the full writeup):
# MAGIC
# MAGIC - **NPI address drift, May 2023 -> July 2025** (1,510 providers present
# MAGIC   in both snapshots): 84 changed zip (5.6%), 51 changed practice city
# MAGIC   (3.4%), 2 changed name. 38 providers dropped out (deactivated or left
# MAGIC   CA/MT), 206 are new registrations. Real, usable SCD2 drift - not
# MAGIC   fabricated.
# MAGIC - **Part D "Cardiology" label originally looked narrow for 2021-2023 -
# MAGIC   that turned out to be a pagination bug in the original pull, not a
# MAGIC   real signal, and has since been corrected (see `DECISIONS.md`, "Part
# MAGIC   D 2021-2023 re-pulled with the corrected pagination method").
# MAGIC   Corrected counts, all four years now via the same paginated
# MAGIC   `data-api/v1` method:**
# MAGIC
# MAGIC   | Year | Rows | Distinct NPIs |
# MAGIC   |------|------|----------------|
# MAGIC   | 2021 | 83,752 | 2,158 |
# MAGIC   | 2022 | 83,747 | 2,176 |
# MAGIC   | 2023 | 84,822 | 2,232 |
# MAGIC   | 2024 | 86,698 | 2,254 |
# MAGIC
# MAGIC   All four years land in the same ~83,700-86,700 row / ~2,150-2,260 NPI
# MAGIC   range - the original 2021-2023 files' 1,272/719/2,356 rows and 33/16/57
# MAGIC   NPIs were a ~36-135x undercount, confirmed by reproducing CY2023 and
# MAGIC   CY2024's counts exactly on independent re-runs. Separately, Part D's
# MAGIC   specialty label (Medicare claims-derived, ~2,150-2,260 CA+MT NPIs/year)
# MAGIC   vs. NPPES's `207R%` taxonomy-derived count (1,548-1,716 CA+MT
# MAGIC   providers, a single roster not per-year) is still a real, expected
# MAGIC   cross-source disagreement on who counts as "cardiology" - exactly the
# MAGIC   kind of conflict Layer 1 exists to reckon with. Layer 1's actual run
# MAGIC   (see `01_identity_resolution.py`) found this disagreement is large:
# MAGIC   2,470 of Part D's 2,561 distinct NPIs (across all 4 years) and 4,040
# MAGIC   of Open Payments' 4,145 distinct NPIs (across all 3 years) don't
# MAGIC   match any NPI in the `207R%`-filtered golden roster at all.
# MAGIC - **Null NPI rate is low in this filtered slice, confirmed on the
# MAGIC   corrected data**: exactly 0% in Part D across all four years (NPI is
# MAGIC   required for inclusion in that dataset - 0 blank out of 83,752-86,698
# MAGIC   rows/year), and near-0% in Open Payments (11/78,327 in 2023, 1/78,617
# MAGIC   in 2024, 28/77,998 in 2025 - 0.00-0.04%). The null-NPI messiness the
# MAGIC   original spec anticipated is more visible in Open Payments' *national*
# MAGIC   data (teaching hospitals, non-covered recipients) than in this
# MAGIC   already-cardiology-filtered slice - confirmed, not just theorized:
# MAGIC   name/address/specialty-label mismatches are the primary messiness
# MAGIC   this slice actually has (see Layer 1's real match-rate numbers above
# MAGIC   and in `DECISIONS.md`), missing NPIs are a much smaller share than
# MAGIC   the original spec anticipated. **This was checked, not assumed to be
# MAGIC   fine**: a slice with 0% null NPIs could look suspiciously clean, but
# MAGIC   Layer 1's real match rates (11 auto_match / 1,220 review / 5,279
# MAGIC   no_match out of 6,510 unmatched-source candidates) confirm the
# MAGIC   messiness is real and substantial - it just shows up as identity-
# MAGIC   matching ambiguity rather than missing NPIs.

# COMMAND ----------

from pyspark.sql import functions as F

for name, df, npi_col in [
    ("Part D 2021", part_d_2021, "Prscrbr_NPI"),
    ("Part D 2022", part_d_2022, "Prscrbr_NPI"),
    ("Part D 2023", part_d_2023, "Prscrbr_NPI"),
    ("Part D 2024", part_d_2024, "Prscrbr_NPI"),
    ("Open Payments 2023", open_payments_2023, "covered_recipient_npi"),
    ("Open Payments 2024", open_payments_2024, "covered_recipient_npi"),
    ("Open Payments 2025", open_payments_2025, "covered_recipient_npi"),
]:
    total = df.count()
    null_npi = df.filter((F.col(npi_col).isNull()) | (F.col(npi_col) == "")).count()
    distinct_npi = df.select(npi_col).distinct().count()
    print(f"{name}: {total} rows, {distinct_npi} distinct NPIs, "
          f"{null_npi} null/blank NPI ({null_npi / total * 100:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Address drift between the two NPI snapshots
# MAGIC
# MAGIC This is the check that confirms the NBER two-snapshot approach actually
# MAGIC gives Layer 2 something real to track, rather than two copies of
# MAGIC identical data.

# COMMAND ----------

drift = (
    npi_2023.select("npi", "plname", "pfname", "ploccityname", "ploczip")
    .withColumnRenamed("plname", "plname_2023")
    .withColumnRenamed("pfname", "pfname_2023")
    .withColumnRenamed("ploccityname", "city_2023")
    .withColumnRenamed("ploczip", "zip_2023")
    .join(
        npi_2025.select("npi", "plname", "pfname", "ploccityname", "ploczip")
        .withColumnRenamed("plname", "plname_2025")
        .withColumnRenamed("pfname", "pfname_2025")
        .withColumnRenamed("ploccityname", "city_2025")
        .withColumnRenamed("ploczip", "zip_2025"),
        on="npi",
        how="inner",
    )
)

common_count = drift.count()
zip_changed = drift.filter(F.col("zip_2023") != F.col("zip_2025")).count()
city_changed = drift.filter(F.col("city_2023") != F.col("city_2025")).count()
name_changed = drift.filter(
    (F.col("plname_2023") != F.col("plname_2025")) | (F.col("pfname_2023") != F.col("pfname_2025"))
).count()

print(f"Providers present in both snapshots: {common_count}")
print(f"  zip changed: {zip_changed} ({zip_changed / common_count * 100:.1f}%)")
print(f"  city changed: {city_changed} ({city_changed / common_count * 100:.1f}%)")
print(f"  name changed: {name_changed} ({name_changed / common_count * 100:.1f}%)")

display(drift.filter(F.col("zip_2023") != F.col("zip_2025")).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next: Layer 2 (history tracking)
# MAGIC
# MAGIC This confirmed the filtered slice has real messiness to work with:
# MAGIC genuine address/name drift over time (Layer 2's reason to exist), and a
# MAGIC real, large specialty-label disagreement between NPPES taxonomy codes
# MAGIC and Part D/Open Payments' claims-derived specialty (Layer 1's reason to
# MAGIC exist). **Layer 1 is now built and run against this data** - see
# MAGIC `01_identity_resolution.py` and `DECISIONS.md` for the full matching
# MAGIC results and threshold-validation writeup. Layer 2 builds on Layer 1's
# MAGIC `golden_table` and `identity_review_queue` outputs.
# MAGIC
# MAGIC **Part D range note (2026-07-30):** with Part D extended to 2021-2024
# MAGIC (see `DECISIONS.md`), Layer 3's compound-flag stretch goal (HHI shift +
# MAGIC payment spike in the same year) now has **two** overlap years with
# MAGIC Open Payments - 2023 and 2024 - instead of one. `LIMITATIONS.md`
# MAGIC updated accordingly.
