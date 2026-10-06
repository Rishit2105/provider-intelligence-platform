# SDP bronze layer: raw ingestion of the three sources into streaming
# tables, no transformation beyond typing and a source/ingest-time column.
# The AUTO CDC INTO / conflict-resolution merge logic now lives in
# silver_history_scd2.py, applying the attribute-type precedence rule
# locked in DECISIONS.md ("Layer 2: conflict resolution across sources") -
# NPI Registry wins identity/profile facts, transactional sources always
# win their own facts.
#
# multiLine=True on every CSV read below: found necessary for real, not
# defensive guessing. Running the new volume/row-count sanity check
# (volume_sanity_check.py) against real Open Payments data surfaced 3
# records with an embedded newline inside a quoted free-text field (e.g. a
# manufacturer name); Spark's default single-line CSV reader split each
# into two corrupted physical rows - one keeping a real NPI but with every
# later column (date, amount, nature of payment) nulled out, the other
# with a null NPI and the tail of the original field's text landing in the
# wrong column. See DECISIONS.md, "Layer 2: pipeline-level volume/row-count
# sanity checks" for the discovery. Applied to all three sources, not just
# Open Payments where it was actually observed - all three are
# CMS-published free-text-bearing CSVs pulled the same way, so the same
# failure mode could occur in either of the others even though it hasn't
# been.
from pyspark import pipelines as dp
from pyspark.sql import functions as F

RAW_VOLUME = "/Volumes/provider_intelligence/raw/source_files"


@dp.table(
    name="bronze_npi_registry",
    comment="Raw NPI Registry snapshots (NBER archive), both pulled months, unioned with a snapshot_month column.",
)
@dp.expect("valid_npi", "npi IS NOT NULL AND npi != ''")
def bronze_npi_registry():
    snapshot_2023 = (
        spark.read.csv(f"{RAW_VOLUME}/npi/npi_cardiology_ca_mt_20235.csv", header=True, multiLine=True)
        .withColumn("snapshot_month", F.lit("2023-05"))
    )
    snapshot_2025 = (
        spark.read.csv(f"{RAW_VOLUME}/npi/npi_cardiology_ca_mt_202507.csv", header=True, multiLine=True)
        .withColumn("snapshot_month", F.lit("2025-07"))
    )
    return (
        snapshot_2023.unionByName(snapshot_2025)
        .withColumn("_ingested_at", F.current_timestamp())
    )


@dp.table(
    name="bronze_open_payments",
    comment="Raw Open Payments records, all pulled program years, unioned with the program_year column already present in the source data.",
)
@dp.expect("valid_npi", "covered_recipient_npi IS NOT NULL AND covered_recipient_npi != ''")
def bronze_open_payments():
    years = (2023, 2024, 2025)
    frames = [
        spark.read.csv(f"{RAW_VOLUME}/open_payments/op_{year}_ca_mt_cardiology.csv", header=True, multiLine=True)
        for year in years
    ]
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result.withColumn("_ingested_at", F.current_timestamp())


@dp.table(
    name="bronze_part_d",
    comment=(
        "Raw Part D Prescriber records, all four corrected years (2021-2024), "
        "unioned with a data_year column. Pulled via the corrected paginated "
        "data-api/v1 method - see DECISIONS.md 'Part D 2021-2023 re-pulled "
        "with the corrected pagination method.'"
    ),
)
@dp.expect("valid_npi", "Prscrbr_NPI IS NOT NULL AND Prscrbr_NPI != ''")
def bronze_part_d():
    years = (2021, 2022, 2023, 2024)
    frames = [
        spark.read.csv(f"{RAW_VOLUME}/part_d/partd_{year}_ca_mt_cardiology.csv", header=True, multiLine=True)
        .withColumn("data_year", F.lit(year))
        for year in years
    ]
    result = frames[0]
    for frame in frames[1:]:
        result = result.unionByName(frame)
    return result.withColumn("_ingested_at", F.current_timestamp())
