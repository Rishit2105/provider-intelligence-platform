# SDP pipeline-level volume/row-count sanity check - a distinct category
# from the row-level warn/expect_or_drop/expect_or_fail gate tiers in
# silver_history_scd2.py (see DECISIONS.md, "Layer 2: data quality gate
# tiers"). Those evaluate one row in isolation; this evaluates whether a
# whole ingested batch is the right SHAPE. Directly motivated by the real
# Open Payments 2024 bug this project hit: a pagination loop silently gave
# up after connection errors and returned ~2,500 rows instead of ~78K, exit
# code 0, every one of those ~2,500 rows individually well-formed - no
# row-level check could have caught it. Full threshold justification
# (~50%, calibrated against that bug and the original Part D 2021-2023
# pagination undercount) and the expect_or_fail-vs-warn call are in
# DECISIONS.md, "Layer 2: pipeline-level volume/row-count sanity checks."
#
# Mechanism: SDP's @dp.expect(...) family is row-level by design - there is
# no first-class pipeline-level assertion. This reuses that row-level
# mechanism deliberately rather than inventing a parallel one: each bronze
# table is grouped by its own partition column (snapshot_month /
# program_year / data_year), producing one row per period carrying that
# period's row count and - via a window lag ordered by the partition value
# - the prior comparable period's row count. The expectation is then
# applied per PERIOD-ROW, not per source record. Runs after bronze
# ingestion completes (reads dp.read("bronze_...")) and, per SDP/Lakeflow
# semantics, an expect_or_fail violation aborts the entire pipeline update,
# not just this table - the same whole-pipeline-halting behavior already
# relied on by stg_golden_roster in silver_history_scd2.py. This project
# hasn't empirically stress-tested whether that abort can still leave a
# partial silver write committed from the same update; flagged honestly in
# LIMITATIONS.md rather than assumed safe.
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window

DEVIATION_THRESHOLD = 0.5


def _period_counts_with_prior(source_table, partition_col):
    """One row per distinct non-null value of partition_col in
    source_table, with that period's row_count and the prior period's
    row_count (via a lag ordered by the partition value itself, not
    ingestion order - so this is "prior comparable period," not "previous
    run"). Null partition values are excluded before grouping, not left to
    sort in as a bogus tiny "period" - found necessary for real: running
    this against real data caught 6 bronze_open_payments rows with a null
    program_year (a CSV multiLine parsing bug, since fixed in bronze.py -
    see DECISIONS.md), which without this filter formed a spurious
    smallest-sorting group and corrupted every real deviation computed
    after it. A row-level null-partition-value defect belongs to the
    row-level gate tiers (see silver_history_scd2.py), not this check -
    this check's job is period-to-period shape, not per-row validity."""
    counts = (
        dp.read(source_table)
        .filter(F.col(partition_col).isNotNull())
        .groupBy(F.col(partition_col).alias("period"))
        .agg(F.count(F.lit(1)).alias("row_count"))
    )
    ordered_by_period = Window.orderBy("period")
    return counts.withColumn(
        "prior_period_count", F.lag("row_count").over(ordered_by_period)
    ).withColumn(
        "pct_deviation",
        F.when(
            F.col("prior_period_count").isNotNull(),
            F.abs(F.col("row_count") - F.col("prior_period_count"))
            / F.col("prior_period_count"),
        ),
    )


# Shared predicate: the first period on record has no prior to compare
# against and passes trivially (nothing to detect a shortfall against, not
# an exemption from the check). Every later period must be within
# DEVIATION_THRESHOLD of the prior comparable period.
_VOLUME_EXPECTATION = (
    "prior_period_count IS NULL OR "
    f"(abs(row_count - prior_period_count) / prior_period_count) <= {DEVIATION_THRESHOLD}"
)


@dp.table(
    name="stg_volume_check_npi_registry",
    comment=(
        "Volume sanity check for bronze_npi_registry, one row per "
        "snapshot_month, each carrying its own row_count and the prior "
        "pulled snapshot's row_count. See module docstring / DECISIONS.md."
    ),
)
@dp.expect_or_fail("volume_within_tolerance", _VOLUME_EXPECTATION)
def stg_volume_check_npi_registry():
    return _period_counts_with_prior("bronze_npi_registry", "snapshot_month")


@dp.table(
    name="stg_volume_check_open_payments",
    comment=(
        "Volume sanity check for bronze_open_payments, one row per "
        "program_year, each carrying its own row_count and the prior "
        "program year's row_count. This is the check that would have "
        "caught the real 2024 truncation bug (~2,500 vs ~78K) - see "
        "DECISIONS.md."
    ),
)
@dp.expect_or_fail("volume_within_tolerance", _VOLUME_EXPECTATION)
def stg_volume_check_open_payments():
    return _period_counts_with_prior("bronze_open_payments", "program_year")


@dp.table(
    name="stg_volume_check_part_d",
    comment=(
        "Volume sanity check for bronze_part_d, one row per data_year, "
        "each carrying its own row_count and the prior data year's "
        "row_count. This is the check that would have caught the "
        "original Part D 2021-2023 pagination undercount - see "
        "DECISIONS.md."
    ),
)
@dp.expect_or_fail("volume_within_tolerance", _VOLUME_EXPECTATION)
def stg_volume_check_part_d():
    return _period_counts_with_prior("bronze_part_d", "data_year")
