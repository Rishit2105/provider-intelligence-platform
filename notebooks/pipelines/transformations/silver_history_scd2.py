# SDP silver layer: SCD2 history via AUTO CDC INTO, applying the
# attribute-type conflict-resolution precedence rule locked in DECISIONS.md
# ("Layer 2: conflict resolution across sources"): NPI Registry always wins
# identity/profile attributes (name, address, specialty); each transactional
# source always wins its own transactional facts, with no cross-source
# override, ever. That split is implemented literally below as two separate
# kinds of silver table, not one merged table with per-field precedence
# logic - identity attributes are never even read from Open Payments/Part D
# here, and transactional facts are never read from the NPI Registry.
#
# Reads Layer 1's published `golden_provider_table` / `identity_review_queue`
# Delta tables directly rather than re-deriving identity resolution here -
# see DECISIONS.md, "Layer 2: bronze layer scaffolded, silver/gold
# deliberately not started."
from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ---------------------------------------------------------------------------
# Golden-NPI resolution, shared by every silver table below
# ---------------------------------------------------------------------------

@dp.table(
    name="stg_golden_roster",
    comment=(
        "Layer 1's golden_provider_table, re-read with a defensive "
        "expect_or_fail checkpoint before it feeds SCD2 processing at all - "
        "see the expectation below."
    ),
)
@dp.expect_or_fail("no_duplicate_golden_npi", "NOT is_duplicate_golden_npi")
def stg_golden_roster():
    golden = spark.read.table("golden_provider_table")
    duplicate_count_per_npi = Window.partitionBy("golden_npi")
    # Fails the whole pipeline run, not just this table, if it ever fires -
    # this is exactly the invariant Layer 1's many-to-one collision check
    # (01_identity_resolution.py Step 5, backed by
    # identity_matching_core.find_many_to_one_collisions) exists to
    # guarantee before golden_provider_table is ever published. If this
    # fires, that check did not run, was bypassed, or regressed - Layer 2
    # must not silently build SCD2 history on top of a golden roster that
    # has already merged two distinct provider identities into one NPI. See
    # DECISIONS.md, "Layer 1: many-to-one collision check is a standing
    # pipeline invariant."
    return golden.withColumn(
        "is_duplicate_golden_npi",
        F.count("golden_npi").over(duplicate_count_per_npi) > 1,
    )


def _resolved_golden_npi_map(source_label):
    """Maps every source record's own NPI to its resolved golden_npi, for
    one source ('open_payments' or 'part_d'). Two ways a source NPI
    resolves to a golden identity: (1) it already equals a golden_npi
    (Layer 1's exact-NPI-match case - the majority of records), or (2) it's
    a `confirmed_match` row in `identity_review_queue` (Layer 1's
    Jaro-Winkler fallback path, manually/mechanically confirmed). Anything
    else - `no_match`, or still `pending` in the review queue - has no
    resolved golden identity yet and is excluded here on purpose: Layer 2
    tracks history for golden identities, not raw unresolved source
    records, matching 01_identity_resolution.py's closing note ("the golden
    table is what gets SCD2-tracked")."""
    golden = dp.read("stg_golden_roster")
    exact = golden.select(
        F.col("golden_npi").alias("source_npi"),
        F.col("golden_npi"),
    )
    confirmed = (
        spark.read.table("identity_review_queue")
        .filter((F.col("status") == "confirmed_match") & (F.col("source") == source_label))
        .select("source_npi", F.col("candidate_npi").alias("golden_npi"))
    )
    return exact.unionByName(confirmed).dropDuplicates(["source_npi"])


# ---------------------------------------------------------------------------
# Identity/profile attribute history - NPI Registry only, ever
# ---------------------------------------------------------------------------
#
# Two fallback triggers were considered for "NPI Registry has no snapshot
# covering a provider for a given window" (see DECISIONS.md):
#   (A) a golden NPI with NO NPI Registry snapshot row at all - structurally
#       unreachable here, not implemented as dead code. golden_provider_table
#       is built directly from the July 2025 NPI Registry extract
#       (01_identity_resolution.py Step 2), so every golden_npi has at least
#       one NPI Registry row by construction. This is a real, checked fact
#       about this pipeline's data flow, not an assumption - if Layer 1's
#       golden-roster construction ever changes to admit golden identities
#       from a non-NPI source, this comment and the code below both need
#       revisiting.
#   (B) a golden NPI present in the July 2025 snapshot but absent from the
#       earlier May 2023 one (a provider who received their NPI, or was
#       first captured by NBER's archive, sometime in between) - the real,
#       reachable case, implemented below as `stg_identity_fallback_backfill`.

@dp.table(
    name="stg_npi_identity_attributes",
    comment=(
        "Golden NPIs' identity/profile attributes from both pulled NPI "
        "Registry snapshots. `snapshot_month` is the AUTO CDC INTO "
        "sequence column and is explicitly a PROXY for the true change "
        "date, not the true change date itself - NPPES gives point-in-time "
        "snapshots, not a change log, so a detected difference between the "
        "2023-05 and 2025-07 rows could reflect a change that actually "
        "happened anywhere in that ~26-month window."
    ),
)
def stg_npi_identity_attributes():
    npi = dp.read("bronze_npi_registry")
    golden = dp.read("stg_golden_roster").select("golden_npi")
    return (
        npi.join(golden, npi.npi == golden.golden_npi, "inner")
        .select(
            F.col("golden_npi"),
            F.col("plname"),
            F.col("pfname"),
            F.col("plocstatename"),
            F.col("ploccityname"),
            F.col("snapshot_month"),
        )
        .withColumn("source_of_truth", F.lit("npi_registry"))
        .withColumn("effective_date_is_proxy", F.lit(True))
        .withColumn("proxy_confidence", F.lit("snapshot_pull_date"))
    )


@dp.table(
    name="stg_identity_fallback_backfill",
    comment=(
        "Low-confidence backfill identity row for fallback trigger (B) "
        "above - golden NPIs present in the 2025-07 NPI Registry snapshot "
        "but absent from the 2023-05 one. No NPI Registry snapshot covers "
        "that earlier window, so the earliest available transactional "
        "record's own embedded name/city stands in, sequenced to sort "
        "before any real NPI Registry row. See DECISIONS.md, 'Layer 2: "
        "conflict resolution across sources', narrow fallback."
    ),
)
def stg_identity_fallback_backfill():
    npi_attrs = dp.read("stg_npi_identity_attributes")
    covered_2023 = (
        npi_attrs.filter(F.col("snapshot_month") == "2023-05")
        .select("golden_npi").distinct()
    )
    needs_backfill = (
        npi_attrs.select("golden_npi").distinct()
        .join(covered_2023, "golden_npi", "left_anti")
    )

    op_map = _resolved_golden_npi_map("open_payments")
    op_candidates = (
        dp.read("bronze_open_payments").alias("op")
        .join(op_map, F.col("op.covered_recipient_npi") == op_map.source_npi)
        .join(needs_backfill, "golden_npi")
        .select(
            "golden_npi",
            F.col("op.covered_recipient_last_name").alias("plname"),
            F.col("op.covered_recipient_first_name").alias("pfname"),
            F.col("op.recipient_city").alias("ploccityname"),
            F.col("op.recipient_state").alias("plocstatename"),
            F.to_date("op.date_of_payment").alias("event_date"),
        )
    )
    partd_map = _resolved_golden_npi_map("part_d")
    partd_candidates = (
        dp.read("bronze_part_d").alias("pd")
        .join(partd_map, F.col("pd.Prscrbr_NPI") == partd_map.source_npi)
        .join(needs_backfill, "golden_npi")
        .select(
            "golden_npi",
            F.col("pd.Prscrbr_Last_Org_Name").alias("plname"),
            F.col("pd.Prscrbr_First_Name").alias("pfname"),
            F.col("pd.Prscrbr_City").alias("ploccityname"),
            F.col("pd.Prscrbr_State_Abrvtn").alias("plocstatename"),
            F.to_date(F.concat(F.col("pd.data_year"), F.lit("-01-01"))).alias("event_date"),
        )
    )

    earliest_per_npi = Window.partitionBy("golden_npi").orderBy(F.col("event_date").asc())
    return (
        op_candidates.unionByName(partd_candidates)
        .withColumn("rank", F.row_number().over(earliest_per_npi))
        .filter(F.col("rank") == 1)
        .drop("rank", "event_date")
        # Sentinel sequence value, not a real snapshot: string-sorts before
        # every real "YYYY-MM" snapshot_month value (including 2023-05),
        # which is what AUTO CDC INTO needs to treat this as the earliest
        # known state for these golden NPIs.
        .withColumn("snapshot_month", F.lit("0000-00"))
        .withColumn("source_of_truth", F.lit("transactional_fallback_low_confidence"))
        .withColumn("effective_date_is_proxy", F.lit(True))
        .withColumn("proxy_confidence", F.lit("earliest_transactional_record_low_confidence"))
    )


dp.create_streaming_table(name="silver_provider_identity_history")

# Two AUTO CDC INTO flows feed one target table (NPI Registry attributes,
# plus the narrow fallback backfill). Found while running for real: Lakeflow
# requires distinct `name=` on each flow once a target has more than one -
# without it, both flows default to the target table's own name and collide
# ("Cannot have multiple queries named ... Additional queries on that table
# must be named" - see DECISIONS.md).
dp.create_auto_cdc_flow(
    name="npi_identity_attributes_flow",
    target="silver_provider_identity_history",
    source="stg_npi_identity_attributes",
    keys=["golden_npi"],
    sequence_by=F.col("snapshot_month"),
    stored_as_scd_type=2,
)

dp.create_auto_cdc_flow(
    name="identity_fallback_backfill_flow",
    target="silver_provider_identity_history",
    source="stg_identity_fallback_backfill",
    keys=["golden_npi"],
    sequence_by=F.col("snapshot_month"),
    stored_as_scd_type=2,
)


# ---------------------------------------------------------------------------
# Transactional fact history - each source tracked separately, never
# overridden by another source. Business-key duplicates are flagged (warn
# tier - see DECISIONS.md, "Layer 2: data quality gate tiers") and counted,
# not deduplicated: still fed into SCD2 as-is.
# ---------------------------------------------------------------------------

@dp.table(
    name="stg_open_payments_resolved",
    comment=(
        "Open Payments rows joined to their resolved golden_npi (see "
        "_resolved_golden_npi_map). Unresolved rows (no_match / still "
        "pending in identity_review_queue) are excluded - not yet "
        "attachable to a golden identity."
    ),
)
@dp.expect("no_unexplained_business_key_duplicate", "NOT is_business_key_duplicate")
def stg_open_payments_resolved():
    op = dp.read("bronze_open_payments")
    resolved = _resolved_golden_npi_map("open_payments")
    business_key = [
        "golden_npi",
        "applicable_manufacturer_or_applicable_gpo_making_payment_name",
        "total_amount_of_payment_usdollars",
        "date_of_payment",
        "nature_of_payment_or_transfer_of_value",
    ]
    duplicate_window = Window.partitionBy(*business_key)
    return (
        op.join(resolved, op.covered_recipient_npi == resolved.source_npi, "inner")
        .withColumn(
            "is_business_key_duplicate",
            (F.count("record_id").over(duplicate_window) > 1) & (F.col("change_type") == "NEW"),
        )
    )


dp.create_streaming_table(name="silver_open_payments_history")

dp.create_auto_cdc_flow(
    target="silver_open_payments_history",
    source="stg_open_payments_resolved",
    keys=["record_id"],
    # payment_publication_date, not date_of_payment: this needs to sequence
    # *versions of the record* (original vs. corrected/disputed), and
    # date_of_payment is the underlying event date, which doesn't change
    # when a record is corrected - see DECISIONS.md's Open Payments
    # correction/dispute mechanic note.
    sequence_by=F.col("payment_publication_date"),
    stored_as_scd_type=2,
)


@dp.table(
    name="stg_part_d_resolved",
    comment=(
        "Part D rows joined to their resolved golden_npi. Part D has no "
        "per-row record id - its natural grain is one row per "
        "(prescriber, drug, year) - so the AUTO CDC INTO key below is "
        "(golden_npi, brand, generic) with data_year as the sequence "
        "column, not a business-key duplicate check like Open Payments' "
        "(a repeated NPI+drug+year *is* the key here, not a possible "
        "duplicate of it)."
    ),
)
def stg_part_d_resolved():
    part_d = dp.read("bronze_part_d")
    resolved = _resolved_golden_npi_map("part_d")
    return part_d.join(resolved, part_d.Prscrbr_NPI == resolved.source_npi, "inner")


dp.create_streaming_table(name="silver_part_d_history")

dp.create_auto_cdc_flow(
    target="silver_part_d_history",
    source="stg_part_d_resolved",
    keys=["golden_npi", "Brnd_Name", "Gnrc_Name"],
    sequence_by=F.col("data_year"),
    stored_as_scd_type=2,
)
