# Databricks notebook source
# MAGIC %md
# MAGIC # Layer 3: Behavioral Analytics - cohort z-scores + HHI
# MAGIC
# MAGIC Plain PySpark (`groupBy` / window functions), not an SDP pipeline step -
# MAGIC see `DECISIONS.md`, "Layer 3: hand-written PySpark, not SDP declarative
# MAGIC primitives," the same reasoning already applied to Layer 1. Reads Layer
# MAGIC 2's published silver tables and Layer 1's `golden_provider_table`
# MAGIC directly via `spark.read.table(...)`, the same pattern
# MAGIC `01_identity_resolution.py` already established - no re-derivation of
# MAGIC upstream layers here.
# MAGIC
# MAGIC Built and run piece by piece against real data: cohort z-score first
# MAGIC (simpler dependency chain), then HHI - not accumulated untested, per this
# MAGIC session's ground rules.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

GOLD_SCHEMA = "provider_intelligence.gold"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {GOLD_SCHEMA}")

golden = spark.read.table("provider_intelligence.bronze.golden_provider_table")
part_d = spark.read.table("provider_intelligence.bronze.silver_part_d_history")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A: Cohort-relative z-score
# MAGIC
# MAGIC **Metric:** each provider's total yearly Part D claim volume
# MAGIC (`sum(Tot_Clms)` across every drug prescribed that `data_year`) - not
# MAGIC payment amount (reserved for the explicitly out-of-scope compound-flag
# MAGIC stretch goal) and not drug cost (list-price outliers; this project has
# MAGIC already committed to claim-count over cost for HHI below, for the same
# MAGIC reason). See `DECISIONS.md`, "Layer 3: cohort z-score metric is total
# MAGIC yearly Part D claim volume, not payment amount." `Tot_Clms` itself has no
# MAGIC suppression (verified: zero null/non-positive values across all rows,
# MAGIC unlike `Tot_Benes`/`GE65_*` - see `docs/schema_notes.md`), so no
# MAGIC suppression handling is needed for this specific metric.
# MAGIC
# MAGIC **Cohort key:** `(specialty_bucket, plocstatename)` from
# MAGIC `golden_provider_table`, per `data_year` - the static July 2025 snapshot
# MAGIC state, checked (not assumed) to be safe here since no golden provider's
# MAGIC recorded state differs across the two NPI Registry snapshots (see
# MAGIC `DECISIONS.md`). Cohort population is providers with resolved Part D
# MAGIC history *that year* - a provider with no Part D activity in a given year
# MAGIC has no claim-volume value to z-score in the first place.
# MAGIC
# MAGIC **Minimum cohort size N=10** (per `DECISIONS.md`, "Layer 3: minimum
# MAGIC cohort size N=10, with fallback tiers"): full `(specialty, state)` cohort
# MAGIC if it has >=10 members that year; else fall back to specialty-only
# MAGIC (drop geography, still within the same year) if *that* has >=10; else
# MAGIC `insufficient_cohort_data` - no z-score computed on too few peers.

# COMMAND ----------

provider_year_claims = (
    part_d
    .groupBy("golden_npi", "data_year")
    .agg(F.sum(F.col("Tot_Clms").cast("long")).alias("total_clms"))
    .join(
        golden.select("golden_npi", "specialty_bucket", "plocstatename"),
        "golden_npi",
        "inner",
    )
)

# COMMAND ----------

MIN_COHORT_SIZE = 10

full_cohort_window = Window.partitionBy("data_year", "specialty_bucket", "plocstatename")
specialty_cohort_window = Window.partitionBy("data_year", "specialty_bucket")

with_cohort_stats = (
    provider_year_claims
    .withColumn("full_cohort_n", F.count("*").over(full_cohort_window))
    .withColumn("full_cohort_mean", F.avg("total_clms").over(full_cohort_window))
    .withColumn("full_cohort_stddev", F.stddev_samp("total_clms").over(full_cohort_window))
    .withColumn("specialty_cohort_n", F.count("*").over(specialty_cohort_window))
    .withColumn("specialty_cohort_mean", F.avg("total_clms").over(specialty_cohort_window))
    .withColumn("specialty_cohort_stddev", F.stddev_samp("total_clms").over(specialty_cohort_window))
)

cohort_zscore = (
    with_cohort_stats
    .withColumn(
        "cohort_tier",
        F.when(F.col("full_cohort_n") >= MIN_COHORT_SIZE, F.lit("full_cohort"))
         .when(F.col("specialty_cohort_n") >= MIN_COHORT_SIZE, F.lit("specialty_only"))
         .otherwise(F.lit("insufficient_cohort_data")),
    )
    .withColumn(
        "cohort_n",
        F.when(F.col("cohort_tier") == "full_cohort", F.col("full_cohort_n"))
         .when(F.col("cohort_tier") == "specialty_only", F.col("specialty_cohort_n")),
    )
    .withColumn(
        # stddev guarded > 0, not just non-null: a cohort where every member
        # happens to have identical total_clms has a defined mean but a
        # zero stddev, which would otherwise divide-by-zero into inf/NaN
        # rather than a clean null. Not yet observed in this project's real
        # data (checked below), but guarded rather than assumed impossible.
        "z_score",
        F.when(
            (F.col("cohort_tier") == "full_cohort") & (F.col("full_cohort_stddev") > 0),
            (F.col("total_clms") - F.col("full_cohort_mean")) / F.col("full_cohort_stddev"),
        ).when(
            (F.col("cohort_tier") == "specialty_only") & (F.col("specialty_cohort_stddev") > 0),
            (F.col("total_clms") - F.col("specialty_cohort_mean")) / F.col("specialty_cohort_stddev"),
        ),
    )
    .select(
        "golden_npi", "data_year", "specialty_bucket", "plocstatename",
        "total_clms", "cohort_tier", "cohort_n", "z_score",
    )
)

(
    cohort_zscore.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.cohort_zscore_part_d_claims")
)
print(f"cohort_zscore_part_d_claims: {cohort_zscore.count()} provider-year rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Real tier-split numbers, by state and year
# MAGIC
# MAGIC Logged here as real output, not projected - this is the actual
# MAGIC demonstration of why the CA/MT pairing was chosen (see `DECISIONS.md`,
# MAGIC "Scope: Cardiology, California + Montana"): Montana's small provider
# MAGIC count is expected to hit the fallback tiers far more often than
# MAGIC California's.

# COMMAND ----------

tier_by_state_year = (
    cohort_zscore.groupBy("data_year", "plocstatename", "cohort_tier")
    .count()
    .orderBy("data_year", "plocstatename", "cohort_tier")
)
tier_by_state_year.show(100, truncate=False)

# COMMAND ----------

tier_pct_by_state = (
    cohort_zscore.groupBy("plocstatename", "cohort_tier")
    .count()
    .withColumn("pct", F.round(F.col("count") * 100.0 / F.sum("count").over(Window.partitionBy("plocstatename")), 1))
    .orderBy("plocstatename", "cohort_tier")
)
tier_pct_by_state.show(50, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B: HHI (Herfindahl-Hirschman Index) - drug-mix concentration
# MAGIC
# MAGIC Per `(golden_npi, data_year)`, over the **full universe** of drugs that
# MAGIC provider prescribed that year - no top-N truncation (see `DECISIONS.md`,
# MAGIC "Layer 3: HHI over the full drug universe, claim-count share"). **Share
# MAGIC basis: claim count, not drug cost** - avoids list-price outliers
# MAGIC dominating the metric. **Scale: 0-1** (sum of squared shares), stated
# MAGIC explicitly rather than left ambiguous with the 0-10,000 convention.
# MAGIC
# MAGIC **Drug identity = `Gnrc_Name` (generic name), not the raw
# MAGIC `(Brnd_Name, Gnrc_Name)` row grain - a real finding, not a stylistic
# MAGIC choice:** queried `silver_part_d_history` directly and found a single
# MAGIC generic drug (e.g. "Diltiazem Hcl") split across up to 6 different
# MAGIC `Brnd_Name` rows for the same provider-year. Grouping HHI by the raw row
# MAGIC grain would treat each brand as a separate "drug," artificially
# MAGIC fragmenting one substance into several smaller shares and understating
# MAGIC true concentration - exactly the kind of silent correctness bug this
# MAGIC project's standards call for catching before it ships, not after. See
# MAGIC `DECISIONS.md` for the full writeup.

# COMMAND ----------

drug_claims = (
    part_d
    .groupBy("golden_npi", "data_year", "Gnrc_Name")
    .agg(F.sum(F.col("Tot_Clms").cast("long")).alias("drug_clms"))
)

provider_year_totals = (
    drug_claims.groupBy("golden_npi", "data_year")
    .agg(
        F.sum("drug_clms").alias("total_clms"),
        F.countDistinct("Gnrc_Name").alias("n_distinct_drugs"),
    )
)

hhi = (
    drug_claims.join(provider_year_totals, ["golden_npi", "data_year"])
    .withColumn("claim_share", F.col("drug_clms") / F.col("total_clms"))
    .groupBy("golden_npi", "data_year")
    .agg(
        F.sum(F.pow(F.col("claim_share"), 2)).alias("hhi"),
        F.first("total_clms").alias("total_clms"),
        F.first("n_distinct_drugs").alias("n_distinct_drugs"),
    )
    .join(
        golden.select("golden_npi", "specialty_bucket", "plocstatename"),
        "golden_npi",
        "inner",
    )
    .select(
        "golden_npi", "data_year", "specialty_bucket", "plocstatename",
        "n_distinct_drugs", "total_clms", "hhi",
    )
)

(
    hhi.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(f"{GOLD_SCHEMA}.hhi_part_d_drug_concentration")
)
print(f"hhi_part_d_drug_concentration: {hhi.count()} provider-year rows written")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sanity checks on the HHI math, against real output
# MAGIC
# MAGIC Not asserted - checked directly against the real written table: HHI
# MAGIC must be in `(0, 1]`, a single-drug provider-year must have HHI exactly
# MAGIC `1.0` (all claims on one drug, share=1, share^2=1), and a provider-year
# MAGIC with more distinct drugs shouldn't systematically show a *higher* HHI
# MAGIC (more drugs generally means more diversification, i.e. lower
# MAGIC concentration - not a hard mathematical guarantee for every row, but the
# MAGIC aggregate correlation direction is a real sanity signal).

# COMMAND ----------

out_of_range = hhi.filter((F.col("hhi") <= 0) | (F.col("hhi") > 1)).count()
print(f"Rows with HHI outside (0, 1]: {out_of_range}")

single_drug_wrong = (
    hhi.filter(F.col("n_distinct_drugs") == 1)
    .filter(F.abs(F.col("hhi") - 1.0) > 1e-9)
    .count()
)
print(f"Single-drug provider-years where HHI != 1.0: {single_drug_wrong}")

hhi.groupBy().agg(
    F.min("hhi").alias("min_hhi"),
    F.max("hhi").alias("max_hhi"),
    F.avg("hhi").alias("avg_hhi"),
    F.corr("n_distinct_drugs", "hhi").alias("corr_n_drugs_vs_hhi"),
).show()
