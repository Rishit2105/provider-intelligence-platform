# Sanity checks on the HHI concentration metric math - a pure-Python/pandas
# reimplementation of the same formula notebooks/03_analytics.py computes in
# PySpark (sum of squared claim-count shares, grouped by generic drug name),
# checked in isolation without a Spark session, the same local-validation
# pattern identity_matching_core.py's functions already use for Layer 1.
# The real PySpark output was verified directly against Databricks
# (zero out-of-range HHI, zero single-drug mismatches, corr(n_drugs, hhi) <
# 0 - see DECISIONS.md); these tests pin the formula itself so a future
# change can't silently break it without a test failing locally first.
import pandas as pd
import pytest


def hhi_from_claims(df: pd.DataFrame) -> float:
    """df has one row per drug with a 'clms' column. Mirrors
    03_analytics.py's Part B: claim_share = drug_clms / total_clms,
    hhi = sum(claim_share ** 2)."""
    total = df["clms"].sum()
    shares = df["clms"] / total
    return (shares ** 2).sum()


def test_single_drug_hhi_is_exactly_one():
    df = pd.DataFrame({"drug": ["Amlodipine"], "clms": [50]})
    assert hhi_from_claims(df) == pytest.approx(1.0)


def test_two_equal_shares_hhi_is_half():
    df = pd.DataFrame({"drug": ["A", "B"], "clms": [50, 50]})
    assert hhi_from_claims(df) == pytest.approx(0.5)


def test_highly_diversified_hhi_is_low():
    # 100 drugs, one claim each - maximally diversified for this claim count.
    df = pd.DataFrame({"drug": [f"drug_{i}" for i in range(100)], "clms": [1] * 100})
    assert hhi_from_claims(df) == pytest.approx(0.01)


def test_hhi_always_in_unit_interval():
    cases = [
        pd.DataFrame({"drug": ["A"], "clms": [1]}),
        pd.DataFrame({"drug": ["A", "B", "C"], "clms": [10, 5, 1]}),
        pd.DataFrame({"drug": [f"d{i}" for i in range(50)], "clms": list(range(1, 51))}),
    ]
    for df in cases:
        hhi = hhi_from_claims(df)
        assert 0 < hhi <= 1


def test_generic_name_aggregation_collapses_brand_fragmentation():
    """Regression test for the real bug found building HHI: a single
    generic drug split across multiple brand-name rows (e.g. real data's
    "Diltiazem Hcl" across up to 6 Brnd_Name values) must be summed to one
    drug before computing shares, not left as separate rows - otherwise
    HHI is silently understated (artificial fragmentation -> more, smaller
    shares -> lower HHI), the exact bug caught in DECISIONS.md."""
    raw_rows = pd.DataFrame({
        "brand": ["Cardizem", "Tiazac", "Cartia", "Generic"],
        "generic": ["Diltiazem Hcl"] * 4,
        "clms": [10, 8, 6, 4],
    })
    other_drug = pd.DataFrame({
        "brand": ["OtherBrand"],
        "generic": ["Metoprolol Tartrate"],
        "clms": [2],
    })
    all_rows = pd.concat([raw_rows, other_drug], ignore_index=True)

    # Wrong: HHI computed on the raw (brand, generic) grain fragments
    # Diltiazem into 4 separate "drugs."
    wrong_hhi = hhi_from_claims(all_rows.rename(columns={"clms": "clms"}))

    # Right: aggregate to generic name first, matching 03_analytics.py's
    # drug_claims groupBy("golden_npi", "data_year", "Gnrc_Name").
    collapsed = all_rows.groupby("generic", as_index=False)["clms"].sum()
    correct_hhi = hhi_from_claims(collapsed)

    assert correct_hhi > wrong_hhi
    # Diltiazem (28 claims) + Metoprolol (2 claims) out of 30 total.
    expected = (28 / 30) ** 2 + (2 / 30) ** 2
    assert correct_hhi == pytest.approx(expected)
