# Known match / no-match pairs for Layer 1's blocking + similarity logic.
#
# Imports the plain-Python scoring core directly from
# notebooks/identity_matching_core.py - a real, ordinarily-importable
# module (not the Databricks-notebook-formatted 01_identity_resolution.py,
# which has unconditional spark.read.csv(...) calls that only work inside
# Databricks). See DECISIONS.md "Layer 1: matching core is a separate
# module, not inline" for why the split exists.
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "notebooks"))

from identity_matching_core import (
    normalize_name,
    component_scores_normalized,
    combined_score,
    block_bucket_from_text,
    specialty_bucket_from_ptaxcode,
    classify_components,
    first_pass_status,
    find_many_to_one_collisions,
    AUTO_FIRST_THRESHOLD,
)


def name_similarity(last1, first1, last2, first2):
    return combined_score(*component_scores_normalized(
        normalize_name(last1), normalize_name(first1),
        normalize_name(last2), normalize_name(first2),
    ))


def test_normalize_name_strips_punctuation_and_case():
    assert normalize_name("O'Brien, M.D.") == "OBRIEN MD"
    assert normalize_name("  Smith  ") == "SMITH"
    assert normalize_name(None) == ""
    assert normalize_name(float("nan")) == ""


def test_exact_name_scores_one():
    assert name_similarity("SMITH", "JOHN", "SMITH", "JOHN") == 1.0


def test_same_surname_different_given_name_scores_low_on_first_component():
    # Real scenario this project's data has repeatedly: two different
    # physicians sharing a common surname in the same blocking group.
    last_score, first_score = component_scores_normalized(
        *[normalize_name(x) for x in ("PATEL", "MANDIRA")],
        *[normalize_name(x) for x in ("PATEL", "NARMISHA")],
    )
    assert last_score == 1.0
    assert first_score < AUTO_FIRST_THRESHOLD


def test_blended_score_alone_would_hide_the_first_name_mismatch():
    # This is exactly why classify_components uses the two components
    # independently instead of a blended average - see DECISIONS.md.
    last_score, first_score = component_scores_normalized(
        normalize_name("PATEL"), normalize_name("MANDIRA"),
        normalize_name("PATEL"), normalize_name("NARMISHA"),
    )
    blended = combined_score(last_score, first_score)
    assert blended >= 0.85  # would look like a plausible match on the blend alone
    assert classify_components(last_score, first_score) != "auto_match"


def test_near_exact_but_different_surname_does_not_auto_match():
    # Real false positive found during validation: "Spiegel" vs "Siegel".
    last_score, first_score = component_scores_normalized(
        normalize_name("SPIEGEL"), normalize_name("DAVID"),
        normalize_name("SIEGEL"), normalize_name("DAVID"),
    )
    assert classify_components(last_score, first_score) != "auto_match"


def test_gendered_name_suffix_does_not_auto_match():
    # Real false positive found during validation: "Syed" (male) vs "Syeda"
    # (female) - different people despite a near-identical surname.
    last_score, first_score = component_scores_normalized(
        normalize_name("HUSSAIN"), normalize_name("SYED"),
        normalize_name("HUSAIN"), normalize_name("SYEDA"),
    )
    assert classify_components(last_score, first_score) != "auto_match"


def test_exact_name_both_components_auto_matches():
    last_score, first_score = component_scores_normalized(
        normalize_name("HUANG"), normalize_name("GARY"),
        normalize_name("HUANG"), normalize_name("GARY"),
    )
    assert classify_components(last_score, first_score) == "auto_match"


def test_wildly_different_names_score_no_match():
    last_score, first_score = component_scores_normalized(
        normalize_name("NGUYEN"), normalize_name("MINH"),
        normalize_name("OCONNOR"), normalize_name("PATRICK"),
    )
    assert classify_components(last_score, first_score) == "no_match"


def test_block_bucket_collapses_heart_failure_into_general():
    # The golden roster can't reliably distinguish heart-failure/transplant
    # from general (see DECISIONS.md "Layer 1: specialty bucketing is a
    # verified-partial taxonomy mapping"), so neither can the block key -
    # otherwise a true heart-failure specialist would never share a block
    # with their real golden-roster match.
    assert block_bucket_from_text("Advanced Heart Failure and Transplant Cardiology") == "general_cardiology"
    assert block_bucket_from_text("Interventional Cardiology") == "interventional"
    assert block_bucket_from_text("Cardiology") == "general_cardiology"


def test_ptaxcode_bucket_defaults_unconfirmed_codes_to_general():
    assert specialty_bucket_from_ptaxcode("207RI0011X") == "interventional"
    assert specialty_bucket_from_ptaxcode("207RA0002X") == "general_cardiology"  # unconfirmed code
    assert specialty_bucket_from_ptaxcode("207R00000X") == "general_cardiology"


def test_first_pass_status_confirms_match_on_near_exact_name():
    assert first_pass_status(1.0, 1.0, "SAN JOSE", "SAN JOSE") == "confirmed_match"


def test_first_pass_status_confirms_no_match_on_surname_collision():
    # Same surname, clearly different first name, different city - the
    # common-surname-collision pattern found repeatedly in this data
    # (Kim, Chen, Patel, Nguyen, Singh).
    assert first_pass_status(1.0, 0.55, "FRESNO", "SACRAMENTO") == "confirmed_no_match"


def test_first_pass_status_defers_genuinely_ambiguous_pairs():
    assert first_pass_status(0.90, 0.70, "FRESNO", "SACRAMENTO") == "pending"


def test_find_many_to_one_collisions_flags_two_distinct_source_npis():
    # Real case from this project's data: NPIs 1225276652 and 1619003860
    # both auto-matched to golden NPI 1003832346 ("Lee, David") - see
    # DECISIONS.md "Layer 1: many-to-one collision check is a standing
    # pipeline invariant."
    pairs = [
        ("1003832346", "1225276652"),
        ("1003832346", "1619003860"),
        ("1099999999", "1888888888"),
    ]
    assert find_many_to_one_collisions(pairs) == {"1003832346"}


def test_find_many_to_one_collisions_ignores_same_source_across_sources():
    # The same source NPI appearing once from Open Payments and once from
    # Part D, both pointing at the same golden NPI, is one real provider
    # correctly resolved from two sources - not a collision.
    pairs = [
        ("1003832346", "1225276652"),
        ("1003832346", "1225276652"),
    ]
    assert find_many_to_one_collisions(pairs) == set()


def test_find_many_to_one_collisions_empty_input():
    assert find_many_to_one_collisions([]) == set()
