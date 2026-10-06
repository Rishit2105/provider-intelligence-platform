# Layer 1 blocking + Jaro-Winkler scoring core - plain Python, no Spark
# dependency. Imported both by `01_identity_resolution.py` (wrapped in a
# PySpark UDF there) and directly by `tests/test_identity_matching.py`.
# A separate, ordinarily-importable module rather than being defined inline
# in the notebook file, specifically so it can be imported for testing
# without pulling in the notebook's unconditional `spark.read.csv(...)`
# calls (which only work inside Databricks - see DECISIONS.md "Layer 1:
# blocking + similarity scoring in plain Python, not a Spark UDF").
import re

import jellyfish


def normalize_name(s):
    if not s or isinstance(s, float):
        return ""
    s = str(s).upper().strip()
    s = re.sub(r"[^A-Z\s]", "", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip()


def component_scores_normalized(last1_norm, first1_norm, last2_norm, first2_norm):
    """Jaro-Winkler on last and first name separately (pre-normalized)."""
    last_score = jellyfish.jaro_winkler_similarity(last1_norm, last2_norm)
    first_score = jellyfish.jaro_winkler_similarity(first1_norm, first2_norm)
    return last_score, first_score


def combined_score(last_score, first_score):
    """Single number for ranking/display only - NOT used for the match/
    no-match decision. See classify_components for why a blended average
    can't be the decision driver: during real validation, an exact-surname /
    different-given-name pair ("PATEL MANDIRA" vs "PATEL NARMISHA") scored
    0.90+ on this blend alone, which would have auto-matched two different
    people - see DECISIONS.md."""
    return round(0.6 * last_score + 0.4 * first_score, 4)


def similarity_normalized(last1_norm, first1_norm, last2_norm, first2_norm):
    last_score, first_score = component_scores_normalized(last1_norm, first1_norm, last2_norm, first2_norm)
    return combined_score(last_score, first_score)


def name_similarity(last1, first1, last2, first2):
    """Weighted Jaro-Winkler: last name weighted higher (more discriminating).
    Display/ranking score only - see classify_components for the match decision."""
    return similarity_normalized(
        normalize_name(last1), normalize_name(first1),
        normalize_name(last2), normalize_name(first2),
    )


def specialty_label_from_text(text):
    """Descriptive 3-way label from free text - NOT used as a block key
    (see block_bucket_from_text below for why)."""
    t = (text or "").lower()
    if "interventional" in t:
        return "interventional"
    if "heart failure" in t or "transplant" in t:
        return "heart_failure_transplant"
    return "general_cardiology"


# Blocking uses only a 2-way split (interventional vs. general_cardiology).
# The NPI roster's ptaxcode->label mapping only independently confirms
# 207RI0011X (Interventional Cardiology) - see DATA_SOURCES.md - so it can't
# reliably distinguish "heart failure/transplant" from "general" among its
# other 28 unconfirmed 207R% codes; those default to general_cardiology. If
# Open Payments/Part D's block bucket used the richer 3-way split from free
# text, a true heart-failure specialist in those sources would never share a
# block key with their real golden-roster match (which defaulted to
# general_cardiology) - both sides of the block key must agree on what they
# can and can't distinguish. See DECISIONS.md "Layer 1: specialty bucketing
# is a verified-partial taxonomy mapping".
def block_bucket_from_text(text):
    return "interventional" if "interventional" in (text or "").lower() else "general_cardiology"


def specialty_bucket_from_ptaxcode(code):
    if code == "207RI0011X":
        return "interventional"
    return "general_cardiology"


def block_key(state, specialty_bucket):
    return (state, specialty_bucket)


AUTO_LAST_THRESHOLD = 0.98
AUTO_FIRST_THRESHOLD = 0.98
REVIEW_LAST_THRESHOLD = 0.85
REVIEW_FIRST_THRESHOLD = 0.55


def classify_components(last_score, first_score):
    """Match decision from the two name components independently, not a
    blended average. Auto-match requires both components >=0.98
    (effectively exact) - tightened from an initial 0.95/0.90 after real
    validation evidence: at 0.95/0.90, "SPIEGEL DAVID" auto-matched to
    "SIEGEL DAVID" (different surname, last_score 0.9571) and "HUSSAIN SYED"
    auto-matched to "HUSAIN SYEDA" (Syed/Syeda is a male/female name-suffix
    distinction in this naming convention - almost certainly different
    people, first_score 0.96). Both would have silently merged two distinct
    providers into one golden identity. See DECISIONS.md "Layer 1:
    threshold validation found real false positives" for the full evidence
    - 8 pairs total were demoted from auto_match to review for this reason,
    out of the original 28 candidates that cleared 0.95/0.90."""
    if last_score >= AUTO_LAST_THRESHOLD and first_score >= AUTO_FIRST_THRESHOLD:
        return "auto_match"
    if last_score >= REVIEW_LAST_THRESHOLD and first_score >= REVIEW_FIRST_THRESHOLD:
        return "review"
    return "no_match"


def find_many_to_one_collisions(auto_match_pairs):
    """Permanent post-match invariant, not a one-off validation script - see
    DECISIONS.md "Layer 1: many-to-one collision check is a standing
    pipeline invariant." `auto_match_pairs` is an iterable of
    (candidate_npi, source_npi) tuples already filtered to tier ==
    'auto_match'. Returns the set of candidate_npi values (golden NPIs)
    claimed by more than one distinct source_npi - a real conflict, not two
    independent confirmations, because it means two different source
    identities would otherwise silently merge into one golden record. (The
    same source_npi appearing once from Open Payments and once from Part D
    pointing at the same golden NPI is expected and is NOT a collision -
    that's one real provider correctly resolved from two sources - which is
    why this groups by candidate_npi and counts *distinct* source_npi, not
    raw row count.) Found for real during validation: NPIs 1225276652 and
    1619003860 both auto-matched to golden NPI 1003832346 ("Lee, David") -
    see DECISIONS.md for the full incident."""
    sources_by_candidate = {}
    for candidate_npi, source_npi in auto_match_pairs:
        sources_by_candidate.setdefault(candidate_npi, set()).add(source_npi)
    return {npi for npi, sources in sources_by_candidate.items() if len(sources) > 1}


def first_pass_status(last_score, first_score, source_city, candidate_city):
    """Mechanical first-pass review-queue status rules, applied ONLY within
    the review band - not judgment calls. Anything not covered stays
    'pending'.
    R1: both components ~exact (>=0.98) - a near-exact name that landed in
        review only by a hair under the auto thresholds.
    R2: last name near-exact (>=0.90) + first name a plausible spelling/
        nickname variant (>=0.75) + same city corroborating - e.g.
        "KATHERINE" vs "KATHRINE".
    R3: last name near-exact (>=0.90, i.e. same/near-same surname) + first
        name clearly different (<0.65) + DIFFERENT city - a common-surname
        collision (this data has many: Kim, Chen, Patel, Nguyen, Singh),
        not a name variant of the same person.
    """
    same_city = (
        source_city is not None and candidate_city is not None
        and source_city.strip().upper() == candidate_city.strip().upper()
    )
    if last_score >= 0.98 and first_score >= 0.98:
        return "confirmed_match"
    if last_score >= 0.90 and first_score >= 0.75 and same_city:
        return "confirmed_match"
    if last_score >= 0.90 and first_score < 0.65 and not same_city:
        return "confirmed_no_match"
    return "pending"
