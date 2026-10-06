# SDP gold layer - deliberately NOT used for Layer 3. Cohort z-scores and
# HHI are ordinary analytical group-by/window-function logic with no
# natural SDP declarative shape (no CDC, no streaming semantics, no
# expectation to express) - same reasoning as Layer 1's plain-PySpark
# decision, applied again. See DECISIONS.md, "Layer 3: hand-written
# PySpark, not SDP declarative primitives." Real implementation lives in
# notebooks/03_analytics.py, reading silver_part_d_history and
# golden_provider_table directly via spark.read.table(...), the same
# pattern 01_identity_resolution.py uses for Layer 1. Left in place as a
# stub rather than deleted, matching how silver_identity_resolution.py was
# left flagged once the Layer 1 plain-PySpark decision made it stale.
