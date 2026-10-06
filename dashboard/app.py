# Streamlit dashboard - event log, quarantine counts, review queue size,
# cohort fallback-tier breakdown. Reads two kinds of source: the committed
# `data/identity_review_queue.csv` (local, always available, carries the
# manual adjudication - see DECISIONS.md "Manual adjudication of the 61-pair
# validation sample") and live Databricks tables/pipeline events (Layer 2's
# quarantine/warn counts, Layer 3's cohort gold table, the Lakeflow pipeline
# event log) via the SQL Statement Execution and Pipelines REST APIs, using
# the same `~/.databrickscfg` token this project's other Databricks work
# already relies on (see DECISIONS.md, "Layer 2: run for real on Databricks
# Free Edition" - never committed to the repo). Plain `urllib`/REST calls,
# not the `databricks-sdk` package, to avoid a new dependency for four
# read-only queries - consistent with requirements.txt's "minimal deps"
# framing.
import configparser
import json
import os
import time
import urllib.error
import urllib.request
from pathlib import Path

import pandas as pd
import streamlit as st

REPO_ROOT = Path(__file__).resolve().parent.parent
REVIEW_QUEUE_CSV = REPO_ROOT / "data" / "identity_review_queue.csv"

# Workspace/pipeline/warehouse IDs, not secrets - host and pipeline name are
# already committed in databricks.yml; the SQL warehouse is the Free
# Edition workspace's default "Serverless Starter Warehouse" (not something
# this project provisioned). Overridable via env vars for a different
# workspace without editing code.
PIPELINE_ID = os.environ.get("PIPELINE_ID", "cbb807b3-e934-44d9-92f8-7d06a63bf30e")
WAREHOUSE_ID = os.environ.get("DATABRICKS_WAREHOUSE_ID", "a8b6cf7829d57be1")

st.set_page_config(page_title="Provider Intelligence Platform", layout="wide")


def _load_databricks_credentials():
    """Env vars first (DATABRICKS_HOST/DATABRICKS_TOKEN), then the DEFAULT
    profile in ~/.databrickscfg - the same file this project's Layer 2/3
    Databricks runs already authenticate with, never committed to the repo
    (see .gitignore's .databricks/ entry and DECISIONS.md)."""
    host = os.environ.get("DATABRICKS_HOST")
    token = os.environ.get("DATABRICKS_TOKEN")
    if host and token:
        return host.rstrip("/"), token
    cfg_path = Path.home() / ".databrickscfg"
    if not cfg_path.exists():
        return None, None
    cfg = configparser.ConfigParser()
    cfg.read(cfg_path)
    if "DEFAULT" not in cfg:
        return None, None
    return cfg["DEFAULT"].get("host", "").rstrip("/"), cfg["DEFAULT"].get("token")


def _api_call(host, token, path, method="GET", body=None, timeout=30):
    data = json.dumps(body).encode() if body is not None else None
    req = urllib.request.Request(
        host + path, data=data, method=method,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read())


def _run_sql(host, token, statement, poll_timeout=90):
    """Statement Execution API against the serverless SQL warehouse. The
    warehouse auto-starts on first query but is often STOPPED between
    sessions (serverless, no idle cost) - polling handles the cold-start
    delay rather than assuming it's already warm."""
    r = _api_call(host, token, "/api/2.0/sql/statements", "POST", {
        "warehouse_id": WAREHOUSE_ID, "statement": statement, "wait_timeout": "30s",
    })
    statement_id = r["statement_id"]
    start = time.time()
    while r["status"]["state"] in ("PENDING", "RUNNING"):
        if time.time() - start > poll_timeout:
            raise TimeoutError(f"SQL statement did not finish within {poll_timeout}s (warehouse cold-start?)")
        time.sleep(2)
        r = _api_call(host, token, f"/api/2.0/sql/statements/{statement_id}")
    if r["status"]["state"] != "SUCCEEDED":
        raise RuntimeError(f"SQL statement failed: {r['status']}")
    cols = [c["name"] for c in r["manifest"]["schema"]["columns"]]
    rows = r.get("result", {}).get("data_array", []) or []
    return pd.DataFrame(rows, columns=cols)


@st.cache_data(ttl=300)
def load_review_queue():
    return pd.read_csv(REVIEW_QUEUE_CSV)


@st.cache_data(ttl=300)
def load_live_data(_host, _token):
    """Everything that requires a live Databricks connection, fetched in
    one pass so a single connectivity failure degrades the whole "live"
    section rather than partially rendering. Returns a dict of DataFrames/
    lists; raises on the first failure so the caller can fall back."""
    mechanical_queue = _run_sql(
        _host, _token,
        "SELECT status, count(*) AS n FROM provider_intelligence.bronze.identity_review_queue "
        "GROUP BY status",
    )
    quarantine = _run_sql(
        _host, _token,
        "SELECT "
        "  count(*) AS total_rows, "
        "  sum(CASE WHEN is_business_key_duplicate THEN 1 ELSE 0 END) AS flagged_warn "
        "FROM provider_intelligence.bronze.silver_open_payments_history",
    )
    cohort_tiers = _run_sql(
        _host, _token,
        "SELECT plocstatename AS state, cohort_tier, count(*) AS n "
        "FROM provider_intelligence.gold.cohort_zscore_part_d_claims "
        "GROUP BY plocstatename, cohort_tier "
        "ORDER BY plocstatename, cohort_tier",
    )
    events_raw = _api_call(
        _host, _token, f"/api/2.0/pipelines/{PIPELINE_ID}/events?max_results=25",
    )
    events = []
    for e in events_raw.get("events", []):
        origin = e.get("origin", {})
        events.append({
            "timestamp": e.get("timestamp"),
            "level": e.get("level"),
            "event_type": e.get("event_type"),
            "flow_or_table": origin.get("flow_name") or origin.get("update_id", ""),
            "message": e.get("message"),
        })
    events_df = pd.DataFrame(events)
    return {
        "mechanical_queue": mechanical_queue,
        "quarantine": quarantine,
        "cohort_tiers": cohort_tiers,
        "events": events_df,
    }


st.title("Provider Intelligence Platform")
st.caption(
    "Identity resolution, history tracking, and behavioral analytics across "
    "NPI Registry, CMS Open Payments, and CMS Part D - cardiology, CA + MT. "
    "See README.md / DECISIONS.md / LIMITATIONS.md for the full writeups "
    "behind every number on this page."
)

host, token = _load_databricks_credentials()
live = None
live_error = None
if host and token:
    try:
        with st.spinner("Querying Databricks (serverless warehouse may be cold-starting, up to ~60s)..."):
            live = load_live_data(host, token)
    except Exception as exc:  # noqa: BLE001 - surfaced to the UI, not swallowed
        live_error = str(exc)
else:
    live_error = "No Databricks credentials found (~/.databrickscfg or DATABRICKS_HOST/DATABRICKS_TOKEN)."

if live is not None:
    st.success("Connected to Databricks - Layer 2/3 panels below are live query results.")
else:
    st.warning(
        f"Running in local-only mode: {live_error} "
        "Review queue panel still renders from the committed CSV; "
        "quarantine, cohort-tier, and event-log panels need a live connection."
    )

st.divider()

# --- Review queue size --------------------------------------------------
st.header("Identity review queue")
queue = load_review_queue()
status_counts = queue["status"].value_counts()
col1, col2, col3, col4 = st.columns(4)
col1.metric("Total rows", len(queue))
col2.metric("pending", int(status_counts.get("pending", 0)))
col3.metric("confirmed_match", int(status_counts.get("confirmed_match", 0)))
col4.metric("confirmed_no_match", int(status_counts.get("confirmed_no_match", 0)))
st.bar_chart(status_counts)
st.caption(
    "Source: `data/identity_review_queue.csv` - includes the manual adjudication "
    "of the 61-pair validation sample and the many-to-one collision-check fix "
    "(see DECISIONS.md). All 1,220 rows are California; Montana's unmatched "
    "records score too low to ever enter the review band at all - see "
    "DECISIONS.md, \"MT's ~6% Part D resolution rate investigated.\""
)

if live is not None:
    with st.expander("Compare against the live Databricks table (mechanical first-pass only, not adjudicated)"):
        mech = live["mechanical_queue"].set_index("status")["n"].astype(int)
        st.dataframe(mech.rename("count"))
        st.caption(
            "This is `provider_intelligence.bronze.identity_review_queue` as written by "
            "`01_identity_resolution.py`'s own mechanical rules - the manual adjudication "
            "was applied only to the exported CSV above, never re-persisted to this table "
            "(see SESSION_LOG.md, 2026-07-30 'later' entry). The two sources are expected "
            "to disagree; this panel exists to make that gap visible, not to reconcile it."
        )

st.divider()

# --- Quarantine / data quality gates ------------------------------------
st.header("Data quality gates")
st.caption(
    "Tiered per DECISIONS.md, \"Layer 2: data quality gate tiers\": `warn` (flagged, "
    "not blocked), `expect_or_drop` (quarantined - none locked yet, see LIMITATIONS.md), "
    "`expect_or_fail` (halts the pipeline run - the volume/row-count sanity check)."
)
if live is not None:
    q = live["quarantine"].iloc[0]
    qcol1, qcol2, qcol3 = st.columns(3)
    qcol1.metric("warn: business-key duplicates", int(q["flagged_warn"]))
    qcol2.metric("silver_open_payments_history rows", int(q["total_rows"]))
    qcol3.metric("expect_or_drop quarantined rows", 0, help="No expect_or_drop-tier check is locked yet - genuinely open, not a gap in this dashboard.")
    st.caption(
        f"{int(q['flagged_warn'])} of {int(q['total_rows'])} Open Payments silver rows are flagged "
        "`is_business_key_duplicate` - counted and kept, not dropped, since it's genuinely "
        "unresolved whether these are real repeated events or a reporting artifact (LIMITATIONS.md)."
    )
else:
    st.info("Needs a live Databricks connection - last known real run (2026-07-31): 8 of 6,443 "
            "`silver_open_payments_history` rows flagged `is_business_key_duplicate`; 0 rows in "
            "`expect_or_drop` (tier not yet locked); 0 `expect_or_fail` firings on the current "
            "corrected data (see DECISIONS.md).")

st.divider()

# --- Cohort fallback-tier breakdown --------------------------------------
st.header("Layer 3: cohort fallback-tier breakdown")
st.caption(
    "N=10 minimum (specialty, state) cohort; falls back to specialty-only, then "
    "`insufficient_cohort_data` - see DECISIONS.md, \"Layer 3: minimum cohort size N=10.\""
)
if live is not None:
    tiers = live["cohort_tiers"].copy()
    # SQL Statement Execution API returns every column as a string
    # (data_array is plain JSON text) - cast before pivoting, or fill_value=0
    # trips a real Arrow string-dtype error trying to fill a numeric gap
    # with an int into a string-typed column.
    tiers["n"] = tiers["n"].astype(int)
    all_tiers = ["full_cohort", "specialty_only", "insufficient_cohort_data"]
    pivot = tiers.pivot_table(index="state", columns="cohort_tier", values="n", aggfunc="sum", fill_value=0)
    pivot = pivot.reindex(columns=all_tiers, fill_value=0)
    st.dataframe(pivot)
    st.bar_chart(pivot)
    st.caption(
        "`insufficient_cohort_data` never fires on this real data (0 rows, either state) - "
        "the specialty-only fallback pools CA+MT, and CA's larger same-specialty population "
        "absorbs Montana's handful of providers past N=10 every time. See LIMITATIONS.md for "
        "why this is stated as 'logically verified, not observed firing,' not oversold."
    )
else:
    st.info("Needs a live Databricks connection - last known real run (2026-07-31), 327 "
            "provider-year rows: CA 323/323 `full_cohort`, MT 4/4 `specialty_only`, "
            "0/327 `insufficient_cohort_data` (see DECISIONS.md).")

st.divider()

# --- Event log ------------------------------------------------------------
st.header("Pipeline event log")
st.caption(f"Most recent events from the Lakeflow Declarative Pipeline `provider_intelligence_history_tracking` "
           f"(`{PIPELINE_ID}`), via the Pipelines REST API.")
if live is not None:
    events_df = live["events"]
    if events_df.empty:
        st.write("No events returned.")
    else:
        st.dataframe(events_df, width="stretch", hide_index=True)
else:
    st.info("Needs a live Databricks connection - event log is fetched live, not cached locally, "
            "since it's the one panel with no static/local fallback (unlike the other three).")
