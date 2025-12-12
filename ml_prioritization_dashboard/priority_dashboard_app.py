"""
priority_dashboard_app.py

Flask app to serve the ML priority dashboard (Command Center, SLA, Demand Trends, Analytics).

Assumes scores have been written by score_priority_xgb.py into:
  boston311-mlops.boston311_service.cases_ranking_ml

Cloud Run–friendly:
- Uses a local sklearn Pipeline stored at ml_prioritization_dashboard/models/priority_model.pkl
- Uses BigQuery for reading scored cases and daily SLA / analytics tables
"""

import os
from pathlib import Path
import datetime as dt

import joblib
import numpy as np
import pandas as pd
from flask import Flask, render_template, request, jsonify
from google.cloud import bigquery

# Paths & core config

ROOT_DIR = Path(__file__).resolve().parent.parent
APP_DIR = Path(__file__).resolve().parent

# Global caches
SLA_CACHE_DF: pd.DataFrame | None = None
SLA_CACHE_TS: dt.datetime | None = None
SLA_CACHE_TTL_MIN = 30  # minutes
ANALYTICS_CACHE: dict | None = None

# Neighborhood performance cache
NEIGHBORHOODS_CACHE: list[dict] | None = None
NEIGHBORHOODS_CACHE_TS: dt.datetime | None = None
NEIGHBORHOODS_TTL_MIN = 30  # minutes

# SLA metrics cache
SLA_METRICS_CACHE: dict | None = None
SLA_METRICS_CACHE_TS: dt.datetime | None = None
SLA_METRICS_TTL_MIN = 30  # minutes

# Demand trends cache
DEMAND_CACHE: dict | None = None
DEMAND_CACHE_TS: dt.datetime | None = None
DEMAND_TTL_MIN = 30  # minutes

FULL_DF: pd.DataFrame | None = None
FULL_DF_TS: dt.datetime | None = None
FULL_DF_TTL_MIN = 10   # pick 5–15 mins for demo

# Local model loading (Cloud Run–only inference)

MODEL_PATH = APP_DIR / "models" / "priority_model.pkl"
print(f"[INFO] Loading local model from {MODEL_PATH}")
PRIORITY_PIPELINE = joblib.load(MODEL_PATH)

# Optional project id from env (mainly for logs / consistency)
PROJECT_ID = os.environ.get("GCP_PROJECT", "boston311-mlops")

# BigQuery config & client

SA_PATH = ROOT_DIR / "secrets" / "bq-dashboard-ro.json"

# Allow turning off local JSON key when running in Cloud Run.
# On Cloud Run, set B311_USE_LOCAL_SA=false so it uses the attached service account.
USE_LOCAL_SA = os.getenv("B311_USE_LOCAL_SA", "true").lower() == "true"

if USE_LOCAL_SA and SA_PATH.exists():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SA_PATH)
    print(f"[INFO] Using service account key at {SA_PATH}")
else:
    print(
        "[INFO] Using default application credentials "
        "(Cloud Run / GCE / local gcloud auth application-default)."
    )

PROJECT = os.getenv("B311_PROJECT_ID", "boston311-mlops")
DATASET = os.getenv("B311_DATASET", "boston311_service")
TABLE = os.getenv("B311_TABLE", "cases_ranking_ml")

SLA_PROJECT = os.getenv("B311_SLA_PROJECT", PROJECT)

SLA_DATASET = os.getenv("B311_SLA_DATASET", DATASET)
SLA_TABLE = os.getenv("B311_SLA_TABLE", "dashboard_daily_metrics")

SLA_RAW_DATASET = os.getenv("B311_SLA_RAW_DATASET", SLA_DATASET)
SLA_RAW_TABLE = os.getenv("B311_SLA_RAW_TABLE", "service_requests_2025")


BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

# Used only for footer / UI text
TRAIN_FEATURE_TABLE = os.getenv(
    "TRAIN_FEATURE_TABLE",
    f"{PROJECT}.{DATASET}.cases_ranking_ml_features",
)

app = Flask(__name__)
bq_client = bigquery.Client(project=PROJECT, location=BQ_LOCATION)

# Static file caching
@app.after_request
def add_cache_headers(response):
    """
    Add caching for static assets so the browser doesn't re-download CSS/JS
    on every tab switch. HTML routes stay uncached.
    """
    if request.path.startswith("/static/"):
        max_age = 7 * 24 * 60 * 60  # 7 days
        response.headers["Cache-Control"] = f"public, max-age={max_age}"
    return response


def create_app():
    """
    App factory for WSGI servers (gunicorn, etc.).
    Cloud Run can use 'priority_dashboard_app:app'
    or 'priority_dashboard_app:create_app()'.
    """
    return app

# Prediction helper using local sklearn pipeline

def predict_priority(instances):
    """
    Local prediction using the sklearn Pipeline loaded from priority_model.pkl.

    instances: list[dict] with features:
      neighborhood, reason, department,
      hour_of_day, day_of_week, month,
      neigh_open_14d, prev_repeat_90d_30m, dept_pressure_30d
    """
    df = pd.DataFrame(instances)
    proba = PRIORITY_PIPELINE.predict_proba(df)[:, 1]
    # Return a simple list of floats
    return proba.tolist()

# Data loading – scored priority table (for Command Center, Work Queues, etc.)

FULL_DF: pd.DataFrame | None = None


def load_data() -> pd.DataFrame:
    """Load latest scored cases from BigQuery into memory."""
    query = f"""
    SELECT
      case_enquiry_id,
      neighborhood,
      reason,
      department,
      priority_score,
      segment,
      rank_overall,
      prev_repeat_90d_30m,
      dept_pressure_30d,
      neigh_open_14d,
      latitude,
      longitude
    FROM `{PROJECT}.{DATASET}.{TABLE}`
    """
    df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)
    return df


def get_full_df() -> pd.DataFrame:
    global FULL_DF, FULL_DF_TS

    now_utc = dt.datetime.utcnow()
    should_refresh = (
        FULL_DF is None
        or FULL_DF_TS is None
        or (now_utc - FULL_DF_TS).total_seconds() > FULL_DF_TTL_MIN * 60
    )

    if should_refresh:
        print("[INFO] Refreshing cases_ranking_ml cache from BigQuery...")
        FULL_DF = load_data()
        FULL_DF_TS = now_utc
        print(f"[INFO] Loaded {len(FULL_DF)} rows into memory.")

    return FULL_DF


# Daily SLA / analytics table loading (dashboard_daily_metrics)

def load_sla_data(days: int = 365) -> pd.DataFrame:
    """
    Load daily 311 metrics for SLA, demand, and analytics from the pre-aggregated
    dashboard_daily_metrics table (or whatever B311_SLA_TABLE points to).

    Expected schema:
      - date (DATE)
      - neighborhood (STRING, nullable)
      - department (STRING, nullable)
      - reason (STRING, nullable)
      - source (STRING, nullable)
      - total_cases (INTEGER)
      - open_cases (INTEGER)
      - closed_cases (INTEGER)
      - sla_eligible_cases (INTEGER)
      - sla_met_cases (INTEGER)
      - sla_compliance_pct (FLOAT)
      - avg_resolution_hrs (FLOAT)
    """
    query = f"""
    SELECT
      date,
      neighborhood,
      department,
      reason,
      source,
      total_cases,
      open_cases,
      closed_cases,
      sla_eligible_cases,
      sla_met_cases,
      sla_compliance_pct,
      avg_resolution_hrs
    FROM `{SLA_PROJECT}.{SLA_DATASET}.{SLA_TABLE}`
    WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
    """

    df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)

    # Normalize date to pandas datetime for resampling
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # Fill missing numeric columns with 0 to simplify aggregations
    numeric_cols = [
        "total_cases",
        "open_cases",
        "closed_cases",
        "sla_eligible_cases",
        "sla_met_cases",
        "sla_compliance_pct",
        "avg_resolution_hrs",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = df[col].fillna(0)

    return df


def get_sla_cached_df(days: int = 365) -> pd.DataFrame:
    """
    Return daily SLA metrics with a shared in-memory cache so /sla-performance,
    /demand-trends and /analytics don't keep hitting BigQuery.

    Also precomputes ANALYTICS_CACHE whenever we refresh from BigQuery.
    """
    global SLA_CACHE_DF, SLA_CACHE_TS, ANALYTICS_CACHE

    now_utc = dt.datetime.utcnow()
    should_refresh = (
        SLA_CACHE_DF is None
        or SLA_CACHE_TS is None
        or (now_utc - SLA_CACHE_TS).total_seconds() > SLA_CACHE_TTL_MIN * 60
    )

    if should_refresh:
        try:
            print("[INFO] Refreshing SLA daily metrics cache from BigQuery...")
            df = load_sla_data(days=days)
            SLA_CACHE_DF = df
            SLA_CACHE_TS = now_utc

            # Precompute analytics metrics here from daily aggregates
            ANALYTICS_CACHE = build_analytics_cache(df)
            print(
                f"[INFO] Refreshed SLA daily cache with {len(df)} rows "
                f"and rebuilt ANALYTICS_CACHE."
            )
        except Exception as e:
            print(f"[ERROR] Failed to load SLA daily metrics from BigQuery: {e}")
            # fall back to empty
            SLA_CACHE_DF = pd.DataFrame()
            SLA_CACHE_TS = now_utc
            ANALYTICS_CACHE = build_analytics_cache(SLA_CACHE_DF)
            return SLA_CACHE_DF
    else:
        df = SLA_CACHE_DF.copy()

    return df

# Helper filters

def apply_near_filter(
    df: pd.DataFrame,
    near_lat: str | None,
    near_lon: str | None,
    near_radius_m: float | None,
) -> pd.DataFrame:
    """Filter df to only rows within near_radius_m of (near_lat, near_lon)."""
    if not near_lat or not near_lon or not near_radius_m:
        return df

    if "latitude" not in df.columns or "longitude" not in df.columns:
        return df

    try:
        lat0 = float(near_lat)
        lon0 = float(near_lon)
        radius = float(near_radius_m)
    except ValueError:
        return df

    df_coords = df.dropna(subset=["latitude", "longitude"]).copy()
    if df_coords.empty:
        return df

    # Haversine distance in meters
    R = 6371000.0
    lat1 = np.radians(lat0)
    lon1 = np.radians(lon0)
    lat2 = np.radians(df_coords["latitude"].astype(float))
    lon2 = np.radians(df_coords["longitude"].astype(float))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2.0) ** 2
    c = 2.0 * np.arctan2(np.sqrt(a), np.sqrt(1.0 - a))
    df_coords["distance_m"] = R * c

    df_near = df_coords[df_coords["distance_m"] <= radius]
    if df_near.empty:
        return df

    return df.loc[df_near.index]

# Local test endpoint (

@app.route("/api/test-vertex", methods=["POST"])
def api_test_vertex():
    """
    Simple test endpoint to verify Cloud Run -> local model wiring.
    """
    payload = request.get_json(silent=True) or {}
    instances = payload.get("instances")

    if not isinstance(instances, list) or not instances:
        return jsonify({"error": "Provide a non-empty 'instances' list"}), 400

    try:
        preds = predict_priority(instances)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    return jsonify({"predictions": preds})

# Routes – Command Center

@app.route("/", methods=["GET"])
def index():
    df = get_full_df().copy()

    # Filters
    sel_neigh = request.args.get("neighborhood", "ALL")
    sel_reason = request.args.get("reason", "ALL")
    sel_dept = request.args.get("department", "ALL")
    sel_case = (request.args.get("case_id") or "").strip()
    near_lat = request.args.get("near_lat")
    near_lon = request.args.get("near_lon")
    near_radius_m = request.args.get("near_radius_m")

    # City-wide critical counts (for charts)
    df_all_crit = df[df["segment"] == "CRITICAL"]

    if not df_all_crit.empty:
        crit_by_neigh_all = (
            df_all_crit.dropna(subset=["neighborhood"])
            .groupby("neighborhood")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
            .head(5)
        )
        crit_neigh_labels_all = crit_by_neigh_all["neighborhood"].tolist()
        crit_neigh_counts_all = crit_by_neigh_all["count"].tolist()
    else:
        crit_neigh_labels_all = []
        crit_neigh_counts_all = []

    if not df_all_crit.empty:
        crit_by_dept_all = (
            df_all_crit.dropna(subset=["department"])
            .groupby("department")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
            .head(5)
        )
        crit_dept_labels_all = crit_by_dept_all["department"].tolist()
        crit_dept_counts_all = crit_by_dept_all["count"].tolist()
    else:
        crit_dept_labels_all = []
        crit_dept_counts_all = []

    # Dropdown options
    neighborhoods = sorted(df["neighborhood"].dropna().unique())
    reasons = sorted(df["reason"].dropna().unique())
    departments = sorted(df["department"].dropna().unique())

    combos_df = (
        df[["neighborhood", "reason", "department"]]
        .dropna()
        .drop_duplicates()
    )
    combos = combos_df.to_dict(orient="records")

    # Apply filters
    df_filt = df.copy()
    if sel_neigh != "ALL":
        df_filt = df_filt[df_filt["neighborhood"] == sel_neigh]
    if sel_reason != "ALL":
        df_filt = df_filt[df_filt["reason"] == sel_reason]
    if sel_dept != "ALL":
        df_filt = df_filt[df_filt["department"] == sel_dept]

    if sel_case:
        df_filt = df_filt[
            df_filt["case_enquiry_id"].astype(str).str.contains(sel_case)
        ]

    if near_lat and near_lon and near_radius_m:
        df_filt = apply_near_filter(df_filt, near_lat, near_lon, float(near_radius_m))

    # Top-10 priority list
    df_crit_filt = df_filt.sort_values("priority_score", ascending=False).head(10)

    # KPIs
    total_open = len(df_filt)
    num_crit = len(df_crit_filt)
    crit_share = (num_crit / total_open * 100.0) if total_open > 0 else 0.0

    stats = {
        "total_open": total_open,
        "num_crit": num_crit,
        "crit_share": f"{crit_share:.1f}",
        "last_run_label": "Last 24 hours",
    }

    # Map data – cap number of markers for perf
    map_cols = [
        "case_enquiry_id",
        "neighborhood",
        "reason",
        "department",
        "priority_score",
        "segment",
        "latitude",
        "longitude",
    ]
    map_cols_existing = [c for c in map_cols if c in df_filt.columns]

    if {"latitude", "longitude"}.issubset(df_filt.columns):
        map_df = df_filt[map_cols_existing].dropna(subset=["latitude", "longitude"])
        # Safety cap – e.g. at most 2000 markers
        map_df = map_df.head(2000)
        map_cases = map_df.to_dict(orient="records")
    else:
        map_cases = []

    return render_template(
        "dashboard.html",
        neighborhoods=neighborhoods,
        reasons=reasons,
        departments=departments,
        sel_neigh=sel_neigh,
        sel_reason=sel_reason,
        sel_dept=sel_dept,
        sel_case=sel_case,
        near_lat=near_lat,
        near_lon=near_lon,
        near_radius_m=near_radius_m,
        critical_cases=df_crit_filt.to_dict(orient="records"),
        stats=stats,
        map_cases=map_cases,
        crit_neigh_labels_all=crit_neigh_labels_all,
        crit_neigh_counts_all=crit_neigh_counts_all,
        crit_dept_labels_all=crit_dept_labels_all,
        crit_dept_counts_all=crit_dept_counts_all,
        combos=combos,
        TRAIN_FEATURE_TABLE=TRAIN_FEATURE_TABLE,
    )

# Routes – Work Queues & Neighborhoods

@app.route("/work-queues")
def work_queues_page():
    df = get_full_df().copy()

    departments = sorted(df["department"].dropna().unique())
    sel_dept = request.args.get("department")
    if sel_dept not in departments:
        sel_dept = None

    if sel_dept:
        dept_df = df[df["department"] == sel_dept].copy()
        dept_df = dept_df.sort_values("priority_score", ascending=False).head(100)
        work_queue = dept_df.to_dict(orient="records")

        if not dept_df.empty:
            total_open = len(dept_df)
            num_crit = int((dept_df["segment"] == "CRITICAL").sum())
            avg_score = float(dept_df["priority_score"].mean())
            stats = {
                "total_open": total_open,
                "num_crit": num_crit,
                "avg_score": f"{avg_score:.3f}",
            }
        else:
            stats = None
    else:
        work_queue = []
        stats = None

    return render_template(
        "work_queues.html",
        departments=departments,
        sel_dept=sel_dept,
        work_queue=work_queue,
        stats=stats,
    )


# Neighborhood performance cache helper 

def get_neighborhood_perf() -> list[dict]:
    global NEIGHBORHOODS_CACHE, NEIGHBORHOODS_CACHE_TS

    now = dt.datetime.utcnow()
    should_refresh = (
        NEIGHBORHOODS_CACHE is None
        or NEIGHBORHOODS_CACHE_TS is None
        or (now - NEIGHBORHOODS_CACHE_TS).total_seconds() > NEIGHBORHOODS_TTL_MIN * 60
    )

    if should_refresh:
        df = get_full_df().copy()

        df["neighborhood_clean"] = (
            df["neighborhood"]
            .fillna("")
            .astype(str)
            .str.strip()
        )

        df["neighborhood_is_missing"] = df["neighborhood_clean"] == ""

        df["neighborhood_group"] = df["neighborhood_clean"].where(
            ~df["neighborhood_is_missing"],
            other="Unknown / missing neighborhood",
        )

        grouped = (
            df.groupby("neighborhood_group", dropna=False)
            .agg(
                total_open=("case_enquiry_id", "count"),
                num_crit=("segment", lambda s: (s == "CRITICAL").sum()),
                avg_score=("priority_score", "mean"),
            )
            .reset_index()
            .rename(columns={"neighborhood_group": "neighborhood"})
        )

        grouped["critical_share"] = np.where(
            grouped["total_open"] > 0,
            grouped["num_crit"] / grouped["total_open"] * 100.0,
            0.0,
        )

        grouped["is_unknown"] = grouped["neighborhood"].eq(
            "Unknown / missing neighborhood"
        )
        grouped = grouped.sort_values(
            by=["is_unknown", "avg_score"],
            ascending=[True, False],
            ignore_index=True,
        )

        NEIGHBORHOODS_CACHE = grouped.to_dict(orient="records")
        NEIGHBORHOODS_CACHE_TS = now
        print(f"[INFO] Rebuilt NEIGHBORHOODS_CACHE with {len(grouped)} rows.")

    return NEIGHBORHOODS_CACHE or []


@app.route("/neighborhoods")
def neighborhoods_page():
    neigh_rows = get_neighborhood_perf()
    return render_template(
        "neighborhoods.html",
        perf_rows=neigh_rows,
    )

# Overdue open cases – from raw service_requests_2025 

def load_overdue_open_cases(days: int = 365) -> pd.DataFrame:
    """
    Load open cases whose SLA deadline has passed directly from the raw
    service_requests_2025 table.

    "Overdue" definition:
      - case_status = 'Open'
      - sla_target_dt IS NOT NULL
      - sla_target_dt < CURRENT_TIMESTAMP()

    We *do not* change the daily metrics table for this – it’s a separate,
    focused query that only pulls overdue open cases.
    """
    query = f"""
    SELECT
      case_enquiry_id,
      neighborhood,
      department,
      reason,
      source,
      latitude,
      longitude,
      open_dt,
      sla_target_dt,
      closed_dt,
      case_status
    FROM `{SLA_PROJECT}.{SLA_RAW_DATASET}.{SLA_RAW_TABLE}`
    WHERE DATE(open_dt) >= DATE_SUB(CURRENT_DATE(), INTERVAL {days} DAY)
      AND case_status = 'Open'
      AND sla_target_dt IS NOT NULL
      AND sla_target_dt < CURRENT_TIMESTAMP()
    """

    df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)

    if "open_dt" in df.columns:
        df["open_dt"] = pd.to_datetime(df["open_dt"], errors="coerce")
        now_ts = pd.Timestamp.utcnow()
        df["days_open"] = (
            (now_ts - df["open_dt"]).dt.total_seconds() / 86400.0
        ).round(1)
        # Approximate "overdue hours" as hours since open (good enough for drilldown)
        df["overdue_hours"] = (df["days_open"] * 24.0).round(1)

    return df

# Route – SLA Performance (from daily metrics + raw overdue query)

def get_sla_metrics() -> dict:
    """
    Compute SLA metrics from the pre-aggregated daily table and cache them.
    Overdue open cases are computed directly from the raw service_requests_2025
    table via load_overdue_open_cases (no change to daily metrics schema).
    """
    global SLA_METRICS_CACHE, SLA_METRICS_CACHE_TS

    now = dt.datetime.utcnow()
    should_refresh = (
        SLA_METRICS_CACHE is None
        or SLA_METRICS_CACHE_TS is None
        or (now - SLA_METRICS_CACHE_TS).total_seconds() > SLA_METRICS_TTL_MIN * 60
    )

    if not should_refresh:
        return SLA_METRICS_CACHE

    # Daily metrics for high-level SLA rates
    df = get_sla_cached_df(days=180)  # 6 months of daily metrics

    # If no data, we still populate overdue from raw table below
    if df.empty:
        overall_sla_rate = None
        avg_resolution_hours_all = None
        avg_resolution_days_all = None
        sla_dept_labels = []
        sla_dept_rates = []
        art_service_labels = []
        art_service_hours = []
    else:
        # Ensure strings
        for col in ["department", "reason", "neighborhood"]:
            if col not in df.columns:
                df[col] = None
            df[col] = df[col].fillna("Unknown").astype(str).str.strip()

        #  1. Overall SLA compliance 
        total_eligible = float(df.get("sla_eligible_cases", pd.Series()).sum())
        total_met = float(df.get("sla_met_cases", pd.Series()).sum())

        if total_eligible > 0:
            overall_sla_rate = total_met / total_eligible * 100.0
        else:
            overall_sla_rate = None

        #  2. SLA by department (top 10)
        if "department" in df.columns and "sla_eligible_cases" in df.columns:
            dept_group = (
                df.groupby("department", as_index=False)[
                    ["sla_eligible_cases", "sla_met_cases"]
                ]
                .sum()
            )
            # drop departments with no SLA-eligible volume
            dept_group = dept_group[dept_group["sla_eligible_cases"] > 0]
            if not dept_group.empty:
                dept_group["sla_rate"] = (
                    dept_group["sla_met_cases"]
                    / dept_group["sla_eligible_cases"]
                    * 100.0
                )
                dept_group["n_closed"] = dept_group["sla_eligible_cases"]
                dept_group = (
                    dept_group.sort_values("n_closed", ascending=False)
                    .head(10)
                )

                sla_dept_labels = dept_group["department"].tolist()
                sla_dept_rates = dept_group["sla_rate"].round(1).tolist()
            else:
                sla_dept_labels = []
                sla_dept_rates = []
        else:
            sla_dept_labels = []
            sla_dept_rates = []

        # 3. Avg resolution time (overall + by service) 
        if "avg_resolution_hrs" in df.columns and "closed_cases" in df.columns:
            # make sure we have clean numeric columns
            df["closed_cases"] = df["closed_cases"].astype(float)
            df["avg_resolution_hrs"] = df["avg_resolution_hrs"].astype(float)

            # total resolution hours for each row
            df["total_resolution_hours"] = df["avg_resolution_hrs"] * df["closed_cases"]

            # overall weighted average (KPI at the top) 
            total_closed_all = float(df["closed_cases"].sum())
            total_hours_all = float(df["total_resolution_hours"].sum())
            if total_closed_all > 0:
                avg_resolution_hours_all = total_hours_all / total_closed_all
            else:
                avg_resolution_hours_all = None
        else:
            avg_resolution_hours_all = None

        # Convert overall average resolution time to days for the KPI
        if avg_resolution_hours_all is not None:
            avg_resolution_days_all = avg_resolution_hours_all / 24.0
        else:
            avg_resolution_days_all = None

        # Avg resolution by reason (service type) for the bar chart
        if (
            "reason" in df.columns
            and "total_resolution_hours" in df.columns
            and "closed_cases" in df.columns
        ):
            by_reason = (
                df.groupby("reason", as_index=False)[
                    ["total_resolution_hours", "closed_cases"]
                ]
                .sum()
            )
            # keep only services that actually closed something
            by_reason = by_reason[by_reason["closed_cases"] > 0]
            if not by_reason.empty:
                by_reason["resolution_hours"] = (
                    by_reason["total_resolution_hours"] / by_reason["closed_cases"]
                )
                by_reason = by_reason.sort_values(
                    "resolution_hours", ascending=False
                ).head(15)
                art_service_labels = by_reason["reason"].tolist()
                art_service_hours = by_reason["resolution_hours"].round(1).tolist()
            else:
                art_service_labels = []
                art_service_hours = []
        else:
            art_service_labels = []
            art_service_hours = []

    # 4. Overdue drilldown: computed from raw table 
    overdue_df = load_overdue_open_cases(days=365)
    overdue_count = int(len(overdue_df))

    if overdue_df.empty:
        overdue_neighborhoods: list[str] = []
        overdue_cases: list[dict] = []
        overdue_map_cases: list[dict] = []
    else:
        overdue_df["neighborhood"] = (
            overdue_df.get("neighborhood")
            .fillna("Unknown")
            .astype(str)
            .str.strip()
        )

        # Neighborhood dropdown – sorted by count desc
        neigh_counts = (
            overdue_df.groupby("neighborhood")["case_enquiry_id"]
            .count()
            .sort_values(ascending=False)
        )
        overdue_neighborhoods = neigh_counts.index.tolist()

        # Table rows – top 100 by days_open (if available)
        cols_for_table = [
            "case_enquiry_id",
            "neighborhood",
            "reason",
            "department",
            "source",
            "days_open",
            "overdue_hours",
        ]
        table_cols_existing = [c for c in cols_for_table if c in overdue_df.columns]
        if "days_open" in overdue_df.columns:
            overdue_sorted = overdue_df.sort_values("days_open", ascending=False)
        else:
            overdue_sorted = overdue_df
        overdue_cases = (
            overdue_sorted[table_cols_existing]
            .head(100)
            .to_dict(orient="records")
        )

        # Map markers – at most 1000 overdue cases with coordinates
        if {"latitude", "longitude"}.issubset(overdue_df.columns):
            map_cols = [
                "case_enquiry_id",
                "neighborhood",
                "reason",
                "department",
                "latitude",
                "longitude",
                "days_open",
            ]
            map_cols_existing = [c for c in map_cols if c in overdue_df.columns]
            overdue_map_cases = (
                overdue_df.dropna(subset=["latitude", "longitude"])[map_cols_existing]
                .head(1000)
                .to_dict(orient="records")
            )
        else:
            overdue_map_cases = []

    SLA_METRICS_CACHE = {
        "overall_sla_rate": overall_sla_rate,
        "avg_resolution_hours_all": avg_resolution_hours_all,
        "avg_resolution_days_all": avg_resolution_days_all,
        "overdue_count": overdue_count,
        "sla_dept_labels": sla_dept_labels,
        "sla_dept_rates": sla_dept_rates,
        "art_service_labels": art_service_labels,
        "art_service_hours": art_service_hours,
        "overdue_cases": overdue_cases,
        "overdue_neighborhoods": overdue_neighborhoods,
        "overdue_map_cases": overdue_map_cases,
    }

    SLA_METRICS_CACHE_TS = now
    print(
        f"[INFO] Rebuilt SLA_METRICS_CACHE; overdue_count={overdue_count}, "
        f"overall_sla_rate={overall_sla_rate}"
    )

    return SLA_METRICS_CACHE


@app.route("/sla-performance")
def sla_performance_page():
    metrics = get_sla_metrics()
    return render_template("sla_performance.html", **metrics)

# Demand Trends – from daily metrics 

def build_demand_cache(df_recent: pd.DataFrame) -> dict:
    """
    Build demand-trend aggregates.

    - Weekly history: uses df_recent (typically last 365 days).
    - Monthly forecast + seasonality: uses a longer window from the same
      daily metrics table so we actually have enough months to avoid flat lines.
    """
    empty_result = {
        "hist_week_labels": [],
        "hist_week_values": [],
        "fc_week_labels": [],
        "fc_week_values": [],
        "month_labels": [],
        "month_totals": [],
        "month_has_data": [],
        "seasonal_datasets": [],
        "topic_labels": [],
        "topic_totals": [],
    }

    # -------------------------------------------------------------------------
    # 0) Prepare recent daily series for weekly history
    # -------------------------------------------------------------------------
    recent_daily = None
    if df_recent is not None and not df_recent.empty and "date" in df_recent.columns and "total_cases" in df_recent.columns:
        df_r = df_recent.dropna(subset=["date"]).copy()
        df_r["date"] = pd.to_datetime(df_r["date"], errors="coerce")
        df_r = df_r.dropna(subset=["date"])
        df_r["total_cases"] = pd.to_numeric(df_r["total_cases"], errors="coerce").fillna(0)

        recent_daily = (
            df_r.groupby("date", as_index=False)["total_cases"]
            .sum()
            .rename(columns={"total_cases": "daily_volume"})
            .sort_values("date")
            .set_index("date")
        )

    hist_week_labels: list[str] = []
    hist_week_values: list[int] = []
    fc_week_labels: list[str] = []
    fc_week_values: list[int] = []

    # 1) Weekly historical volume (last ~12 months)
    if recent_daily is not None and not recent_daily.empty:
        max_date = recent_daily.index.max()

        weekly_counts = (
            recent_daily["daily_volume"]
            .resample("W-MON")
            .sum()
            .rename("volume")
        )
        weekly_counts = weekly_counts[weekly_counts > 0]

        # Drop incomplete last week
        if len(weekly_counts) >= 1:
            last_week_start = weekly_counts.index[-1]
            if max_date < last_week_start + pd.Timedelta(days=6):
                weekly_counts = weekly_counts.iloc[:-1]

        if len(weekly_counts) > 52:
            weekly_counts = weekly_counts.tail(52)

        hist_week_labels = [ts.strftime("%Y-%m-%d") for ts in weekly_counts.index]
        hist_week_values = [int(v) for v in weekly_counts.values]

    # 2) Long window for monthly history + forecast + seasonality/topics
    try:
        df_long = load_sla_data(days=730)  # ~2 years
    except Exception as e:
        print(f"[WARN] Failed to load long-window SLA data for demand: {e}")
        df_long = pd.DataFrame()

    # Build daily_long from df_long (for monthly)
    daily_long = None
    if df_long is not None and not df_long.empty and "date" in df_long.columns and "total_cases" in df_long.columns:
        df_l = df_long.dropna(subset=["date"]).copy()
        df_l["date"] = pd.to_datetime(df_l["date"], errors="coerce")
        df_l = df_l.dropna(subset=["date"])
        df_l["total_cases"] = pd.to_numeric(df_l["total_cases"], errors="coerce").fillna(0).astype(float)

        daily_long = (
            df_l.groupby("date", as_index=False)["total_cases"]
            .sum()
            .rename(columns={"total_cases": "daily_volume"})
            .sort_values("date")
            .set_index("date")
        )

    # 3) Monthly forecast (next 6 months)
    #    Prefer monthly_ts from daily_long; fallback to recent_daily.
    monthly_ts = pd.Series(dtype=float)
    max_period = None

    if daily_long is not None and not daily_long.empty:
        max_date_fc = daily_long.index.max()
        max_period = max_date_fc.to_period("M")

        monthly_ts = (
            daily_long["daily_volume"]
            .resample("MS")
            .sum()
            .rename("volume")
        )
        monthly_ts = monthly_ts[monthly_ts.index.to_period("M") <= max_period]
    elif recent_daily is not None and not recent_daily.empty:
        max_date_fc = recent_daily.index.max()
        max_period = max_date_fc.to_period("M")

        monthly_ts = (
            recent_daily["daily_volume"]
            .resample("MS")
            .sum()
            .rename("volume")
        )
        monthly_ts = monthly_ts[monthly_ts.index.to_period("M") <= max_period]

    # Forecast logic:
    # - If >=12 months, use "same month last year"
    # - Else, use a simple trend delta (non-flat)
    if len(monthly_ts) > 0:
        if len(monthly_ts) >= 12:
            month_lookup = {
                ts.to_period("M"): int(round(float(val)))
                for ts, val in monthly_ts.items()
            }

            last_hist_period = monthly_ts.index.max().to_period("M")
            future_periods = pd.period_range(last_hist_period + 1, periods=6, freq="M")

            for p in future_periods:
                prior_year = p - 12
                forecast_val = month_lookup.get(
                    prior_year,
                    int(round(float(monthly_ts.iloc[-1])))
                )
                fc_week_labels.append(p.strftime("%Y-%m"))
                fc_week_values.append(int(max(0, forecast_val)))
        else:
            vals = monthly_ts.values.astype(float)
            last_val = float(vals[-1]) if len(vals) else 0.0
            delta = float(vals[-1] - vals[-2]) if len(vals) >= 2 else 0.0

            last_hist_period = monthly_ts.index.max().to_period("M")
            future_periods = pd.period_range(last_hist_period + 1, periods=6, freq="M")

            for i, p in enumerate(future_periods, start=1):
                pred = last_val + delta * i
                fc_week_labels.append(p.strftime("%Y-%m"))
                fc_week_values.append(int(max(0, round(pred))))

    # 4) Seasonality (Jan–Dec totals) + seasonal topic lines + top topics bar
    month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    month_totals = [0] * 12
    month_has_data = [False] * 12
    seasonal_datasets: list[dict] = []
    topic_labels: list[str] = []
    topic_totals: list[int] = []

    if daily_long is not None and not daily_long.empty:
        # 4a) total volume by month-of-year (all topics)
        tmp = daily_long.reset_index().rename(columns={"index": "date"})
        tmp["month_num"] = tmp["date"].dt.month

        month_counts = (
            tmp.groupby("month_num")["daily_volume"]
            .sum()
            .to_dict()
        )

        month_totals = [int(round(float(month_counts.get(m, 0)))) for m in range(1, 13)]
        month_has_data = [month_counts.get(m, 0) > 0 for m in range(1, 13)]

    # Topic-based seasonality and top topics need "reason" in df_long
    if df_long is not None and not df_long.empty and "reason" in df_long.columns and "date" in df_long.columns and "total_cases" in df_long.columns:
        df_reason = df_long.dropna(subset=["date"]).copy()
        df_reason["date"] = pd.to_datetime(df_reason["date"], errors="coerce")
        df_reason = df_reason.dropna(subset=["date"])
        df_reason["total_cases"] = pd.to_numeric(df_reason["total_cases"], errors="coerce").fillna(0).astype(int)
        df_reason["reason"] = df_reason["reason"].fillna("Unknown").astype(str).str.strip()
        df_reason["month_num"] = df_reason["date"].dt.month

        top_topics = (
            df_reason.groupby("reason")["total_cases"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
            .index
            .tolist()
        )

        # 4b) seasonal lines (month-of-year) for top 5 reasons
        seasonal_datasets = []
        for topic in top_topics:
            topic_df = df_reason[df_reason["reason"] == topic]
            topic_month_counts = (
                topic_df.groupby("month_num")["total_cases"]
                .sum()
                .to_dict()
            )
            counts_for_topic = [int(topic_month_counts.get(m, 0)) for m in range(1, 13)]
            seasonal_datasets.append({"label": topic, "data": counts_for_topic})

        # 4c) top topics bar (volume)
        topic_counts = (
            df_reason.groupby("reason")["total_cases"]
            .sum()
            .sort_values(ascending=False)
            .head(5)
        )
        topic_labels = topic_counts.index.tolist()
        topic_totals = [int(v) for v in topic_counts.values]

    return {
        "hist_week_labels": hist_week_labels,
        "hist_week_values": hist_week_values,
        "fc_week_labels": fc_week_labels,
        "fc_week_values": fc_week_values,
        "month_labels": month_labels,
        "month_totals": month_totals,
        "month_has_data": month_has_data,
        "seasonal_datasets": seasonal_datasets,
        "topic_labels": topic_labels,
        "topic_totals": topic_totals,
    }    

def get_demand_metrics() -> dict:
    global DEMAND_CACHE, DEMAND_CACHE_TS

    now = dt.datetime.utcnow()
    should_refresh = (
        DEMAND_CACHE is None
        or DEMAND_CACHE_TS is None
        or (now - DEMAND_CACHE_TS).total_seconds() > DEMAND_TTL_MIN * 60
    )

    if not should_refresh:
        return DEMAND_CACHE

    # Recent window: last 365 days for weekly history + forecast
    df_recent = get_sla_cached_df(days=365)

    DEMAND_CACHE = build_demand_cache(df_recent)
    DEMAND_CACHE_TS = now
    print("[INFO] Rebuilt DEMAND_CACHE from daily metrics (recent + long window).")
    return DEMAND_CACHE


@app.route("/demand-trends")
def demand_trends_page():
    """
    Demand Trends & Forecasting view.
    Uses cached demand metrics built from daily SLA data.
    """
    metrics = get_demand_metrics()
    return render_template("demand_trends.html", **metrics)

# Route – Analytics (from daily metrics)

def build_analytics_cache(df: pd.DataFrame) -> dict:
    """
    Given the daily SLA dataframe, compute all analytics aggregates once
    and return a dictionary suitable for passing directly into render_template.
    """
    # If no data, just return empty structures
    if df.empty:
        return {
            "neigh_labels": [],
            "neigh_counts": [],
            "dept_labels": [],
            "dept_counts": [],
            "reason_labels": [],
            "reason_counts": [],
            "sla_dept_labels": [],
            "sla_dept_rates": [],
            "src_labels": [],
            "src_counts": [],
            "src_art_labels": [],
            "src_art_hours": [],
        }

    # Make sure key columns exist / are strings
    for col in ["neighborhood", "department", "reason", "source"]:
        if col not in df.columns:
            df[col] = None
        df[col] = df[col].fillna("Unknown").astype(str).str.strip()

    if "total_cases" not in df.columns:
        df["total_cases"] = 0

    if "closed_cases" not in df.columns:
        df["closed_cases"] = 0

    if "avg_resolution_hrs" not in df.columns:
        df["avg_resolution_hrs"] = 0.0

    # 1. Cases by neighborhood (top 10, drop zero-volume neighborhoods)
    neigh_series = (
        df.groupby("neighborhood")["total_cases"]
        .sum()
    )
    neigh_series = neigh_series[neigh_series > 0]
    neigh_series = (
        neigh_series
        .sort_values(ascending=False)
        .head(10)
    )
    neigh_labels = neigh_series.index.tolist()
    neigh_counts = [int(v) for v in neigh_series.values]

    # 2. Cases by department (top 10, drop zero-volume departments)
    dept_series = (
        df.groupby("department")["total_cases"]
        .sum()
    )
    dept_series = dept_series[dept_series > 0]
    dept_series = (
        dept_series
        .sort_values(ascending=False)
        .head(10)
    )
    dept_labels = dept_series.index.tolist()
    dept_counts = [int(v) for v in dept_series.values]

    # 3. Top reasons (top 10 request topics, drop zero-volume reasons)
    reason_series = (
        df.groupby("reason")["total_cases"]
        .sum()
    )
    reason_series = reason_series[reason_series > 0]
    reason_series = (
        reason_series
        .sort_values(ascending=False)
        .head(10)
    )
    reason_labels = reason_series.index.tolist()
    reason_counts = [int(v) for v in reason_series.values]

    # 4. SLA compliance by department (top 10 by volume, only where there are SLA-eligible cases)
    if "sla_eligible_cases" in df.columns and "sla_met_cases" in df.columns:
        dept_sla = (
            df.groupby("department", as_index=False)[
                ["sla_eligible_cases", "sla_met_cases"]
            ]
            .sum()
        )
        dept_sla = dept_sla[dept_sla["sla_eligible_cases"] > 0]
        if not dept_sla.empty:
            dept_sla["sla_rate"] = (
                dept_sla["sla_met_cases"] / dept_sla["sla_eligible_cases"] * 100.0
            )
            dept_sla = (
                dept_sla
                .sort_values("sla_eligible_cases", ascending=False)
                .head(10)
            )
            sla_dept_labels = dept_sla["department"].tolist()
            sla_dept_rates = dept_sla["sla_rate"].round(1).tolist()
        else:
            sla_dept_labels = []
            sla_dept_rates = []
    else:
        sla_dept_labels = []
        sla_dept_rates = []

    # 5. Volume by source (all cases, drop zero-volume sources)
    src_series = (
        df.groupby("source")["total_cases"]
        .sum()
    )
    src_series = src_series[src_series > 0]
    src_series = (
        src_series
        .sort_values(ascending=False)
        .head(10)
    )
    src_labels = src_series.index.tolist()
    src_counts = [int(v) for v in src_series.values]

    # 6. Average resolution hours by source (only closed, and only where there are closed cases)
    if "closed_cases" in df.columns and "avg_resolution_hrs" in df.columns:
        src_df = df.copy()
        src_df["closed_cases"] = src_df["closed_cases"].astype(float)
        src_df["avg_resolution_hrs"] = src_df["avg_resolution_hrs"].astype(float)

        src_group = (
            src_df.groupby("source", as_index=False)[
                ["avg_resolution_hrs", "closed_cases"]
            ]
            .sum()
        )
        src_group = src_group[src_group["closed_cases"] > 0]
        if not src_group.empty:
            src_group["resolution_hours"] = (
                src_group["avg_resolution_hrs"] * src_group["closed_cases"]
            ) / src_group["closed_cases"]
            src_group = (
                src_group
                .sort_values("resolution_hours", ascending=False)
                .head(10)
            )
            src_art_labels = src_group["source"].tolist()
            src_art_hours = src_group["resolution_hours"].round(1).tolist()
        else:
            src_art_labels = []
            src_art_hours = []
    else:
        src_art_labels = []
        src_art_hours = []

    return {
        "neigh_labels": neigh_labels,
        "neigh_counts": neigh_counts,
        "dept_labels": dept_labels,
        "dept_counts": dept_counts,
        "reason_labels": reason_labels,
        "reason_counts": reason_counts,
        "sla_dept_labels": sla_dept_labels,
        "sla_dept_rates": sla_dept_rates,
        "src_labels": src_labels,
        "src_counts": src_counts,
        "src_art_labels": src_art_labels,
        "src_art_hours": src_art_hours,
    }


@app.route("/analytics")
def analytics_page():
    """
    3. Analytics – categorical breakdowns (no maps).

    Uses shared SLA cache + precomputed ANALYTICS_CACHE so we don't
    redo heavy aggregations on each request.
    """
    global ANALYTICS_CACHE

    # Ensure SLA cache (and thus ANALYTICS_CACHE) is initialized/refreshed
    df = get_sla_cached_df(days=365)

    if ANALYTICS_CACHE is None:
        # Fallback: build on the fly (should be rare)
        ANALYTICS_CACHE = build_analytics_cache(df)

    return render_template("analytics.html", **ANALYTICS_CACHE)

@app.route("/api/ping", methods=["GET"])
def api_ping():
    """
    Lightweight health check endpoint for deployment verification.
    Used in the assignment video and CI to confirm the service is live.
    """
    return {
        "status": "ok",
        "service": "b311-priority-dashboard",
        "timestamp": dt.datetime.utcnow().isoformat() + "Z",
    }

@app.route("/api/debug", methods=["GET"])
def api_debug():
    df_cases = get_full_df()
    df_sla = get_sla_cached_df(days=365)

    out = {
        "cases_table": f"{PROJECT}.{DATASET}.{TABLE}",
        "sla_table": f"{SLA_PROJECT}.{SLA_DATASET}.{SLA_TABLE}",
        "raw_table": f"{SLA_PROJECT}.{SLA_RAW_DATASET}.{SLA_RAW_TABLE}",
        "full_df_rows": int(len(df_cases)) if df_cases is not None else None,
        "sla_df_rows": int(len(df_sla)) if df_sla is not None else None,
        "sla_max_date": (
            df_sla["date"].max().strftime("%Y-%m-%d")
            if (df_sla is not None and not df_sla.empty and "date" in df_sla.columns and pd.notna(df_sla["date"].max()))
            else None
        ),
        "now_utc": dt.datetime.utcnow().isoformat() + "Z",
        "sla_cache_ts_utc": SLA_CACHE_TS.isoformat() + "Z" if SLA_CACHE_TS else None,
    }
    return jsonify(out)


# Cloud Run–friendly entrypoint

if __name__ == "__main__":
    # Cloud Run injects PORT; default to 8080 for local dev
    port = int(os.getenv("PORT", "8080"))
    debug_flag = os.getenv("FLASK_DEBUG", "false").lower() == "true"
    app.run(host="0.0.0.0", port=port, debug=debug_flag)