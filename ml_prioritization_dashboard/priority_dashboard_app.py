"""
priority_dashboard_app.py

Flask app to serve the ML priority dashboard (Command Center, SLA, Demand Trends).

Assumes scores have been written by score_priority_xgb.py into:
  boston311-mlops.boston311_service.cases_ranking_ml
"""

import os
from pathlib import Path
from typing import List
import datetime as dt

import numpy as np
import pandas as pd
from flask import Flask, render_template, request
from google.cloud import bigquery

SLA_CACHE_DF: pd.DataFrame | None = None
SLA_CACHE_TS: dt.datetime | None = None
SLA_CACHE_TTL_MIN = 10

# -----------------------------------------------------------------------------
# Config & BigQuery client
# -----------------------------------------------------------------------------

# ROOT_DIR = repo root (.../boston-311-ai-system)
ROOT_DIR = Path(__file__).resolve().parent.parent
APP_DIR = Path(__file__).resolve().parent

SA_PATH = ROOT_DIR / "secrets" / "bq-dashboard-ro.json"

# Allow turning off local JSON key when running in Cloud Run / Vertex.
USE_LOCAL_SA = os.getenv("B311_USE_LOCAL_SA", "true").lower() == "true"

if USE_LOCAL_SA and SA_PATH.exists():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SA_PATH)
    print(f"[INFO] Using service account key at {SA_PATH}")
else:
    print(
        "[INFO] Using default application credentials "
        "(Cloud Run / Vertex AI service account)."
    )

PROJECT = os.getenv("B311_PROJECT_ID", "boston311-mlops")
DATASET = os.getenv("B311_DATASET", "boston311_service")
TABLE = os.getenv("B311_TABLE", "cases_ranking_ml")

SLA_PROJECT = os.getenv("B311_SLA_PROJECT", PROJECT)
SLA_DATASET = os.getenv("B311_SLA_DATASET", "boston311")
SLA_TABLE   = os.getenv("B311_SLA_TABLE", "service_requests_2025")

BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

# Used only for footer / UI text
TRAIN_FEATURE_TABLE = os.getenv(
    "TRAIN_FEATURE_TABLE",
    f"{PROJECT}.{DATASET}.cases_ranking_ml_features",
)

app = Flask(__name__)
bq_client = bigquery.Client(project=PROJECT, location=BQ_LOCATION)

# (b) Optional app factory for gunicorn / WSGI usage
def create_app():
    """
    App factory for WSGI servers (gunicorn, etc.).
    Cloud Run can use 'priority_dashboard_app:app' or 'priority_dashboard_app:create_app()'.
    """
    return app

# -----------------------------------------------------------------------------
# Data loading
# -----------------------------------------------------------------------------

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


print("[INFO] Loading cases_ranking_ml once at startup...")
FULL_DF = load_data()
print(f"[INFO] Loaded {len(FULL_DF)} rows into memory.")


def load_sla_data(days: int = 365) -> pd.DataFrame:
    """
    Load raw 311 data for SLA metrics and analytics from the main table
    (boston311.service_requests_2025).

    We only use this for the SLA Performance and Analytics pages; it does NOT touch FULL_DF.
    """
    query = f"""
    SELECT
      case_enquiry_id,
      neighborhood,
      department,
      case_status,
      on_time,
      open_dt,
      closed_dt,
      sla_target_dt,
      reason,
      latitude,
      longitude,
      source   -- <<<<<< ADD THIS
    FROM `{PROJECT}.{SLA_DATASET}.{SLA_TABLE}`
    WHERE open_dt IS NOT NULL
      AND open_dt >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {days} DAY)
    """

    df = bq_client.query(query).to_dataframe(create_bqstorage_client=False)

    # Basic datetime parsing
    for col in ["open_dt", "closed_dt", "sla_target_dt"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    return df


def get_sla_cached_df(days: int = 365) -> pd.DataFrame:
    """
    Return SLA raw data with a shared in-memory cache so both
    /sla-performance and /demand-trends don't keep hitting BigQuery.
    """
    global SLA_CACHE_DF, SLA_CACHE_TS

    now_utc = dt.datetime.utcnow()
    should_refresh = (
        SLA_CACHE_DF is None
        or SLA_CACHE_TS is None
        or (now_utc - SLA_CACHE_TS).total_seconds() > SLA_CACHE_TTL_MIN * 60
    )

    if should_refresh:
        df = load_sla_data(days=days)
        SLA_CACHE_DF = df
        SLA_CACHE_TS = now_utc
    else:
        df = SLA_CACHE_DF.copy()

    return df

# -----------------------------------------------------------------------------
# Helper filters
# -----------------------------------------------------------------------------

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

# -----------------------------------------------------------------------------
# Routes – Command Center
# -----------------------------------------------------------------------------

@app.route("/", methods=["GET"])
def index():
    df = FULL_DF.copy()

    # Filters
    sel_neigh = request.args.get("neighborhood", "ALL")
    sel_reason = request.args.get("reason", "ALL")
    sel_dept = request.args.get("department", "ALL")
    sel_case = (request.args.get("case_id") or "").strip()
    near_lat = request.args.get("near_lat")
    near_lon = request.args.get("near_lon")
    near_radius_m = request.args.get("near_radius_m")

    # City-wide critical counts (keep in case you want these charts later)
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

    # Map data
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

# -----------------------------------------------------------------------------
# Routes – Work Queues & Neighborhoods
# -----------------------------------------------------------------------------

@app.route("/work-queues")
def work_queues_page():
    df = FULL_DF.copy()

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


@app.route("/neighborhoods")
def neighborhoods_page():
    df = FULL_DF.copy()

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

    neigh_rows = grouped.to_dict(orient="records")

    return render_template(
        "neighborhoods.html",
        perf_rows=neigh_rows,
    )

# -----------------------------------------------------------------------------
# Route – SLA Performance  (fixes /sla-performance 404)
# -----------------------------------------------------------------------------

def get_sla_cached_df(days: int = 365) -> pd.DataFrame:
    """
    Return SLA raw data with a shared in-memory cache so both
    /sla-performance and /demand-trends don't keep hitting BigQuery.
    """
    global SLA_CACHE_DF, SLA_CACHE_TS

    now_utc = dt.datetime.utcnow()
    should_refresh = (
        SLA_CACHE_DF is None
        or SLA_CACHE_TS is None
        or (now_utc - SLA_CACHE_TS).total_seconds() > SLA_CACHE_TTL_MIN * 60
    )

    if should_refresh:
        df = load_sla_data(days=days)
        SLA_CACHE_DF = df
        SLA_CACHE_TS = now_utc
    else:
        df = SLA_CACHE_DF.copy()

    return df


@app.route("/sla-performance")
def sla_performance_page():
    """
    SLA Performance & Compliance view.

    Uses ONLY the raw/main 311 table via load_sla_data(),
    but caches it in memory so we don't hit BigQuery on every request.
    """
    df = get_sla_cached_df(days=365)

    # If no data, just render an empty page with placeholders
    if df.empty:
        return render_template(
            "sla_performance.html",
            overall_sla_rate=None,
            avg_resolution_hours_all=None,
            overdue_count=0,
            sla_dept_labels=[],
            sla_dept_rates=[],
            art_service_labels=[],
            art_service_hours=[],
            overdue_cases=[],
            overdue_neighborhoods=[],
            overdue_map_cases=[],
        )

    # ------------------ Normalise dates & status ------------------
    for col in ["open_dt", "closed_dt", "sla_target_dt"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], utc=True, errors="coerce")

    status_str = df["case_status"].astype(str).str.upper()
    closed_mask = status_str.eq("CLOSED")

    # ------------------ Per-case SLA status (internal only) ------------------
    # (We keep this for metrics, but don't show per-case SLA in the UI now.)
    df["sla_applicable"] = df["sla_target_dt"].notna()
    df["sla_status_case"] = "No SLA target"

    now = dt.datetime.utcnow().replace(tzinfo=dt.timezone.utc)

    closed_with_sla = closed_mask & df["sla_applicable"] & df["closed_dt"].notna()
    df.loc[
        closed_with_sla & (df["closed_dt"] <= df["sla_target_dt"]),
        "sla_status_case",
    ] = "Met SLA"
    df.loc[
        closed_with_sla & (df["closed_dt"] > df["sla_target_dt"]),
        "sla_status_case",
    ] = "Breached SLA"

    open_with_sla = (~closed_mask) & df["sla_applicable"]
    df.loc[
        open_with_sla & (df["sla_target_dt"] >= now),
        "sla_status_case",
    ] = "On track (before SLA)"
    df.loc[
        open_with_sla & (df["sla_target_dt"] < now),
        "sla_status_case",
    ] = "Overdue (after SLA)"

    # Margin (how early / late vs SLA target) for closed cases (not used in UI now)
    df["sla_margin_hours"] = pd.NA
    df.loc[closed_with_sla, "sla_margin_hours"] = (
        (df.loc[closed_with_sla, "sla_target_dt"] - df.loc[closed_with_sla, "closed_dt"])
        .dt.total_seconds()
        / 3600.0
    )

    # ------------------ 1. Overall SLA compliance ------------------
    closed_with_sla_df = df[closed_with_sla].copy()
    if not closed_with_sla_df.empty:
        met_mask = closed_with_sla_df["sla_status_case"].eq("Met SLA")
        overall_sla_rate = float(met_mask.mean() * 100.0)
    else:
        overall_sla_rate = 0.0

    # ------------------ 2. SLA by department (top 10) ------------------
    if not closed_with_sla_df.empty and "department" in closed_with_sla_df.columns:
        dept_group = (
            closed_with_sla_df.dropna(subset=["department"])
            .groupby("department", as_index=False)["sla_status_case"]
            .agg(
                sla_rate=lambda s: (s == "Met SLA").mean() * 100.0,
                n_closed="count",
            )
        )
        dept_group = dept_group.sort_values("n_closed", ascending=False).head(10)
        sla_dept_labels = dept_group["department"].tolist()
        sla_dept_rates = dept_group["sla_rate"].round(1).tolist()
    else:
        sla_dept_labels = []
        sla_dept_rates = []

    # ------------------ 3. Avg resolution time (overall + by service) ------------------
    closed_for_art = df[closed_mask & df["open_dt"].notna() & df["closed_dt"].notna()].copy()
    if not closed_for_art.empty:
        closed_for_art["resolution_hours"] = (
            (closed_for_art["closed_dt"] - closed_for_art["open_dt"])
            .dt.total_seconds()
            / 3600.0
        )
        # Drop obviously bad values
        closed_for_art = closed_for_art[
            (closed_for_art["resolution_hours"] >= 0)
            & (closed_for_art["resolution_hours"] <= 90 * 24)
        ]

        if not closed_for_art["resolution_hours"].empty:
            avg_resolution_hours_all = float(closed_for_art["resolution_hours"].mean())
        else:
            avg_resolution_hours_all = None

        service_col = "reason" if "reason" in closed_for_art.columns else None
        if service_col:
            svc_group = (
                closed_for_art.dropna(subset=[service_col])
                .groupby(service_col, as_index=False)["resolution_hours"]
                .mean()
            )
            svc_group = svc_group.sort_values(
                "resolution_hours", ascending=False
            ).head(15)
            art_service_labels = svc_group[service_col].tolist()
            art_service_hours = svc_group["resolution_hours"].round(1).tolist()
        else:
            art_service_labels = []
            art_service_hours = []
    else:
        avg_resolution_hours_all = None
        art_service_labels = []
        art_service_hours = []

    # ------------------ 4. Overdue drilldown + map ------------------
    df["is_overdue"] = (
        ~closed_mask & df["sla_target_dt"].notna() & (df["sla_target_dt"] < now)
    )
    overdue_df = df[df["is_overdue"]].copy()

    overdue_df["days_overdue"] = (
        (now - overdue_df["sla_target_dt"]).dt.total_seconds() / 86400.0
    ).round(1)

    overdue_count = len(overdue_df)

    overdue_neighborhoods = (
        overdue_df["neighborhood"]
        .dropna()
        .astype(str)
        .sort_values()
        .unique()
        .tolist()
    )

    overdue_cases: list[dict] = []
    for _, row in overdue_df.iterrows():
        open_label = None
        if pd.notna(row.get("open_dt")):
            open_label = str(row["open_dt"].date())
        overdue_cases.append(
            {
                "case_enquiry_id": row.get("case_enquiry_id"),
                "neighborhood": row.get("neighborhood"),
                "department": row.get("department"),
                "case_status": row.get("case_status"),
                "open_date_label": open_label,
                "open_dt": row.get("open_dt"),
                "days_overdue": row.get("days_overdue"),
            }
        )

    # Map markers
    overdue_map_cases: list[dict] = []
    if {"latitude", "longitude"}.issubset(overdue_df.columns):
        map_df = overdue_df.dropna(subset=["latitude", "longitude"]).copy()
        map_df = map_df.head(1000)  # safety cap
        for _, row in map_df.iterrows():
            try:
                lat = float(row["latitude"])
                lon = float(row["longitude"])
            except (TypeError, ValueError):
                continue
            overdue_map_cases.append(
                {
                    "case_enquiry_id": row.get("case_enquiry_id"),
                    "latitude": lat,
                    "longitude": lon,
                    "neighborhood": row.get("neighborhood"),
                    "department": row.get("department"),
                    "reason": row.get("reason"),
                    "days_overdue": row.get("days_overdue"),
                }
            )

    # ------------------ Render template (no case lookup anymore) ------------------
    return render_template(
        "sla_performance.html",
        overall_sla_rate=overall_sla_rate,
        avg_resolution_hours_all=avg_resolution_hours_all,
        overdue_count=overdue_count,
        sla_dept_labels=sla_dept_labels,
        sla_dept_rates=sla_dept_rates,
        art_service_labels=art_service_labels,
        art_service_hours=art_service_hours,
        overdue_cases=overdue_cases,
        overdue_neighborhoods=overdue_neighborhoods,
        overdue_map_cases=overdue_map_cases,
    )

# -----------------------------------------------------------------------------
# Route – Demand Trends & Forecasting
# -----------------------------------------------------------------------------

@app.route("/demand-trends")
def demand_trends_page():
    """
    Demand Trends & Forecasting view.

    Uses the same raw 311 table as SLA (service_requests_2025 via load_sla_data),
    and shows:

      Top-left:  Weekly case volume – last 12 months (history only)
      Top-right: Seasonal monthly baseline forecast – next 6 months
      Bottom-left: Seasonality – total volume by month (all topics)
      Bottom-right: Top request topics by volume (bar chart)
      Full-width below: Seasonal peaks by topic (multi-series)
    """
    df = get_sla_cached_df(days=365)

    if "open_dt" not in df.columns:
        return render_template(
            "demand_trends.html",
            hist_week_labels=[],
            hist_week_values=[],
            fc_week_labels=[],
            fc_week_values=[],
            month_labels=[],
            month_totals=[],
            month_has_data=[],
            seasonal_datasets=[],
            topic_labels=[],
            topic_totals=[],
        )

    df["open_dt"] = pd.to_datetime(df["open_dt"], utc=True, errors="coerce")
    df = df.dropna(subset=["open_dt"])

    if df.empty:
        return render_template(
            "demand_trends.html",
            hist_week_labels=[],
            hist_week_values=[],
            fc_week_labels=[],
            fc_week_values=[],
            month_labels=[],
            month_totals=[],
            month_has_data=[],
            seasonal_datasets=[],
            topic_labels=[],
            topic_totals=[],
        )

    # Sort and use open_dt as index
    df = df.sort_values("open_dt").set_index("open_dt")
    max_open = df.index.max()

    # ----------------------------------------------------
    # 1) Weekly historical volume (last 12 months, no cliff)
    # ----------------------------------------------------
    weekly_counts = (
        df["case_enquiry_id"]
        .resample("W-MON")   # weekly, anchored on Monday
        .count()
        .rename("volume")
    )

    weekly_counts = weekly_counts[weekly_counts > 0]

    # Drop the last week if it's incomplete (avoid fake drop)
    if len(weekly_counts) >= 1:
        last_week_start = weekly_counts.index[-1]
        if max_open < last_week_start + pd.Timedelta(days=6):
            weekly_counts = weekly_counts.iloc[:-1]

    # Keep at most last 52 weeks
    if len(weekly_counts) > 52:
        weekly_counts = weekly_counts.tail(52)

    hist_week_labels = [ts.strftime("%Y-%m-%d") for ts in weekly_counts.index]
    hist_week_values = [int(v) for v in weekly_counts.values]

    # ----------------------------------------------------
    # 2) Seasonal baseline forecast – next 6 months
    #    Repeat last 12 FULL months' pattern forward.
    # ----------------------------------------------------
    # Monthly totals anchored at month start
    monthly_ts = (
        df["case_enquiry_id"]
        .resample("MS")  # Month Start
        .count()
        .rename("volume")
    )

    # Keep only full months (exclude the current partial month)
    max_period = max_open.to_period("M")
    monthly_ts = monthly_ts[monthly_ts.index.to_period("M") < max_period]

    if not monthly_ts.empty:
        # Use up to last 12 months as pattern
        history = monthly_ts.tail(12)
        pattern = history.values.astype(float)

        # If there is no pattern, we fallback to flat line
        if len(pattern) == 0:
            fc_week_labels = []
            fc_week_values = []
        else:
            last_hist_period = history.index[-1].to_period("M")
            future_periods = pd.period_range(last_hist_period + 1, periods=6, freq="M")
            future_index = future_periods.to_timestamp()  # month start timestamps

            fc_week_labels = [ts.strftime("%Y-%m") for ts in future_index]
            fc_week_values = [
                int(round(pattern[i % len(pattern)]))
                for i in range(len(future_index))
            ]
    else:
        fc_week_labels = []
        fc_week_values = []

    # ----------------------------------------------------
    # 3) Month-of-year totals (all topics combined)
    # ----------------------------------------------------
    df_recent = df.reset_index()  # bring open_dt back as a column
    df_recent["month_num"] = df_recent["open_dt"].dt.month

    month_counts = (
        df_recent.groupby("month_num")["case_enquiry_id"]
        .count()
        .to_dict()
    )

    month_labels = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    month_totals = [int(month_counts.get(m, 0)) for m in range(1, 13)]
    month_has_data = [month_counts.get(m, 0) > 0 for m in range(1, 13)]

    # ----------------------------------------------------
    # 4) Seasonal peaks by topic (month-of-year, top 5 reasons)
    # ----------------------------------------------------
    seasonal_datasets = []
    topic_col = "reason" if "reason" in df_recent.columns else None

    if topic_col:
        top_topics = (
            df_recent[topic_col]
            .dropna()
            .astype(str)
            .value_counts()
            .head(5)
            .index
            .tolist()
        )

        for topic in top_topics:
            topic_df = df_recent[df_recent[topic_col] == topic]
            topic_month_counts = (
                topic_df.groupby("month_num")["case_enquiry_id"]
                .count()
                .to_dict()
            )
            counts_for_topic = [int(topic_month_counts.get(m, 0)) for m in range(1, 13)]
            seasonal_datasets.append(
                {
                    "label": topic,
                    "data": counts_for_topic,
                }
            )

    # ----------------------------------------------------
    # 5) Top request topics by volume (bar chart)
    # ----------------------------------------------------
    if "reason" in df_recent.columns:
        topic_counts = (
            df_recent["reason"]
            .dropna()
            .astype(str)
            .value_counts()
            .head(5)
        )
        topic_labels = topic_counts.index.tolist()
        topic_totals = [int(v) for v in topic_counts.values]
    else:
        topic_labels = []
        topic_totals = []

    return render_template(
        "demand_trends.html",
        hist_week_labels=hist_week_labels,
        hist_week_values=hist_week_values,
        fc_week_labels=fc_week_labels,   # now monthly, but same variable name
        fc_week_values=fc_week_values,
        month_labels=month_labels,
        month_totals=month_totals,
        month_has_data=month_has_data,
        seasonal_datasets=seasonal_datasets,
        topic_labels=topic_labels,
        topic_totals=topic_totals,
    )

@app.route("/analytics")
def analytics_page():
    """
    3. Analytics – categorical breakdowns (no maps).

    Uses the same cached raw 311 table as SLA / demand trends
    via get_sla_cached_df(days=365).
    """
    df = get_sla_cached_df(days=365)

    if df.empty:
        return render_template(
            "analytics.html",
            neigh_labels=[], neigh_counts=[],
            dept_labels=[], dept_counts=[],
            reason_labels=[], reason_counts=[],
            sla_dept_labels=[], sla_dept_rates=[],
            src_labels=[], src_counts=[],
            src_art_labels=[], src_art_hours=[],
        )

    # Make sure key columns exist / are strings
    for col in ["neighborhood", "department", "reason", "case_status", "source"]:
        if col not in df.columns:
            df[col] = None
        df[col] = df[col].fillna("Unknown").astype(str).str.strip()

    # ------------------------------------------------------------------
    # 1. Cases by neighborhood (top 10)
    # ------------------------------------------------------------------
    neigh_series = (
        df["neighborhood"]
        .value_counts()
        .head(10)
    )
    neigh_labels = neigh_series.index.tolist()
    neigh_counts = [int(v) for v in neigh_series.values]

    # ------------------------------------------------------------------
    # 2. Cases by department (top 10)
    # ------------------------------------------------------------------
    dept_series = (
        df["department"]
        .value_counts()
        .head(10)
    )
    dept_labels = dept_series.index.tolist()
    dept_counts = [int(v) for v in dept_series.values]

    # ------------------------------------------------------------------
    # 3. Top reasons (top 10 request topics)
    # ------------------------------------------------------------------
    reason_series = (
        df["reason"]
        .value_counts()
        .head(10)
    )
    reason_labels = reason_series.index.tolist()
    reason_counts = [int(v) for v in reason_series.values]

    # ------------------------------------------------------------------
    # 4. SLA compliance by department (top 10 by volume)
    # ------------------------------------------------------------------
    # Closed vs open
    status_str = df["case_status"].astype(str).str.upper()
    closed_mask = status_str.eq("CLOSED")

    df_sla = df.copy()
    # Ensure datetime types
    for col in ["open_dt", "closed_dt", "sla_target_dt"]:
        if col in df_sla.columns:
            df_sla[col] = pd.to_datetime(df_sla[col], utc=True, errors="coerce")

    df_sla["on_time_flag"] = False
    has_both = (
        closed_mask
        & df_sla["closed_dt"].notna()
        & df_sla["sla_target_dt"].notna()
    )
    df_sla.loc[has_both, "on_time_flag"] = (
        df_sla.loc[has_both, "closed_dt"]
        <= df_sla.loc[has_both, "sla_target_dt"]
    )

    closed_df = df_sla[closed_mask & df_sla["sla_target_dt"].notna()].copy()

    if not closed_df.empty:
        dept_group = (
            closed_df
            .dropna(subset=["department"])
            .groupby("department", as_index=False)["on_time_flag"]
            .mean()
        )
        dept_group["sla_rate"] = dept_group["on_time_flag"] * 100.0

        dept_counts_sla = (
            closed_df
            .dropna(subset=["department"])
            .groupby("department")["case_enquiry_id"]
            .count()
            .reset_index(name="n_closed")
        )
        dept_group = dept_group.merge(dept_counts_sla, on="department", how="left")
        dept_group = dept_group.sort_values("n_closed", ascending=False).head(10)

        sla_dept_labels = dept_group["department"].tolist()
        sla_dept_rates = dept_group["sla_rate"].round(1).tolist()
    else:
        sla_dept_labels = []
        sla_dept_rates = []

    # ------------------------------------------------------------------
    # 5 + 6. Volume by source + avg resolution time by source
    # ------------------------------------------------------------------
    # Volume by source (all cases)
    src_series = (
        df["source"]
        .value_counts()
        .head(10)
    )
    src_labels = src_series.index.tolist()
    src_counts = [int(v) for v in src_series.values]

    # Average resolution hours by source (only closed)
    if not closed_df.empty:
        closed_df["resolution_hours"] = (
            (closed_df["closed_dt"] - closed_df["open_dt"])
            .dt.total_seconds() / 3600.0
        )
        # Filter crazy values
        closed_df = closed_df[
            (closed_df["resolution_hours"] >= 0)
            & (closed_df["resolution_hours"] <= 90 * 24)
        ]

        src_art_group = (
            closed_df
            .dropna(subset=["source"])
            .groupby("source", as_index=False)["resolution_hours"]
            .mean()
        )
        src_art_group = src_art_group.sort_values(
            "resolution_hours", ascending=False
        ).head(10)

        src_art_labels = src_art_group["source"].tolist()
        src_art_hours = src_art_group["resolution_hours"].round(1).tolist()
    else:
        src_art_labels = []
        src_art_hours = []

    return render_template(
        "analytics.html",
        neigh_labels=neigh_labels,
        neigh_counts=neigh_counts,
        dept_labels=dept_labels,
        dept_counts=dept_counts,
        reason_labels=reason_labels,
        reason_counts=reason_counts,
        sla_dept_labels=sla_dept_labels,
        sla_dept_rates=sla_dept_rates,
        src_labels=src_labels,
        src_counts=src_counts,
        src_art_labels=src_art_labels,
        src_art_hours=src_art_hours,
    )

# (c) Cloud Run–friendly entrypoint
if __name__ == "__main__":
    # Cloud Run injects PORT; default to 8080 for local dev
    port = int(os.getenv("PORT", "8080"))
    debug_flag = os.getenv("FLASK_DEBUG", "false").lower() == "true"

    app.run(host="0.0.0.0", port=port, debug=debug_flag)
