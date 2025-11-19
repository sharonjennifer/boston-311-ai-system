from pathlib import Path
import os

from flask import Flask, render_template, request
from google.cloud import bigquery
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent
SA_PATH = BASE_DIR.parent / "services" / "prioritization-api" / "secrets" / "bq-dashboard-ro.json"

if SA_PATH.exists():
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = str(SA_PATH)
    print(f"[INFO] Using service account key at {SA_PATH}")
else:
    print(
        f"[WARN] Service account file not found at {SA_PATH}. "
        "Falling back to default ADC. Make sure creds are set."
    )

PROJECT = "boston311-mlops"
DATASET = "boston311_service"
TABLE = "cases_ranking_ml"
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

app = Flask(__name__)
bq_client = bigquery.Client(project=PROJECT, location=BQ_LOCATION)


def load_data() -> pd.DataFrame:
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
    df = bq_client.query(query).to_dataframe()
    return df


@app.route("/", methods=["GET"])
def index():
    df = load_data()

    df_all_crit = df[df["segment"] == "CRITICAL"]

    # Critical by neighborhood (top 5) – NOT filtered
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

    # Critical by department (top 5) – NOT filtered
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

    neighborhoods = sorted(df["neighborhood"].dropna().unique())
    reasons = sorted(df["reason"].dropna().unique())
    departments = sorted(df["department"].dropna().unique())

    sel_neigh = request.args.get("neighborhood", "ALL")
    sel_reason = request.args.get("reason", "ALL")
    sel_dept = request.args.get("department", "ALL")

    df_filt = df.copy()
    if sel_neigh != "ALL":
        df_filt = df_filt[df_filt["neighborhood"] == sel_neigh]
    if sel_reason != "ALL":
        df_filt = df_filt[df_filt["reason"] == sel_reason]
    if sel_dept != "ALL":
        df_filt = df_filt[df_filt["department"] == sel_dept]

    df_crit_filt = df_filt[df_filt["segment"] == "CRITICAL"].sort_values(
        "priority_score", ascending=False
    ).head(100)
    df_sec_filt = df_filt[df_filt["segment"] == "SECONDARY"].sort_values(
        "priority_score", ascending=False
    ).head(100)

    total_open = len(df_filt)
    num_crit = len(df_crit_filt)
    crit_share = (num_crit / total_open * 100) if total_open > 0 else 0.0

    stats = {
        "total_open": total_open,
        "num_crit": num_crit,
        "crit_share": f"{crit_share:.1f}",
    }

    # Map data = filtered + only rows with lat/lon
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
        critical_cases=df_crit_filt.to_dict(orient="records"),
        secondary_cases=df_sec_filt.to_dict(orient="records"),
        stats=stats,
        map_cases=map_cases,
        crit_neigh_labels_all=crit_neigh_labels_all,
        crit_neigh_counts_all=crit_neigh_counts_all,
        crit_dept_labels_all=crit_dept_labels_all,
        crit_dept_counts_all=crit_dept_counts_all,
    )


if __name__ == "__main__":
    app.run(debug=True)
