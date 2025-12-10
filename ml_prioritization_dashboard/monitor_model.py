"""
monitor_model.py

Model monitoring script for the Boston 311 Priority Dashboard.

- Reads recent predictions + labels from BigQuery
  (boston311-mlops.boston311_service.cases_ranking_ml)
- Computes AUC and RMSE for a sliding lookback window
- Logs results to a dedicated monitoring table:
    boston311-mlops.monitoring.priority_model_metrics
- Sets a decay_flag when performance falls below thresholds
- Returns exit code:
    0 → model healthy
    1 → model decay detected (scheduler / Airflow can trigger retraining)
"""

import os
import sys
import logging
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery
from sklearn.metrics import roc_auc_score, mean_squared_error
import numpy as np

# ---------------------------------------------------------------------------
# Logging config
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# ---------------------------------------------------------------------------
# Core config – adjust these if your schema changes
# ---------------------------------------------------------------------------

PROJECT_ID = os.getenv("B311_PROJECT_ID", "boston311-mlops")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

# Table with predictions + labels
DATASET_ID = "boston311_service"
PRED_TABLE = "cases_ranking_ml"

# Monitoring dataset + table (created automatically if missing)
MONITOR_DATASET = "monitoring"
MONITOR_TABLE = "priority_model_metrics"

# Column names in PRED_TABLE – update if your schema differs
PRED_COL = "priority_score"           # numeric prediction (0–1)
LABEL_COL = "resolved_ontime_label"   # 0/1 label
TIMESTAMP_COL = "prediction_timestamp"  # timestamp when score was produced

# Monitoring window (e.g., last 30 days for more robust metrics)
LOOKBACK_DAYS = int(os.getenv("MONITOR_LOOKBACK_DAYS", "30"))

# Thresholds to flag model decay (tune based on your baseline performance)
MIN_AUC = float(os.getenv("MONITOR_MIN_AUC", "0.80"))
MAX_RMSE = float(os.getenv("MONITOR_MAX_RMSE", "0.30"))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def get_bq_client() -> bigquery.Client:
    """Create a BigQuery client with the configured project + location."""
    return bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)


def ensure_monitoring_table(client: bigquery.Client) -> None:
    """
    Ensure the monitoring dataset + table exist.

    Schema:
      run_timestamp (TIMESTAMP)
      window_start (TIMESTAMP)
      window_end   (TIMESTAMP)
      n_rows       (INTEGER)
      auc          (FLOAT)
      rmse         (FLOAT)
      decay_flag   (BOOL)
      notes        (STRING)
    """
    dataset_id = f"{PROJECT_ID}.{MONITOR_DATASET}"
    table_id = f"{dataset_id}.{MONITOR_TABLE}"

    # Dataset
    dataset_ref = bigquery.Dataset(dataset_id)
    dataset_ref.location = BQ_LOCATION

    try:
        client.get_dataset(dataset_ref)
        logging.info("Monitoring dataset already exists: %s", dataset_id)
    except Exception:
        logging.info("Monitoring dataset not found, creating: %s", dataset_id)
        client.create_dataset(dataset_ref)

    # Table
    try:
        client.get_table(table_id)
        logging.info("Monitoring table already exists: %s", table_id)
        return
    except Exception:
        logging.info("Monitoring table not found, creating: %s", table_id)

    schema = [
        bigquery.SchemaField("run_timestamp", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("window_start", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("window_end", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("n_rows", "INTEGER", mode="REQUIRED"),
        bigquery.SchemaField("auc", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("rmse", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("decay_flag", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("notes", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)
    logging.info("Monitoring table created: %s", table_id)


def fetch_recent_data(client: bigquery.Client):
    """
    Fetch predictions + labels from the last LOOKBACK_DAYS days.

    If you want to test quickly, temporarily bump LOOKBACK_DAYS to 365 to
    include older predictions.
    """
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{PRED_TABLE}"

    window_end = datetime.now(timezone.utc)
    window_start = window_end - timedelta(days=LOOKBACK_DAYS)

    query = f"""
        SELECT
          {PRED_COL} AS y_pred,
          {LABEL_COL} AS y_true,
          {TIMESTAMP_COL} AS ts
        FROM {table_id}
        WHERE
          {TIMESTAMP_COL} BETWEEN @start_ts AND @end_ts
          AND {PRED_COL} IS NOT NULL
          AND {LABEL_COL} IS NOT NULL
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_ts", "TIMESTAMP", window_start),
            bigquery.ScalarQueryParameter("end_ts", "TIMESTAMP", window_end),
        ]
    )

    logging.info(
        "Querying recent predictions from %s (window=%s → %s, %d days)",
        table_id,
        window_start.isoformat(),
        window_end.isoformat(),
        LOOKBACK_DAYS,
    )
    df = client.query(query, job_config=job_config).to_dataframe()
    logging.info("Fetched %d rows from last %d days", len(df), LOOKBACK_DAYS)
    return df, window_start, window_end


def compute_metrics(df):
    """Compute AUC and RMSE on the monitoring window."""
    if df.empty:
        logging.warning(
            "No data available in the lookback window (n_rows=0). "
            "This is expected if scoring has not run recently."
        )
        return None, None, 0

    y_true = df["y_true"].to_numpy()
    y_pred = df["y_pred"].to_numpy()

    # Clamp predictions to [0,1] in case model produces slightly out-of-range scores
    y_pred = np.clip(y_pred, 0.0, 1.0)

    try:
        auc = float(roc_auc_score(y_true, y_pred))
    except ValueError as e:
        logging.warning("Could not compute AUC (likely only one class present): %s", e)
        auc = None

    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))

    auc_str = f"{auc:.4f}" if auc is not None else "NA"
    logging.info(
        "Metrics on %d rows: AUC=%s, RMSE=%.4f",
        len(df),
        auc_str,
        rmse,
    )
    return auc, rmse, len(df)


def _to_ts(val):
    """Convert datetime → RFC3339 string for BigQuery JSON inserts."""
    if val is None:
        return None
    if isinstance(val, str):
        return val
    if isinstance(val, datetime):
        if val.tzinfo is None:
            val = val.replace(tzinfo=timezone.utc)
        return val.isoformat().replace("+00:00", "Z")
    return val


def write_monitoring_row(
    client: bigquery.Client,
    run_ts,
    window_start,
    window_end,
    n_rows,
    auc,
    rmse,
    decay_flag,
    notes="",
):
    """Insert one row of monitoring output into BigQuery."""
    table_id = f"{PROJECT_ID}.{MONITOR_DATASET}.{MONITOR_TABLE}"

    rows_to_insert = [
        {
            "run_timestamp": _to_ts(run_ts),
            "window_start": _to_ts(window_start),
            "window_end": _to_ts(window_end),
            "n_rows": int(n_rows),
            "auc": auc,
            "rmse": rmse,
            "decay_flag": bool(decay_flag),
            "notes": notes,
        }
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        logging.error("Failed to insert monitoring row: %s", errors)
    else:
        logging.info("Monitoring row inserted into %s", table_id)


# ---------------------------------------------------------------------------
# Main entrypoint
# ---------------------------------------------------------------------------

def main() -> int:
    client = get_bq_client()
    ensure_monitoring_table(client)

    df, window_start, window_end = fetch_recent_data(client)
    auc, rmse, n_rows = compute_metrics(df)

    run_ts = datetime.now(timezone.utc)

    # Case 1: no data → log it, but do not flag decay
    if n_rows == 0:
        decay_flag = False
        notes = "No rows in monitoring window. Possibly no recent scoring runs."
        write_monitoring_row(
            client,
            run_ts,
            window_start,
            window_end,
            n_rows,
            auc,
            rmse,
            decay_flag,
            notes,
        )
        logging.info("Monitoring completed with n_rows=0 (no data).")
        return 0

    # Case 2: there is data → check thresholds
    decay_flag = False
    notes_parts = []

    if auc is not None and auc < MIN_AUC:
        decay_flag = True
        notes_parts.append(f"AUC below threshold ({auc:.3f} < {MIN_AUC:.3f}).")

    if rmse is not None and rmse > MAX_RMSE:
        decay_flag = True
        notes_parts.append(f"RMSE above threshold ({rmse:.3f} > {MAX_RMSE:.3f}).")

    notes = " ".join(notes_parts) if notes_parts else "Model metrics within thresholds."

    write_monitoring_row(
        client,
        run_ts,
        window_start,
        window_end,
        n_rows,
        auc,
        rmse,
        decay_flag,
        notes,
    )

    if decay_flag:
        logging.warning("Model decay detected: %s", notes)
        # Exit code 1 → scheduler / Airflow can treat this as “trigger retrain”
        return 1

    logging.info("Model is healthy. No retrain needed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())