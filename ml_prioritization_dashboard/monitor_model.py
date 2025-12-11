"""
Model monitoring script for the Boston 311 Priority Dashboard.

- Reads recent predictions + labels from BigQuery
  (boston311-mlops.boston311_service.cases_ranking_ml)
- Computes performance metrics over a sliding lookback window:
    * AUC  – ranking quality
    * RMSE – calibration error
- Computes a simple data drift signal based on class balance:
    * class_balance = mean(y_true) in the window
    * compares it to a baseline positive rate from training
- Logs results to a dedicated monitoring table:
    boston311-mlops.monitoring.priority_model_metrics
- Sets flags:
    * decay_flag – performance thresholds violated
    * drift_flag – class balance drift exceeds threshold
- Optionally sends an EMAIL notification when decay and/or drift are detected.
- Returns exit code:
    0 → model healthy (no decay, no drift)
    1 → issues detected (decay and/or drift), retraining can be triggered
"""

import os
import sys
import logging
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta, timezone

from google.cloud import bigquery
from sklearn.metrics import roc_auc_score, mean_squared_error
import numpy as np

# Logging config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)

# Core config – adjust these if your schema changes

PROJECT_ID = os.getenv("B311_PROJECT_ID", "boston311-mlops")
BQ_LOCATION = os.getenv("BQ_LOCATION", "US")

# Table with predictions + labels
DATASET_ID = "boston311_service"
PRED_TABLE = "cases_ranking_ml"

# Monitoring dataset + table (created automatically if missing)
MONITOR_DATASET = "monitoring"
MONITOR_TABLE = "priority_model_metrics"

# Column names in PRED_TABLE – update if your schema differs
PRED_COL = "priority_score"            # numeric prediction (0–1)
LABEL_COL = "resolved_ontime_label"    # 0/1 label
TIMESTAMP_COL = "prediction_timestamp" # timestamp when score was produced

# Monitoring window (e.g., last 30 days for more robust metrics)
LOOKBACK_DAYS = int(os.getenv("MONITOR_LOOKBACK_DAYS", "30"))

# Performance thresholds to flag model decay
MIN_AUC = float(os.getenv("MONITOR_MIN_AUC", "0.80"))
MAX_RMSE = float(os.getenv("MONITOR_MAX_RMSE", "0.30"))

# Simple data drift check: class balance
# Baseline positive rate estimated from training data (or logs).
BASELINE_POS_RATE = float(os.getenv("MONITOR_BASELINE_POS_RATE", "0.65"))

# Maximum allowed absolute difference in class balance before we call it drift.
MAX_CLASS_BALANCE_DRIFT = float(os.getenv("MONITOR_MAX_CLASS_DRIFT", "0.10"))

# Email notification config 

MONITOR_EMAIL_TO = os.getenv("MONITOR_EMAIL_TO", "")  # comma-separated emails
MONITOR_EMAIL_FROM = os.getenv("MONITOR_EMAIL_FROM", "")
MONITOR_SMTP_HOST = os.getenv("MONITOR_SMTP_HOST", "")
MONITOR_SMTP_PORT = int(os.getenv("MONITOR_SMTP_PORT", "587"))  # TLS default
MONITOR_SMTP_USER = os.getenv("MONITOR_SMTP_USER", "")
MONITOR_SMTP_PASS = os.getenv("MONITOR_SMTP_PASS", "")
MONITOR_SMTP_USE_TLS = os.getenv("MONITOR_SMTP_USE_TLS", "true").lower() == "true"

# Helpers

def get_bq_client() -> bigquery.Client:
    """Create a BigQuery client with the configured project + location."""
    return bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)


def ensure_monitoring_table(client: bigquery.Client) -> None:
    """
    Ensure the monitoring dataset + table exist.

    Schema:
      run_timestamp   (TIMESTAMP)
      window_start    (TIMESTAMP)
      window_end      (TIMESTAMP)
      n_rows          (INTEGER)
      auc             (FLOAT)
      rmse            (FLOAT)
      decay_flag      (BOOL)
      drift_flag      (BOOL)
      class_balance   (FLOAT)
      notes           (STRING)
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
        bigquery.SchemaField("drift_flag", "BOOL", mode="REQUIRED"),
        bigquery.SchemaField("class_balance", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("notes", "STRING", mode="NULLABLE"),
    ]

    table = bigquery.Table(table_id, schema=schema)
    client.create_table(table)
    logging.info("Monitoring table created: %s", table_id)


def fetch_recent_data(client: bigquery.Client):
    """
    Fetch predictions + labels from the last LOOKBACK_DAYS days.
    """
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{PRED_TABLE}"

    window_end = datetime.now(timezone.utc)
    window_start = window_end - timedelta(days=LOOKBACK_DAYS)

    query = f"""
        SELECT
          {PRED_COL} AS y_pred,
          {LABEL_COL} AS y_true,
          {TIMESTAMP_COL} AS ts
        FROM `{table_id}`
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

    # disable BigQuery Storage API to avoid permission errors
    df = client.query(query, job_config=job_config).to_dataframe(
        create_bqstorage_client=False
    )
    logging.info("Fetched %d rows from last %d days", len(df), LOOKBACK_DAYS)
    return df, window_start, window_end


def compute_metrics_and_drift(df):
    """
    Compute AUC, RMSE, and a simple data drift signal (class balance).

    Returns:
      auc, rmse, n_rows, class_balance, class_balance_diff
    """
    if df.empty:
        logging.warning(
            "No data available in the lookback window (n_rows=0). "
            "This is expected if scoring has not run recently."
        )
        return None, None, 0, None, None

    y_true = df["y_true"].to_numpy()
    y_pred = df["y_pred"].to_numpy()

    # Clamp predictions to [0,1] in case model produces slightly out-of-range scores
    y_pred = np.clip(y_pred, 0.0, 1.0)

    # Performance metrics
    try:
        auc = float(roc_auc_score(y_true, y_pred))
    except ValueError as e:
        logging.warning("Could not compute AUC (likely only one class present): %s", e)
        auc = None

    rmse = float(np.sqrt(mean_squared_error(y_true, y_pred)))

    # Data drift proxy: class balance (fraction of positive labels)
    class_balance = float(y_true.mean())
    class_balance_diff = abs(class_balance - BASELINE_POS_RATE)

    auc_str = f"{auc:.4f}" if auc is not None else "NA"
    logging.info(
        "Metrics on %d rows: AUC=%s, RMSE=%.4f, class_balance=%.4f (baseline=%.4f, diff=%.4f)",
        len(df),
        auc_str,
        rmse,
        class_balance,
        BASELINE_POS_RATE,
        class_balance_diff,
    )
    return auc, rmse, len(df), class_balance, class_balance_diff


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
    drift_flag,
    class_balance,
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
            "drift_flag": bool(drift_flag),
            "class_balance": class_balance,
            "notes": notes,
        }
    ]

    errors = client.insert_rows_json(table_id, rows_to_insert)
    if errors:
        logging.error("Failed to insert monitoring row: %s", errors)
    else:
        logging.info("Monitoring row inserted into %s", table_id)


def send_email_notification(subject: str, body: str) -> None:
    """
    Send an email notification when decay/drift is detected.

    Uses standard SMTP with configuration from environment variables.
    If any required configuration is missing, this becomes a no-op and logs a warning.
    """
    if not (MONITOR_EMAIL_TO and MONITOR_EMAIL_FROM and MONITOR_SMTP_HOST):
        logging.warning(
            "Email notification config incomplete. "
            "MONITOR_EMAIL_TO, MONITOR_EMAIL_FROM, and MONITOR_SMTP_HOST must be set."
        )
        return

    recipients = [addr.strip() for addr in MONITOR_EMAIL_TO.split(",") if addr.strip()]
    if not recipients:
        logging.warning("MONITOR_EMAIL_TO is set but empty after parsing. Skipping email.")
        return

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = MONITOR_EMAIL_FROM
    msg["To"] = ", ".join(recipients)
    msg.set_content(body)

    try:
        if MONITOR_SMTP_USE_TLS:
            with smtplib.SMTP(MONITOR_SMTP_HOST, MONITOR_SMTP_PORT) as server:
                server.starttls()
                if MONITOR_SMTP_USER and MONITOR_SMTP_PASS:
                    server.login(MONITOR_SMTP_USER, MONITOR_SMTP_PASS)
                server.send_message(msg)
        else:
            with smtplib.SMTP(MONITOR_SMTP_HOST, MONITOR_SMTP_PORT) as server:
                if MONITOR_SMTP_USER and MONITOR_SMTP_PASS:
                    server.login(MONITOR_SMTP_USER, MONITOR_SMTP_PASS)
                server.send_message(msg)

        logging.info("Email notification sent to: %s", ", ".join(recipients))
    except Exception as e:
        logging.warning("Failed to send email notification: %s", e)

# Main entrypoint

def main() -> int:
    client = get_bq_client()
    ensure_monitoring_table(client)

    df, window_start, window_end = fetch_recent_data(client)
    auc, rmse, n_rows, class_balance, class_balance_diff = compute_metrics_and_drift(df)

    run_ts = datetime.now(timezone.utc)

    # Case 1: no data → log it, but do not flag decay or drift
    if n_rows == 0:
        decay_flag = False
        drift_flag = False
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
            drift_flag,
            class_balance,
            notes,
        )
        logging.info("Monitoring completed with n_rows=0 (no data).")
        return 0

    # Case 2: there is data → check thresholds
    decay_flag = False
    drift_flag = False
    notes_parts = []

    # Performance-based decay
    if auc is not None and auc < MIN_AUC:
        decay_flag = True
        notes_parts.append(f"AUC below threshold ({auc:.3f} < {MIN_AUC:.3f}).")

    if rmse is not None and rmse > MAX_RMSE:
        decay_flag = True
        notes_parts.append(f"RMSE above threshold ({rmse:.3f} > {MAX_RMSE:.3f}).")

    # Data drift: class balance shifted from training baseline
    if class_balance is not None and class_balance_diff is not None:
        if class_balance_diff > MAX_CLASS_BALANCE_DRIFT:
            drift_flag = True
            notes_parts.append(
                f"Class balance drift detected (current={class_balance:.3f}, "
                f"baseline={BASELINE_POS_RATE:.3f}, diff={class_balance_diff:.3f} > "
                f"{MAX_CLASS_BALANCE_DRIFT:.3f})."
            )

    if not notes_parts:
        notes = "Model metrics and class balance within thresholds."
    else:
        notes = " ".join(notes_parts)

    write_monitoring_row(
        client,
        run_ts,
        window_start,
        window_end,
        n_rows,
        auc,
        rmse,
        decay_flag,
        drift_flag,
        class_balance,
        notes,
    )

    # Decide exit code:
    #  - if either performance decay OR data drift → exit 1 (trigger retrain)
    if decay_flag or drift_flag:
        logging.warning(
            "Monitoring detected an issue (decay_flag=%s, drift_flag=%s).",
            decay_flag,
            drift_flag,
        )
        logging.warning("Details: %s", notes)

        subject = "[Boston 311] Model decay/drift detected"
        body = (
            f"Project: {PROJECT_ID}\n"
            f"Window: {window_start.isoformat()} → {window_end.isoformat()}\n"
            f"AUC={auc}, RMSE={rmse}, class_balance={class_balance}\n"
            f"Flags: decay={decay_flag}, drift={drift_flag}\n"
            f"Notes: {notes}\n"
        )
        send_email_notification(subject, body)

        return 1

    logging.info("Model is healthy. No retrain needed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
