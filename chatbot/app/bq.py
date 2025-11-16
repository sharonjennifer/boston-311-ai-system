# app/bq.py
import re
from datetime import datetime
from google.cloud import bigquery
from app.config import BQ_LOCATION, ALLOWLIST, PROJECT_ID, GOOGLE_APPLICATION_CREDENTIALS

SELECT_ONLY = re.compile(r"^\s*SELECT\b", re.IGNORECASE)

_client = None
def _get_client():
    global _client
    if _client is None:
        try:
            _client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
        except Exception as e:
            hint = f"(GOOGLE_APPLICATION_CREDENTIALS={GOOGLE_APPLICATION_CREDENTIALS or 'NOT SET'})"
            raise RuntimeError(f"Failed to create BigQuery client {hint}: {e}") from e
    return _client

def _check_allowlist(sql: str):
    if not SELECT_ONLY.search(sql):
        raise ValueError("Only SELECT statements are allowed.")
    for part in re.findall(r"`([^`]+)`", sql):
        if "." in part and part not in ALLOWLIST:
            raise ValueError(f"Query touches disallowed object: {part}")

def _infer_bq_type(v):
    if isinstance(v, bool): return "BOOL"
    if isinstance(v, int): return "INT64"
    if isinstance(v, float): return "FLOAT64"
    if isinstance(v, datetime): return "TIMESTAMP"
    if v is None: return "STRING"
    return "STRING"

def _bind_params(job_config: bigquery.QueryJobConfig, params: dict):
    if not params:
        return job_config
    bq_params = []
    for k, v in params.items():
        name = k.rstrip("?")
        if k.endswith("?") and v is None:
            continue
        bq_params.append(bigquery.ScalarQueryParameter(name, _infer_bq_type(v), v))
    job_config.query_parameters = bq_params
    return job_config

def dry_run(sql: str, params: dict):
    _check_allowlist(sql)
    client = _get_client()
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job_config = _bind_params(job_config, params)
    job = client.query(sql, job_config=job_config)
    return {"total_bytes_processed": job.total_bytes_processed}

def run(sql: str, params: dict, page_rows: int = 200):
    _check_allowlist(sql)
    client = _get_client()
    job_config = bigquery.QueryJobConfig()
    job_config = _bind_params(job_config, params)
    job = client.query(sql, job_config=job_config)
    rows = [dict(r) for r in job.result(page_size=min(page_rows, 200))]
    return rows
