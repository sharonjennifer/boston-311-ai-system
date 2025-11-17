# app/bq.py

import re, logging

from datetime import datetime
from google.cloud import bigquery
from app.config import BQ_LOCATION, ALLOWLIST, PROJECT_ID, GOOGLE_APPLICATION_CREDENTIALS



logger = logging.getLogger("b311.bq")
client = None



def get_client():
    global client
    if client is None:
        try:
            logger.info("Creating BigQuery client project=%s location=%s (ADC=%s)",
                        PROJECT_ID, BQ_LOCATION, GOOGLE_APPLICATION_CREDENTIALS or "NOT SET")
            client = bigquery.Client(project=PROJECT_ID, location=BQ_LOCATION)
            logger.info("BigQuery client ready")
        except Exception as e:
            hint = f"(GOOGLE_APPLICATION_CREDENTIALS={GOOGLE_APPLICATION_CREDENTIALS or 'NOT SET'})"
            logger.exception("Failed to create BigQuery client")
            raise RuntimeError(f"Failed to create BigQuery client {hint}: {e}") from e
    return client

def check_allowlist(sql: str):
    for part in re.findall(r"`([^`]+)`", sql):
        if "." in part and part not in ALLOWLIST:
            raise ValueError(f"Query touches disallowed object: {part}")
    logger.debug("Allowlist check passed")

def infer_bq_type(v):
    if isinstance(v, bool): return "BOOL"
    if isinstance(v, int): return "INT64"
    if isinstance(v, float): return "FLOAT64"
    if isinstance(v, datetime): return "TIMESTAMP"
    if v is None: return "STRING"
    return "STRING"

def bind_params(job_config: bigquery.QueryJobConfig, params: dict):
    if not params:
        logger.debug("No query params to bind")
        return job_config
    
    bq_params = []
    for k, v in params.items():
        name = k.rstrip("?")
        if k.endswith("?") and v is None:
            continue
        bq_params.append(bigquery.ScalarQueryParameter(name, infer_bq_type(v), v))
    job_config.query_parameters = bq_params
    logger.debug("Bound %d query params", len(bq_params))
    return job_config

def dry_run(sql: str, params: dict):
    check_allowlist(sql)
    client = get_client()
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    job_config = bind_params(job_config, params)
    
    logger.debug("Submitting DRY RUN")
    job = client.query(sql, job_config=job_config)
    bytes_proc = job.total_bytes_processed
    
    logger.info("DRY RUN bytes=%s", bytes_proc)
    return {"total_bytes_processed": bytes_proc}

def run(sql: str, params: dict, page_rows: int = 200):
    check_allowlist(sql)
    client = get_client()
    job_config = bigquery.QueryJobConfig()
    job_config = bind_params(job_config, params)
    
    logger.debug("Executing query (page_rows=%d)", page_rows)
    job = client.query(sql, job_config=job_config)
    rows = [dict(r) for r in job.result(page_size=min(page_rows, 200))]
    
    logger.info("Query done, rows=%d", len(rows))
    return rows
