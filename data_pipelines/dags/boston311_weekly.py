import os, json, logging
from datetime import datetime, timedelta, timezone

from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from google.cloud import bigquery
from google.api_core.exceptions import NotFound

import fetch_data 

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

GCP_PROJECT_ID = Variable.get("BOSTON311_PROJECT", default_var="boston311-mlops")
BQ_DATASET     = Variable.get("BOSTON311_DATASET", default_var="boston311")
BQ_TABLE_TGT   = Variable.get("BOSTON311_TABLE_TGT", default_var="service_requests_2025")
BQ_TABLE_STG   = Variable.get("BOSTON311_TABLE_STG_WEEKLY", default_var="service_requests_2025_staging_weekly")
GCS_BUCKET     = Variable.get("BOSTON311_BUCKET",  default_var="boston311-bucket")
GCS_PREFIX     = Variable.get("BOSTON311_PREFIX",  default_var="boston311/raw/2025")
PAGE_SIZE      = fetch_data.PAGE_SIZE
BQ_LOCATION    = Variable.get("BOSTON311_BQ_LOCATION", default_var="US")

TARGET_COLS = [
    "_id","case_enquiry_id","open_dt","sla_target_dt","closed_dt","on_time","case_status",
    "closure_reason","case_title","subject","reason","type","queue","department",
    "submitted_photo","closed_photo","location","fire_district","pwd_district",
    "city_council_district","police_district","neighborhood","neighborhood_services_district",
    "ward","precinct","location_street_name","location_zipcode","latitude","longitude",
    "geom_4326","source","_ingested_at","_full_text"
]

def get_recent_iso(days):
    d = (datetime.now(timezone.utc) - timedelta(days=days)).date().isoformat()
    return f"{d}T00:00:00Z"

def fetch_weekly_data_to_local(path):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    where_sql = "1=1"

    iso_now = datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00","Z")
    total = 0
    last_id = 0
    min_id = None
    max_id = None
    pages = 0

    logger.info("[full] Fetching entire dataset")
    logger.info("[full] Writing to %s", path)

    with open(path, "w", encoding="utf-8") as f:
        while True:
            records = fetch_data.fetch_window(where_sql, start_after_id=last_id, limit=PAGE_SIZE)
            if not records:
                break
            pages += 1
            for r in records:
                r["_ingested_at"] = iso_now
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
                rid = r.get("_id")
                if isinstance(rid, int):
                    last_id = rid
                    min_id = rid if min_id is None else min(min_id, rid)
                    max_id = rid if max_id is None else max(max_id, rid)
            total += len(records)
            logger.info("[full] Page %d: +%d rows (last_id=%s)", pages, len(records), last_id)
            if len(records) < PAGE_SIZE:
                break

    logger.info("[full] Done. Wrote %d rows, pages=%d, _id range=[%s..%s], _ingested_at=%s",
                total, pages, min_id, max_id, iso_now)
    
    return True

def file_exists(path):
    exists = os.path.exists(path)
    if not exists:
        logger.info("[full] No file at %s", path)
    return exists

def overwrite_sql(staging, target):
    cols  = ", ".join(TARGET_COLS)
    s_cols= ", ".join([f"S.{c}" for c in TARGET_COLS])
    return f"""
    CREATE OR REPLACE TEMP TABLE _dedup AS
    SELECT *
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{staging}`
    WHERE case_enquiry_id IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (PARTITION BY case_enquiry_id ORDER BY _ingested_at DESC) = 1;
    
    TRUNCATE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.{target}`;

    INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET}.{target}` ({cols})
    SELECT {cols}
    FROM _dedup;
    """


default_args = {"owner": "boston311", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="boston311_weekly",
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule="0 4 * * 1",
    catchup=False,
    max_active_runs=1,
    tags=["boston311","bigquery","gcs", "weekly"],
) as dag:

    local_path = "/tmp/boston311_weekly_{{ ds_nodash }}.jsonl"
    gcs_obj    = f"{GCS_PREFIX}/{{{{ ds }}}}/boston311_weekly_{{{{ ds_nodash }}}}.jsonl"
    
    create_target_table = BigQueryInsertJobOperator(
        task_id="create_target_if_missing",
        configuration={"query": {"query":
            f"""
            CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_TGT}`
            (
            _ingested_at TIMESTAMP,
            source STRING,
            geom_4326 STRING,
            longitude FLOAT64,
            precinct STRING,
            neighborhood_services_district STRING,
            _full_text STRING,
            neighborhood STRING,
            pwd_district STRING,
            case_status STRING,
            location STRING,
            queue STRING,
            closure_reason STRING,
            subject STRING,
            fire_district STRING,
            type STRING,
            _id INT64,
            reason STRING,
            sla_target_dt TIMESTAMP,
            on_time STRING,
            department STRING,
            location_zipcode INT64,
            case_title STRING,
            city_council_district STRING,
            closed_dt TIMESTAMP,
            latitude FLOAT64,
            police_district STRING,
            location_street_name STRING,
            open_dt TIMESTAMP,
            submitted_photo STRING,
            closed_photo STRING,
            ward STRING,
            case_enquiry_id INT64
            )
            PARTITION BY DATE(_ingested_at)
            """,
            "useLegacySql": False}},
        location=BQ_LOCATION,
    )

    fetch_full = PythonOperator(
        task_id="fetch_weekly_data",
        python_callable=fetch_weekly_data_to_local,
        op_kwargs={"path": local_path},
    )

    check_nonempty = ShortCircuitOperator(
        task_id="check_nonempty",
        python_callable=file_exists,
        op_kwargs={"path": local_path},
    )

    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src=local_path,
        dst=gcs_obj,
        bucket=GCS_BUCKET,
        mime_type="application/json",
    )

    load_to_bq_staging = GCSToBigQueryOperator(
        task_id="load_to_bq_staging",
        bucket=GCS_BUCKET,
        source_objects=[gcs_obj],
        destination_project_dataset_table=f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_STG}",
        source_format="NEWLINE_DELIMITED_JSON",
        autodetect=True,
        write_disposition="WRITE_TRUNCATE",
        create_disposition="CREATE_IF_NEEDED",
        max_bad_records=50,
        location=BQ_LOCATION,
    )

    overwrite_target = BigQueryInsertJobOperator(
        task_id="overwrite_to_target",
        configuration={"query": {"query": overwrite_sql(BQ_TABLE_STG, BQ_TABLE_TGT), "useLegacySql": False}},
        location=BQ_LOCATION,
    )

    # truncate_stg = BigQueryInsertJobOperator(
    #     task_id="truncate_staging",
    #     configuration={"query": {"query": f"TRUNCATE TABLE `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_STG}`", "useLegacySql": False}},
    #     location=BQ_LOCATION,
    #     trigger_rule=TriggerRule.ALL_DONE,
    # )

    create_target_table >> fetch_full >> check_nonempty >> upload_to_gcs >> load_to_bq_staging >> overwrite_target