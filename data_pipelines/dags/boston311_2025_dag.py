import os, json, time
import sys
from pathlib import Path
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

DAG_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = DAG_DIR.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))
    
import fetch_data 

GCP_PROJECT_ID = "boston311-mlops"
BQ_DATASET     = "boston311"
BQ_TABLE_TGT   = "service_requests_2025"
BQ_TABLE_STG   = "service_requests_2025_staging"
GCS_BUCKET     = "boston311-bucket"
GCS_PREFIX     = "boston311/raw/2025"
WM_VAR_KEY     = "BOSTON311_LAST_ID_2025"
PAGE_SIZE      = getattr(fetch_data, "PAGE_SIZE", 5000)

MERGE_COLS = [
    "_id","case_enquiry_id","open_dt","sla_target_dt","closed_dt","on_time","case_status",
    "closure_reason","case_title","subject","reason","type","queue","department",
    "submitted_photo","closed_photo","location","fire_district","pwd_district",
    "city_council_district","police_district","neighborhood","neighborhood_services_district",
    "ward","precinct","location_street_name","location_zipcode","latitude","longitude",
    "geom_4326","source","_ingested_at"
]



def read_watermark():
    return int(Variable.get(WM_VAR_KEY, default_var="0"))

def fetch_delta_to_local(path):
    last_id = read_watermark()
    iso_now = datetime.utcnow().isoformat(timespec="seconds") + "Z"
    total, max_id_seen = 0, last_id

    os.makedirs(os.path.dirname(path), exist_ok=True)

    with open(path, "w", encoding="utf-8") as f:
        while True:
            records = fetch_data.fetch_page(max_id_seen, limit=PAGE_SIZE)
            if not records:
                break
            for r in records:
                r["_ingested_at"] = iso_now
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
                rid = r.get("_id", max_id_seen)
                if isinstance(rid, int) and rid > max_id_seen:
                    max_id_seen = rid
            total += len(records)
            if len(records) < PAGE_SIZE:
                break

    if total == 0:
        try: os.remove(path)
        except FileNotFoundError: pass
        return False
    
    Variable.set(WM_VAR_KEY, str(max_id_seen))
    return True

def file_exists(path):
    return os.path.exists(path)
        
default_args = {"owner": "boston311", "retries": 1, "retry_delay": timedelta(minutes=5)}

with DAG(
    dag_id="boston311_2025_to_bigquery_daily_local",
    default_args=default_args,
    start_date=datetime(2025, 10, 1),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["boston311","local","bigquery","gcs"],
) as dag:

    local_path = "/tmp/boston311_2025_delta_{{ ds_nodash }}.jsonl"
    gcs_obj    = f"{GCS_PREFIX}/{{{{ ds }}}}/boston311_2025_delta_{{{{ ds_nodash }}}}.jsonl"

    fetch_delta = PythonOperator(
        task_id="fetch_delta",
        python_callable=fetch_delta_to_local,
        op_kwargs={"path": local_path},   # templated by Airflow
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
        write_disposition="WRITE_APPEND",
        create_disposition="CREATE_IF_NEEDED",
        max_bad_records=50,
    )

    merge_sql = f"""
    MERGE `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_TGT}` T
    USING `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_STG}` S
    ON T._id = S._id
    WHEN NOT MATCHED THEN
      INSERT ({", ".join(MERGE_COLS)})
      VALUES ({", ".join([f"S.{c}" for c in MERGE_COLS])});
    """
    merge_to_target = BigQueryInsertJobOperator(
        task_id="merge_to_target",
        configuration={"query": {"query": merge_sql, "useLegacySql": False}},
        location="US",
    )

    fetch_delta >> check_nonempty >> upload_to_gcs >> load_to_bq_staging >> merge_to_target
