from __future__ import annotations

from datetime import datetime, timedelta
import json
import os
import tempfile

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.gcs import GCSDeleteObjectsOperator

# ---- CONFIG (Airflow Variables or defaults) ----
PROJECT_ID       = Variable.get("BOSTON311_PROJECT",            default_var="boston311-mlops")
LOCATION         = Variable.get("BOSTON311_BQ_LOCATION",        default_var="US")
RAW_DATASET      = Variable.get("BOSTON311_DATASET",            default_var="boston311")
RAW_TABLE        = Variable.get("BOSTON311_TABLE_TGT",          default_var="service_requests_2025")
SERVING_DATASET  = Variable.get("BOSTON311_DATASET_SERVING",    default_var="boston311_service")
GCP_CONN_ID      = Variable.get("BOSTON311_GCP_CONN_ID",             default_var="google_cloud_default")

# Source GeoJSON (FeatureCollection) and temp NDJSON destination
GCS_NEIGHBORHOODS_GEOJSON = Variable.get("BOSTON311_NEIGHBORHOODS_GEOJSON_URI", default_var="gs://boston311-bucket/boston_neighborhoods.geojson")
GCS_TMP_NDJSON_URI = Variable.get("BOSTON311_NEIGHBORHOODS_NDJSON_URI", default_var="gs://boston311-bucket/tmp/boston_neighborhoods.ndjson")

def _split_gs(uri: str) -> tuple[str, str]:
    assert uri.startswith("gs://"), f"Unexpected GCS URI: {uri}"
    path = uri[len("gs://"):]
    bucket, _, blob = path.partition("/")
    return bucket, blob

NDJSON_BUCKET, NDJSON_OBJECT = _split_gs(GCS_TMP_NDJSON_URI)

RAW_FQN = f"`{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}`"

# ---- TABLES (CTAS) ----

SQL_TBL_COUNTS_BY_NEIGHBORHOOD_WEEK = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{SERVING_DATASET}.tbl_counts_by_neighborhood_week`
PARTITION BY week_start
CLUSTER BY neighborhood, type AS
SELECT
  DATE_TRUNC(DATE(DATETIME(open_dt, "America/New_York")), WEEK(MONDAY)) AS week_start,
  neighborhood,
  UPPER(TRIM(type))  AS type,
  COUNT(*)           AS n
FROM {RAW_FQN}
GROUP BY week_start, neighborhood, type;
"""

SQL_TBL_COUNTS_BY_DEPARTMENT_WEEK = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{SERVING_DATASET}.tbl_counts_by_department_week`
PARTITION BY week_start
CLUSTER BY department AS
SELECT
  DATE_TRUNC(DATE(DATETIME(open_dt, "America/New_York")), WEEK(MONDAY)) AS week_start,
  UPPER(TRIM(department)) AS department,
  COUNT(*) AS n
FROM {RAW_FQN}
GROUP BY week_start, department;
"""

SQL_TBL_CASE_DURATIONS = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{SERVING_DATASET}.tbl_case_durations`
PARTITION BY DATE(open_dt)
CLUSTER BY department, reason, type AS
SELECT
  CAST(_id AS STRING)                      AS case_enquiry_id,
  UPPER(TRIM(department))                  AS department,
  UPPER(TRIM(reason))                      AS reason,
  UPPER(TRIM(type))                        AS type,
  DATE(DATETIME(open_dt, "America/New_York"))   AS open_date_local,
  SAFE_CAST(open_dt   AS TIMESTAMP)        AS open_dt,
  SAFE_CAST(closed_dt AS TIMESTAMP)        AS closed_dt,
  CASE
    WHEN closed_dt IS NOT NULL AND open_dt IS NOT NULL
      THEN TIMESTAMP_DIFF(SAFE_CAST(closed_dt AS TIMESTAMP), SAFE_CAST(open_dt AS TIMESTAMP), DAY)
    ELSE NULL
  END AS days_to_close
FROM {RAW_FQN};
"""

# ---- SEARCH INDEX (on RAW) ----
SQL_CREATE_SEARCH_INDEX = f"""
CREATE SEARCH INDEX IF NOT EXISTS `{PROJECT_ID}.{RAW_DATASET}.idx_b311_text`
ON `{PROJECT_ID}.{RAW_DATASET}.{RAW_TABLE}`(subject, case_title, location);
"""

# ---- PYTHON: Convert FeatureCollection → NDJSON (one feature per line) ----
def geojson_to_ndjson(**context):
    from google.cloud import storage

    src_bucket_name, src_blob_name = _split_gs(GCS_NEIGHBORHOODS_GEOJSON)
    dst_bucket_name, dst_blob_name = _split_gs(GCS_TMP_NDJSON_URI)

    client = storage.Client()
    src_bucket = client.bucket(src_bucket_name)
    src_blob = src_bucket.blob(src_blob_name)

    content = src_blob.download_as_bytes()
    doc = json.loads(content)

    # Normalize acceptable inputs: FeatureCollection or a single Feature
    if isinstance(doc, dict) and doc.get("type") == "FeatureCollection":
        features = doc.get("features", [])
    elif isinstance(doc, dict) and doc.get("type") == "Feature":
        features = [doc]
    else:
        raise ValueError("Input must be a GeoJSON FeatureCollection or Feature")

    # Write temp NDJSON: {"name": "...", "geometry": {...}} per line
    with tempfile.NamedTemporaryFile("w", delete=False) as f:
        tmp_path = f.name
        for ftr in features:
            props = ftr.get("properties") or {}
            name = (
                props.get("name")
                or props.get("Neighborhood")
                or props.get("NEIGHBORHOOD")
                or props.get("Name")
                or ""
            )
            geom = ftr.get("geometry")
            if not geom:
                continue
            rec = {"name": name, "geometry": geom}
            f.write(json.dumps(rec, separators=(",", ":")) + "\n")

    # Upload NDJSON
    dst_bucket = client.bucket(dst_bucket_name)
    dst_blob = dst_bucket.blob(dst_blob_name)
    dst_blob.upload_from_filename(tmp_path, content_type="application/x-ndjson")
    os.remove(tmp_path)
    print(f"NDJSON written to {GCS_TMP_NDJSON_URI}")

# ---- LOAD NDJSON -> STAGING TABLE (explicit schema) ----
# Staging table with JSON column for geometry; we’ll convert to GEOGRAPHY in CTAS.
STAGING_TABLE_FQN = f"{PROJECT_ID}.{SERVING_DATASET}._stg_neighborhoods_json"

# ---- FINAL ref_neighborhoods CTAS ----
SQL_CREATE_REF_NEIGHBORHOODS_FROM_STAGING = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{SERVING_DATASET}.ref_neighborhoods` AS
SELECT
  UPPER(TRIM(COALESCE(name, ''))) AS neighborhood_name,
  ST_GEOGFROMGEOJSON(TO_JSON_STRING(geometry), make_valid => TRUE) AS geom
FROM `{STAGING_TABLE_FQN}`
WHERE COALESCE(name, '') <> '';
"""

default_args = {
    "owner": "b311-mlops",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="boston311_chat_service",
    start_date=datetime(2025, 11, 1),
    schedule="0 4 * * *",   # daily at 04:00
    catchup=False,
    default_args=default_args,
    tags=["boston311", "serving", "bigquery", "tables", "search", "geo"],
) as dag:

    # 0) Ensure serving dataset exists
    create_serving_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_serving_dataset",
        dataset_id=SERVING_DATASET,
        project_id=PROJECT_ID,
        location=LOCATION,
        gcp_conn_id=GCP_CONN_ID,
    )

    # 1) Core serving tables (CTAS)
    tbl_counts_by_neighborhood_week = BigQueryInsertJobOperator(
        task_id="tbl_counts_by_neighborhood_week",
        configuration={"query": {"query": SQL_TBL_COUNTS_BY_NEIGHBORHOOD_WEEK, "useLegacySql": False}},
        gcp_conn_id=GCP_CONN_ID,
        location=LOCATION,
        deferrable=False,
    )

    tbl_counts_by_department_week = BigQueryInsertJobOperator(
        task_id="tbl_counts_by_department_week",
        configuration={"query": {"query": SQL_TBL_COUNTS_BY_DEPARTMENT_WEEK, "useLegacySql": False}},
        gcp_conn_id=GCP_CONN_ID,
        location=LOCATION,
        deferrable=False,
    )

    tbl_case_durations = BigQueryInsertJobOperator(
        task_id="tbl_case_durations",
        configuration={"query": {"query": SQL_TBL_CASE_DURATIONS, "useLegacySql": False}},
        gcp_conn_id=GCP_CONN_ID,
        location=LOCATION,
        deferrable=False,
    )

    # 2) Search index on RAW (asynchronous build by BigQuery)
    create_search_index = BigQueryInsertJobOperator(
        task_id="create_search_index",
        configuration={"query": {"query": SQL_CREATE_SEARCH_INDEX, "useLegacySql": False}},
        gcp_conn_id=GCP_CONN_ID,
        location=LOCATION,
        deferrable=False,
    )

    # 3) Neighborhood polygons pipeline: GeoJSON → NDJSON → staging → ref_neighborhoods
    convert_geojson_to_ndjson = PythonOperator(
        task_id="convert_geojson_to_ndjson",
        python_callable=geojson_to_ndjson,
    )

    load_neighborhoods_staging = GCSToBigQueryOperator(
        task_id="load_neighborhoods_staging",
        bucket=NDJSON_BUCKET,
        source_objects=[NDJSON_OBJECT],
        destination_project_dataset_table=STAGING_TABLE_FQN,
        source_format="NEWLINE_DELIMITED_JSON",
        schema_fields=[
            {"name": "name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "geometry", "type": "JSON", "mode": "NULLABLE"},
        ],
        write_disposition="WRITE_TRUNCATE",
        autodetect=False,
        gcp_conn_id=GCP_CONN_ID,
    )

    build_ref_neighborhoods = BigQueryInsertJobOperator(
        task_id="build_ref_neighborhoods",
        configuration={"query": {"query": SQL_CREATE_REF_NEIGHBORHOODS_FROM_STAGING, "useLegacySql": False}},
        gcp_conn_id=GCP_CONN_ID,
        location=LOCATION,
        deferrable=False,
    )

    cleanup_tmp_ndjson = GCSDeleteObjectsOperator(
        task_id="cleanup_tmp_ndjson",
        bucket_name=NDJSON_BUCKET,
        objects=[NDJSON_OBJECT],
        gcp_conn_id=GCP_CONN_ID,
        trigger_rule="all_done",
    )

    create_serving_dataset >> tbl_counts_by_neighborhood_week >> tbl_counts_by_department_week >> tbl_case_durations >> create_search_index >> convert_geojson_to_ndjson >> load_neighborhoods_staging >> build_ref_neighborhoods >> cleanup_tmp_ndjson

