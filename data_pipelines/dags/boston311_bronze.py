from __future__ import annotations
import logging
from datetime import timedelta

import pendulum
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from google.api_core.exceptions import NotFound
from google.cloud import bigquery

GCP_PROJECT_ID = Variable.get("BOSTON311_PROJECT", default_var="boston311-mlops")
BQ_DATASET     = Variable.get("BOSTON311_DATASET", default_var="boston311")
BQ_TABLE_TGT   = Variable.get("BOSTON311_TABLE_TGT", default_var="service_requests_2025")
BQ_LOCATION    = Variable.get("BOSTON311_BQ_LOCATION", default_var="US")

BRONZE_DATASET = Variable.get("BOSTON311_BRONZE_DATASET", default_var="boston311_medallion")
BRONZE_TABLE   = Variable.get("BOSTON311_BRONZE_TABLE", default_var="bronze")

GCS_BUCKET     = Variable.get("BOSTON311_BUCKET",  default_var="boston311-bucket")
GCS_PREFIX     = Variable.get("BOSTON311_PREFIX",  default_var="boston311/raw/2025")

RAW_FQN    = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_TGT}"
BRONZE_FQN = f"{GCP_PROJECT_ID}.{BRONZE_DATASET}.{BRONZE_TABLE}"

TMP_URI_TEMPLATE = f"gs://{GCS_BUCKET}/tmp/bronze/{{{{ ds_nodash }}}}/b311_bronze.parquet"

LOCAL_TZ = pendulum.timezone("America/New_York")

def _bq():
    return bigquery.Client(project=GCP_PROJECT_ID)

def _read_df_from_gcs(uri):
    return pd.read_parquet(uri, storage_options={"token": "cloud"})

def _write_df_to_gcs(df, uri):
    df.to_parquet(uri, index=False, storage_options={"token": "cloud"})

def ensure_bronze_exists():
    client = _bq()

    ds_id = f"{GCP_PROJECT_ID}.{BRONZE_DATASET}"
    try:
        client.get_dataset(ds_id)
        logging.info("Dataset exists: %s", ds_id)
    except NotFound:
        ds = bigquery.Dataset(ds_id)
        ds.location = BQ_LOCATION
        client.create_dataset(ds)
        logging.info("Created dataset: %s (location=%s)", ds_id, BQ_LOCATION)

    try:
        client.get_table(BRONZE_FQN)
        logging.info("Bronze exists: %s", BRONZE_FQN)
        return
    except NotFound:
        pass

    sql = f"""
    CREATE TABLE `{BRONZE_FQN}`
    PARTITION BY DATE(open_dt)
    CLUSTER BY type, department, neighborhood AS
    SELECT
      _ingested_at, source, geom_4326, longitude, precinct,
      neighborhood_services_district, _full_text, neighborhood, pwd_district,
      case_status, location, queue, closure_reason, subject, fire_district,
      type, _id, reason, sla_target_dt, on_time, department, location_zipcode,
      case_title, city_council_district, closed_dt, latitude, police_district,
      location_street_name, open_dt, submitted_photo, closed_photo, ward, case_enquiry_id
    FROM `{RAW_FQN}`
    WHERE 1=0
    """
    client.query(sql, location=BQ_LOCATION).result()
    logging.info("Created bronze (empty CTAS; partitioned & clustered).")

def extract_df(tmp_uri):
    client = _bq()
    sql = f"""
    SELECT
      _ingested_at, source, geom_4326, longitude, precinct,
      neighborhood_services_district, _full_text, neighborhood, pwd_district,
      case_status, location, queue, closure_reason, subject, fire_district,
      type, _id, reason, sla_target_dt, on_time, department, location_zipcode,
      case_title, city_council_district, closed_dt, latitude, police_district,
      location_street_name, open_dt, submitted_photo, closed_photo, ward, case_enquiry_id
    FROM `{RAW_FQN}`
    WHERE open_dt IS NOT NULL
    """
    try:
        from google.cloud import bigquery_storage
        bqs = bigquery_storage.BigQueryReadClient()
        df = client.query(sql, location=BQ_LOCATION).to_dataframe(bqstorage_client=bqs)
    except Exception:
        logging.warning("BigQuery Storage API unavailable; using standard to_dataframe().")
        df = client.query(sql, location=BQ_LOCATION).to_dataframe()

    _write_df_to_gcs(df, tmp_uri)
    logging.info("FULL LOAD extracted %d rows to %s", len(df), tmp_uri)

def validate_id(tmp_uri):
    df = _read_df_from_gcs(tmp_uri)
    dup_id = int(df["_id"].duplicated().sum()) if "_id" in df else 0
    logging.info("[ID] duplicate _id (full load) = %s", dup_id)
    if {"case_enquiry_id", "open_dt"}.issubset(df.columns):
        dup_combo = int(df.duplicated(subset=["case_enquiry_id", "open_dt"]).sum())
        logging.info("[ID] duplicate (case_enquiry_id, open_dt) = %s", dup_combo)

def validate_time(tmp_uri):
    df = _read_df_from_gcs(tmp_uri)
    to_ts = lambda s: pd.to_datetime(s, errors="coerce", utc=True) if s is not None else None
    o = to_ts(df.get("open_dt"))
    c = to_ts(df.get("closed_dt"))
    sla = to_ts(df.get("sla_target_dt"))
    ing = to_ts(df.get("_ingested_at"))

    open_in_future = int((o > (pendulum.now("UTC") + pendulum.duration(days=1))).sum()) if o is not None else 0
    closed_before_open = int(((c.notna()) & (o.notna()) & (c < o)).sum()) if c is not None and o is not None else 0
    sla_before_open = int(((sla.notna()) & (o.notna()) & (sla < o)).sum()) if sla is not None and o is not None else 0
    ingest_far_before_open = int(((ing.notna()) & (o.notna()) & (ing < (o - pd.Timedelta(days=7)))).sum()) if ing is not None and o is not None else 0

    logging.info(
        "[TIME] open_in_future=%s closed_before_open=%s sla_before_open=%s ingest_far_before_open=%s",
        open_in_future, closed_before_open, sla_before_open, ingest_far_before_open
    )

def validate_geo(tmp_uri):
    df = _read_df_from_gcs(tmp_uri)
    lat, lon = df.get("latitude"), df.get("longitude")
    missing_lat_or_lon = int((lat.isna() | lon.isna()).sum()) if lat is not None and lon is not None else 0
    out_of_bounds = int(((lat < 41.0) | (lat > 43.5) | (lon < -71.8) | (lon > -70.3)).sum()) if lat is not None and lon is not None else 0
    logging.info("[GEO] missing_lat_or_lon=%s out_of_bounds=%s", missing_lat_or_lon, out_of_bounds)

def dedupe_df(tmp_uri):
    df = _read_df_from_gcs(tmp_uri)
    if "_ingested_at" in df and "_id" in df:
        df["_ingested_at"] = pd.to_datetime(df["_ingested_at"], errors="coerce", utc=True)
        df = df.sort_values(["_id", "_ingested_at"]).drop_duplicates(subset=["_id"], keep="last")
    _write_df_to_gcs(df, tmp_uri)
    logging.info("After dedupe, rows=%d saved to %s", len(df), tmp_uri)

def overwrite_bronze(tmp_uri):
    client = _bq()
    df = _read_df_from_gcs(tmp_uri)
    job_config = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)
    job = client.load_table_from_dataframe(df, BRONZE_FQN, job_config=job_config, location=BQ_LOCATION)
    job.result()
    logging.info("Replaced %s with %d rows (full refresh).", BRONZE_FQN, len(df))

default_args = {
    "owner": "boston311",
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="boston311_bronze",
    start_date=pendulum.datetime(2025, 10, 1, tz=LOCAL_TZ),
    schedule_interval="0 4 * * *",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["boston311", "bronze", "full-refresh", "gcs-handoff", "no-xcom"],
) as dag:

    t1 = PythonOperator(
        task_id="ensure_bronze_exists",
        python_callable=ensure_bronze_exists,
    )

    t2 = PythonOperator(
        task_id="extract_df_full",
        python_callable=extract_df,
        op_kwargs={"tmp_uri": TMP_URI_TEMPLATE},
    )

    t3 = PythonOperator(
        task_id="validate_id",
        python_callable=validate_id,
        op_kwargs={"tmp_uri": TMP_URI_TEMPLATE},
    )

    t4 = PythonOperator(
        task_id="validate_time",
        python_callable=validate_time,
        op_kwargs={"tmp_uri": TMP_URI_TEMPLATE},
    )

    t5 = PythonOperator(
        task_id="validate_geo",
        python_callable=validate_geo,
        op_kwargs={"tmp_uri": TMP_URI_TEMPLATE},
    )

    t6 = PythonOperator(
        task_id="dedupe_df_latest_by_id",
        python_callable=dedupe_df,
        op_kwargs={"tmp_uri": TMP_URI_TEMPLATE},
    )

    t7 = PythonOperator(
        task_id="overwrite_bronze",
        python_callable=overwrite_bronze,
        op_kwargs={"tmp_uri": TMP_URI_TEMPLATE},
    )

    t1 >> t2 >> [t3, t4, t5] >> t6 >> t7
