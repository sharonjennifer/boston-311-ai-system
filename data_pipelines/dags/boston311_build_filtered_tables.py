from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

PROJECT_ID = "boston311-mlops"
DATASET = "boston311"
LOCATION = "US"  
MAIN_TABLE      = f"`{PROJECT_ID}.{DATASET}.service_requests_2025`"
CHATBOT_TABLE   = f"`{PROJECT_ID}.{DATASET}.chatbot`"
DASHBOARD_TABLE = f"`{PROJECT_ID}.{DATASET}.dashboard`"

default_args = {
    "owner": "boston311",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

CHATBOT_SQL = f"""
CREATE OR REPLACE TABLE {CHATBOT_TABLE} AS
SELECT
  longitude,
  neighborhood,
  case_status,
  closure_reason,
  subject,
  type,
  reason,
  sla_target_dt,
  on_time,  -- string in your schema
  department,
  case_title,
  closed_dt,
  latitude,
  open_dt,
  CAST(case_enquiry_id AS STRING) AS case_enquiry_id,
  geom_4326
FROM {MAIN_TABLE}
WHERE case_enquiry_id IS NOT NULL;
"""

DASHBOARD_SQL = f"""
CREATE OR REPLACE TABLE {DASHBOARD_TABLE} AS
SELECT
  DATE(open_dt) AS open_date,
  neighborhood,
  department,
  type,
  reason,
  case_status,
  COUNT(*) AS case_count,
  -- on_time is STRING; cast safely to BOOL for aggregation
  SUM(CASE WHEN SAFE_CAST(on_time AS BOOL) THEN 1 ELSE 0 END) AS on_time_count
FROM {MAIN_TABLE}
WHERE case_enquiry_id IS NOT NULL
GROUP BY 1,2,3,4,5,6;
"""

with DAG(
    dag_id="boston311_build_filtered_tables",
    description="Rebuilds chatbot and dashboard tables daily from service_requests_2025",
    start_date=datetime(2025, 10, 27),
    schedule="0 1 * * *",   
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["boston311", "bigquery", "filtered"],
) as dag:

    start = EmptyOperator(task_id="start")

    build_chatbot = BigQueryInsertJobOperator(
        task_id="build_chatbot",
        configuration={"query": {"query": CHATBOT_SQL, "useLegacySql": False}},
        location=LOCATION,
    )

    build_dashboard = BigQueryInsertJobOperator(
        task_id="build_dashboard",
        configuration={"query": {"query": DASHBOARD_SQL, "useLegacySql": False}},
        location=LOCATION,
    )

    end = EmptyOperator(task_id="end")

    start >> build_chatbot >> build_dashboard >> end