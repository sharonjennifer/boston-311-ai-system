import logging
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import ShortCircuitOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

from google.cloud import bigquery

from email_alerts import on_dag_success, on_dag_failure, _send_email

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

GCP_PROJECT_ID = Variable.get("BOSTON311_PROJECT", default_var="boston311-mlops")
BQ_DATASET     = Variable.get("BOSTON311_DATASET", default_var="boston311")
BQ_TABLE_TGT   = Variable.get("BOSTON311_TABLE_TGT", default_var="service_requests_2025")
BQ_LOCATION    = Variable.get("BOSTON311_BQ_LOCATION", default_var="US")

SLICE_METRICS_TABLE       = "bos311_slice_metrics_weekly"
BIAS_ALERTS_TABLE         = "bos311_bias_alerts_weekly"
BIAS_ACTIONS_TABLE        = "bos311_bias_actions_taken"

LOOKBACK_DAYS    = 30     
FAIRNESS_GAP_PCT = 0.10   

default_args = {
    "owner": "boston311-fairness",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def create_fairness_tables_sql():
    """
    Create the monitoring / audit tables if they don't exist.
    1. bos311_slice_metrics_weekly: snapshot of service quality per slice (neighborhood/department/source)
    2. bos311_bias_alerts_weekly: slices that are significantly below median
    3. bos311_bias_actions_taken: mitigation log / accountability surface
    """
    return f"""
    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET}.{SLICE_METRICS_TABLE}`
    (
      snapshot_date DATE,
      slice_type STRING,       -- 'neighborhood', 'department', 'source'
      slice_group STRING,      -- e.g. 'Charlestown'
      total_cases INT64,
      ontime_rate FLOAT64,
      closed_rate FLOAT64
    )
    PARTITION BY snapshot_date
    ;

    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET}.{BIAS_ALERTS_TABLE}`
    (
      snapshot_date DATE,
      slice_type STRING,
      slice_group STRING,
      total_cases INT64,
      ontime_rate FLOAT64,
      closed_rate FLOAT64,
      median_ontime_rate FLOAT64,
      gap_below_median FLOAT64,
      status STRING            -- 'needs_attention' or 'ok'
    )
    PARTITION BY snapshot_date
    ;

    CREATE TABLE IF NOT EXISTS `{GCP_PROJECT_ID}.{BQ_DATASET}.{BIAS_ACTIONS_TABLE}`
    (
      action_timestamp TIMESTAMP,   -- when this row was created or updated
      snapshot_date DATE,           -- which fairness snapshot this refers to
      slice_type STRING,            -- 'neighborhood', 'department', 'source'
      slice_group STRING,           -- e.g. 'Charlestown'
      issue_summary STRING,         -- description of disparity
      action_taken STRING,          -- what ops decided to do to mitigate
      expected_tradeoff STRING,     -- any trade-off accepted (e.g. slowing other areas)
      owner STRING                  -- accountable person/team
    )
    PARTITION BY DATE(action_timestamp)
    ;
    """


def build_slice_metrics_union_query():
    """
    Generate a single BigQuery statement that:
    - builds slice stats for neighborhood, department, source
    - unions them
    - inserts into bos311_slice_metrics_weekly
    """

    return f"""
    INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET}.{SLICE_METRICS_TABLE}`
    (snapshot_date,
     slice_type,
     slice_group,
     total_cases,
     ontime_rate,
     closed_rate)

    WITH
    recent_neighborhood AS (
      SELECT
        neighborhood AS slice_value,
        on_time,
        case_status
      FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_TGT}`
      WHERE TIMESTAMP(_ingested_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_DAYS} DAY)
        AND neighborhood IS NOT NULL
    ),
    agg_neighborhood AS (
      SELECT
        slice_value,
        COUNT(*) AS total_cases,
        SUM(CASE WHEN UPPER(TRIM(on_time)) = 'ONTIME' THEN 1 ELSE 0 END) AS ontime_cases,
        SUM(CASE WHEN INITCAP(TRIM(case_status)) = 'Closed' THEN 1 ELSE 0 END) AS closed_cases
      FROM recent_neighborhood
      GROUP BY slice_value
    ),

    recent_department AS (
      SELECT
        department AS slice_value,
        on_time,
        case_status
      FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_TGT}`
      WHERE TIMESTAMP(_ingested_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_DAYS} DAY)
        AND department IS NOT NULL
    ),
    agg_department AS (
      SELECT
        slice_value,
        COUNT(*) AS total_cases,
        SUM(CASE WHEN UPPER(TRIM(on_time)) = 'ONTIME' THEN 1 ELSE 0 END) AS ontime_cases,
        SUM(CASE WHEN INITCAP(TRIM(case_status)) = 'Closed' THEN 1 ELSE 0 END) AS closed_cases
      FROM recent_department
      GROUP BY slice_value
    ),

    recent_source AS (
      SELECT
        source AS slice_value,
        on_time,
        case_status
      FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_TGT}`
      WHERE TIMESTAMP(_ingested_at) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL {LOOKBACK_DAYS} DAY)
        AND source IS NOT NULL
    ),
    agg_source AS (
      SELECT
        slice_value,
        COUNT(*) AS total_cases,
        SUM(CASE WHEN UPPER(TRIM(on_time)) = 'ONTIME' THEN 1 ELSE 0 END) AS ontime_cases,
        SUM(CASE WHEN INITCAP(TRIM(case_status)) = 'Closed' THEN 1 ELSE 0 END) AS closed_cases
      FROM recent_source
      GROUP BY slice_value
    )

    SELECT
      CURRENT_DATE() AS snapshot_date,
      'neighborhood' AS slice_type,
      slice_value     AS slice_group,
      total_cases,
      SAFE_DIVIDE(ontime_cases, total_cases) AS ontime_rate,
      SAFE_DIVIDE(closed_cases, total_cases) AS closed_rate
    FROM agg_neighborhood

    UNION ALL

    SELECT
      CURRENT_DATE() AS snapshot_date,
      'department' AS slice_type,
      slice_value   AS slice_group,
      total_cases,
      SAFE_DIVIDE(ontime_cases, total_cases) AS ontime_rate,
      SAFE_DIVIDE(closed_cases, total_cases) AS closed_rate
    FROM agg_department

    UNION ALL

    SELECT
      CURRENT_DATE() AS snapshot_date,
      'source' AS slice_type,
      slice_value AS slice_group,
      total_cases,
      SAFE_DIVIDE(ontime_cases, total_cases) AS ontime_rate,
      SAFE_DIVIDE(closed_cases, total_cases) AS closed_rate
    FROM agg_source
    ;
    """

def build_bias_alerts_query():
    """
    For today's snapshot in bos311_slice_metrics_weekly:
    - compute median ontime_rate per slice_type
    - flag any slice_group whose ontime_rate is more than FAIRNESS_GAP_PCT below median
    - insert those rows into bos311_bias_alerts_weekly
    """

    return f"""
    INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET}.{BIAS_ALERTS_TABLE}`
    (snapshot_date,
     slice_type,
     slice_group,
     total_cases,
     ontime_rate,
     closed_rate,
     median_ontime_rate,
     gap_below_median,
     status)
    WITH
    today_metrics AS (
      SELECT
        snapshot_date,
        slice_type,
        slice_group,
        total_cases,
        ontime_rate,
        closed_rate
      FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{SLICE_METRICS_TABLE}`
      WHERE snapshot_date = CURRENT_DATE()
    ),
    medians AS (
      SELECT
        slice_type,
        APPROX_QUANTILES(ontime_rate, 100)[OFFSET(50)] AS median_ontime_rate
      FROM today_metrics
      GROUP BY slice_type
    ),
    flagged AS (
      SELECT
        m.snapshot_date,
        m.slice_type,
        m.slice_group,
        m.total_cases,
        m.ontime_rate,
        m.closed_rate,
        med.median_ontime_rate,
        (med.median_ontime_rate - m.ontime_rate) AS gap_below_median,
        CASE
          WHEN m.ontime_rate < (med.median_ontime_rate - {FAIRNESS_GAP_PCT})
          THEN 'needs_attention'
          ELSE 'ok'
        END AS status
      FROM today_metrics m
      JOIN medians med
      ON m.slice_type = med.slice_type
      WHERE m.ontime_rate < (med.median_ontime_rate - {FAIRNESS_GAP_PCT})
    )
    SELECT
      snapshot_date,
      slice_type,
      slice_group,
      total_cases,
      ontime_rate,
      closed_rate,
      median_ontime_rate,
      gap_below_median,
      status
    FROM flagged
    ;
    """

def build_action_seed_query():
    """
    Create/seed mitigation records in bos311_bias_actions_taken
    for every underserved slice (status='needs_attention') from today's run.

    We only insert if there isn't already an action row for that same
    snapshot_date + slice_type + slice_group. This gives you a to-do
    item to fill in 'action_taken', 'expected_tradeoff', and 'owner'.
    """
    return f"""
    INSERT INTO `{GCP_PROJECT_ID}.{BQ_DATASET}.{BIAS_ACTIONS_TABLE}`
    (action_timestamp,
     snapshot_date,
     slice_type,
     slice_group,
     issue_summary,
     action_taken,
     expected_tradeoff,
     owner)
    SELECT
      CURRENT_TIMESTAMP() AS action_timestamp,
      snapshot_date,
      slice_type,
      slice_group,
      CONCAT(
        'ontime_rate=', ROUND(ontime_rate*100,1), '%; ',
        'median=', ROUND(median_ontime_rate*100,1), '%; ',
        'gap=', ROUND(gap_below_median*100,1), 'pts below median'
      ) AS issue_summary,
      '' AS action_taken,
      '' AS expected_tradeoff,
      '' AS owner
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BIAS_ALERTS_TABLE}` alerts
    WHERE snapshot_date = CURRENT_DATE()
      AND status = 'needs_attention'
      AND NOT EXISTS (
        SELECT 1
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BIAS_ACTIONS_TABLE}` actions
        WHERE actions.snapshot_date = alerts.snapshot_date
          AND actions.slice_type   = alerts.slice_type
          AND actions.slice_group  = alerts.slice_group
      )
    ;
    """

def _bias_alert_check(**context):
    """
    Return True if any slice was flagged 'needs_attention' today.
    We will only send an email if this is True.
    """
    client = bigquery.Client(project=GCP_PROJECT_ID, location=BQ_LOCATION)
    sql = f"""
    SELECT COUNT(*) AS cnt
    FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BIAS_ALERTS_TABLE}`
    WHERE snapshot_date = CURRENT_DATE()
      AND status = 'needs_attention'
    """
    rows = list(client.query(sql).result())
    cnt = rows[0].cnt if rows else 0
    logger.info("Fairness alert check: %s groups need attention", cnt)
    return cnt > 0


def _send_bias_email(**context):
    """
    Send a fairness alert email using the same SMTP and recipient config
    your other DAGs already use (email_alerts._send_email).
    """
    subject = "311 Fairness Alert (Weekly Bias Monitor)"

    html = f"""
    <h3>Potential Equity Issue Detected</h3>
    <p>At least one slice group (neighborhood / department / source) has an on-time
    completion rate more than {int(FAIRNESS_GAP_PCT*100)} percentage points below
    the median for that slice type over the last {LOOKBACK_DAYS} days.</p>

    <p>Next steps:</p>
    <ol>
      <li>Review underserved slices in
        <code>{GCP_PROJECT_ID}.{BQ_DATASET}.{BIAS_ALERTS_TABLE}</code>
        (status='needs_attention').</li>
      <li>Open <code>{GCP_PROJECT_ID}.{BQ_DATASET}.{BIAS_ACTIONS_TABLE}</code>
        and fill in action_taken, expected_tradeoff, and owner for today's rows.</li>
    </ol>

    <p>This closes the loop on bias mitigation and documentation.</p>
    """

    _send_email(subject, html)

with DAG(
    dag_id="boston311_fairness_weekly",
    default_args=default_args,
    start_date=datetime(2025, 10, 1),

    schedule="0 5 * * 1",
    catchup=False,
    max_active_runs=1,
    tags=["boston311", "fairness", "bias", "equity", "monitoring"],

    on_success_callback=on_dag_success,
    on_failure_callback=on_dag_failure,
) as dag:

    create_fairness_tables = BigQueryInsertJobOperator(
        task_id="create_fairness_tables_if_missing",
        configuration={
            "query": {
                "query": create_fairness_tables_sql(),
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    build_slice_metrics_task = BigQueryInsertJobOperator(
        task_id="build_slice_metrics_task",
        configuration={
            "query": {
                "query": build_slice_metrics_union_query(),
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
    )

    build_bias_alerts_task = BigQueryInsertJobOperator(
        task_id="build_bias_alerts_task",
        configuration={
            "query": {
                "query": build_bias_alerts_query(),
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    seed_mitigation_actions_task = BigQueryInsertJobOperator(
        task_id="seed_mitigation_actions_task",
        configuration={
            "query": {
                "query": build_action_seed_query(),
                "useLegacySql": False,
            }
        },
        location=BQ_LOCATION,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    bias_alert_condition = ShortCircuitOperator(
        task_id="bias_alert_condition",
        python_callable=_bias_alert_check,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    send_bias_email = ShortCircuitOperator(
        task_id="send_bias_email",
        python_callable=_send_bias_email,
        provide_context=True,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    create_fairness_tables >> build_slice_metrics_task >> build_bias_alerts_task >> seed_mitigation_actions_task
    seed_mitigation_actions_task >> bias_alert_condition >> send_bias_email