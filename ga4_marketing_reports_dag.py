from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# ---------------------------------------------------------------------------
# CONSTANTS – adjust these to your environment
# ---------------------------------------------------------------------------
GCP_PROJECT_ID = "marketing_data"
GA4_SOURCE_TABLE = "marketing_data.analytics_448974598.events_*"
REPORTS_DATASET = "marketing_reports"

# If your reporting tables are named differently, update here:
CORE_WEB_TABLE = "core_web_performance"
USER_JOURNEY_TABLE = "user_journey_cohorts"
MARKETING_ACQ_TABLE = "marketing_acquisition"
# Example 4th table – adjust name to whatever we used previously
FUNNEL_TABLE = "funnel_performance"

# Path to the folder containing your .sql files
SQL_QUERIES_PATH = Path(__file__).parent / "sql_queries"

default_args = {
    "owner": "marketing_analytics",
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}

# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------
with DAG(
    dag_id="ga4_marketing_reports_rebuild_slice_hourly",
    default_args=default_args,
    schedule_interval="0 * * * *",  # every hour at :00
    catchup=False,  # start from “now” going forward
    max_active_runs=1,
    tags=["ga4", "marketing", "reports", "partition-rebuild"],
    # Make Airflow/Jinja look in sql_queries for template files
    template_searchpath=[str(SQL_QUERIES_PATH)],
) as dag:

    start = EmptyOperator(task_id="start")

    # -----------------------------------------------------------------------
    # 1) Core Web & Performance – rebuild the daily slice
    #    Uses sql_queries/core_web_performance.sql
    # -----------------------------------------------------------------------
    rebuild_core_web = BigQueryInsertJobOperator(
        task_id="rebuild_core_web_performance",
        gcp_conn_id="google_cloud_default",
        location="US",  # change if you’re in EU, etc.
        configuration={
            "query": {
                # The SQL file should contain the full CREATE OR REPLACE TABLE
                # statement and can reference {{ ds_nodash }} etc. if needed.
                "query": "{% include 'core_web_performance.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # -----------------------------------------------------------------------
    # 2) User Journey & Cohorts – rebuild the daily slice
    #    Uses sql_queries/user_journey_analysis.sql
    # -----------------------------------------------------------------------
    rebuild_user_journey = BigQueryInsertJobOperator(
        task_id="rebuild_user_journey_cohorts",
        gcp_conn_id="google_cloud_default",
        location="US",
        configuration={
            "query": {
                "query": "{% include 'user_journey_analysis.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # -----------------------------------------------------------------------
    # 3) Marketing & Acquisition – rebuild the daily slice
    #    Uses sql_queries/session_acquisition.sql
    # -----------------------------------------------------------------------
    rebuild_marketing_acq = BigQueryInsertJobOperator(
        task_id="rebuild_marketing_acquisition",
        gcp_conn_id="google_cloud_default",
        location="US",
        configuration={
            "query": {
                "query": "{% include 'session_acquisition.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # -----------------------------------------------------------------------
    # 4) Funnel / Page-level performance – rebuild the daily slice
    #    Uses sql_queries/page_level_performance.sql
    # -----------------------------------------------------------------------
    rebuild_funnel = BigQueryInsertJobOperator(
        task_id="rebuild_funnel_performance",
        gcp_conn_id="google_cloud_default",
        location="US",
        configuration={
            "query": {
                "query": "{% include 'page_level_performance.sql' %}",
                "useLegacySql": False,
            }
        },
    )

    # Run all report rebuilds in parallel after start
    start >> [rebuild_core_web, rebuild_user_journey, rebuild_marketing_acq, rebuild_funnel]
