import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.cloud import bigquery

sys.path.append(os.path.join(os.path.dirname(__file__), ".."))
from config import GCP_PROJECT_ID, STAGING_DATASET

# Pulled from config.py / .env rather than hardcoded, so there's one place to set
# the project ID instead of a string to remember to replace in every DAG and SQL file.
PROJECT_ID = GCP_PROJECT_ID
GCP_CONN_ID = "google_cloud_default"
SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")

# Data quality gate: fail the DAG if the staging table has fewer rows than this.
# A near-empty staging table almost always means the API returned partial data or
# the raw load silently truncated something, better to fail loudly here than let
# a thin gold layer reach a dashboard unnoticed.
MIN_STAGING_ROW_COUNT = int(os.getenv("MIN_STAGING_ROW_COUNT", "10"))

# Comma-separated list in .env, e.g. ALERT_EMAIL_TO=you@example.com,teammate@example.com
ALERT_EMAIL_TO = [
    addr.strip() for addr in os.getenv("ALERT_EMAIL_TO", "").split(",") if addr.strip()
]


def read_sql(filename):
    with open(os.path.join(SQL_DIR, filename), "r", encoding="utf-8") as f:
        return f.read().replace("your-gcp-project-id", PROJECT_ID)


def check_staging_row_count():
    """
    Queries stg_matches directly (not via an Airflow connection/hook, a plain
    bigquery.Client is enough for a single read) and raises if the row count is
    below MIN_STAGING_ROW_COUNT. Raising here marks the task failed, which stops
    the downstream gold tasks from running on thin or broken data.
    """
    client = bigquery.Client(project=PROJECT_ID)
    query = f"SELECT COUNT(*) AS row_count FROM `{PROJECT_ID}.{STAGING_DATASET}.stg_matches`"
    result = list(client.query(query).result())
    row_count = result[0]["row_count"]

    print(f"stg_matches row count: {row_count} (minimum required: {MIN_STAGING_ROW_COUNT})")

    if row_count < MIN_STAGING_ROW_COUNT:
        raise ValueError(
            f"Data quality check failed: stg_matches has {row_count} rows, "
            f"below the minimum threshold of {MIN_STAGING_ROW_COUNT}. "
            f"Failing the DAG rather than building gold tables on incomplete data."
        )


default_args = {
    "owner": "wfi-data-engineering",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="wfi_football_transform_dag",
    description="Transform BigQuery raw tables into staging and gold layer tables",
    default_args=default_args,
    schedule_interval=None,  # triggered by wfi_football_ingestion_dag
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["wfi", "transform"],
) as dag:

    create_staging_matches = BigQueryInsertJobOperator(
        task_id="create_staging_matches",
        configuration={
            "query": {
                "query": read_sql("create_staging_matches.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    validate_staging_row_count = PythonOperator(
        task_id="validate_staging_row_count",
        python_callable=check_staging_row_count,
    )

    create_gold_team_stats = BigQueryInsertJobOperator(
        task_id="create_gold_team_stats",
        configuration={
            "query": {
                "query": read_sql("create_gold_team_stats.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    create_gold_competition_summary = BigQueryInsertJobOperator(
        task_id="create_gold_competition_summary",
        configuration={
            "query": {
                "query": read_sql("create_gold_competition_summary.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    create_gold_match_results = BigQueryInsertJobOperator(
        task_id="create_gold_match_results",
        configuration={
            "query": {
                "query": read_sql("create_gold_match_results.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    create_gold_team_match_log = BigQueryInsertJobOperator(
        task_id="create_gold_team_match_log",
        configuration={
            "query": {
                "query": read_sql("create_gold_team_match_log.sql"),
                "useLegacySql": False,
            }
        },
        gcp_conn_id=GCP_CONN_ID,
    )

    gold_tasks = [
        create_gold_team_stats,
        create_gold_competition_summary,
        create_gold_match_results,
        create_gold_team_match_log,
    ]

    # Only added if ALERT_EMAIL_TO is set in .env, so the DAG doesn't break for
    # anyone who hasn't configured SMTP yet.
    if ALERT_EMAIL_TO:
        notify_success = EmailOperator(
            task_id="notify_pipeline_success",
            to=ALERT_EMAIL_TO,
            subject="WFI Football Pipeline: daily run completed successfully",
            html_content="""
            <h3>WFI Football Data Pipeline</h3>
            <p>The daily ingestion and transform run completed successfully.</p>
            <p>Gold layer tables (<code>gold_team_statistics</code>,
            <code>gold_competition_summary</code>, <code>gold_match_results</code>)
            are refreshed and ready for query.</p>
            <p>Run: {{ dag_run.run_id }}<br>Completed: {{ ts }}</p>
            """,
            trigger_rule="all_success",
        )
        create_staging_matches >> validate_staging_row_count
        validate_staging_row_count >> gold_tasks
        gold_tasks >> notify_success
    else:
        create_staging_matches >> validate_staging_row_count
        validate_staging_row_count >> gold_tasks