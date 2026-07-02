import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

# Update this value to match your GCP project before running the DAG
PROJECT_ID = "your-gcp-project-id"
GCP_CONN_ID = "google_cloud_default"
SQL_DIR = os.path.join(os.path.dirname(__file__), "..", "sql")


def read_sql(filename):
    with open(os.path.join(SQL_DIR, filename), "r", encoding="utf-8") as f:
        return f.read().replace("your-gcp-project-id", PROJECT_ID)


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

    create_staging_matches >> [create_gold_team_stats, create_gold_competition_summary]
