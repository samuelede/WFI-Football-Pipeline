import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

sys.path.append(os.path.join(os.path.dirname(__file__), "..", "scripts"))
sys.path.append(os.path.join(os.path.dirname(__file__), ".."))

from extract_football_data import run_extraction
from gcs_upload import run_upload

# Update these two values to match your GCP project before running the DAG
PROJECT_ID = "wfi-football-pipeline"
BUCKET_NAME = "wfi-football-raw-data"

RAW_DATASET = "wfi_raw"
GCP_CONN_ID = "google_cloud_default"

default_args = {
    "owner": "wfi-data-engineering",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="wfi_football_ingestion_dag",
    description="Extract World Cup data from football-data.org, land it in GCS, load it into BigQuery raw tables",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["wfi", "ingestion"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_football_data",
        python_callable=run_extraction,
    )

    upload_task = PythonOperator(
        task_id="upload_to_gcs",
        python_callable=run_upload,
    )

    sense_matches = GCSObjectExistenceSensor(
        task_id="sense_matches_file",
        bucket=BUCKET_NAME,
        object="raw/worldcup_matches.ndjson",
        gcp_conn_id=GCP_CONN_ID,
        timeout=300,
        poke_interval=15,
    )

    sense_teams = GCSObjectExistenceSensor(
        task_id="sense_teams_file",
        bucket=BUCKET_NAME,
        object="raw/worldcup_teams.ndjson",
        gcp_conn_id=GCP_CONN_ID,
        timeout=300,
        poke_interval=15,
    )

    sense_standings = GCSObjectExistenceSensor(
        task_id="sense_standings_file",
        bucket=BUCKET_NAME,
        object="raw/worldcup_standings.ndjson",
        gcp_conn_id=GCP_CONN_ID,
        timeout=300,
        poke_interval=15,
    )

    load_matches = GCSToBigQueryOperator(
        task_id="load_raw_matches",
        bucket=BUCKET_NAME,
        source_objects=["raw/worldcup_matches.ndjson"],
        destination_project_dataset_table=f"{PROJECT_ID}.{RAW_DATASET}.raw_matches",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID,
    )

    load_teams = GCSToBigQueryOperator(
        task_id="load_raw_teams",
        bucket=BUCKET_NAME,
        source_objects=["raw/worldcup_teams.ndjson"],
        destination_project_dataset_table=f"{PROJECT_ID}.{RAW_DATASET}.raw_teams",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID,
    )

    load_standings = GCSToBigQueryOperator(
        task_id="load_raw_standings",
        bucket=BUCKET_NAME,
        source_objects=["raw/worldcup_standings.ndjson"],
        destination_project_dataset_table=f"{PROJECT_ID}.{RAW_DATASET}.raw_standings",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        autodetect=True,
        gcp_conn_id=GCP_CONN_ID,
    )

    trigger_transform = TriggerDagRunOperator(
        task_id="trigger_transform_dag",
        trigger_dag_id="wfi_football_transform_dag",
    )

    extract_task >> upload_task
    upload_task >> [sense_matches, sense_teams, sense_standings]
    sense_matches >> load_matches
    sense_teams >> load_teams
    sense_standings >> load_standings
    [load_matches, load_teams, load_standings] >> trigger_transform
