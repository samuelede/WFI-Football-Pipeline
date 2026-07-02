import sys
from pathlib import Path

from google.cloud import storage

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import GCS_BUCKET_NAME, SERVICE_ACCOUNT_PATH, LOCAL_DATA_DIR, validate_config

FILES_TO_UPLOAD = [
    "worldcup_matches.ndjson",
    "worldcup_teams.ndjson",
    "worldcup_standings.ndjson",
]


def get_client():
    return storage.Client.from_service_account_json(SERVICE_ACCOUNT_PATH)


def upload_file(client, local_path, bucket_name, destination_blob_name):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(local_path)
    print(f"Uploaded {local_path} to gs://{bucket_name}/{destination_blob_name}")


def run_upload():
    """Entry point used both by the CLI and by the Airflow PythonOperator."""
    validate_config()
    client = get_client()

    for filename in FILES_TO_UPLOAD:
        local_path = Path(LOCAL_DATA_DIR) / filename
        if not local_path.exists():
            raise FileNotFoundError(
                f"{local_path} not found. Run extract_football_data.py first."
            )
        destination_blob_name = f"raw/{filename}"
        upload_file(client, str(local_path), GCS_BUCKET_NAME, destination_blob_name)


if __name__ == "__main__":
    run_upload()
