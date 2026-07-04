import json
import os
import sys
from pathlib import Path

from google.cloud import storage

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import GCP_PROJECT_ID, GCS_BUCKET_NAME, SERVICE_ACCOUNT_PATH, LOCAL_DATA_DIR, validate_config

FILES_TO_UPLOAD = [
    "worldcup_matches.ndjson",
    "worldcup_teams.ndjson",
    "worldcup_standings.ndjson",
]


def get_client():
    """
    service_account.json can be either of two different formats depending on how
    it was obtained, and they need different handling:

    1. A real service account key (type: "service_account") - from a downloaded
       key file. storage.Client.from_service_account_json() reads this directly.

    2. Application Default Credentials (type: "authorized_user") - from
       `gcloud auth application-default login`, used when the org blocks service
       account key creation. This format is NOT accepted by
       from_service_account_json(). Instead, point GOOGLE_APPLICATION_CREDENTIALS
       at it and let storage.Client() discover it the normal ADC way.

    If neither file nor env var is present, storage.Client() still works as long
    as ADC has been set up globally on the machine.
    """
    key_path = Path(SERVICE_ACCOUNT_PATH)

    if key_path.exists():
        with open(key_path) as f:
            creds_info = json.load(f)

        if creds_info.get("type") == "service_account":
            return storage.Client.from_service_account_json(str(key_path))

        # ADC file (authorized_user, or anything else that isn't a service account key)
        os.environ.setdefault("GOOGLE_APPLICATION_CREDENTIALS", str(key_path))
        return storage.Client(project=GCP_PROJECT_ID)

    # No file at all: rely on ADC already configured on the machine
    return storage.Client(project=GCP_PROJECT_ID)


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