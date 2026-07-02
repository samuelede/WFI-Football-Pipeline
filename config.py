import os
from dotenv import load_dotenv

load_dotenv()

FOOTBALL_API_KEY = os.getenv("FOOTBALL_API_KEY")
FOOTBALL_API_BASE_URL = os.getenv("FOOTBALL_API_BASE_URL", "https://api.football-data.org/v4")
COMPETITION_CODE = os.getenv("COMPETITION_CODE", "WC")

GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "wfi-football-raw-data")
SERVICE_ACCOUNT_PATH = os.getenv("SERVICE_ACCOUNT_PATH", "service_account.json")

RAW_DATASET = os.getenv("RAW_DATASET", "wfi_raw")
STAGING_DATASET = os.getenv("STAGING_DATASET", "wfi_staging")
GOLD_DATASET = os.getenv("GOLD_DATASET", "wfi_gold")

LOCAL_DATA_DIR = os.getenv("LOCAL_DATA_DIR", "data/raw")

REQUIRED_VARS = ["FOOTBALL_API_KEY", "GCP_PROJECT_ID"]


def validate_config():
    """Fail fast if required environment variables are missing."""
    missing = [name for name in REQUIRED_VARS if not globals().get(name)]
    if missing:
        raise EnvironmentError(
            f"Missing required environment variables: {', '.join(missing)}. "
            f"Check your .env file against .env.example."
        )
