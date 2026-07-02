import json
import sys
import time
from pathlib import Path

import requests

sys.path.append(str(Path(__file__).resolve().parent.parent))
from config import (
    FOOTBALL_API_KEY,
    FOOTBALL_API_BASE_URL,
    COMPETITION_CODE,
    LOCAL_DATA_DIR,
    validate_config,
)

HEADERS = {"X-Auth-Token": FOOTBALL_API_KEY}

ENDPOINTS = {
    "matches": f"/competitions/{COMPETITION_CODE}/matches",
    "teams": f"/competitions/{COMPETITION_CODE}/teams",
    "standings": f"/competitions/{COMPETITION_CODE}/standings",
}


def call_api(endpoint_path, retries=3, backoff_seconds=5):
    """Call a football-data.org endpoint with basic retry handling for rate limits."""
    url = f"{FOOTBALL_API_BASE_URL}{endpoint_path}"
    for attempt in range(1, retries + 1):
        response = requests.get(url, headers=HEADERS, timeout=30)
        if response.status_code == 200:
            return response.json()
        if response.status_code == 429:
            wait_time = backoff_seconds * attempt
            print(f"Rate limited on {url}, waiting {wait_time}s before retry {attempt}/{retries}")
            time.sleep(wait_time)
            continue
        response.raise_for_status()
    raise RuntimeError(f"Failed to fetch {url} after {retries} retries")


def extract_matches():
    data = call_api(ENDPOINTS["matches"])
    return data.get("matches", [])


def extract_teams():
    data = call_api(ENDPOINTS["teams"])
    return data.get("teams", [])


def extract_standings():
    data = call_api(ENDPOINTS["standings"])
    return data.get("standings", [])


def write_ndjson(records, filename):
    """Write a list of dicts to a newline delimited JSON file, one record per line."""
    output_dir = Path(LOCAL_DATA_DIR)
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / filename

    with open(output_path, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record) + "\n")

    print(f"Wrote {len(records)} records to {output_path}")
    return str(output_path)


def run_extraction():
    """Entry point used both by the CLI and by the Airflow PythonOperator."""
    validate_config()

    matches = extract_matches()
    teams = extract_teams()
    standings = extract_standings()

    return {
        "matches": write_ndjson(matches, "worldcup_matches.ndjson"),
        "teams": write_ndjson(teams, "worldcup_teams.ndjson"),
        "standings": write_ndjson(standings, "worldcup_standings.ndjson"),
    }


if __name__ == "__main__":
    run_extraction()
