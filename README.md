# WFI Football Data Pipeline

## Project Overview

This project is an automated ELT pipeline built for WFI (World Football Intelligence), a sports analytics company that sells football data and insights to clubs, broadcasters and betting platforms. It replaces a manual process of downloading spreadsheets and cleaning them by hand with a cloud-native pipeline that pulls World Cup data from a public API, lands it in cloud storage, loads it into a data warehouse, and transforms it into tables that are ready for analysts and dashboards.

The pipeline runs on a daily schedule with retries and full task history, so the data can be trusted to be fresh and complete without anyone needing to trigger a single step manually.

## Architecture

The pipeline follows a standard ELT pattern with three layers inside BigQuery.

```
football-data.org API
        |
        | HTTP requests with API key
        v
Python extraction script
        |
        | writes 3 NDJSON files locally
        v
Google Cloud Storage (wfi-football-raw-data)
    raw/worldcup_matches.ndjson
    raw/worldcup_teams.ndjson
    raw/worldcup_standings.ndjson
        |
        | GCS sensor confirms the files landed
        v
BigQuery raw layer (wfi_raw)
    raw_matches, raw_teams, raw_standings
        |
        | SQL transformation
        v
BigQuery staging layer (wfi_staging)
    stg_matches
        |
        | SQL aggregation
        v
BigQuery gold layer (wfi_gold)
    gold_team_statistics
    gold_competition_summary
        |
        v
Dashboard / analyst queries
```

Three layers, three audiences:

| Layer | Dataset | Used by | Purpose |
|---|---|---|---|
| Raw | wfi_raw | Data engineers | Keep the original API response untouched, in case anything needs to be reprocessed |
| Staging | wfi_staging | Data analysts | Cleaned and standardized records, with derived fields like match winner and total goals |
| Gold | wfi_gold | Business users, dashboards | Pre-aggregated tables that answer specific business questions directly |

GCS sits between the API and BigQuery deliberately. If the BigQuery load step fails, the raw file is still sitting safely in the bucket and can be reloaded without calling the API again, which matters because free API tiers are usually rate limited.

Two Airflow DAGs drive the pipeline. The ingestion DAG extracts data, uploads it to GCS, waits for the files to exist, then loads them into the three raw tables. On success it triggers the transform DAG, which runs the staging query first and then the two gold queries in parallel.

## Project Structure

```
wfi-football-pipeline/
├── README.md
├── requirements.txt              # pinned dependencies for the local venv
├── requirements-airflow.txt      # dependencies installed inside the Airflow image
├── .env.example                  # template for your local .env file
├── .gitignore
├── config.py                     # loads and validates environment variables
├── Dockerfile.airflow            # custom Airflow image with the Google provider
├── docker-compose.yml            # starts the 4 Airflow services
├── scripts/
│   ├── extract_football_data.py  # calls the API, writes NDJSON files
│   └── gcs_upload.py             # uploads NDJSON files to GCS
├── dags/
│   ├── wfi_ingestion_dag.py      # extract, upload, sense, load into wfi_raw
│   └── wfi_transform_dag.py      # runs the SQL files below
├── sql/
│   ├── create_staging_matches.sql
│   ├── create_gold_team_stats.sql
│   └── create_gold_competition_summary.sql
└── data/
    └── raw/                      # local NDJSON output, ignored by git
```

## Prerequisites

Accounts and access:

1. Register at football-data.org and get a free API key.
2. Create a Google Cloud Platform account and a new GCP project.
3. Enable the BigQuery API and the Cloud Storage API on that project.
4. Create a GCS bucket named `wfi-football-raw-data`.
5. Create three BigQuery datasets: `wfi_raw`, `wfi_staging`, `wfi_gold`.

Service account:

1. In GCP IAM, create a service account for this project.
2. Grant it three roles: Storage Object Viewer, BigQuery Job User, BigQuery Data Editor.
3. Download the JSON key and save it as `service_account.json` in the project root. This file is already in `.gitignore` and should never be committed.

Local tools:

1. Python 3.10 or newer.
2. Docker Desktop, installed and running.
3. A code editor, VS Code works well for this.

## Setup Guide

### Step 1: Environment setup

Copy the environment template and fill in your real values.

```bash
cp .env.example .env
```

Open `.env` and set `FOOTBALL_API_KEY` and `GCP_PROJECT_ID` at minimum. Then create and activate a virtual environment.

```bash
python -m venv venv
# Windows (Git Bash)
source venv/Scripts/activate
# Mac or Linux
source venv/bin/activate

pip install -r requirements.txt
```

Confirm Docker Desktop is running before moving on, the later steps depend on it.

### Step 2: Test extraction

This step confirms the API key works and that the extraction logic produces valid files before anything touches the cloud.

```bash
python scripts/extract_football_data.py
```

Expected result: three NDJSON files appear inside `data/raw/`. Open one and check that each line is a valid JSON object with fields like team names and scores.

### Step 3: Test the GCS upload

This step confirms the service account has the right permissions before Airflow relies on it.

```bash
python scripts/gcs_upload.py
```

Check the GCS console for your bucket and confirm the three files appear under the `raw/` prefix. A 403 error here almost always means the service account is missing the Storage Object Viewer role.

### Step 4: Start Airflow with Docker

```bash
docker compose up -d --build
```

This builds the custom Airflow image, then starts four services: `airflow-db` (Postgres metadata store), `airflow-init` (creates the admin user), `airflow-webserver`, and `airflow-scheduler`. Give it two to three minutes to finish initializing.

Open `http://localhost:8080` and log in with `admin` / `admin`. You should see the Airflow UI with no DAGs visible yet, that is expected since the DAG files are mounted but not registered until the scheduler picks them up. If a service fails to start, check the logs.

```bash
docker compose logs
```

### Step 5: Run the ingestion DAG

Open `dags/wfi_ingestion_dag.py` and update `PROJECT_ID` and `BUCKET_NAME` at the top to match your GCP project.

In the Airflow UI, unpause `wfi_football_ingestion_dag` and trigger a manual run. Watch the graph view, tasks should turn green in order: extract, upload, the three sensors, the three BigQuery loads, then the trigger to the transform DAG. If a task turns red, click it and check the logs for the actual error message.

Once it finishes, confirm data landed in BigQuery.

```sql
SELECT COUNT(*) FROM wfi_raw.raw_matches;
SELECT COUNT(*) FROM wfi_raw.raw_teams;
SELECT COUNT(*) FROM wfi_raw.raw_standings;
```

### Step 6: Run the transform DAG

Open each file in `sql/` and replace `your-gcp-project-id` with your real project ID. The transform DAG is triggered automatically at the end of the ingestion DAG, but it can also be triggered manually from the Airflow UI to test it in isolation.

Once it finishes, confirm the staging and gold tables have data.

```sql
SELECT * FROM wfi_staging.stg_matches LIMIT 10;
SELECT * FROM wfi_gold.gold_team_statistics ORDER BY win_percentage DESC LIMIT 5;
SELECT * FROM wfi_gold.gold_competition_summary;
```

### Step 7: Validate the full pipeline end to end

Re-trigger `wfi_football_ingestion_dag` from scratch to simulate a fresh daily run and confirm both DAGs complete with every task green. If that holds, the pipeline is working end to end and will keep running on the `@daily` schedule without anyone touching it.

## Key Files

| File | What it does |
|---|---|
| `.env` | Stores secrets and config, read by `config.py` at runtime, never committed |
| `config.py` | Loads `.env` and exposes the values used across every script and DAG |
| `scripts/extract_football_data.py` | Calls the three API endpoints and writes NDJSON files to `data/raw/` |
| `scripts/gcs_upload.py` | Uploads the NDJSON files to the GCS bucket |
| `dags/wfi_ingestion_dag.py` | Extract, upload, sense, load into `wfi_raw`, then trigger the transform DAG |
| `dags/wfi_transform_dag.py` | Runs the three SQL files to build staging and gold tables |
| `sql/create_staging_matches.sql` | Cleans `raw_matches`, derives match winner and total goals, writes `stg_matches` |
| `sql/create_gold_team_stats.sql` | Unions home and away rows per team, aggregates wins, goals and win rate |
| `sql/create_gold_competition_summary.sql` | Aggregates total matches, total goals and draws per competition |
| `docker-compose.yml` | Defines the four Airflow services |
| `Dockerfile.airflow` | Extends the base Airflow image with the Google provider package |

## Common Errors

| Error | Likely cause | Fix |
|---|---|---|
| 403 Permission Denied | Service account missing an IAM role | Add Storage Object Viewer, BigQuery Job User and BigQuery Data Editor |
| ModuleNotFoundError | Virtual env not activated or dependencies not installed | Activate `venv` and rerun `pip install -r requirements.txt` |
| 401 Unauthorized from the API | Missing or wrong API key | Check `FOOTBALL_API_KEY` in `.env`, no quotes, no extra spaces |
| DAG not showing in the UI | Syntax error in the DAG file | Run `python dags/wfi_ingestion_dag.py` locally and read the traceback |
| Sensor times out | File never reached GCS | Confirm `upload_to_gcs` completed successfully before the sensor runs |
| WRITE_TRUNCATE not working | Wrong project, dataset or table name | Double check `PROJECT_ID`, `RAW_DATASET` and the table name match BigQuery exactly |
| `docker compose up` fails | Docker Desktop not running | Start Docker Desktop, wait for it to fully load, then rerun the command |
| Division by zero in SQL | Used `/` instead of `SAFE_DIVIDE()` | Replace with `SAFE_DIVIDE()` anywhere division could hit a zero denominator |

## Extension Ideas

- Connect Looker Studio to the gold layer and build a dashboard for top scorers, standings and goal trends.
- Add a fourth gold table, `gold_match_results`, with full match details sorted by date and the derived winner column.
- Add email alerting through Airflow's `EmailOperator` when the daily run completes successfully.
- Move `PROJECT_ID` out of the DAG files and into an Airflow connection instead.
- Deploy Airflow to a GCP Compute Engine VM so the pipeline runs in the cloud rather than locally.
- Add a data quality check that queries the staging table after load and fails the DAG if the row count drops below a threshold.

## Contributing

This project was built as a personal data engineering exercise, but suggestions and improvements are welcome.

1. Fork the repository and create a feature branch off `main`.
2. Keep changes scoped, one feature or fix per pull request makes review easier.
3. If you change a SQL model, update the corresponding section in this README so the setup steps stay accurate.
4. Test any DAG changes locally with `docker compose up -d --build` before opening a pull request.
5. Open a pull request with a short description of what changed and why.

Bug reports and questions are also welcome through the issues tab.
