#!/usr/bin/env bash
# Starts the WFI Football Data Pipeline end to end: builds the Airflow image,
# brings up all four Docker services, confirms both DAGs parsed without error,
# unpauses them, and triggers a fresh ingestion run (which auto-triggers the
# transform DAG on success).
#
# Run from the project root: bash scripts/run_pipeline.sh
#
# Assumes .env and service_account.json are already in place, this script
# doesn't create them, see the README's Prerequisites and Google Cloud CLI
# Setup sections for that.

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_ROOT"

echo "== WFI Football Data Pipeline: startup =="

# --- Preflight checks -------------------------------------------------------

if [ ! -f .env ]; then
  echo "ERROR: .env not found in project root. Copy .env.example to .env and fill in your values first."
  exit 1
fi

if [ ! -f service_account.json ]; then
  echo "ERROR: service_account.json not found in project root."
  echo "See the README's 'Google Cloud CLI Setup' section for how to generate one (service account key or ADC)."
  exit 1
fi

echo "-- .env and service_account.json found"

# --- Build and start ---------------------------------------------------------

echo "-- Building and starting containers (this can take a few minutes on first run)"
docker compose up -d --build --force-recreate

# --- Wait for the webserver to actually be ready, not just "started" -------

echo "-- Waiting for the Airflow webserver to become healthy"
ATTEMPTS=0
MAX_ATTEMPTS=30
until docker compose exec -T airflow-webserver airflow db check >/dev/null 2>&1; do
  ATTEMPTS=$((ATTEMPTS + 1))
  if [ "$ATTEMPTS" -ge "$MAX_ATTEMPTS" ]; then
    echo "ERROR: Airflow did not become ready in time. Check: docker compose logs airflow-webserver"
    exit 1
  fi
  sleep 5
done
echo "-- Airflow is up"

# --- Validate both DAGs actually parsed, this has caught real bugs before ---

echo "-- Checking for DAG import errors"
IMPORT_ERRORS=$(docker compose exec -T airflow-webserver airflow dags list-import-errors 2>&1 || true)
if echo "$IMPORT_ERRORS" | grep -q "wfi_"; then
  echo "ERROR: one or more DAGs failed to import:"
  echo "$IMPORT_ERRORS"
  exit 1
fi
echo "-- Both DAGs parsed cleanly"

# --- Unpause and trigger -----------------------------------------------------

echo "-- Unpausing DAGs"
docker compose exec -T airflow-webserver airflow dags unpause wfi_football_ingestion_dag
docker compose exec -T airflow-webserver airflow dags unpause wfi_football_transform_dag

echo "-- Triggering the ingestion DAG (transform DAG runs automatically on success)"
docker compose exec -T airflow-webserver airflow dags trigger wfi_football_ingestion_dag

echo ""
echo "== Pipeline started =="
echo "Airflow UI: http://localhost:8085"
echo "Watch the run:  docker compose exec airflow-webserver airflow dags list-runs -d wfi_football_ingestion_dag"
echo ""
echo "Once both DAGs show 'success', validate in BigQuery:"
echo "  SELECT COUNT(*) FROM wfi_raw.raw_matches;"
echo "  SELECT COUNT(*) FROM wfi_staging.stg_matches;"
echo "  SELECT * FROM wfi_gold.gold_team_statistics ORDER BY win_percentage DESC LIMIT 5;"