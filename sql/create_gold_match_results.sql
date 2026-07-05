-- Reads stg_matches and produces one row per match with full detail,
-- sorted by date. This is the table the Looker Studio detail view reads from,
-- match_winner and total_goals are already derived in the staging layer,
-- this table just selects and orders them for direct consumption.

CREATE OR REPLACE TABLE `wfi-football-pipeline.wfi_gold.gold_match_results` AS
SELECT
  match_id,
  match_date,
  competition_name,
  stage,
  matchday,
  home_team,
  away_team,
  home_goals,
  away_goals,
  total_goals,
  match_winner,
  status
FROM `wfi-football-pipeline.wfi_staging.stg_matches`
ORDER BY match_date ASC;