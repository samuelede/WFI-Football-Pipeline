-- Reads wfi_raw.raw_matches and produces one clean row per finished match.
-- Adds two derived columns that the gold layer depends on: match_winner and total_goals.

CREATE OR REPLACE TABLE `your-gcp-project-id.wfi_staging.stg_matches` AS
SELECT
  id AS match_id,
  utcDate AS match_date,
  status,
  matchday,
  stage,
  homeTeam.name AS home_team,
  awayTeam.name AS away_team,
  score.fullTime.home AS home_goals,
  score.fullTime.away AS away_goals,
  IFNULL(score.fullTime.home, 0) + IFNULL(score.fullTime.away, 0) AS total_goals,
  CASE
    WHEN score.fullTime.home > score.fullTime.away THEN homeTeam.name
    WHEN score.fullTime.away > score.fullTime.home THEN awayTeam.name
    WHEN score.fullTime.home = score.fullTime.away THEN 'Draw'
    ELSE NULL
  END AS match_winner,
  competition.name AS competition_name
FROM `your-gcp-project-id.wfi_raw.raw_matches`
WHERE status = 'FINISHED';
