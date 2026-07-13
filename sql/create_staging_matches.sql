-- Reads wfi_raw.raw_matches and produces one clean row per finished match.
-- Adds two derived columns that the gold layer depends on: match_winner and total_goals.
--
-- home_goals/away_goals are coalesced to 0 here, not just the derived total_goals
-- column, since gold_match_results and gold_team_match_log both reference
-- home_goals/away_goals directly. Fixing it once here means no NULL score can
-- reach any downstream gold table through any path.

CREATE OR REPLACE TABLE `wfi-football-pipeline.wfi_staging.stg_matches` AS
SELECT
  id AS match_id,
  utcDate AS match_date,
  status,
  matchday,
  stage,
  homeTeam.name AS home_team,
  awayTeam.name AS away_team,
  IFNULL(score.fullTime.home, 0) AS home_goals,
  IFNULL(score.fullTime.away, 0) AS away_goals,
  IFNULL(score.fullTime.home, 0) + IFNULL(score.fullTime.away, 0) AS total_goals,
  CASE
    WHEN score.fullTime.home > score.fullTime.away THEN homeTeam.name
    WHEN score.fullTime.away > score.fullTime.home THEN awayTeam.name
    WHEN score.fullTime.home = score.fullTime.away THEN 'Draw'
    ELSE NULL
  END AS match_winner,
  competition.name AS competition_name
FROM `wfi-football-pipeline.wfi_raw.raw_matches`
WHERE status = 'FINISHED';