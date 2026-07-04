-- Reads stg_matches and produces one summary row per competition:
-- total matches, total goals, average goals per match, draws and distinct teams.

CREATE OR REPLACE TABLE `wfi-football-pipeline.wfi_gold.gold_competition_summary` AS
WITH team_list AS (
  SELECT home_team AS team_name, competition_name
  FROM `wfi-football-pipeline.wfi_staging.stg_matches`
  UNION DISTINCT
  SELECT away_team AS team_name, competition_name
  FROM `wfi-football-pipeline.wfi_staging.stg_matches`
)

SELECT
  m.competition_name,
  COUNT(*) AS total_matches,
  SUM(m.total_goals) AS total_goals,
  ROUND(SAFE_DIVIDE(SUM(m.total_goals), COUNT(*)), 2) AS average_goals_per_match,
  COUNTIF(m.match_winner = 'Draw') AS total_draws,
  (
    SELECT COUNT(DISTINCT team_name)
    FROM team_list t
    WHERE t.competition_name = m.competition_name
  ) AS unique_teams
FROM `wfi-football-pipeline.wfi_staging.stg_matches` m
GROUP BY m.competition_name;
