-- Reads stg_matches, unions each match into a home row and an away row per team,
-- then aggregates to one row per team with wins, draws, losses and win percentage.

CREATE OR REPLACE TABLE `your-gcp-project-id.wfi_gold.gold_team_statistics` AS
WITH team_matches AS (
  SELECT
    home_team AS team_name,
    home_goals AS goals_for,
    away_goals AS goals_against,
    CASE WHEN match_winner = home_team THEN 1 ELSE 0 END AS win,
    CASE WHEN match_winner = 'Draw' THEN 1 ELSE 0 END AS draw,
    CASE WHEN match_winner = away_team THEN 1 ELSE 0 END AS loss
  FROM `your-gcp-project-id.wfi_staging.stg_matches`

  UNION ALL

  SELECT
    away_team AS team_name,
    away_goals AS goals_for,
    home_goals AS goals_against,
    CASE WHEN match_winner = away_team THEN 1 ELSE 0 END AS win,
    CASE WHEN match_winner = 'Draw' THEN 1 ELSE 0 END AS draw,
    CASE WHEN match_winner = home_team THEN 1 ELSE 0 END AS loss
  FROM `your-gcp-project-id.wfi_staging.stg_matches`
)

SELECT
  team_name,
  COUNT(*) AS matches_played,
  SUM(win) AS wins,
  SUM(draw) AS draws,
  SUM(loss) AS losses,
  SUM(goals_for) AS goals_for,
  SUM(goals_against) AS goals_against,
  SUM(goals_for) - SUM(goals_against) AS goal_difference,
  ROUND(SAFE_DIVIDE(SUM(win), COUNT(*)) * 100, 2) AS win_percentage
FROM team_matches
GROUP BY team_name
ORDER BY win_percentage DESC;
