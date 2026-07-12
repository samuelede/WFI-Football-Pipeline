-- Unpivots stg_matches into one row per team per match (a home row and an away
-- row for every match), so downstream tools like Looker Studio can blend this
-- against gold_team_statistics using a single team_name join key, instead of
-- needing separate blends for home_team and away_team.

CREATE OR REPLACE TABLE `wfi-football-pipeline.wfi_gold.gold_team_match_log` AS

SELECT
  match_id,
  match_date,
  competition_name,
  stage,
  home_team AS team_name,
  away_team AS opponent,
  TRUE AS is_home,
  home_goals AS goals_for,
  away_goals AS goals_against,
  home_goals - away_goals AS goal_difference,
  CASE
    WHEN match_winner = home_team THEN 'Win'
    WHEN match_winner = 'Draw' THEN 'Draw'
    WHEN match_winner = away_team THEN 'Loss'
    ELSE NULL
  END AS result
FROM `wfi-football-pipeline.wfi_staging.stg_matches`

UNION ALL

SELECT
  match_id,
  match_date,
  competition_name,
  stage,
  away_team AS team_name,
  home_team AS opponent,
  FALSE AS is_home,
  away_goals AS goals_for,
  home_goals AS goals_against,
  away_goals - home_goals AS goal_difference,
  CASE
    WHEN match_winner = away_team THEN 'Win'
    WHEN match_winner = 'Draw' THEN 'Draw'
    WHEN match_winner = home_team THEN 'Loss'
    ELSE NULL
  END AS result
FROM `wfi-football-pipeline.wfi_staging.stg_matches`

ORDER BY team_name, match_date;