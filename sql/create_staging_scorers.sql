-- Reads wfi_raw.raw_scorers and produces one clean row per player.
-- Nested player/team objects from the API get flattened here, same pattern as
-- create_staging_matches.sql does for homeTeam/awayTeam.

CREATE OR REPLACE TABLE `wfi-football-pipeline.wfi_staging.stg_scorers` AS
SELECT
  player.name AS player_name,
  player.nationality AS nationality,
  team.name AS team_name,
  playedMatches AS matches_played,
  goals,
  assists,
  penalties,
  IFNULL(goals, 0) + IFNULL(assists, 0) AS goal_contributions
FROM `wfi-football-pipeline.wfi_raw.raw_scorers`;