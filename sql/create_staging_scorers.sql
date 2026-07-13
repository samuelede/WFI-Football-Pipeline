-- Reads wfi_raw.raw_scorers and produces one clean row per player.
-- Nested player/team objects from the API get flattened here, same pattern as
-- create_staging_matches.sql does for homeTeam/awayTeam.
--
-- goals/assists/penalties come back NULL from the API when a player has zero
-- of that stat, not when the value is unknown, so they're coalesced to 0 here
-- rather than left as NULL. Fixing it once in staging means every downstream
-- gold table and dashboard sees clean 0s instead of each having to handle it
-- separately, same reasoning as the IFNULL calls in create_staging_matches.sql.

CREATE OR REPLACE TABLE `wfi-football-pipeline.wfi_staging.stg_scorers` AS
SELECT
  player.name AS player_name,
  player.nationality AS nationality,
  team.name AS team_name,
  IFNULL(playedMatches, 0) AS matches_played,
  IFNULL(goals, 0) AS goals,
  IFNULL(assists, 0) AS assists,
  IFNULL(penalties, 0) AS penalties,
  IFNULL(goals, 0) + IFNULL(assists, 0) AS goal_contributions
FROM `wfi-football-pipeline.wfi_raw.raw_scorers`;