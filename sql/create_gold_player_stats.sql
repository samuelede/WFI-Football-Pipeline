-- Reads stg_scorers and produces the player-level gold table for the dashboard.
-- One combined table rather than separate gold_top_scorers/gold_top_assists
-- tables, since it's the same underlying rows, Looker Studio (or any BI tool)
-- can sort this one table by goals for a "top scorers" view or by assists for
-- a "top assists" view without maintaining duplicate BigQuery objects.
--
-- Note: football-data.org does not expose goalkeeper/saves data on any tier,
-- so there is no equivalent gold table for saves, see the README's Future
-- Recommendations section.

CREATE OR REPLACE TABLE `wfi-football-pipeline.wfi_gold.gold_player_stats` AS
SELECT
  player_name,
  team_name,
  nationality,
  matches_played,
  goals,
  assists,
  penalties,
  goal_contributions
FROM `wfi-football-pipeline.wfi_staging.stg_scorers`
ORDER BY goals DESC, assists DESC;