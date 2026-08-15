-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###deleting all the tables

-- COMMAND ----------

drop table epl.bronze.dim_teams;
drop table epl.bronze.dim_players;
drop table epl.bronze.fact_matches;
drop table epl.bronze.staging_teams;
drop table epl.bronze.staging_players;
drop table epl.bronze.staging_matches;


-- COMMAND ----------

drop table epl.silver.dim_teams;
drop table epl.silver.dim_players;
drop table epl.silver.fact_matches;
drop table epl.silver.staging_teams;
drop table epl.silver.staging_players;
drop table epl.silver.staging_matches;


-- COMMAND ----------

drop table epl.gold.dim_teams;
drop table epl.gold.dim_players;
drop table epl.gold.fact_matches;