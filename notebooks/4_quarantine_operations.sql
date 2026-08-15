-- Databricks notebook source
-- MAGIC %md
-- MAGIC ###select query
-- MAGIC

-- COMMAND ----------

select * from epl.silver.players_quarantine;

-- COMMAND ----------

select * from epl.silver.players_quarantine 
where status = 'unresolved'

-- COMMAND ----------

select * from epl.silver.players_quarantine 
where status = 'solved'

-- COMMAND ----------

select * from epl.silver.players_quarantine 
where status = 'rejected'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###insert into silver and gold

-- COMMAND ----------

insert into epl.silver.dim_players(
    player_id,
    player_name,
    team_id,
    position,
    nationality,
    market_value
)
values(
    376,
    'Dr Matthew Barton',
    5,
    'Forward',
    'South Korea',
    36955837
)

-- COMMAND ----------

insert into epl.gold.dim_players(
    player_id,
    player_name,
    team_id,
    position,
    nationality,
    market_value
)
values(
    376,
    'Dr Matthew Barton',
    5,
    'Forward',
    'South Korea',
    36955837
)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###update quarantine table

-- COMMAND ----------

update epl.silver.players_quarantine
set status = 'solved'
where player_id = 376 and market_value = 36955837

-- COMMAND ----------

update epl.silver.players_quarantine
set status = 'rejected'
where market_value = 36000000 and player_id = 376 

-- COMMAND ----------

