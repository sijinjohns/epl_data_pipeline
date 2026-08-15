# Databricks notebook source
# MAGIC %md
# MAGIC ##players processing

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run /Workspace/epl_project/1_basic/1_utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ###utilities

# COMMAND ----------

dbutils.widgets.text("catalog","epl","catalog")
dbutils.widgets.text("source_folder","players","source_folder")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
source_folder = dbutils.widgets.get("source_folder")

# COMMAND ----------

# MAGIC %md
# MAGIC ###paths

# COMMAND ----------

base_path = f'/Volumes/epl/source_system/epl_volume/epl_source/players/'
landing = f'{base_path}/p_landing'
processed = f'{base_path}/p_processed'


# COMMAND ----------

# MAGIC %md
# MAGIC ###bronze

# COMMAND ----------

#checking any file is present or not
#=====================================================

files = dbutils.fs.ls(landing)

csv_files = [f for f in files if f.name.endswith('.csv')]

if len(csv_files) > 0:
    df = spark.read\
        .format("csv")\
        .options(header=True, inferSchema=True)\
        .load(f'{landing}/*.csv')\
        .withColumn("raed_timestamp",F.current_timestamp())\
        .withColumn("file_name",F.col("_metadata.file_name"))\
        .withColumn("file_size",F.col("_metadata.file_size"))

else:
    print("no csv file found")

    schema = StructType([])
    df = spark.createDataFrame([],schema)




# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####writing into bronze

# COMMAND ----------

#appending into bronze table

df.write\
    .mode("append")\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .saveAsTable(f'{catalog}.{bronze_schema}.dim_{source_folder}')


# COMMAND ----------

# MAGIC %md
# MAGIC ####creating staging bronze table

# COMMAND ----------

#creating staging bronze table

df.write\
    .format("delta")\
    .mode("overwrite")\
    .option("delta.enableChangeDataFeed","true")\
    .saveAsTable(f'{catalog}.{bronze_schema}.staging_{source_folder}')


# COMMAND ----------

# MAGIC %md
# MAGIC ####moving files from landing to processed

# COMMAND ----------

#moving files from landing to processed

files = dbutils.fs.ls(landing)

if len(files) > 0:
    for file in files:
        dbutils.fs.mv(file.path, f'{processed}/{file.name}')

else:
    print("no csv files to move")

# COMMAND ----------

# MAGIC %md
# MAGIC ###silver

# COMMAND ----------

#reading from bronze table

df_silver = spark.read.table(f'{catalog}.{bronze_schema}.staging_{source_folder}')

display(df_silver)

# COMMAND ----------

df_silver = df_silver.dropDuplicates()

# COMMAND ----------

duplicate_player_ids = df_silver.groupBy("player_id").agg(F.count("*").alias("count")).filter(F.col("count") > 1).select(F.col("player_id"),F.col("count"))

display(duplicate_player_ids)

# COMMAND ----------

# DBTITLE 1,Create quarantine_silver table
# Trim whitespace from string columns
df_silver = df_silver \
    .withColumn("player_name", F.trim(F.col("player_name"))) \
    .withColumn("position", F.trim(F.col("position"))) \
    .withColumn("nationality", F.trim(F.col("nationality"))) \
    .withColumn("market_value", F.trim(F.col("market_value")))

# COMMAND ----------

# Capitalize/title case for string columns
df_silver = df_silver \
    .withColumn("player_name", F.initcap(F.col("player_name"))) \
    .withColumn("position", F.initcap(F.col("position"))) \
    .withColumn("nationality", F.initcap(F.col("nationality")))


# COMMAND ----------


df_silver = df_silver.withColumn(
    "market_value_cleaned",
    F.when(
        F.col("market_value").rlike(".*[Mm].*"),
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(F.col("market_value"), "[€$£,]", ""),
                "[Mm]",
                "000000"
            ),
            "\\.0$",
            ""
        )
    ).when(
        F.col("market_value").rlike(".*[Kk].*"),
        F.regexp_replace(
            F.regexp_replace(
                F.regexp_replace(F.col("market_value"), "[€$£,]", ""),
                "[Kk]",
                "000"
            ),
            "\\.0$",
            ""
        )
    ).otherwise(
        F.regexp_replace(
            F.regexp_replace(F.col("market_value"), "[€$£,]", ""),
            "\\.0$",
            ""
        )
    )
)

# COMMAND ----------

df_silver=df_silver.drop("market_value").withColumnRenamed("market_value_cleaned","market_value")
df_silver=df_silver.withColumn("market_value",F.col("market_value").cast("double"))

# COMMAND ----------

duplicate_player_ids = df_silver.groupBy("player_id").agg(F.count("*").alias("count")).filter(F.col("count") > 1).select(F.col("player_id"),F.col("count"))

display(duplicate_player_ids)

# COMMAND ----------



# Move duplicate player_ids to quarantine
df_quarantine = (
    df_silver
    .join(
        duplicate_player_ids,
        "player_id",
        "inner"
    )
    .select(
        F.col("player_id"),
        F.col("player_name"),
        F.col("team_id"),
        F.col("position"),
        F.col("nationality"),
        F.col("market_value")
    )
    .withColumn("status", F.lit("unresolved"))
)

display(df_quarantine)


# COMMAND ----------

# Keep only non-duplicate player_ids in df_silver
df_silver = (
    df_silver
    .join(
        duplicate_player_ids,
        "player_id",
        "left_anti"
    )
)

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ####creating quarantine table

# COMMAND ----------

#creating quarartine table
#======================================================
#appending the values

df_quarantine.write\
    .mode("append")\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .saveAsTable(f"{catalog}.{silver_schema}.{source_folder}_quarantine")



# COMMAND ----------

# MAGIC %md
# MAGIC ####create or merge for silver table

# COMMAND ----------

dim_players = f"{catalog}.{silver_schema}.dim_{source_folder}"

#checking table exists or not

if spark.catalog.tableExists(dim_players):

    table1 = DeltaTable.forName(spark, dim_players)

    table1.alias("target")\
        .merge(df_silver.alias("source"),
               "target.player_id = source.player_id")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    #create new silver table for players

    df_silver.write\
        .mode("overwrite")\
        .format("delta")\
        .option("delta.enableChangeDataFeed","true")\
        .saveAsTable(dim_players)



# COMMAND ----------

# MAGIC %md
# MAGIC ####staging silver table

# COMMAND ----------

#creating staging table

df_silver.write\
    .format("delta")\
    .mode("overwrite")\
    .option("delta.enableChangeDataFeed","true")\
    .saveAsTable(f"{catalog}.{silver_schema}.staging_{source_folder}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###gold

# COMMAND ----------

df_gold = spark.read.table(f"{catalog}.{silver_schema}.staging_{source_folder}")


# COMMAND ----------

display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ####create or merge gold table

# COMMAND ----------

gold_players = f"{catalog}.{gold_schema}.dim_{source_folder}"

#checking gold players table exists 

if spark.catalog.tableExists(gold_players):
    # merge table

    table1 = DeltaTable.forName(spark, gold_players)

    table1.alias("target")\
        .merge(df_gold.alias("source"),
               "target.player_id = source.player_id")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    #create new gold table

    df_gold.write\
        .mode("overwrite")\
        .format("delta")\
        .option("delta.enableChangeDataFeed","true")\
        .saveAsTable(gold_players)

# COMMAND ----------

# MAGIC %md
# MAGIC ###truncating staging tables

# COMMAND ----------

#truncating staging tables

spark.sql(f"truncate table {catalog}.{bronze_schema}.staging_{source_folder}")

spark.sql(f"truncate table {catalog}.{silver_schema}.staging_{source_folder}")

# COMMAND ----------

