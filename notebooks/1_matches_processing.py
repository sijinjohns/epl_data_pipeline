# Databricks notebook source
# MAGIC %md
# MAGIC ##match processing

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###utilities

# COMMAND ----------

# MAGIC %run /Workspace/epl_project/1_basic/1_utilities

# COMMAND ----------

dbutils.widgets.text("catalog","epl","catalog")
dbutils.widgets.text("source_folder","matches","source_folder")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
source_folder = dbutils.widgets.get("source_folder")


# COMMAND ----------

# MAGIC %md
# MAGIC ###paths

# COMMAND ----------

base_path = f"/Volumes/{catalog}/source_system/epl_volume/epl_source/{source_folder}/"
landing = f"{base_path}/m_landing/"
processed = f"{base_path}/m_processed/"

# COMMAND ----------

# MAGIC %md
# MAGIC ###bronze

# COMMAND ----------

#checking csv files present or not
#=========================================================

files = dbutils.fs.ls(landing)

csv_files = [f for f in files if f.name.endswith(".csv")]

if len(csv_files) > 0:
    df =  spark.read\
        .format("csv")\
        .options(header=True,inferSchema=True)\
        .load(f"{landing}/*.csv")\
        .withColumn("read_timestamp",F.current_timestamp())\
        .withColumn("file_name",F.col("_metadata.file_name"))\
        .withColumn("file_size",F.col("_metadata.file_size"))

else:
    #no csv file
    print("no csv file found")

    schema = StructType([])
    df = spark.createDataFrame([],schema)

# COMMAND ----------

df = df.sort(F.col("file_name"))
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ####creating bronze table

# COMMAND ----------

#creating bromze table

df.write\
    .mode("append")\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .saveAsTable(f"{catalog}.{bronze_schema}.fact_{source_folder}")
                 
                 

# COMMAND ----------

# MAGIC %md
# MAGIC ####creating staging table

# COMMAND ----------

#staging table bronze

df.write\
    .mode("overwrite")\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .saveAsTable(f"{catalog}.{bronze_schema}.staging_{source_folder}")
    

# COMMAND ----------

# MAGIC %md
# MAGIC ####moving csv files from landing to processed

# COMMAND ----------

#moving csv files from landing to processed

files = dbutils.fs.ls(landing)

if len(files) > 0:
    for file in files:
        dbutils.fs.mv(file.path,f"{processed}/{file.name}")

else:
    print("no csv file to move")



# COMMAND ----------

# MAGIC %md
# MAGIC ###silver

# COMMAND ----------

df_silver = spark.read.table(f"{catalog}.{bronze_schema}.staging_{source_folder}")

display(df_silver)

# COMMAND ----------

df_silver = df_silver.dropDuplicates()

# COMMAND ----------

# Create match_code column by combining match_id and season
df_silver = df_silver.withColumn(
    "match_code",
    F.concat_ws("-", F.col("match_id").cast("string"), F.col("season").cast("string"))
)

# COMMAND ----------


df_silver = df_silver.withColumn(
    "match_date",
    F.coalesce(
        F.expr("try_to_timestamp(match_date, 'yyyy-MM-dd')"),
        F.expr("try_to_timestamp(match_date, 'MM-dd-yyyy')"),
        F.expr("try_to_timestamp(match_date, 'dd/MM/yyyy')")
    ).cast("date")
)

# COMMAND ----------

df_silver = df_silver.select(
    F.col("match_code"),
    F.col("match_id"),
    F.col("match_date"),
    F.col("season"),
    F.col("home_team_id"),
    F.col("away_team_id"),
    F.col("home_goals"),
    F.col("away_goals"),
    F.col("attendance"),
)
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ####merge or create silver table

# COMMAND ----------

#for merge
fact_match = f"{catalog}.{silver_schema}.fact_{source_folder}"

if spark.catalog.tableExists(fact_match):

    table1 = DeltaTable.forName(spark, fact_match)

    table1.alias("target")\
        .merge(df_silver.alias("source"),
               "target.match_code = source.match_code")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    #create silver table for match

    df_silver.write\
        .mode("overwrite")\
        .format("delta")\
        .option("delta.enableChangeDataFeed","true")\
        .saveAsTable(fact_match)





# COMMAND ----------

# MAGIC %md
# MAGIC ####staging silver table

# COMMAND ----------

#staging silver match

df_silver.write\
    .mode("overwrite")\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .saveAsTable(f"{catalog}.{silver_schema}.staging_{source_folder}")



# COMMAND ----------

# MAGIC %md
# MAGIC ###gold

# COMMAND ----------

df_gold = spark.read.table(f"{catalog}.{silver_schema}.staging_{source_folder}")
                           
display(df_gold)

# COMMAND ----------

#total goals
df_gold=df_gold.withColumn("total_goals",F.col("home_goals")+F.col("away_goals"))

# COMMAND ----------

df_gold = df_gold.withColumn(
    "result",
    F.when(F.col("home_goals") > F.col("away_goals"), "home wins")
    .when(F.col("away_goals") > F.col("home_goals"), "away wins")
    .otherwise("draw")
)

# COMMAND ----------

display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ####merging or creating new gold table

# COMMAND ----------

#for merge
fact_match = f"{catalog}.{gold_schema}.fact_{source_folder}"

if spark.catalog.tableExists(fact_match):

    table1 = DeltaTable.forName(spark, fact_match)

    table1.alias("target")\
        .merge(df_gold.alias("source"),
               "target.match_code = source.match_code")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()
    
else:
    #create new gold table for match
    df_gold.write\
        .mode("overwrite")\
        .format("delta")\
        .option("delta.enableChangeDataFeed","true")\
        .saveAsTable(fact_match)




# COMMAND ----------

# MAGIC %md
# MAGIC ###truncating staging tables

# COMMAND ----------

#trucating staaging tables

spark.sql(f" truncate table {catalog}.{bronze_schema}.staging_{source_folder}")
          
spark.sql(f" truncate table {catalog}.{silver_schema}.staging_{source_folder}")
          

# COMMAND ----------

