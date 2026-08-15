# Databricks notebook source
# MAGIC %md
# MAGIC ##Teams processing

# COMMAND ----------

from pyspark.sql import functions as F
from delta.tables import DeltaTable
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %run /Workspace/epl_project/1_basic/1_utilities

# COMMAND ----------

# MAGIC %md
# MAGIC ####utilities

# COMMAND ----------

dbutils.widgets.text("catalog","epl","catalog")
dbutils.widgets.text("source_folder","teams","source_folder")

# COMMAND ----------

catalog = dbutils.widgets.get("catalog")
source_folder = dbutils.widgets.get("source_folder")

# COMMAND ----------

# MAGIC %md
# MAGIC ####paths

# COMMAND ----------

base_path = f'/Volumes/epl/source_system/epl_volume/epl_source/teams/'
landing = f'{base_path}/t_landing/'
processed = f'{base_path}/t_processed/'


# COMMAND ----------

# MAGIC %md
# MAGIC ###bronze

# COMMAND ----------

#checking csv files present in landing folder

files = dbutils.fs.ls(landing)
#=======================================================
csv_files = [f for f in files if f.name.endswith('csv')]

if len(csv_files)>0:
    df = spark.read\
        .format('csv')\
        .options(header = True, inferSchema = True)\
        .load(f'{landing}/*.csv')\
        .withColumn("read_timestamp",F.current_timestamp())\
        .withColumn("file_name",F.col("_metadata.file_name"))\
        .withColumn("file_size",F.col("_metadata.file_size"))

else:
    print("no csv file found")

    schema = StructType([])
    df = spark.createDataFrame([],schema)

display(df)


# COMMAND ----------

# MAGIC %md
# MAGIC ####writing into bronze
# MAGIC

# COMMAND ----------

print(df.count())

# COMMAND ----------

#writing into bronze table
 
df.write.mode("append")\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .saveAsTable(f"{catalog}.{bronze_schema}.dim_{source_folder}")
    
    

# COMMAND ----------

# MAGIC %md
# MAGIC ####writing into bronze staging table

# COMMAND ----------

#writing into bronze stagig table

df.write.mode("overwrite")\
    .format("delta")\
    .option("delta.enableChangeDataFeed","true")\
    .saveAsTable(f"{catalog}.{bronze_schema}.staging_{source_folder}")
    

# COMMAND ----------

# MAGIC %md
# MAGIC ####moving files from landing to processed

# COMMAND ----------

#moving files from landing to processed

files = dbutils.fs.ls(landing)

if len(files) > 0:
    for file in files:
        dbutils.fs.mv(file.path,f"{processed}/{file.name}")

else:
    print("no files to move")

# COMMAND ----------

# MAGIC %md
# MAGIC ###silver

# COMMAND ----------

df_silver = spark.read.table(f"{catalog}.{bronze_schema}.staging_{source_folder}")

display(df_silver)

# COMMAND ----------

#removing duplicates

df_silver=df_silver.dropDuplicates()

# COMMAND ----------

# Trim whitespace from string columns
df_silver = df_silver \
    .withColumn("team_name", F.trim(F.col("team_name"))) \
    .withColumn("manager", F.trim(F.col("manager"))) \
    .withColumn("stadium", F.trim(F.col("stadium")))


# COMMAND ----------


# Capitalize first letter of string columns
df_silver = df_silver \
    .withColumn("manager", F.initcap(F.col("manager")))\
    .withColumn("team_name", F.initcap(F.col("team_name")))\
    .withColumn("stadium", F.initcap(F.col("stadium")))\
    

# COMMAND ----------

# Validate base_attendance is numeric, set to null if not
df_silver = df_silver.withColumn(
    "base_attendance",
    F.when(F.col("base_attendance").cast("integer").isNotNull(), F.col("base_attendance").cast("integer"))
    .otherwise(None)
)


# COMMAND ----------

#changing the stadium name to unknown if it is null

df_silver=df_silver.withColumn(
    "stadium",F.when(F.col("stadium").isNull(),"Unknown")
    .otherwise(F.col("stadium"))
    )

# COMMAND ----------

df_silver = df_silver.sort(F.col("team_id"))

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ####merge or create silver table

# COMMAND ----------

#checking silver table exists or not

dim_teams = f"{catalog}.{silver_schema}.dim_{source_folder}"

if spark.catalog.tableExists(dim_teams):

    table1 = DeltaTable.forName(spark, dim_teams)
    
    #merging table(upsert)

    table1.alias("target")\
        .merge(df_silver.alias("source"),
               "target.team_id = source.team_id")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
    #create new silver table

    df_silver.write\
        .format("delta")\
        .mode("overwrite")\
        .option("delta.enableChangeDataFeed","true")\
        .saveAsTable(dim_teams)





# COMMAND ----------

#staging silver table for teams

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

display(df_gold.limit(1))

# COMMAND ----------

df_gold = df_gold.select(
    F.col("team_id"),
    F.col("team_name"),
    F.col("stadium"),
    F.col("manager"),
    F.col("base_attendance")
)



# COMMAND ----------

display(df_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ####merge or create gold table

# COMMAND ----------

dim_teams = f"{catalog}.{gold_schema}.dim_{source_folder}"

# check table exist or not

if spark.catalog.tableExists(dim_teams):

    table1 = DeltaTable.forName(spark, dim_teams)

    #merge table

    table1.alias("target")\
        .merge(df_gold.alias("source"),
               "target.team_id = source.team_id")\
        .whenMatchedUpdateAll()\
        .whenNotMatchedInsertAll()\
        .execute()

else:
     # create new gold table

     df_gold.write\
         .format("delta")\
         .mode("overwrite")\
         .option("delta.enableChangeDataFeed","true")\
         .saveAsTable(dim_teams)

         

# COMMAND ----------

teams_gold = spark.read.table(dim_teams)
display(teams_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ###truncating staging tables

# COMMAND ----------

spark.sql(f"truncate table {catalog}.{bronze_schema}.staging_{source_folder}")

spark.sql(f"truncate table {catalog}.{silver_schema}.staging_{source_folder}")

# COMMAND ----------

