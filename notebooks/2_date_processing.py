# Databricks notebook source
from pyspark.sql import functions as F

dim_date = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.to_date(F.lit("2024-01-01")),
                F.to_date(F.lit("2026-12-31")),
                F.expr("interval 1 day")
            )
        ).alias("date")
    )
    .withColumn(
        "date_key",
        F.date_format("date", "yyyyMMdd").cast("int")
    )
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("month", F.month("date"))
    .withColumn("month_name", F.date_format("date", "MMMM"))
    .withColumn("year", F.year("date"))
    .withColumn("quarter", F.quarter("date"))
    .withColumn("week", F.weekofyear("date"))
    .withColumn("day_name", F.date_format("date", "EEEE"))
    .withColumn(
        "is_weekend",
        F.when(F.dayofweek("date").isin(1, 7), True)
         .otherwise(False)
    )
    .select(
        "date_key",
        "date",
        "day",
        "month",
        "month_name",
        "year",
        "quarter",
        "week",
    )
)

# COMMAND ----------

display(dim_date)

# COMMAND ----------

dim_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("epl.gold.dim_date")

# COMMAND ----------

