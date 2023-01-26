# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
pit_stops_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                      StructField("driverId", IntegerType(), True),
                                      StructField("stop", StringType(), True),
                                      StructField("lap", IntegerType(), True),
                                      StructField("time", StringType(), True),
                                      StructField("duration", StringType(), True),
                                      StructField("milliseconds", IntegerType(), True)
                                     ])
pit_stops_df = spark.read \
.schema(pit_stops_schema) \
.option("multiLine", True) \
.json("/mnt/formula1dlake3/raw/pit_stops.json")

# COMMAND ----------

from pyspark.sql.functions import current_timestamp

final_df = pit_stops_df.withColumnRenamed("driverId", "driver_id") \
.withColumnRenamed("raceId", "race_id") \
.withColumn("ingestion_date", current_timestamp())

final_df.write.mode("overwrite").parquet("/mnt/formula1dlake3/processed/pit_stops")