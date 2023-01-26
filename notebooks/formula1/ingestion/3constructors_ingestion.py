# Databricks notebook source
# DDL Schema
constructors_schema = "constructorId INT, constructorRef STRING, name STRING, nationality STRING, url STRING"
constructor_df = spark.read \
.schema(constructors_schema) \
.json("/mnt/formula1dlake3/raw/constructors.json")

# COMMAND ----------

# Dropping the Column and rnaming the Columns
from pyspark.sql.functions import col, current_timestamp
constructor_dropped_df = constructor_df.drop(col('url'))
constructor_final_df = constructor_dropped_df.withColumnRenamed("constructorId", "constructor_id") \
                                             .withColumnRenamed("constructorRef", "constructor_ref") \
                                             .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# Save the processed data back
constructor_final_df.write.mode("overwrite").parquet("/mnt/formula1dlake3/processed/constructors")