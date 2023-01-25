# Databricks notebook source
# Required Imports and Schema
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                  StructField("year", IntegerType(), True),
                                  StructField("round", IntegerType(), True),
                                  StructField("circuitId", IntegerType(), True),
                                  StructField("name", StringType(), True),
                                  StructField("date", DateType(), True),
                                  StructField("time", StringType(), True),
                                  StructField("url", StringType(), True) 
                                  ])


# COMMAND ----------

# Redaing the data file into given Schema
races_df = spark.read \
.option("header", True) \
.schema(races_schema) \
.csv("/mnt/formula1dlake3/raw/races.csv")


# COMMAND ----------

# Insertion of new Columns
from pyspark.sql.functions import current_timestamp, to_timestamp, concat, col, lit
races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                  .withColumn("race_timestamp", to_timestamp(concat(col('date'), lit(' '), col('time')), 'yyyy-MM-dd HH:mm:ss'))

# Selecting the required columns and change their names
races_selected_df = races_with_timestamp_df.select(col('raceId').alias('race_id'), col('year').alias('race_year'), 
                                                   col('round'), col('circuitId').alias('circuit_id'),col('name'), col('ingestion_date'), col('race_timestamp'))

# COMMAND ----------

# writing the data back into processed folder
races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet('/mnt/formula1dlake3/processed/races')