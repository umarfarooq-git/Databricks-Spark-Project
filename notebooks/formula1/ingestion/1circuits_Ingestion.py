# Databricks notebook source
# Necessary Imports
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

# COMMAND ----------

# Required Schema for the Ciruits file
circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

# Reading a CSV file header and aforementioned Schema
circuits_df = spark.read \
.option("header", True) \
.schema(circuits_schema) \
.csv("/mnt/formula1dlake3/raw/circuits.csv")

# COMMAND ----------

# To select particular columns from the dataframe
from pyspark.sql.functions import col
circuits_selected_df = circuits_df.select(col("circuitId"), col("circuitRef"), col("name"), col("location"), col("country"), col("lat"), col("lng"), col("alt"))

# COMMAND ----------

# Required Renameing the column nanmes
circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitId", "circuit_id") \
.withColumnRenamed("circuitRef", "circuit_ref") \
.withColumnRenamed("lat", "latitude") \
.withColumnRenamed("lng", "longitude") \
.withColumnRenamed("alt", "altitude") 

# COMMAND ----------

# Insertion of new column in dataframe as Ingestion timestamp
from pyspark.sql.functions import current_timestamp
circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()) 

# COMMAND ----------

# Store the data in processed container with override mode
circuits_final_df.write.mode("overwrite").parquet("/mnt/formula1dlake3/processed/circuits")

# COMMAND ----------

# View the data from processed container
display(spark.read.parquet("/mnt/formula1dlake3/processed/circuits"))