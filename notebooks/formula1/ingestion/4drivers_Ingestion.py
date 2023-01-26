# Databricks notebook source
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
name_schema = StructType(fields=[StructField("forename", StringType(), True),
                                 StructField("surname", StringType(), True)])
drivers_schema = StructType(fields=[StructField("driverId", IntegerType(), False),
                                    StructField("driverRef", StringType(), True),
                                    StructField("number", IntegerType(), True),
                                    StructField("code", StringType(), True),
                                    StructField("name", name_schema),
                                    StructField("dob", DateType(), True),
                                    StructField("nationality", StringType(), True),
                                    StructField("url", StringType(), True)  
])

drivers_df = spark.read \
.schema(drivers_schema) \
.json("/mnt/formula1dlake3/raw/drivers.json")

# COMMAND ----------

# saving the processed data back
drivers_final_df = drivers_with_columns_df.drop(col("url"))
drivers_final_df.write.mode("overwrite").parquet("/mnt/formula1dlake3/processed/drivers")