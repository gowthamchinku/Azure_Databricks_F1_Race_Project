# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Mutliple CSV files

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

lap_schema = StructType(fields=[StructField("raceId",IntegerType(),False),
                                 StructField("driverId",IntegerType(),False),
                                 StructField("lap",IntegerType(),False),
                                 StructField("position",IntegerType(),True),
                                 StructField("time",StringType(),True),
                                 StructField("milliseconds",IntegerType(),True),
                                 ])

# COMMAND ----------

lap_df = spark.read.schema(lap_schema).csv("/mnt/formula1dlgo/raw/lap_times")

# COMMAND ----------

display(lap_df)

# COMMAND ----------

#Step 2 : Rename the columns and add the ingestion_date

# COMMAND ----------

final_df = lap_df.withColumnRenamed("driverId","driver_id")\
  .withColumnRenamed("raceId","race_id")\
  .withColumn("ingestion_date", current_timestamp())

# COMMAND ----------

# Step3 - Write Output to processed container in parquet format

# COMMAND ----------

final_df.write.mode("overwrite").parquet("/mnt/formula1dlgo/processed/lap_times")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlgo/processed/lap_times"))

# COMMAND ----------


