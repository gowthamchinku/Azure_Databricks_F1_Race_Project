# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting the pits_stop Json File  - MULTILINE JSON

# COMMAND ----------

#Step 1 : Read the Json file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

pit_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                 StructField("driverId", IntegerType(), False),
                                 StructField("stop", StringType(), False),
                                 StructField("lap", IntegerType(), False),
                                 StructField("time", StringType(), False),
                                 StructField("duration", StringType(), True),
                                 StructField("milliseconds", IntegerType(), True)
                                 ])

# COMMAND ----------

pits_stop_file = spark.read.schema(pit_schema).option("multiLine",True).json(f"{raw_folder_path}/pit_stops.json")

# COMMAND ----------

display(pits_stop_file)

# COMMAND ----------

# Step 2   Rename the columns and new columns

final_df = pits_stop_file.withColumnRenamed("driverId","driver_id")\
  .withColumnRenamed("raceId","race_id")\
  .withColumn("ingested_date",current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

#Step 3: final writing the output to the processed container in the parquet format


# COMMAND ----------

final_df.write.format("parquet").mode("overwrite").save(f"{processed_folder_path}/pit_stops")

# COMMAND ----------

display(spark.read.format("parquet").load("/mnt/formula1dlgo/processed/pit_stops"))

# COMMAND ----------


