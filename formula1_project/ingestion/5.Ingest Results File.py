# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Results file 
# MAGIC
# MAGIC - Step 1 : Read the Json file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp,col

# COMMAND ----------

results_schema = StructType(fields=[StructField("resultId",IntegerType(),False),
                                    StructField("raceId",IntegerType(),False),
                                    StructField("driverId",IntegerType(),False),
                                    StructField("constructorId",IntegerType(),False),
                                    StructField("number",IntegerType(),True),
                                    StructField("grid",IntegerType(),False),
                                    StructField("position",IntegerType(),True),
                                    StructField("positionText",StringType(),False),
                                    StructField("positionOrder",IntegerType(),False),
                                    StructField("points",FloatType(),False),
                                    StructField("laps",IntegerType(),False),
                                    StructField("time",StringType(),True),
                                    StructField("milliseconds",IntegerType(),True),
                                    StructField("fastestLap",IntegerType(),True),
                                    StructField("rank", IntegerType(), True),
                                    StructField("fastestLapTime", StringType(), True),
                                    StructField("fastestLapSpeed", FloatType(), True),
                                    StructField("statusId", IntegerType(),False)

                                    ])

# COMMAND ----------


results_file1 = spark.read.schema(results_schema).json("/mnt/formula1dlgo/raw/results.json")
results_file1.printSchema()

# COMMAND ----------

# Step 2 - Rename the columns

results_file_rename = results_file1.withColumnRenamed("resultId","result_id")\
  .withColumnRenamed("raceId","race_id")\
  .withColumnRenamed("driverId","driver_id")\
  .withColumnRenamed("constructorId","constructor_id")\
  .withColumnRenamed("positionText","position_text")\
  .withColumnRenamed("positionOrder","position_order")\
  .withColumnRenamed("fastestLap","fastest_lap")\
  .withColumnRenamed("fastestLapTime","fastest_lap_time")\
  .withColumnRenamed("fastestLapSpeed","fastest_lap_speed")\
  .withColumn("ingestiontime",current_timestamp())

# COMMAND ----------

display(results_file_rename)

# COMMAND ----------

#Drop Unwanted_Columns

results_file_final = results_file_rename.drop(col("statusId"))

# COMMAND ----------

#Write the status to the output with partitionBy clause in parquet format

results_file_final.write.format("parquet").partitionBy("race_id").save("/mnt/formula1dlgo/processed/results")

# COMMAND ----------

display(spark.read.parquet("/mnt/formula1dlgo/processed/results"))

# COMMAND ----------


