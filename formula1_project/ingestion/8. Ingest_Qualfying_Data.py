# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting the Qualyfying Json Folder  - MULTILINE JSON

# COMMAND ----------

#Step 1 : Read the Json file using spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp

# COMMAND ----------

qualyfying_schema = StructType(fields=[StructField("qualifyId", IntegerType(), False),
                                 StructField("raceId", IntegerType(), False),
                                 StructField("driverId", IntegerType(), False),
                                 StructField("constructorId", IntegerType(), False),
                                 StructField("number", IntegerType(), False),
                                 StructField("position", IntegerType(), True),
                                 StructField("q1", StringType(), True),
                                 StructField("q2", StringType(), True),
                                 StructField("q3", StringType(), True)
                                 ])

# COMMAND ----------

qualifying_df = spark.read.schema(qualyfying_schema)\
.option("multiLine",True)\
.json(f"{raw_folder_path}/qualifying")

# COMMAND ----------

display(qualifying_df)

# COMMAND ----------

# Step 2   Rename the columns and new columns

final_df = qualifying_df.withColumnRenamed("driverId","driver_id")\
  .withColumnRenamed("raceId","race_id")\
  .withColumnRenamed("constructorId","constructor_id")\
  .withColumnRenamed("qualifyId","qualify_id")\
  .withColumn("ingested_date",current_timestamp())

# COMMAND ----------

display(final_df)

# COMMAND ----------

#Step 3: final writing the output to the processed container in the parquet format


# COMMAND ----------

final_df.write.format("parquet").mode("overwrite").save(f"{processed_folder_path}/qualifying")

# COMMAND ----------

display(spark.read.format("parquet").load("/mnt/formula1dlgo/processed/qualifying"))

# COMMAND ----------


