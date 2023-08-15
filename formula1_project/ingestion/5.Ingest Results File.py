# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest Results file 
# MAGIC
# MAGIC - Step 1 : Read the Json file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import *
results_file1 = spark.read.json("/mnt/formula1dlgo/raw/results.json")
results_file1.printSchema()

# COMMAND ----------

results_file = spark.read.option("inferSchema",True).json("/mnt/formula1dlgo/raw/results.json")
results_file.printSchema()


# COMMAND ----------

# MAGIC %fs
# MAGIC
# MAGIC ls /mnt/formula1dlgo/raw/
# MAGIC

# COMMAND ----------


