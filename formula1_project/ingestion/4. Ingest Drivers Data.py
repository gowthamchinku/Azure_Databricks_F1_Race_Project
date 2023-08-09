# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingesting the divers data with nested json
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC -- Step 1: Read the JSON File using spark dataframe reader API
# MAGIC

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DateType

# COMMAND ----------


